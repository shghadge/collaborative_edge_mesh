import unittest

import docker

from src.services.docker_manager import (
    DockerManager,
    NETWORK_PROFILE_SETTINGS,
    build_network_profile_commands,
    verify_network_profile_state,
)

EVALUATOR_IP = "172.28.0.9"
WIFI_QDISC = (
    "qdisc htb 1: root refcnt 2 r2q 10 default 0x10 direct_packets_stat 0\n"
    "qdisc netem 10: parent 1:10 limit 1000 delay 50ms 15ms loss 2%\n"
)
WIFI_CLASS = (
    "class htb 1:1 root prio 0 rate 10Gbit ceil 10Gbit burst 0b cburst 0b\n"
    "class htb 1:10 root leaf 10: prio 0 rate 10Mbit ceil 10Mbit burst 32Kb\n"
)
EVALUATOR_FILTER = (
    "filter parent 1: protocol ip pref 1 u32 chain 0\n"
    "filter parent 1: protocol ip pref 1 u32 chain 0 fh 800: ht divisor 1\n"
    "filter parent 1: protocol ip pref 1 u32 chain 0 fh 800::800 order 2048 "
    "key ht 800 bkt 0 flowid 1:1 not_in_hw\n"
    "  match ac1c0009/ffffffff at 16\n"
)


class _FakeContainer:
    """Edge node that interprets one ``sh -c`` script of ``&&``-chained commands."""

    def __init__(self, name="edge-node-1", failed_command=None):
        self.name = name
        self.failed_command = failed_command
        self.commands = []

    def exec_run(self, command):
        self.commands.append(command)
        output = ""
        for step in command[2].split(" && "):
            if self.failed_command and self.failed_command in step:
                return 2, (output + "RTNETLINK answers: File exists\n").encode()
            if step.startswith("echo "):
                output += step.removeprefix("echo ") + "\n"
            elif step == "tc qdisc show dev eth0":
                output += WIFI_QDISC
            elif step == "tc class show dev eth0":
                output += WIFI_CLASS
            elif step == "tc filter show dev eth0":
                output += EVALUATOR_FILTER
        return 0, output.encode()


class _FakeEvaluator:
    attrs = {
        "NetworkSettings": {
            "Networks": {"collaborative_edge_mesh_mesh": {"IPAddress": EVALUATOR_IP}}
        }
    }


class _FakeContainers:
    def __init__(self, containers, evaluator):
        self._containers = containers
        self._evaluator = evaluator

    def list(self):
        return list(self._containers)

    def get(self, name):
        if self._evaluator is None:
            raise docker.errors.NotFound(f"No such container: {name}")
        return self._evaluator


class _FakeApi:
    def __init__(self, failing_urls=()):
        self.posts = []
        self.failing_urls = set(failing_urls)

    def _url(self, pathfmt, *args):
        return pathfmt.format(*args)

    def post(self, url, json):
        self.posts.append((url, json))
        return url

    def _raise_for_status(self, response):
        if response in self.failing_urls:
            raise docker.errors.APIError("Conflict: cannot update container")


class _FakeClient:
    def __init__(self, containers, evaluator, api):
        self.containers = _FakeContainers(containers, evaluator)
        self.api = api


class NetworkProfileTests(unittest.TestCase):
    def _manager(self, containers, evaluator=_FakeEvaluator(), api=None):
        manager = DockerManager.__new__(DockerManager)
        manager.client = _FakeClient(containers, evaluator, api or _FakeApi())
        return manager

    def test_report_profiles_have_exact_network_settings(self):
        self.assertEqual(
            NETWORK_PROFILE_SETTINGS,
            {
                "clean_lan": {
                    "delay_ms": 1.0,
                    "jitter_ms": 0.2,
                    "loss_percent": 0.0,
                    "bandwidth_mbps": 1000.0,
                },
                "constrained_wifi": {
                    "delay_ms": 50.0,
                    "jitter_ms": 15.0,
                    "loss_percent": 2.0,
                    "bandwidth_mbps": 10.0,
                },
                "degraded": {
                    "delay_ms": 300.0,
                    "jitter_ms": 100.0,
                    "loss_percent": 10.0,
                    "bandwidth_mbps": 2.0,
                },
                "severe": {
                    "delay_ms": 600.0,
                    "jitter_ms": 200.0,
                    "loss_percent": 20.0,
                    "bandwidth_mbps": 0.5,
                },
            },
        )

    def test_profile_commands_impair_default_class_and_bypass_evaluator(self):
        script = " && ".join(build_network_profile_commands("severe", EVALUATOR_IP))

        self.assertIn("htb default 10", script)
        self.assertIn("classid 1:10 htb rate 0.5mbit ceil 0.5mbit", script)
        self.assertIn("parent 1:10 handle 10: netem delay 600ms 200ms loss 20%", script)
        self.assertIn("classid 1:1 htb rate 10gbit ceil 10gbit", script)
        self.assertIn(f"match ip dst {EVALUATOR_IP}/32 flowid 1:1", script)

    def test_apply_profile_verifies_every_node_with_one_exec_each(self):
        first = _FakeContainer("edge-node-1")
        second = _FakeContainer("edge-node-2")
        manager = self._manager([first, second])

        receipt = manager.apply_netem_profile("constrained_wifi")

        self.assertEqual(receipt["status"], "applied")
        self.assertEqual(receipt["bypass_ip"], EVALUATOR_IP)
        self.assertEqual(
            receipt["settings"], NETWORK_PROFILE_SETTINGS["constrained_wifi"]
        )
        self.assertEqual(
            [result["node"] for result in receipt["results"]],
            ["edge-node-1", "edge-node-2"],
        )
        for result in receipt["results"]:
            self.assertEqual(len(result["verification"]), 3)
            self.assertTrue(all(item["verified"] for item in result["verification"]))
            self.assertEqual(
                result["verification"][2]["output"].strip(), EVALUATOR_FILTER.strip()
            )
        for container in (first, second):
            self.assertEqual(len(container.commands), 1)
            self.assertEqual(container.commands[0][:2], ["sh", "-c"])

    def test_exact_profile_verification_accepts_normalized_tc_units(self):
        self.assertTrue(
            verify_network_profile_state(
                NETWORK_PROFILE_SETTINGS["clean_lan"],
                "qdisc netem 10: parent 1:10 delay 1ms 200us",
                "class htb 1:1 root rate 10Gbit ceil 10Gbit\n"
                "class htb 1:10 root leaf 10: rate 1Gbit ceil 1Gbit",
                EVALUATOR_FILTER,
                EVALUATOR_IP,
            )
        )

    def test_exact_profile_verification_rejects_wrong_loss(self):
        self.assertFalse(
            verify_network_profile_state(
                NETWORK_PROFILE_SETTINGS["constrained_wifi"],
                "qdisc netem 10: parent 1:10 delay 50ms 15ms loss 10%",
                WIFI_CLASS,
                EVALUATOR_FILTER,
                EVALUATOR_IP,
            )
        )

    def test_verification_reads_rate_from_impaired_class_not_bypass_class(self):
        self.assertFalse(
            verify_network_profile_state(
                NETWORK_PROFILE_SETTINGS["constrained_wifi"],
                WIFI_QDISC,
                "class htb 1:1 root prio 0 rate 10Mbit ceil 10Mbit\n"
                "class htb 1:10 root leaf 10: prio 0 rate 1Gbit ceil 1Gbit\n",
                EVALUATOR_FILTER,
                EVALUATOR_IP,
            )
        )

    def test_verification_rejects_missing_or_wrong_bypass_filter(self):
        wrong_filters = {
            "missing": "",
            "other_ip": EVALUATOR_FILTER.replace("ac1c0009", "ac1c000a"),
            "impaired_flow": EVALUATOR_FILTER.replace("flowid 1:1 ", "flowid 1:10 "),
        }
        for label, filter_output in wrong_filters.items():
            with self.subTest(label):
                self.assertFalse(
                    verify_network_profile_state(
                        NETWORK_PROFILE_SETTINGS["constrained_wifi"],
                        WIFI_QDISC,
                        WIFI_CLASS,
                        filter_output,
                        EVALUATOR_IP,
                    )
                )

    def test_apply_profile_fails_when_any_setup_command_fails(self):
        healthy = _FakeContainer("edge-node-1")
        broken = _FakeContainer(
            "edge-node-2", failed_command="parent 1: classid 1:10 htb"
        )
        manager = self._manager([healthy, broken])

        receipt = manager.apply_netem_profile("constrained_wifi")

        self.assertEqual(receipt["status"], "failed")
        broken_result = receipt["results"][1]
        self.assertEqual(broken_result["commands"][0]["exit_code"], 2)
        self.assertEqual(broken_result["verification"], [])
        self.assertEqual(len(broken.commands), 1)

    def test_apply_profile_fails_without_edge_nodes(self):
        manager = self._manager([])

        receipt = manager.apply_netem_profile("clean_lan")

        self.assertEqual(receipt["status"], "failed")

    def test_apply_profile_fails_when_bypass_ip_unresolvable(self):
        container = _FakeContainer("edge-node-1")
        manager = self._manager([container], evaluator=None)

        receipt = manager.apply_netem_profile("degraded")

        self.assertEqual(receipt["status"], "failed")
        self.assertEqual(container.commands, [])

    def test_update_node_resources_posts_limits_and_reports_node_failure(self):
        api = _FakeApi(failing_urls={"/containers/edge-node-2/update"})
        manager = self._manager([], api=api)

        receipt = manager.update_node_resources(
            [
                {
                    "container": "edge-node-1",
                    "cpus": 0.25,
                    "memory": "256m",
                    "memory_mib": 256,
                },
                {
                    "container": "edge-node-2",
                    "cpus": 1.0,
                    "memory": "1g",
                    "memory_mib": 1024,
                },
            ]
        )

        self.assertEqual(
            sorted(api.posts),
            [
                (
                    "/containers/edge-node-1/update",
                    {
                        "NanoCpus": 250_000_000,
                        "Memory": 256 * 1024 * 1024,
                        "MemorySwap": 512 * 1024 * 1024,
                    },
                ),
                (
                    "/containers/edge-node-2/update",
                    {
                        "NanoCpus": 1_000_000_000,
                        "Memory": 1024 * 1024 * 1024,
                        "MemorySwap": 2048 * 1024 * 1024,
                    },
                ),
            ],
        )
        self.assertEqual(receipt["status"], "failed")
        self.assertEqual(
            [(result["node"], result["ok"]) for result in receipt["results"]],
            [("edge-node-1", True), ("edge-node-2", False)],
        )
        self.assertIsNotNone(receipt["results"][1]["error"])


if __name__ == "__main__":
    unittest.main()
