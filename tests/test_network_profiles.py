import unittest

from src.services.docker_manager import (
    DockerManager,
    NETWORK_PROFILE_SETTINGS,
    build_network_profile_commands,
)


class _FakeContainer:
    def __init__(self, name="edge-node-1", failed_command=None):
        self.name = name
        self.failed_command = failed_command
        self.commands = []

    def exec_run(self, command):
        self.commands.append(command)
        if self.failed_command and self.failed_command in command:
            return 2, b"failed"
        if command == "tc qdisc show dev eth0":
            return 0, b"qdisc netem 10: parent 1:10"
        if command == "tc class show dev eth0":
            return 0, b"class htb 1:10 root"
        return 0, command.encode()


class _FakeContainers:
    def __init__(self, containers):
        self._containers = containers

    def list(self):
        return list(self._containers)


class _FakeClient:
    def __init__(self, containers):
        self.containers = _FakeContainers(containers)


class NetworkProfileTests(unittest.TestCase):
    def _manager(self, containers):
        manager = DockerManager.__new__(DockerManager)
        manager.client = _FakeClient(containers)
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

    def test_profile_commands_include_htb_bandwidth_and_netem_impairment(self):
        commands = build_network_profile_commands("severe")

        self.assertIn("htb default 10", commands[0])
        self.assertIn("rate 0.5mbit", commands[1])
        self.assertIn("ceil 0.5mbit", commands[1])
        self.assertIn("delay 600ms 200ms loss 20%", commands[2])

    def test_apply_profile_verifies_qdisc_and_class_on_every_node(self):
        first = _FakeContainer("edge-node-1")
        second = _FakeContainer("edge-node-2")
        manager = self._manager([first, second])

        receipt = manager.apply_netem_profile("constrained_wifi")

        self.assertEqual(receipt["status"], "applied")
        self.assertEqual(
            receipt["settings"], NETWORK_PROFILE_SETTINGS["constrained_wifi"]
        )
        for container in (first, second):
            self.assertEqual(len(container.commands), 5)
            self.assertIn("rate 10mbit", container.commands[1])
            self.assertEqual(container.commands[-2], "tc qdisc show dev eth0")
            self.assertEqual(container.commands[-1], "tc class show dev eth0")

    def test_apply_profile_fails_when_any_tc_command_fails(self):
        container = _FakeContainer(failed_command="tc class replace")
        manager = self._manager([container])

        receipt = manager.apply_netem_profile("degraded")

        self.assertEqual(receipt["status"], "failed")
        self.assertEqual(len(container.commands), 2)

    def test_apply_profile_fails_without_edge_nodes(self):
        manager = self._manager([])

        receipt = manager.apply_netem_profile("clean_lan")

        self.assertEqual(receipt["status"], "failed")


if __name__ == "__main__":
    unittest.main()
