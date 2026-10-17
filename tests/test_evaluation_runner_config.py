import asyncio
import random
import time
import unittest
from unittest.mock import AsyncMock, patch


from src.config import Config
from src.evaluation.runner import SYNC_MODES, EvalScenarioConfig, EvaluationRunner
from src.evaluation.resources import build_resource_assignments, resource_totals


class _AcceptedResponse:
    status = 200

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, traceback):
        return None

    async def json(self):
        return {"status": "created"}


class _CapturingSession:
    def __init__(self):
        self.payloads = []

    def post(self, url, json):
        self.payloads.append(json)
        return _AcceptedResponse()


class _FailingResponse:
    async def __aenter__(self):
        raise OSError("response lost")

    async def __aexit__(self, exc_type, exc, traceback):
        return None


class _FailingSession:
    def post(self, url, json):
        return _FailingResponse()


class EvaluationRunnerConfigTests(unittest.TestCase):
    def setUp(self):
        self.runner = EvaluationRunner(
            docker_mgr=None,
            candidate_urls=lambda *urls: [url for url in urls if url],
            discover_gateways=lambda: None,
            sync_gateways_with_nodes=lambda **_: None,
        )

    def test_planned_steps_respects_scenario_filter(self):
        cfg = EvalScenarioConfig(
            node_counts=[5, 10],
            repetitions=3,
            partition_splits=[0.5, 0.7],
            scenarios=["baseline", "partition_heal"],
        )

        self.assertEqual(self.runner.planned_steps(cfg), 18)

    def test_shared_key_scenario_adds_one_step_per_repetition(self):
        cfg = EvalScenarioConfig(
            node_counts=[5, 10, 15],
            repetitions=10,
            scenarios=["shared_key"],
            network_profile="clean_lan",
        )

        self.runner.validate_config(cfg)
        self.assertEqual(self.runner.planned_steps(cfg), 30)

    def test_crdt_shared_scenarios_add_one_step_each(self):
        cfg = EvalScenarioConfig(
            node_counts=[5, 10, 15],
            repetitions=10,
            scenarios=["shared_counter", "shared_set"],
            network_profile="clean_lan",
        )

        self.runner.validate_config(cfg)
        self.assertEqual(self.runner.planned_steps(cfg), 60)

    def test_shared_workloads_submit_crdt_specific_events(self):
        expected = {
            "sensor": ("sensor", "water_level"),
            "resource": ("resource", "shelter_occupancy"),
            "infrastructure": ("infrastructure", "road_block"),
        }
        for workload, (category, event_type) in expected.items():
            with self.subTest(workload=workload):
                session = _CapturingSession()
                result = asyncio.run(
                    self.runner.send_events(
                        session,
                        [{"url": "http://edge-node-1"}],
                        1,
                        rng=random.Random(1),
                        shared_workload=workload,
                    )
                )

                self.assertEqual(result["accepted"], 1)
                self.assertEqual(
                    result["attempted_tracking_ids"], result["tracking_ids"]
                )
                self.assertEqual(session.payloads[0]["category"], category)
                self.assertEqual(session.payloads[0]["type"], event_type)
                self.assertEqual(session.payloads[0]["location"], "shared_site")

    def test_request_error_preserves_attempted_tracking_id(self):
        result = asyncio.run(
            self.runner.send_events(
                _FailingSession(),
                [{"url": "http://edge-node-1"}],
                1,
                rng=random.Random(1),
            )
        )

        self.assertEqual(result["attempted"], 1)
        self.assertEqual(result["accepted"], 0)
        self.assertEqual(result["request_errors"], 1)
        self.assertEqual(result["tracking_ids"], [])
        self.assertEqual(len(result["attempted_tracking_ids"]), 1)

    def test_degraded_network_plans_partition_heal_steps(self):
        cfg = EvalScenarioConfig(
            node_counts=[5],
            repetitions=2,
            partition_splits=[0.5, 0.7],
            network_profile="degraded",
            scenarios=["partition_heal"],
        )

        self.runner.validate_config(cfg)
        self.assertEqual(self.runner.planned_steps(cfg), 4)

    def test_validation_rejects_empty_effective_matrix(self):
        cfg = EvalScenarioConfig(
            partition_splits=[],
            scenarios=["partition_heal"],
        )

        with self.assertRaisesRegex(ValueError, "no evaluation steps"):
            self.runner.validate_config(cfg)

    def test_default_evaluation_uses_warm_topology(self):
        self.assertTrue(EvalScenarioConfig().warm_topology)

    def test_config_exposes_delta_log_and_partition_write_controls(self):
        cfg = EvalScenarioConfig(delta_log_limit=25, partition_events_per_node=40)

        self.assertIn("delta_log_25", cfg.implementation_label())
        self.assertEqual(cfg.delta_log_limit, 25)
        self.assertEqual(cfg.partition_events_per_node, 40)

    def test_repetition_seeds_are_deterministic_and_distinct(self):
        cfg = EvalScenarioConfig(scenario_seed=7301, repetitions=3)

        self.assertEqual(
            [cfg.seed_for_repetition(repetition) for repetition in range(1, 4)],
            [7301, 7302, 7303],
        )

    def test_supported_sync_modes_are_exactly_the_peer_strategies(self):
        self.assertEqual(
            SYNC_MODES,
            {
                "full_state",
                "optimized_full_state",
                "naive_key_delta",
                "true_delta",
            },
        )

    def test_report_network_profiles_and_sync_modes_are_executable(self):
        for network_profile in (
            "clean_lan",
            "constrained_wifi",
            "degraded",
            "severe",
        ):
            for sync_mode in (
                "full_state",
                "optimized_full_state",
                "naive_key_delta",
                "true_delta",
            ):
                with self.subTest(
                    network_profile=network_profile,
                    sync_mode=sync_mode,
                ):
                    cfg = EvalScenarioConfig(
                        network_profile=network_profile,
                        sync_mode=sync_mode,
                        scenarios=["baseline"],
                    )
                    self.runner.validate_config(cfg)

    def test_full_state_is_unbounded_and_optimized_full_state_is_bounded(self):
        full_state = EvalScenarioConfig(
            sync_mode="full_state",
            event_id_retention_limit=100,
        )
        optimized = EvalScenarioConfig(
            sync_mode="optimized_full_state",
            event_id_retention_limit=100,
        )

        self.assertEqual(full_state.effective_event_id_retention_limit(), 0)
        self.assertIn("unbounded_retention", full_state.implementation_label())
        self.assertEqual(optimized.effective_event_id_retention_limit(), 100)
        self.assertIn("retention_100", optimized.implementation_label())

    def test_optimized_full_state_rejects_unbounded_retention(self):
        cfg = EvalScenarioConfig(
            sync_mode="optimized_full_state",
            event_id_retention_limit=0,
        )

        with self.assertRaisesRegex(ValueError, "positive"):
            self.runner.validate_config(cfg)

    def test_result_row_records_effective_network_profile(self):
        cfg = EvalScenarioConfig(
            network_profile="constrained_wifi",
            scenarios=["baseline"],
        )
        self.runner._network_profile_receipt = {"status": "applied"}

        row = self.runner._row_base(
            cfg,
            "run-1",
            1,
            "baseline",
            5,
            {"attempted": 0, "accepted": 0},
        )

        self.assertEqual(row["network_delay_ms"], 50.0)
        self.assertEqual(row["network_jitter_ms"], 15.0)
        self.assertEqual(row["network_loss_percent"], 2.0)
        self.assertEqual(row["network_bandwidth_mbps"], 10.0)
        self.assertTrue(row["network_rules_verified"])

    def test_node_config_accepts_all_report_sync_modes(self):
        for sync_mode in (
            "full_state",
            "optimized_full_state",
            "naive_key_delta",
            "true_delta",
        ):
            with self.subTest(sync_mode=sync_mode):
                with patch.dict("os.environ", {"SYNC_MODE": sync_mode}):
                    self.assertEqual(Config().sync_mode, sync_mode)

    def test_validation_rejects_negative_delta_log_limit(self):
        cfg = EvalScenarioConfig(delta_log_limit=-1)

        with self.assertRaisesRegex(ValueError, "delta_log_limit"):
            self.runner.validate_config(cfg)

    def test_offered_rate_must_be_positive(self):
        with self.assertRaises(ValueError):
            EvalScenarioConfig(offered_rate_per_second=0)

    def test_reserve_run_prevents_start_race(self):
        run_id = self.runner.reserve_run()

        self.assertTrue(self.runner.state["running"])
        self.assertEqual(self.runner.state["run_id"], run_id)
        self.assertEqual(self.runner.state["current_step"], "Starting")

    def test_status_exposes_repetition_progress(self):
        cfg = EvalScenarioConfig(node_counts=[5], repetitions=4)
        self.runner.state["total_steps"] = self.runner.planned_steps(cfg)

        self.runner._set_progress(
            current_step="[rep 2/4] [5N] Baseline",
            completed_steps=3,
            cfg=cfg,
            repetition=2,
            node_count=5,
            scenario="baseline",
        )

        status = self.runner.status()

        self.assertEqual(status["current_repetition"], 2)
        self.assertEqual(status["total_repetitions"], 4)
        self.assertEqual(status["repetitions_remaining"], 2)
        self.assertEqual(status["completed_steps"], 3)
        self.assertEqual(status["steps_remaining"], 17)
        self.assertEqual(status["current_node_count"], 5)
        self.assertEqual(status["current_scenario"], "baseline")

    def test_resource_summary_uses_cumulative_cpu_counter_deltas(self):
        def sample(
            node,
            cpu,
            system,
            memory,
            network_rx,
            periods=0,
            throttled_periods=0,
            throttled_time_ns=0,
        ):
            return {
                "node": node,
                "cpu_usage_ns": cpu,
                "system_cpu_usage_ns": system,
                "online_cpus": 4,
                "cpu_periods": periods,
                "cpu_throttled_periods": throttled_periods,
                "cpu_throttled_time_ns": throttled_time_ns,
                "memory_bytes": memory,
                "network_rx_bytes": network_rx,
                "network_tx_bytes": 0,
                "block_read_bytes": 0,
                "block_write_bytes": 0,
            }

        self.runner._resource_samples = [
            [
                sample(
                    "edge-node-1",
                    1_000,
                    10_000,
                    10 * 1024 * 1024,
                    100,
                    100,
                    10,
                    1_000_000_000,
                ),
                sample(
                    "edge-node-2",
                    2_000,
                    10_000,
                    20 * 1024 * 1024,
                    200,
                    200,
                    20,
                    2_000_000_000,
                ),
            ],
            [
                sample(
                    "edge-node-1",
                    1_100,
                    11_000,
                    12 * 1024 * 1024,
                    140,
                    110,
                    14,
                    2_000_000_000,
                ),
                sample(
                    "edge-node-2",
                    2_050,
                    11_000,
                    22 * 1024 * 1024,
                    260,
                    220,
                    25,
                    4_000_000_000,
                ),
            ],
        ]

        summary = self.runner._resource_summary()

        self.assertEqual(summary["resource_sample_count"], 2)
        self.assertEqual(summary["cpu_mean_percent"], 30.0)
        self.assertEqual(summary["cpu_peak_percent"], 40.0)
        self.assertEqual(summary["memory_peak_mib"], 22.0)
        self.assertEqual(summary["network_rx_bytes"], 100)
        self.assertEqual(summary["cpu_periods"], 30)
        self.assertEqual(summary["cpu_throttled_periods"], 9)
        self.assertEqual(summary["cpu_throttled_period_percent"], 30.0)
        self.assertEqual(summary["cpu_throttled_time_s"], 3.0)

    def test_balanced_profile_is_deterministic_and_equal_budget(self):
        first = build_resource_assignments(
            node_count=5,
            resource_mode="heterogeneous",
            resource_profile="balanced_edge_v1",
            uniform_cpus=0.5,
            uniform_memory="512m",
            repetition_seed=9201,
        )
        second = build_resource_assignments(
            node_count=5,
            resource_mode="heterogeneous",
            resource_profile="balanced_edge_v1",
            uniform_cpus=0.5,
            uniform_memory="512m",
            repetition_seed=9201,
        )

        self.assertEqual(first, second)
        self.assertEqual(
            {
                tier: sum(item["tier"] == tier for item in first)
                for tier in ("low", "medium", "high")
            },
            {"low": 2, "medium": 2, "high": 1},
        )
        self.assertEqual(
            resource_totals(first),
            {"total_cpus": 2.5, "total_memory_mib": 2560.0},
        )

    def test_heterogeneous_config_allows_warm_topology_and_requires_five_node_blocks(
        self,
    ):
        self.runner.validate_config(
            EvalScenarioConfig(
                node_counts=[5, 10],
                resource_mode="heterogeneous",
                resource_profile="balanced_edge_v1",
                docker_cpus=0.5,
                docker_memory="512m",
                warm_topology=True,
            )
        )

        with self.assertRaisesRegex(ValueError, "divisible by 5"):
            self.runner.validate_config(
                EvalScenarioConfig(
                    node_counts=[7],
                    resource_mode="heterogeneous",
                    resource_profile="balanced_edge_v1",
                    docker_cpus=0.5,
                    docker_memory="512m",
                    warm_topology=True,
                )
            )

    def test_resource_summary_preserves_node_and_tier_metrics(self):
        self.runner._resource_assignments = {
            "edge-node-1": {
                "tier": "low",
                "cpus": 0.25,
                "memory_mib": 256,
            }
        }
        self.runner._resource_samples = [
            [
                {
                    "node": "edge-node-1",
                    "cpu_percent": 12.5,
                    "memory_bytes": 128 * 1024 * 1024,
                    "memory_limit_bytes": 256 * 1024 * 1024,
                    "network_rx_bytes": 10,
                    "network_tx_bytes": 20,
                    "block_read_bytes": 0,
                    "block_write_bytes": 0,
                }
            ]
        ]

        summary = self.runner._resource_summary()

        self.assertEqual(summary["low_cpu_limit_mean_percent"], 50.0)
        self.assertEqual(summary["low_memory_limit_peak_percent"], 50.0)
        self.assertIn('"edge-node-1"', summary["per_node_resource_metrics"])


class EvaluationRunnerTimeoutTests(unittest.IsolatedAsyncioTestCase):
    async def test_convergence_observed_after_deadline_is_a_timeout(self):
        runner = EvaluationRunner(
            docker_mgr=None,
            candidate_urls=lambda *urls: [url for url in urls if url],
            discover_gateways=lambda: None,
            sync_gateways_with_nodes=lambda **_: None,
        )
        runner.poll_all_gateways = AsyncMock()
        runner.collect_metrics = AsyncMock(
            return_value={"is_converged": True, "missing_at_cutoff_count": 0}
        )
        runner._collect_resource_sample = AsyncMock()
        cfg = EvalScenarioConfig(convergence_wait_seconds=45)

        with patch("src.evaluation.runner.time.time", side_effect=[0.0, 46.0, 47.0]):
            metrics = await runner.wait_for_metrics(
                session=None,
                nodes=[],
                gateways=[],
                cfg=cfg,
                accepted_events=0,
                tracking_ids=[],
            )

        self.assertFalse(metrics["converged_within_timeout"])
        self.assertIsNone(metrics["convergence_time_s"])
        self.assertEqual(runner.collect_metrics.await_count, 2)

    async def test_tracking_timestamp_proves_convergence_before_polling_finishes(self):
        runner = EvaluationRunner(
            docker_mgr=None,
            candidate_urls=lambda *urls: [url for url in urls if url],
            discover_gateways=lambda: None,
            sync_gateways_with_nodes=lambda **_: None,
        )
        runner.poll_all_gateways = AsyncMock()
        runner.collect_metrics = AsyncMock(
            side_effect=[
                {"is_converged": True, "missing_at_cutoff_count": 1},
                {
                    "is_converged": True,
                    "missing_at_cutoff_count": 0,
                    "time_to_last_node_s": 40.0,
                },
            ]
        )
        runner._collect_resource_sample = AsyncMock()
        cfg = EvalScenarioConfig(convergence_wait_seconds=45)

        with patch("src.evaluation.runner.time.time", side_effect=[0.0, 46.0, 47.0]):
            metrics = await runner.wait_for_metrics(
                session=None,
                nodes=[],
                gateways=[],
                cfg=cfg,
                accepted_events=1,
                tracking_ids=["accepted"],
                attempted_tracking_ids=["attempted"],
            )

        self.assertTrue(metrics["converged_within_timeout"])
        self.assertEqual(metrics["convergence_time_s"], 40.0)
        self.assertEqual(runner.collect_metrics.await_count, 2)

    async def test_gateways_are_polled_once_before_final_metrics(self):
        runner = EvaluationRunner(
            docker_mgr=None,
            candidate_urls=lambda *urls: [url for url in urls if url],
            discover_gateways=lambda: None,
            sync_gateways_with_nodes=lambda **_: None,
        )
        calls = []
        runner.poll_all_gateways = AsyncMock(
            side_effect=lambda *_: calls.append("poll")
        )
        light_results = iter(
            [
                {"is_converged": False, "missing_at_cutoff_count": 3},
                {"is_converged": False, "missing_at_cutoff_count": 1},
                {"is_converged": True, "missing_at_cutoff_count": 0},
            ]
        )

        async def collect_metrics(*_, include_state, **__):
            calls.append("full" if include_state else "light")
            if include_state:
                return {"is_converged": True, "missing_at_cutoff_count": 0}
            return next(light_results)

        runner.collect_metrics = collect_metrics
        runner._collect_resource_sample = AsyncMock()
        cfg = EvalScenarioConfig(convergence_wait_seconds=45)

        with (
            patch(
                "src.evaluation.runner.time.time",
                side_effect=[0.0, 1.0, 2.0, 3.0, 4.0],
            ),
            patch("src.evaluation.runner.asyncio.sleep", new=AsyncMock()),
        ):
            metrics = await runner.wait_for_metrics(
                session=None,
                nodes=[],
                gateways=[{"name": "gateway-1"}],
                cfg=cfg,
                accepted_events=0,
                tracking_ids=[],
            )

        self.assertTrue(metrics["converged_within_timeout"])
        self.assertEqual(metrics["convergence_time_s"], 4.0)
        self.assertEqual(calls, ["light", "light", "light", "poll", "full"])


class _WarmResourceDockerManager:
    def __init__(self):
        self.calls = []
        self.limits = {}

    def apply_netem_profile(self, profile):
        self.calls.append(("apply_netem_profile", profile))
        return {"status": "applied"}

    def clear_netem(self):
        self.calls.append(("clear_netem",))
        return {"status": "cleared"}

    def heal_all(self):
        self.calls.append(("heal_all",))
        return {"status": "healed"}

    def update_node_resources(self, assignments):
        self.calls.append(("update_node_resources", assignments))
        self.limits = {
            assignment["container"]: {
                "running": True,
                "cpus": assignment["cpus"],
                "memory_bytes": assignment["memory_mib"] * 1024 * 1024,
            }
            for assignment in assignments
        }
        return {"status": "updated", "results": []}

    def inspect_node_resource_limits(self, node_ids):
        return self.limits


class _PartitionDockerManager(_WarmResourceDockerManager):
    def create_split_brain(self, split):
        self.calls.append(("create_split_brain", split))
        return {"status": "split_brain", "group_a": [], "group_b": []}


class EvaluationRunnerWarmTopologyTests(unittest.IsolatedAsyncioTestCase):
    async def test_warm_heterogeneous_run_updates_resources_per_repetition_seed(self):
        docker_mgr = _WarmResourceDockerManager()
        runner = EvaluationRunner(
            docker_mgr=docker_mgr,
            candidate_urls=lambda *urls: [url for url in urls if url],
            discover_gateways=lambda: None,
            sync_gateways_with_nodes=lambda **_: None,
        )
        runner.recreate_nodes = AsyncMock(return_value=([], []))
        runner.reset_runtime = AsyncMock()
        runner.send_events = AsyncMock(
            return_value={
                "attempted": 0,
                "accepted": 0,
                "tracking_ids": [],
                "attempted_tracking_ids": [],
            }
        )
        runner.wait_for_metrics = AsyncMock(side_effect=lambda *_, **__: {})
        limits_verified_per_row = []
        runner._append_result = lambda row, metrics: limits_verified_per_row.append(
            runner._resource_limit_verified
        )
        cfg = EvalScenarioConfig(
            node_counts=[5],
            repetitions=3,
            scenario_seed=9201,
            scenarios=["baseline"],
            network_profile="clean_lan",
            resource_mode="heterogeneous",
            resource_profile="balanced_edge_v1",
            docker_cpus=0.5,
            docker_memory="512m",
            warm_topology=True,
        )
        runner.validate_config(cfg)

        await runner.run(cfg)

        self.assertIsNone(runner.state["error"])
        runner.recreate_nodes.assert_awaited_once()
        updates = [
            call[1] for call in docker_mgr.calls if call[0] == "update_node_resources"
        ]
        self.assertEqual(
            updates,
            [
                build_resource_assignments(
                    node_count=5,
                    resource_mode="heterogeneous",
                    resource_profile="balanced_edge_v1",
                    uniform_cpus=0.5,
                    uniform_memory="512m",
                    repetition_seed=seed,
                )
                for seed in (9202, 9203)
            ],
        )
        self.assertEqual(limits_verified_per_row[1:], [True, True])
        self.assertEqual(
            [call for call in docker_mgr.calls if call[0] != "update_node_resources"],
            [("apply_netem_profile", "clean_lan"), ("clear_netem",)],
        )

    async def test_partition_run_applies_netem_and_paces_both_send_phases(self):
        docker_mgr = _PartitionDockerManager()
        runner = EvaluationRunner(
            docker_mgr=docker_mgr,
            candidate_urls=lambda *urls: [url for url in urls if url],
            discover_gateways=lambda: None,
            sync_gateways_with_nodes=lambda **_: None,
        )
        runner.recreate_nodes = AsyncMock(return_value=([], []))
        runner.reset_runtime = AsyncMock()
        runner.wait_for_nodes_ready = AsyncMock()
        runner.wait_for_metrics = AsyncMock(side_effect=lambda *_, **__: {})
        send_calls = []

        async def send_events(session, nodes, events_per_node, **kwargs):
            send_calls.append((events_per_node, kwargs.get("offered_rate")))
            return {
                "attempted": 0,
                "accepted": 0,
                "tracking_ids": [],
                "attempted_tracking_ids": [],
                "duplicates": 0,
                "rejected": 0,
                "request_errors": 0,
                "failed_request_latencies_ms": [],
                "request_latencies_ms": [],
                "submission_duration_s": 1.0,
            }

        runner.send_events = send_events
        partition_rows = []
        runner._append_result = lambda row, metrics: partition_rows.append(row)
        cfg = EvalScenarioConfig(
            node_counts=[5],
            scenarios=["partition_heal"],
            partition_splits=[0.8],
            network_profile="constrained_wifi",
            events_per_node=45,
            partition_events_per_node=15,
            partition_wait_seconds=0,
            offered_rate_per_second=50,
        )
        runner.validate_config(cfg)

        await runner.run(cfg)

        self.assertIsNone(runner.state["error"])
        self.assertEqual(send_calls, [(45, 50), (15, 50)])
        self.assertEqual(len(partition_rows), 1)
        self.assertEqual(
            docker_mgr.calls,
            [
                ("apply_netem_profile", "constrained_wifi"),
                ("create_split_brain", 0.8),
                ("heal_all",),
                ("clear_netem",),
            ],
        )

    async def test_partition_recovery_is_measured_from_heal_and_censored_on_timeout(
        self,
    ):
        latest_arrival = time.time() + 1000.0
        for converged, expect_value in ((True, True), (False, False)):
            runner = EvaluationRunner(
                docker_mgr=_PartitionDockerManager(),
                candidate_urls=lambda *urls: [url for url in urls if url],
                discover_gateways=lambda: None,
                sync_gateways_with_nodes=lambda **_: None,
            )
            runner.recreate_nodes = AsyncMock(return_value=([], []))
            runner.reset_runtime = AsyncMock()
            runner.wait_for_nodes_ready = AsyncMock()
            runner.wait_for_metrics = AsyncMock(
                return_value={
                    "converged_within_timeout": converged,
                    "latest_first_seen_at_unix_s": latest_arrival,
                }
            )
            runner.send_events = AsyncMock(
                return_value={
                    "attempted": 1,
                    "accepted": 1,
                    "tracking_ids": [],
                    "attempted_tracking_ids": [],
                    "duplicates": 0,
                    "rejected": 0,
                    "request_errors": 0,
                    "failed_request_latencies_ms": [],
                    "request_latencies_ms": [],
                    "submission_duration_s": 1.0,
                }
            )
            appended = []
            runner._append_result = lambda row, metrics: appended.append(
                (row, dict(metrics))
            )
            await runner.run(
                EvalScenarioConfig(
                    node_counts=[5],
                    scenarios=["partition_heal"],
                    partition_splits=[0.5],
                    partition_wait_seconds=0,
                )
            )

            row, metrics = appended[0]
            if expect_value:
                self.assertAlmostEqual(
                    metrics["recovery_after_heal_s"],
                    latest_arrival - row["fault_removed_at_unix_s"],
                    delta=0.002,
                )
            else:
                self.assertEqual(metrics["recovery_after_heal_s"], "")


if __name__ == "__main__":
    unittest.main()
