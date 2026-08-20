import unittest
from unittest.mock import AsyncMock, patch

from src.evaluation.runner import EvalScenarioConfig, EvaluationRunner


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

    def test_degraded_network_skips_partition_heal_steps(self):
        cfg = EvalScenarioConfig(
            node_counts=[5],
            repetitions=2,
            partition_splits=[0.5, 0.7],
            network_profile="degraded_300ms_10loss",
            scenarios=["baseline", "partition_heal", "high_load"],
        )

        self.assertEqual(self.runner.planned_steps(cfg), 4)

    def test_validation_rejects_empty_effective_matrix(self):
        cfg = EvalScenarioConfig(
            network_profile="degraded_300ms_10loss",
            scenarios=["partition_heal"],
        )

        with self.assertRaisesRegex(ValueError, "no evaluation steps"):
            self.runner.validate_config(cfg)

    def test_default_evaluation_disables_dashboard_observability(self):
        self.assertEqual(EvalScenarioConfig().dashboard_observability, "off")
        self.assertTrue(EvalScenarioConfig().warm_topology)

    def test_config_exposes_delta_log_and_partition_write_controls(self):
        cfg = EvalScenarioConfig(delta_log_limit=25, partition_events_per_node=40)

        self.assertIn("delta_log_25", cfg.implementation_label())
        self.assertEqual(cfg.delta_log_limit, 25)
        self.assertEqual(cfg.partition_events_per_node, 40)

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
        def sample(node, cpu, system, memory, network_rx):
            return {
                "node": node,
                "cpu_usage_ns": cpu,
                "system_cpu_usage_ns": system,
                "online_cpus": 4,
                "memory_bytes": memory,
                "network_rx_bytes": network_rx,
                "network_tx_bytes": 0,
                "block_read_bytes": 0,
                "block_write_bytes": 0,
            }

        self.runner._resource_samples = [
            [
                sample("edge-node-1", 1_000, 10_000, 10 * 1024 * 1024, 100),
                sample("edge-node-2", 2_000, 10_000, 20 * 1024 * 1024, 200),
            ],
            [
                sample("edge-node-1", 1_100, 11_000, 12 * 1024 * 1024, 140),
                sample("edge-node-2", 2_050, 11_000, 22 * 1024 * 1024, 260),
            ],
        ]

        summary = self.runner._resource_summary()

        self.assertEqual(summary["resource_sample_count"], 2)
        self.assertEqual(summary["cpu_mean_percent"], 30.0)
        self.assertEqual(summary["cpu_peak_percent"], 40.0)
        self.assertEqual(summary["memory_peak_mib"], 22.0)
        self.assertEqual(summary["network_rx_bytes"], 100)


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

        with patch(
            "src.evaluation.runner.time.time", side_effect=[0.0, 46.0, 47.0]
        ):
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


if __name__ == "__main__":
    unittest.main()
