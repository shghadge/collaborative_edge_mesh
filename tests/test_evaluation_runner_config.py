import unittest

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


if __name__ == "__main__":
    unittest.main()
