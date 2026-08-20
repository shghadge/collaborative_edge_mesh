import unittest
from pathlib import Path

from scripts.generate_synthetic_visualization_data import (
    load_current_anchors,
    simulate_row,
)
from scripts.generate_theoretical_model_dataset import (
    CPU_LIMITS,
    MEMORY_LIMITS_MIB,
    NETWORK_PROFILES,
    NODE_COUNTS,
    PLOTTING_COLUMN_GROUPS,
    PLOTTING_FIELDS,
    REMOVED_FIELDS,
    REPETITION_SEEDS,
    RESOURCE_FIELDS,
    apply_resource_limits,
    build_plotting_rows,
    build_resource_scenarios,
    output_fields,
)


class TheoreticalModelDatasetTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        project_root = Path(__file__).resolve().parents[1]
        cls.anchors = load_current_anchors(
            project_root
            / "results/reviewer_revision/paper_evidence/paper_evidence.json"
        )

    def test_resource_matrix_covers_every_requested_combination(self) -> None:
        scenarios = build_resource_scenarios()

        self.assertEqual(len(scenarios), 396)
        self.assertEqual(
            {scenario.node_count for scenario, _, _ in scenarios}, set(NODE_COUNTS)
        )
        self.assertEqual(
            {scenario.network_profile for scenario, _, _ in scenarios},
            set(NETWORK_PROFILES),
        )
        self.assertEqual({cpu for _, cpu, _ in scenarios}, set(CPU_LIMITS))
        self.assertEqual({memory for _, _, memory in scenarios}, set(MEMORY_LIMITS_MIB))
        self.assertEqual(len(REPETITION_SEEDS), 30)

    def test_clean_schema_has_resource_fields_without_removed_fields(self) -> None:
        fields = set(output_fields())

        self.assertTrue(set(RESOURCE_FIELDS).issubset(fields))
        self.assertFalse(fields.intersection(REMOVED_FIELDS))
        self.assertFalse(any(field.startswith("central_") for field in fields))

    def test_plotting_schema_is_grouped_and_reduced(self) -> None:
        source = {field: field for field in output_fields()}

        plotting_row = build_plotting_rows([source])[0]

        grouped_fields = tuple(
            field for group in PLOTTING_COLUMN_GROUPS.values() for field in group
        )
        self.assertEqual(len(PLOTTING_FIELDS), 53)
        self.assertEqual(PLOTTING_FIELDS, grouped_fields)
        self.assertEqual(tuple(plotting_row), PLOTTING_FIELDS)

    def test_tighter_limits_reduce_throughput_and_cap_resources(self) -> None:
        scenario, _, _ = next(
            item
            for item in build_resource_scenarios()
            if item[0].node_count == 50 and item[0].network_profile == "severe"
        )
        base = simulate_row(
            "theoretical",
            "naive_key_delta",
            scenario,
            1,
            REPETITION_SEEDS[0],
            self.anchors,
            "not-exported",
        )

        constrained = apply_resource_limits(base, 0.25, 100)

        self.assertLessEqual(
            constrained["accepted_throughput_events_s"],
            base["accepted_throughput_events_s"],
        )
        self.assertGreaterEqual(
            constrained["write_latency_p95_ms"], base["write_latency_p95_ms"]
        )
        self.assertLessEqual(constrained["cpu_peak_percent"], 25.0)
        self.assertLessEqual(constrained["memory_peak_mib"], 100.0)
        self.assertLessEqual(constrained["cpu_peak_of_limit_percent"], 100.0)
        self.assertLessEqual(constrained["memory_peak_of_limit_percent"], 100.0)
        self.assertEqual(constrained["resource_configuration"], "cpu0.25_mem100")


if __name__ == "__main__":
    unittest.main()
