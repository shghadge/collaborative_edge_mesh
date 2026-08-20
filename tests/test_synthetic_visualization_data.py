import unittest
from pathlib import Path

from scripts.generate_synthetic_visualization_data import (
    CURRENT_METHODS,
    VARIANTS,
    build_scenarios,
    generate_rows,
    load_current_anchors,
    summarize_rows,
    validate_rows,
)


class SyntheticVisualizationDataTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls) -> None:
        project_root = Path(__file__).resolve().parents[1]
        evidence_path = (
            project_root
            / "results/reviewer_revision/paper_evidence/paper_evidence.json"
        )
        cls.anchors = load_current_anchors(evidence_path)
        cls.generated_at = "2026-08-18T00:00:00+00:00"
        cls.rows = {
            variant: generate_rows(variant, cls.anchors, cls.generated_at)
            for variant in VARIANTS
        }

    def test_matrix_has_fifty_unique_scenarios(self) -> None:
        scenarios = build_scenarios()

        self.assertEqual(len(scenarios), 50)
        self.assertEqual(len({scenario.configuration_id for scenario in scenarios}), 50)

    def test_each_variant_has_six_thousand_valid_rows(self) -> None:
        for variant, rows in self.rows.items():
            validate_rows(rows, variant)
            self.assertEqual(len(rows), 6000)
            self.assertEqual(len(summarize_rows(rows)), 200)

    def test_current_method_metrics_are_identical_between_variants(self) -> None:
        ignored = {
            "dataset_variant",
            "run_id",
            "case_id",
        }
        theoretical = [
            row
            for row in self.rows["theoretical"]
            if row["sync_mode"] in CURRENT_METHODS
        ]
        calibrated = [
            row
            for row in self.rows["old_figure_calibrated"]
            if row["sync_mode"] in CURRENT_METHODS
        ]

        for left, right in zip(theoretical, calibrated):
            left_metrics = {
                key: value for key, value in left.items() if key not in ignored
            }
            right_metrics = {
                key: value for key, value in right.items() if key not in ignored
            }
            self.assertEqual(left_metrics, right_metrics)

    def test_historical_variants_change_modeled_outcomes(self) -> None:
        theoretical = next(
            row
            for row in self.rows["theoretical"]
            if row["sync_mode"] == "naive_key_delta"
            and row["case_id"].startswith("scale-15n")
        )
        calibrated = next(
            row
            for row in self.rows["old_figure_calibrated"]
            if row["sync_mode"] == "naive_key_delta"
            and row["case_id"].startswith("scale-15n")
        )

        self.assertNotEqual(
            theoretical["bytes_per_accepted_event"],
            calibrated["bytes_per_accepted_event"],
        )
        self.assertNotEqual(
            theoretical["observed_completion_time_s"],
            calibrated["observed_completion_time_s"],
        )


if __name__ == "__main__":
    unittest.main()
