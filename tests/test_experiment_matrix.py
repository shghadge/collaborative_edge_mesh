import csv
import tempfile
import unittest
from collections import Counter, defaultdict
from pathlib import Path

from scripts.run_reviewer_matrix import _case_command
from src.evaluation.matrix import (
    EXPECTED_CONFIGURATION_COUNT,
    EXPECTED_CONDITION_COUNT,
    EXPECTED_SWEEP_COUNTS,
    METHODS,
    generate_experiment_cases,
    load_experiment_matrix,
    write_experiment_matrix,
)


class ExperimentMatrixTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cases = generate_experiment_cases(repetitions=30, base_seed=7301)

    def test_complete_matrix_has_expected_configuration_and_result_counts(self):
        sweep_counts = Counter(case["sweep"] for case in self.cases)

        self.assertEqual(len(self.cases), EXPECTED_CONFIGURATION_COUNT)
        self.assertEqual(sweep_counts, EXPECTED_SWEEP_COUNTS)
        self.assertEqual(
            len({case["condition_id"] for case in self.cases}),
            EXPECTED_CONDITION_COUNT,
        )
        self.assertEqual(
            sum(int(case["repetitions"]) for case in self.cases),
            52800,
        )

    def test_each_condition_has_four_methods_and_one_paired_seed(self):
        condition_methods = defaultdict(set)
        condition_seeds = defaultdict(set)
        for case in self.cases:
            condition_methods[case["condition_id"]].add(case["sync_mode"])
            condition_seeds[case["condition_id"]].add(case["seed"])

        for condition_id in condition_methods:
            self.assertEqual(condition_methods[condition_id], set(METHODS))
            self.assertEqual(len(condition_seeds[condition_id]), 1)

    def test_sweeps_use_the_reported_fixed_settings(self):
        scale = [case for case in self.cases if case["sweep"] == "scale"]
        self.assertEqual({case["network_profile"] for case in scale}, {"clean_lan"})
        self.assertEqual({case["events_per_node"] for case in scale}, {15})
        self.assertEqual({case["docker_cpus"] for case in scale}, {1.0})
        self.assertEqual({case["docker_memory"] for case in scale}, {"512m"})

        partition = [case for case in self.cases if case["sweep"] == "partition_shape"]
        self.assertEqual({case["node_count"] for case in partition}, {15, 30})
        self.assertEqual(
            {case["partition_split"] for case in partition}, {0.5, 0.7, 0.8}
        )
        self.assertEqual({case["events_per_node"] for case in partition}, {45})
        self.assertEqual(
            {case["partition_events_per_node"] for case in partition}, {45}
        )
        self.assertEqual({case["partition_wait_seconds"] for case in partition}, {30})

    def test_matrix_round_trip_preserves_all_rows(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "matrix.csv"
            write_experiment_matrix(path, self.cases)
            loaded = load_experiment_matrix(path)

        self.assertEqual(len(loaded), EXPECTED_CONFIGURATION_COUNT)
        self.assertEqual(loaded[0]["case_id"], "scale-n03-full_state")
        self.assertEqual(loaded[-1]["case_id"], self.cases[-1]["case_id"])

    def test_matrix_runner_passes_sweep_condition_and_repetitions(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "matrix.csv"
            write_experiment_matrix(path, self.cases[:4])
            with path.open(newline="", encoding="utf-8") as handle:
                case = next(csv.DictReader(handle))

        command = _case_command(
            case,
            base_url="http://localhost:8000",
            out_dir=Path("results/final"),
            resume=True,
        )

        self.assertEqual(command[command.index("--condition-id") + 1], "scale-n03")
        self.assertEqual(command[command.index("--sweep") + 1], "scale")
        self.assertEqual(command[command.index("--repetitions") + 1], "30")
        self.assertIn("--resume", command)


if __name__ == "__main__":
    unittest.main()
