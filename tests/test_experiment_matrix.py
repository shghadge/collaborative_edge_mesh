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
    generate_combined_cases,
    generate_experiment_cases,
    generate_resource_heterogeneity_cases,
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
        self.assertEqual({case["network_profile"] for case in partition}, {"clean_lan"})

    def test_resource_heterogeneity_matrix_pairs_equal_total_budgets(self):
        cases = generate_resource_heterogeneity_cases(repetitions=10, base_seed=9201)

        self.assertEqual(len(cases), 24)
        self.assertEqual(sum(int(case["repetitions"]) for case in cases), 240)
        self.assertEqual({case["node_count"] for case in cases}, {5, 10, 15})
        self.assertEqual(
            {case["resource_mode"] for case in cases},
            {"uniform", "heterogeneous"},
        )
        for node_count in (5, 10, 15):
            node_cases = [case for case in cases if case["node_count"] == node_count]
            self.assertEqual(len(node_cases), 8)
            self.assertEqual(len({case["seed"] for case in node_cases}), 1)
            self.assertEqual({case["docker_cpus"] for case in node_cases}, {0.5})
            self.assertEqual({case["docker_memory"] for case in node_cases}, {"512m"})

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
        self.assertEqual(command[command.index("--resource-mode") + 1], "uniform")


class CombinedMatrixTests(unittest.TestCase):
    @classmethod
    def setUpClass(cls):
        cls.cases = generate_combined_cases(repetitions=10, seed=11001)

    @staticmethod
    def _event_total(case):
        return case["events_per_node"] + case["partition_events_per_node"]

    def test_combined_matrix_has_144_configurations_with_one_shared_seed(self):
        self.assertEqual(len(self.cases), 144)
        self.assertEqual(len({case["case_id"] for case in self.cases}), 144)
        self.assertEqual({case["sweep"] for case in self.cases}, {"combined"})
        self.assertEqual({case["seed"] for case in self.cases}, {11001})
        self.assertEqual({case["repetitions"] for case in self.cases}, {10})

        condition_methods = defaultdict(set)
        for case in self.cases:
            condition_methods[case["condition_id"]].add(case["sync_mode"])
        self.assertEqual(len(condition_methods), 36)
        for methods in condition_methods.values():
            self.assertEqual(methods, set(METHODS))

    def test_scale_and_load_blocks_cross_every_device_and_partition_level(self):
        cells = defaultdict(set)
        for case in self.cases:
            cells[(case["node_count"], self._event_total(case))].add(
                (case["resource_mode"], case["partition_split"])
            )

        self.assertEqual(
            set(cells),
            {(5, 90), (10, 90), (15, 30), (15, 90), (15, 180), (15, 300)},
        )
        expected_levels = {
            (resource_mode, split)
            for resource_mode in ("uniform", "heterogeneous")
            for split in ("", 0.5, 0.8)
        }
        for levels in cells.values():
            self.assertEqual(levels, expected_levels)

    def test_partition_cells_split_total_events_into_equal_halves(self):
        for case in self.cases:
            if case["partition_split"] == "":
                self.assertEqual(case["scenario"], "baseline")
                self.assertEqual(case["partition_events_per_node"], 0)
                self.assertEqual(case["partition_wait_seconds"], 0)
            else:
                self.assertEqual(case["scenario"], "partition_heal")
                self.assertEqual(
                    case["events_per_node"], case["partition_events_per_node"]
                )
                self.assertEqual(case["partition_wait_seconds"], 30)
            self.assertIn(self._event_total(case), {30, 90, 180, 300})

    def test_fixed_settings_rate_network_and_device_profiles(self):
        for case in self.cases:
            self.assertEqual(case["offered_rate_per_second"], 10 * case["node_count"])
            self.assertEqual(case["network_profile"], "clean_lan")
            self.assertEqual(case["docker_cpus"], 0.5)
            self.assertEqual(case["docker_memory"], "512m")
            expected_profile = (
                "balanced_edge_v1" if case["resource_mode"] == "heterogeneous" else ""
            )
            self.assertEqual(case["resource_profile"], expected_profile)

    def test_run_order_is_deterministic_and_not_in_factor_order(self):
        case_ids = [case["case_id"] for case in self.cases]
        self.assertNotEqual(case_ids, sorted(case_ids))
        adjacent_same_condition = sum(
            left["condition_id"] == right["condition_id"]
            for left, right in zip(self.cases, self.cases[1:])
        )
        self.assertLess(adjacent_same_condition, len(self.cases) // 4)
        self.assertEqual(
            case_ids,
            [
                case["case_id"]
                for case in generate_combined_cases(repetitions=10, seed=11001)
            ],
        )


if __name__ == "__main__":
    unittest.main()
