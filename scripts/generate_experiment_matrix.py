#!/usr/bin/env python3
import argparse
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.evaluation.matrix import generate_experiment_cases, write_experiment_matrix


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate the complete focused experiment matrix."
    )
    parser.add_argument("--repetitions", type=int, default=30)
    parser.add_argument("--base-seed", type=int, default=7301)
    parser.add_argument(
        "--out",
        type=Path,
        default=Path("scripts/final_experiment_matrix.csv"),
    )
    args = parser.parse_args()

    try:
        cases = generate_experiment_cases(
            repetitions=args.repetitions,
            base_seed=args.base_seed,
        )
    except ValueError as error:
        print(f"matrix generation failed: {error}", file=sys.stderr)
        return 2

    write_experiment_matrix(args.out, cases)
    sweep_counts = Counter(str(case["sweep"]) for case in cases)
    result_rows = sum(int(case["repetitions"]) for case in cases)
    print(args.out)
    print(f"configurations={len(cases)}")
    print(f"result_rows={result_rows}")
    for sweep, count in sorted(sweep_counts.items()):
        print(f"{sweep}={count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
