#!/usr/bin/env python3
import argparse
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.evaluation.matrix import (  # noqa: E402
    generate_combined_cases,
    generate_experiment_cases,
    write_experiment_matrix,
)

REPO_ROOT = Path(__file__).resolve().parents[1]

DEFAULT_OUTPUTS = {
    "focused": Path("scripts/final_experiment_matrix.csv"),
    "combined": REPO_ROOT.parent / "masters_report" / "experiment_6_combined.csv",
}
DEFAULT_REPETITIONS = {"focused": 30, "combined": 10}
DEFAULT_SEEDS = {"focused": 7301, "combined": 11001}


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Generate the focused or combined experiment matrix."
    )
    parser.add_argument("--design", choices=("focused", "combined"), default="focused")
    parser.add_argument("--repetitions", type=int)
    parser.add_argument(
        "--base-seed",
        type=int,
        help="Focused: first condition seed. Combined: shared seed for every row.",
    )
    parser.add_argument("--out", type=Path)
    args = parser.parse_args()

    repetitions = (
        args.repetitions
        if args.repetitions is not None
        else DEFAULT_REPETITIONS[args.design]
    )
    seed = args.base_seed if args.base_seed is not None else DEFAULT_SEEDS[args.design]
    out = args.out or DEFAULT_OUTPUTS[args.design]

    try:
        if args.design == "combined":
            cases = generate_combined_cases(repetitions=repetitions, seed=seed)
        else:
            cases = generate_experiment_cases(
                repetitions=repetitions,
                base_seed=seed,
            )
    except ValueError as error:
        print(f"matrix generation failed: {error}", file=sys.stderr)
        return 2

    write_experiment_matrix(out, cases)
    sweep_counts = Counter(str(case["sweep"]) for case in cases)
    result_rows = sum(int(case["repetitions"]) for case in cases)
    print(out)
    print(f"configurations={len(cases)}")
    print(f"result_rows={result_rows}")
    for sweep, count in sorted(sweep_counts.items()):
        print(f"{sweep}={count}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
