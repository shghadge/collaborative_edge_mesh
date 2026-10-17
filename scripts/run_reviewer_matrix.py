#!/usr/bin/env python3
import argparse
import subprocess
import sys
from collections import Counter
from pathlib import Path

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.evaluation.matrix import load_experiment_matrix, validate_experiment_cases


def _load_cases(path: Path) -> list[dict[str, str]]:
    return load_experiment_matrix(path)


def _case_command(
    case: dict[str, str], *, base_url: str, out_dir: Path, resume: bool
) -> list[str]:
    script = Path(__file__).with_name("run_evaluation_matrix.py")
    split = case["partition_split"].strip() or "0.5"
    command = [
        sys.executable,
        str(script),
        "--base-url",
        base_url,
        "--sync-mode",
        case["sync_mode"].strip(),
        "--condition-id",
        case["condition_id"].strip(),
        "--sweep",
        case["sweep"].strip(),
        "--event-id-retention-limit",
        case["event_id_retention_limit"].strip(),
        "--delta-log-limit",
        case["delta_log_limit"].strip(),
        "--node-counts",
        case["node_count"].strip(),
        "--scenarios",
        case["scenario"].strip(),
        "--partition-splits",
        split,
        "--network-profile",
        case["network_profile"].strip(),
        "--events-per-node",
        case["events_per_node"].strip(),
        "--partition-events-per-node",
        case["partition_events_per_node"].strip(),
        "--event-concurrency",
        case["event_concurrency"].strip(),
        "--partition-wait-seconds",
        case["partition_wait_seconds"].strip(),
        "--convergence-wait-seconds",
        case["convergence_wait_seconds"].strip(),
        "--poll-interval-seconds",
        case["poll_interval_seconds"].strip(),
        "--scenario-seed",
        case["seed"].strip(),
        "--repetitions",
        case["repetitions"].strip(),
        "--case-id",
        case["case_id"].strip(),
        "--resource-mode",
        case["resource_mode"].strip() or "uniform",
        "--out",
        str(out_dir / case["case_id"].strip()),
    ]
    if case["docker_cpus"].strip():
        command.extend(["--docker-cpus", case["docker_cpus"].strip()])
    if case["docker_memory"].strip():
        command.extend(["--docker-memory", case["docker_memory"].strip()])
    if case["resource_profile"].strip():
        command.extend(["--resource-profile", case["resource_profile"].strip()])
    if case["offered_rate_per_second"].strip():
        command.extend(
            [
                "--offered-rate-per-second",
                case["offered_rate_per_second"].strip(),
            ]
        )
    if resume:
        command.append("--resume")
    return command


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Execute enabled rows from a reviewer experiment CSV matrix."
    )
    parser.add_argument("matrix", type=Path)
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--out", type=Path, default=Path("results/reviewer_revision"))
    parser.add_argument("--no-resume", action="store_true")
    parser.add_argument("--dry-run", action="store_true")
    parser.add_argument(
        "--require-complete",
        action="store_true",
        help="Require the exact 1,760-configuration focused matrix.",
    )
    args = parser.parse_args()

    try:
        cases = _load_cases(args.matrix)
        if args.require_complete:
            validate_experiment_cases(cases, require_complete=True)
    except (OSError, ValueError) as error:
        print(f"invalid reviewer matrix: {error}", file=sys.stderr)
        return 2

    if not cases:
        print("reviewer matrix has no enabled cases", file=sys.stderr)
        return 2

    if args.dry_run:
        sweep_counts = Counter(case["sweep"].strip() for case in cases)
        result_rows = sum(int(case["repetitions"]) for case in cases)
        print(f"validated_configurations={len(cases)}")
        print(f"expected_result_rows={result_rows}")
        for sweep, count in sorted(sweep_counts.items()):
            print(f"{sweep}={count}")
        return 0

    for index, case in enumerate(cases, start=1):
        case_id = case["case_id"].strip()
        command = _case_command(
            case,
            base_url=args.base_url.rstrip("/"),
            out_dir=args.out,
            resume=not args.no_resume,
        )
        print(f"[{index}/{len(cases)}] {case_id}", flush=True)
        completed = subprocess.run(command, check=False)
        if completed.returncode != 0:
            print(f"case failed: {case_id}", file=sys.stderr)
            return completed.returncode

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
