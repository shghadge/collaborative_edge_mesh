#!/usr/bin/env python3
import argparse
import csv
import subprocess
import sys
from pathlib import Path

REQUIRED_COLUMNS = {
    "case_id",
    "enabled",
    "sync_mode",
    "node_count",
    "event_id_retention_limit",
    "delta_log_limit",
    "scenario",
    "partition_split",
    "network_profile",
    "events_per_node",
    "partition_events_per_node",
    "event_concurrency",
    "offered_rate_per_second",
    "partition_wait_seconds",
    "convergence_wait_seconds",
    "poll_interval_seconds",
    "seed",
    "docker_cpus",
    "docker_memory",
    "observability",
}


def _enabled(value: str) -> bool:
    return value.strip().lower() in {"1", "true", "yes", "on"}


def _load_cases(path: Path) -> list[dict[str, str]]:
    with path.open(newline="", encoding="utf-8") as handle:
        reader = csv.DictReader(handle)
        missing = REQUIRED_COLUMNS - set(reader.fieldnames or [])
        if missing:
            names = ", ".join(sorted(missing))
            raise ValueError(f"matrix is missing required columns: {names}")
        cases = [row for row in reader if _enabled(row["enabled"])]

    case_ids = [row["case_id"].strip() for row in cases]
    if any(not case_id for case_id in case_ids):
        raise ValueError("every enabled matrix row requires a case_id")
    if len(case_ids) != len(set(case_ids)):
        raise ValueError("enabled matrix case_id values must be unique")
    return cases


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
        "1",
        "--dashboard-observability",
        case["observability"].strip(),
        "--case-id",
        case["case_id"].strip(),
        "--cold-topology",
        "--out",
        str(out_dir / case["case_id"].strip()),
    ]
    if case["docker_cpus"].strip():
        command.extend(["--docker-cpus", case["docker_cpus"].strip()])
    if case["docker_memory"].strip():
        command.extend(["--docker-memory", case["docker_memory"].strip()])
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
    args = parser.parse_args()

    try:
        cases = _load_cases(args.matrix)
    except (OSError, ValueError) as error:
        print(f"invalid reviewer matrix: {error}", file=sys.stderr)
        return 2

    if not cases:
        print("reviewer matrix has no enabled cases", file=sys.stderr)
        return 2

    for index, case in enumerate(cases, start=1):
        case_id = case["case_id"].strip()
        command = _case_command(
            case,
            base_url=args.base_url.rstrip("/"),
            out_dir=args.out,
            resume=not args.no_resume,
        )
        print(f"[{index}/{len(cases)}] {case_id}", flush=True)
        if args.dry_run:
            print(" ".join(command))
            continue
        completed = subprocess.run(command, check=False)
        if completed.returncode != 0:
            print(f"case failed: {case_id}", file=sys.stderr)
            return completed.returncode

    return 0


if __name__ == "__main__":
    raise SystemExit(main())
