#!/usr/bin/env python3
"""Run a tiny Docker-backed evaluation and check that it fully converged.

Usage: python3 scripts/run_evaluation_smoke.py [--base-url URL]
[--sync-mode full_state|true_delta] [--node-counts 2] [--events-per-node N]
[--out DIR]. Calls run_evaluation_matrix.py with the degraded network profile,
then validates the resulting CSV.
"""

import argparse
import csv
import subprocess
import sys
from pathlib import Path


def _csv_path_from_output(output: str) -> Path:
    """Return the first line of output ending in .csv as a Path.

    Raises ValueError if no such line exists.
    """
    for line in output.splitlines():
        stripped = line.strip()
        if stripped.endswith(".csv"):
            return Path(stripped)
    raise ValueError("run_evaluation_matrix.py did not print a CSV path")


def _validate_smoke_csv(path: Path) -> None:
    """Check that every CSV row converged, missed no events, and wrote all events.

    Raises ValueError listing each failing row, or if the CSV has no rows.
    """
    with path.open(newline="", encoding="utf-8") as handle:
        rows = list(csv.DictReader(handle))

    if not rows:
        raise ValueError(f"{path} did not contain any result rows")

    failures = []
    for row in rows:
        label = f"{row.get('scenario')} nodes={row.get('node_count')}"
        if row.get("converged_within_timeout") != "True":
            failures.append(f"{label}: did not converge")
        if row.get("missing_at_cutoff_count") not in {"0", "0.0"}:
            failures.append(
                f"{label}: missing_at_cutoff_count={row.get('missing_at_cutoff_count')}"
            )
        if float(row.get("write_success_rate") or 0.0) != 1.0:
            failures.append(
                f"{label}: write_success_rate={row.get('write_success_rate')}"
            )

    if failures:
        joined = "\n  - ".join(failures)
        raise ValueError(f"smoke evaluation failed:\n  - {joined}")


def main() -> int:
    """Run the smoke evaluation subprocess and validate its CSV output.

    Returns the subprocess exit code if it failed, 1 if validation fails, else 0.
    """
    parser = argparse.ArgumentParser(
        description="Run a tiny Docker-backed evaluation to validate runtime wiring."
    )
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument(
        "--sync-mode", choices=["full_state", "true_delta"], default="true_delta"
    )
    parser.add_argument("--out", default="results/evaluation_smoke")
    parser.add_argument("--node-counts", default="2")
    parser.add_argument("--events-per-node", type=int, default=2)
    parser.add_argument("--convergence-wait-seconds", type=float, default=12)
    parser.add_argument("--poll-interval-seconds", type=float, default=1)
    args = parser.parse_args()

    command = [
        sys.executable,
        "scripts/run_evaluation_matrix.py",
        "--base-url",
        args.base_url,
        "--sync-mode",
        args.sync_mode,
        "--event-id-retention-limit",
        "100",
        "--node-counts",
        args.node_counts,
        "--events-per-node",
        str(args.events_per_node),
        "--network-profile",
        "degraded",
        "--convergence-wait-seconds",
        str(args.convergence_wait_seconds),
        "--poll-interval-seconds",
        str(args.poll_interval_seconds),
        "--out",
        args.out,
    ]
    print(f"==> smoke evaluation: {' '.join(command)}", flush=True)
    completed = subprocess.run(command, text=True, capture_output=True)
    if completed.stdout:
        print(completed.stdout, end="")
    if completed.stderr:
        print(completed.stderr, end="", file=sys.stderr)
    if completed.returncode != 0:
        return completed.returncode

    try:
        csv_path = _csv_path_from_output(completed.stdout)
        _validate_smoke_csv(csv_path)
    except (OSError, ValueError) as error:
        print(error, file=sys.stderr)
        return 1

    print(f"smoke evaluation passed: {csv_path}")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
