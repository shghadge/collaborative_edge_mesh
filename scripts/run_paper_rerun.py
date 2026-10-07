#!/usr/bin/env python3
"""Rerun the AIBThings Fig. 3 partition-heal matrix with repetitions, in batches.

Matrix: 4 sync methods x {5, 10, 15} nodes x {50/50, 70/30, 80/20} splits x
10 repetitions = 360 independent runs. Every run uses freshly created
containers limited to 1 CPU and 500 MiB of memory each, the Fig. 3 workload
(15 events/node before the partition, 3 during it, 8 s partition, 25 s
convergence budget, 2 s polling), and a hard iptables partition. All four
methods share the same seed within a repetition, so they see the same workload.

Commands:
  up         build images from this working tree and start simulator + gateway
  run        execute pending runs; stop starting new runs after --max-minutes
  status     show completed/pending counts per configuration
  summarize  write mean/SD tables from completed runs
  down       stop simulator, gateway, and edge-node containers

Interrupting `run` is safe: completed runs are skipped on the next `run`.
"""

import argparse
import csv
import json
import statistics
import subprocess
import sys
import time
from collections import defaultdict
from pathlib import Path
from urllib.error import URLError
from urllib.request import urlopen

ROOT = Path(__file__).resolve().parents[1]
COMPOSE = [
    "docker",
    "compose",
    "-f",
    str(ROOT / "docker-compose.yml"),
    "-f",
    str(ROOT / "docker-compose.paper-rerun.yml"),
]
CODE_LABEL = "collaborative_edge_mesh_old"
OUT_DIR = ROOT / "results" / "paper_rerun_10x"
BASE_URL = "http://localhost:8000"

METHODS = ("full_state", "optimized_full_state", "naive_key_delta", "true_delta")
METHOD_RETENTION = {
    "full_state": 0,
    "optimized_full_state": 100,
    "naive_key_delta": 100,
    "true_delta": 100,
}
NODE_COUNTS = (5, 10, 15)
SPLITS = (0.5, 0.7, 0.8)
REPETITIONS = 10
SEED_BASE = 9100
CONVERGENCE_WAIT_SECONDS = 25
DOCKER_CPUS = "1"
DOCKER_MEMORY = "500m"  # Docker "m" = MiB


def _split_label(split: float) -> str:
    return f"{round(split * 100)}/{round((1 - split) * 100)}"


def _case_id(method: str, nodes: int, split: float, rep: int) -> str:
    return f"{method}-n{nodes:02d}-s{round(split * 100)}-r{rep:02d}"


def _units() -> list[dict]:
    """Repetition-major order so a partial batch still covers every cell evenly."""
    units = []
    for rep in range(1, REPETITIONS + 1):
        shift = (rep - 1) % len(METHODS)
        methods = METHODS[shift:] + METHODS[:shift]
        for nodes in NODE_COUNTS:
            for split in SPLITS:
                for method in methods:
                    units.append(
                        {
                            "case_id": _case_id(method, nodes, split, rep),
                            "method": method,
                            "nodes": nodes,
                            "split": split,
                            "rep": rep,
                            "seed": SEED_BASE + rep,
                        }
                    )
    return units


def _completed_csv(case_id: str) -> Path | None:
    case_dir = OUT_DIR / case_id
    for manifest_path in case_dir.glob("*/*.json"):
        if manifest_path.name.endswith("-results.json"):
            continue
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        csv_path = Path(manifest.get("csv_path", ""))
        if not csv_path.is_absolute():
            csv_path = ROOT / csv_path
        if csv_path.exists():
            return csv_path
    return None


def _command(unit: dict) -> list[str]:
    return [
        sys.executable,
        str(ROOT / "scripts" / "run_evaluation_matrix.py"),
        "--base-url",
        BASE_URL,
        "--sync-mode",
        unit["method"],
        "--event-id-retention-limit",
        str(METHOD_RETENTION[unit["method"]]),
        "--delta-log-limit",
        "1000",
        "--node-counts",
        str(unit["nodes"]),
        "--scenarios",
        "partition_heal",
        "--partition-splits",
        str(unit["split"]),
        "--network-profile",
        "hard_partition",
        "--events-per-node",
        "15",
        "--partition-events-per-node",
        "3",
        "--partition-wait-seconds",
        "8",
        "--convergence-wait-seconds",
        str(CONVERGENCE_WAIT_SECONDS),
        "--poll-interval-seconds",
        "2",
        "--event-concurrency",
        "25",
        "--repetitions",
        "1",
        "--scenario-seed",
        str(unit["seed"]),
        "--case-id",
        unit["case_id"],
        "--condition-id",
        f"n{unit['nodes']:02d}-s{round(unit['split'] * 100)}-r{unit['rep']:02d}",
        "--sweep",
        "paper_rerun_10x",
        "--cold-topology",
        "--docker-cpus",
        DOCKER_CPUS,
        "--docker-memory",
        DOCKER_MEMORY,
        "--resume",
        "--out",
        str(OUT_DIR / unit["case_id"]),
    ]


def _docker_output(*args: str) -> str:
    completed = subprocess.run(
        ["docker", *args], capture_output=True, text=True, check=False
    )
    return completed.stdout.strip() if completed.returncode == 0 else ""


def _api_status() -> dict | None:
    try:
        with urlopen(f"{BASE_URL}/evaluation/status", timeout=10) as response:
            return json.loads(response.read().decode("utf-8"))
    except (URLError, OSError, json.JSONDecodeError):
        return None


def _check_simulator() -> str | None:
    """Return an error message unless the simulator runs this tree's image."""
    label = _docker_output(
        "inspect", "simulator", "--format", '{{index .Config.Labels "edge_rerun.code"}}'
    )
    if label != CODE_LABEL:
        return (
            "simulator is not running the collaborative_edge_mesh_old image; "
            "start it with: uv run python scripts/run_paper_rerun.py up"
        )
    if _api_status() is None:
        return f"simulator API at {BASE_URL} is not reachable"
    return None


def _wait_until_idle() -> None:
    """A run interrupted by Ctrl-C keeps executing inside the simulator."""
    announced = False
    while True:
        status = _api_status()
        if status is not None and not status.get("running"):
            return
        if not announced:
            print("waiting for an interrupted simulator run to finish...", flush=True)
            announced = True
        time.sleep(5)


def cmd_up(_args) -> int:
    subprocess.run([*COMPOSE, "up", "-d", "--build"], cwd=ROOT, check=True)
    deadline = time.time() + 60
    while time.time() < deadline:
        if _api_status() is not None:
            print(f"simulator ready at {BASE_URL}")
            return 0
        time.sleep(2)
    print("simulator did not become ready within 60 s", file=sys.stderr)
    return 1


def cmd_down(_args) -> int:
    nodes = _docker_output("ps", "-aq", "--filter", "name=edge-node-").split()
    if nodes:
        subprocess.run(["docker", "rm", "-f", *nodes], check=False)
    subprocess.run([*COMPOSE, "stop"], cwd=ROOT, check=False)
    return 0


def cmd_run(args) -> int:
    error = _check_simulator()
    if error:
        print(error, file=sys.stderr)
        return 1

    units = _units()
    pending = [unit for unit in units if _completed_csv(unit["case_id"]) is None]
    print(
        f"{len(units) - len(pending)}/{len(units)} runs complete, {len(pending)} pending"
    )
    started = time.time()
    completed: list[str] = []
    for unit in pending:
        elapsed_min = (time.time() - started) / 60
        if args.max_minutes and elapsed_min >= args.max_minutes:
            print(f"time budget reached after {elapsed_min:.1f} min")
            break
        if args.max_runs and len(completed) >= args.max_runs:
            break
        _wait_until_idle()
        done = len(units) - len(pending) + len(completed)
        print(f"[{done + 1}/{len(units)}] {unit['case_id']}", flush=True)
        result = subprocess.run(_command(unit), cwd=ROOT, check=False)
        if result.returncode != 0 or _completed_csv(unit["case_id"]) is None:
            print(f"run failed: {unit['case_id']}", file=sys.stderr)
            return 1
        completed.append(unit["case_id"])
    remaining = len(pending) - len(completed)
    print(f"batch finished: {len(completed)} new runs, {remaining} still pending")
    return 0


def _load_rows() -> list[dict]:
    rows = []
    for unit in _units():
        csv_path = _completed_csv(unit["case_id"])
        if csv_path is None:
            continue
        with csv_path.open(newline="", encoding="utf-8") as handle:
            for row in csv.DictReader(handle):
                if row.get("scenario") == "partition_heal":
                    rows.append({**row, "_unit": unit})
    return rows


def cmd_status(_args) -> int:
    done = defaultdict(int)
    for unit in _units():
        if _completed_csv(unit["case_id"]) is not None:
            done[(unit["method"], unit["nodes"], unit["split"])] += 1
    total = sum(done.values())
    print(f"{total}/{len(_units())} runs complete")
    for nodes in NODE_COUNTS:
        for split in SPLITS:
            cells = "  ".join(
                f"{method}={done[(method, nodes, split)]}" for method in METHODS
            )
            print(f"{nodes:>2} nodes {_split_label(split)}: {cells}")
    return 0


def _float(value) -> float | None:
    try:
        return float(value)
    except (TypeError, ValueError):
        return None


def _mean_sd(values: list[float]) -> tuple[float | None, float | None]:
    if not values:
        return None, None
    sd = statistics.stdev(values) if len(values) > 1 else 0.0
    return statistics.mean(values), sd


def cmd_summarize(_args) -> int:
    grouped = defaultdict(list)
    for row in _load_rows():
        unit = row["_unit"]
        grouped[(unit["nodes"], unit["split"], unit["method"])].append(row)

    summary = []
    for nodes in NODE_COUNTS:
        for split in SPLITS:
            for method in METHODS:
                rows = grouped.get((nodes, split, method), [])
                if not rows:
                    continue
                times = [
                    t
                    for t in (_float(r.get("convergence_time_s")) for r in rows)
                    if t is not None
                ]
                megabytes = [
                    float(r["sync_bytes_total"]) / 1_000_000
                    for r in rows
                    if _float(r.get("sync_bytes_total")) is not None
                ]
                conv_mean, conv_sd = _mean_sd(times)
                mb_mean, mb_sd = _mean_sd(megabytes)
                summary.append(
                    {
                        "nodes": nodes,
                        "split": _split_label(split),
                        "method": method,
                        "runs": len(rows),
                        "converged_runs": len(times),
                        "conv_mean_s": conv_mean,
                        "conv_sd_s": conv_sd,
                        "conv_median_s": statistics.median(times) if times else None,
                        "sync_mean_mb": mb_mean,
                        "sync_sd_mb": mb_sd,
                        "missing_events_total": sum(
                            int(_float(r.get("missing_at_cutoff_count")) or 0)
                            for r in rows
                        ),
                    }
                )

    if not summary:
        print("no completed runs yet")
        return 0

    OUT_DIR.mkdir(parents=True, exist_ok=True)
    csv_path = OUT_DIR / "summary.csv"
    with csv_path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(summary[0]))
        writer.writeheader()
        writer.writerows(summary)

    def fmt(value, digits):
        return "—" if value is None else f"{value:.{digits}f}"

    lines = [
        "# Fig. 3 rerun: partition-heal, mean ± SD",
        "",
        f"Convergence time is the mean over runs that converged within "
        f"{CONVERGENCE_WAIT_SECONDS} s; `conv/runs` counts them.",
        "",
        "| Nodes | Split | Method | conv/runs | Conv. mean ± SD (s) | Median (s) "
        "| Sync mean ± SD (MB) | Missing events |",
        "|---:|---|---|---:|---:|---:|---:|---:|",
    ]
    for item in summary:
        lines.append(
            f"| {item['nodes']} | {item['split']} | {item['method']} "
            f"| {item['converged_runs']}/{item['runs']} "
            f"| {fmt(item['conv_mean_s'], 1)} ± {fmt(item['conv_sd_s'], 1)} "
            f"| {fmt(item['conv_median_s'], 1)} "
            f"| {fmt(item['sync_mean_mb'], 2)} ± {fmt(item['sync_sd_mb'], 2)} "
            f"| {item['missing_events_total']} |"
        )
    md_path = OUT_DIR / "summary.md"
    md_path.write_text("\n".join(lines) + "\n", encoding="utf-8")
    print(csv_path)
    print(md_path)
    return 0


def main() -> int:
    parser = argparse.ArgumentParser(description=__doc__.splitlines()[0])
    sub = parser.add_subparsers(dest="command", required=True)
    sub.add_parser("up").set_defaults(func=cmd_up)
    sub.add_parser("down").set_defaults(func=cmd_down)
    run = sub.add_parser("run")
    run.add_argument(
        "--max-minutes",
        type=float,
        default=0,
        help="Do not start new runs after this many minutes (0 = no limit).",
    )
    run.add_argument("--max-runs", type=int, default=0, help="0 = no limit")
    run.set_defaults(func=cmd_run)
    sub.add_parser("status").set_defaults(func=cmd_status)
    sub.add_parser("summarize").set_defaults(func=cmd_summarize)
    args = parser.parse_args()
    return args.func(args)


if __name__ == "__main__":
    raise SystemExit(main())
