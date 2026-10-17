#!/usr/bin/env python3
import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen

sys.path.insert(0, str(Path(__file__).resolve().parents[1]))

from src.evaluation.runner import NETWORK_PROFILES, SYNC_MODES
from src.evaluation.resources import DEFAULT_RESOURCE_PROFILE, RESOURCE_PROFILES
from src.services.docker_manager import NETWORK_PROFILE_SETTINGS


def _csv_list_int(value: str) -> list[int]:
    return [int(part.strip()) for part in value.split(",") if part.strip()]


def _csv_list_float(value: str) -> list[float]:
    return [float(part.strip()) for part in value.split(",") if part.strip()]


def _csv_list_str(value: str) -> list[str]:
    return [part.strip() for part in value.split(",") if part.strip()]


def _manifest_matches(manifest: dict, expected: dict) -> bool:
    return all(manifest.get(key) == value for key, value in expected.items())


def _completed_run(out_dir: Path, expected: dict) -> tuple[Path, Path] | None:
    if not out_dir.exists():
        return None
    for manifest_path in sorted(out_dir.glob("*.json"), reverse=True):
        try:
            manifest = json.loads(manifest_path.read_text(encoding="utf-8"))
        except (OSError, json.JSONDecodeError):
            continue
        if not _manifest_matches(manifest, expected):
            continue
        csv_path = Path(manifest.get("csv_path", ""))
        if not csv_path.is_absolute():
            csv_path = Path.cwd() / csv_path
        if csv_path.exists():
            return csv_path, manifest_path
    return None


def _json_request(
    method: str, url: str, payload: dict | None = None, timeout: int = 30
):
    data = None
    headers = {}
    if payload is not None:
        data = json.dumps(payload).encode("utf-8")
        headers["Content-Type"] = "application/json"
    request = Request(url, data=data, headers=headers, method=method)
    with urlopen(request, timeout=timeout) as response:
        raw = response.read().decode("utf-8")
        return json.loads(raw) if raw else {}


def _text_request(url: str, timeout: int = 30) -> str:
    with urlopen(url, timeout=timeout) as response:
        return response.read().decode("utf-8")


def _status_line(status: dict) -> str:
    completed = status.get("completed_steps", status.get("progress", 0))
    total = status.get("total_steps", 0)
    if not total:
        return status.get("current_step") or "Starting evaluation"

    remaining = status.get("steps_remaining")
    current = status.get("current_step") or "Starting"
    repetition = status.get("current_repetition")
    total_repetitions = status.get("total_repetitions")
    repetitions_remaining = status.get("repetitions_remaining")
    parts = [f"{completed}/{total} scenario runs"]
    if remaining is not None:
        parts.append(f"{remaining} remaining")
    if repetition and total_repetitions:
        parts.append(
            f"rep {repetition}/{total_repetitions} "
            f"({repetitions_remaining} reps after current)"
        )
    parts.append(current)
    return " | ".join(parts)


def _wait_for_api(base_url: str, timeout_seconds: float = 30.0) -> bool:
    deadline = time.time() + timeout_seconds
    while time.time() < deadline:
        try:
            _json_request("GET", f"{base_url}/evaluation/status", timeout=2)
            return True
        except (HTTPError, URLError, TimeoutError, OSError):
            time.sleep(0.5)
    return False


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run an evaluation matrix and save CSV output."
    )
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument(
        "--sync-mode",
        choices=sorted(SYNC_MODES),
        default="true_delta",
    )
    parser.add_argument("--event-id-retention-limit", type=int, default=100)
    parser.add_argument("--delta-log-limit", type=int, default=1000)
    parser.add_argument("--node-counts", default="5,10,15")
    parser.add_argument("--events-per-node", type=int, default=15)
    parser.add_argument("--repetitions", type=int, default=1)
    parser.add_argument("--scenario-seed", type=int)
    parser.add_argument("--partition-splits", default="0.5,0.7,0.8")
    parser.add_argument("--scenarios", default="baseline,partition_heal,high_load")
    parser.add_argument("--convergence-wait-seconds", type=float, default=25)
    parser.add_argument("--partition-wait-seconds", type=float, default=8)
    parser.add_argument("--partition-events-per-node", type=int, default=3)
    parser.add_argument("--poll-interval-seconds", type=float, default=2)
    parser.add_argument("--event-concurrency", type=int, default=25)
    parser.add_argument("--offered-rate-per-second", type=float)
    parser.add_argument(
        "--network-profile",
        choices=sorted(NETWORK_PROFILES),
        default="clean_lan",
    )
    parser.add_argument("--docker-cpus", type=float)
    parser.add_argument("--docker-memory")
    parser.add_argument(
        "--resource-mode",
        choices=["uniform", "heterogeneous"],
        default="uniform",
    )
    parser.add_argument(
        "--resource-profile",
        choices=sorted(RESOURCE_PROFILES),
    )
    parser.add_argument(
        "--cold-topology",
        action="store_true",
        help="Recreate containers for every repetition instead of reusing topology.",
    )
    parser.add_argument("--node-readiness-timeout-seconds", type=float, default=30.0)
    parser.add_argument("--case-id")
    parser.add_argument("--condition-id")
    parser.add_argument("--sweep")
    parser.add_argument(
        "--resume",
        action="store_true",
        help="Skip the run if an exact completed manifest already exists.",
    )
    parser.add_argument(
        "--force",
        action="store_true",
        help="Run even when --resume finds a completed matching manifest.",
    )
    parser.add_argument("--out", default="results/evaluation")
    args = parser.parse_args()

    base_url = args.base_url.rstrip("/")
    node_counts = _csv_list_int(args.node_counts)
    partition_splits = _csv_list_float(args.partition_splits)
    scenarios = _csv_list_str(args.scenarios)
    warm_topology = not args.cold_topology
    effective_resource_profile = (
        args.resource_profile or DEFAULT_RESOURCE_PROFILE
        if args.resource_mode == "heterogeneous"
        else None
    )
    effective_event_retention = (
        0 if args.sync_mode == "full_state" else args.event_id_retention_limit
    )
    implementation_label = (
        f"{args.sync_mode}_retention_{effective_event_retention}_{args.network_profile}"
    )
    if args.delta_log_limit != 1000:
        delta_label = (
            "unbounded_delta_log"
            if args.delta_log_limit <= 0
            else f"delta_log_{args.delta_log_limit}"
        )
        implementation_label = f"{implementation_label}_{delta_label}"
    if args.resource_mode == "heterogeneous":
        implementation_label += f"_{effective_resource_profile}"
    if args.docker_cpus:
        implementation_label += (
            f"_cpu_{args.docker_cpus:g}_mem_{args.docker_memory or 'default'}"
        )
    elif args.docker_memory:
        implementation_label += f"_mem_{args.docker_memory}"

    expected_manifest = {
        "case_id": args.case_id,
        "condition_id": args.condition_id,
        "sweep": args.sweep,
        "implementation_label": implementation_label,
        "sync_mode": args.sync_mode,
        "event_id_retention_limit": effective_event_retention,
        "delta_log_limit": args.delta_log_limit,
        "node_counts": node_counts,
        "events_per_node": args.events_per_node,
        "partition_splits": partition_splits,
        "repetitions": args.repetitions,
        "scenario_seed": args.scenario_seed,
        "scenarios": scenarios,
        "convergence_wait_seconds": args.convergence_wait_seconds,
        "partition_wait_seconds": args.partition_wait_seconds,
        "poll_interval_seconds": args.poll_interval_seconds,
        "partition_events_per_node": args.partition_events_per_node,
        "event_concurrency": args.event_concurrency,
        "network_profile": args.network_profile,
        "network_settings": NETWORK_PROFILE_SETTINGS.get(args.network_profile),
        "offered_rate_per_second": args.offered_rate_per_second,
        "docker_cpus": args.docker_cpus,
        "docker_memory": args.docker_memory,
        "resource_mode": args.resource_mode,
        "resource_profile": effective_resource_profile,
        "warm_topology": warm_topology,
        "node_readiness_timeout_seconds": args.node_readiness_timeout_seconds,
    }
    out_dir = Path(args.out) / implementation_label
    if args.resume and not args.force:
        completed = _completed_run(out_dir, expected_manifest)
        if completed is not None:
            csv_path, json_path = completed
            print(csv_path)
            print(json_path)
            return 0

    if not _wait_for_api(base_url):
        print("evaluation API did not become ready", file=sys.stderr)
        return 1

    payload = {
        "case_id": args.case_id,
        "condition_id": args.condition_id,
        "sweep": args.sweep,
        "node_counts": node_counts,
        "events_per_node": args.events_per_node,
        "partition_splits": partition_splits,
        "repetitions": args.repetitions,
        "scenario_seed": args.scenario_seed,
        "scenarios": scenarios,
        "convergence_wait_seconds": args.convergence_wait_seconds,
        "partition_wait_seconds": args.partition_wait_seconds,
        "partition_events_per_node": args.partition_events_per_node,
        "poll_interval_seconds": args.poll_interval_seconds,
        "event_concurrency": args.event_concurrency,
        "sync_mode": args.sync_mode,
        "offered_rate_per_second": args.offered_rate_per_second,
        "event_id_retention_limit": args.event_id_retention_limit,
        "delta_log_limit": args.delta_log_limit,
        "network_profile": args.network_profile,
        "docker_cpus": args.docker_cpus,
        "docker_memory": args.docker_memory,
        "resource_mode": args.resource_mode,
        "resource_profile": effective_resource_profile,
        "warm_topology": warm_topology,
        "node_readiness_timeout_seconds": args.node_readiness_timeout_seconds,
    }

    started_at = _utc_now()
    try:
        started = _json_request("POST", f"{base_url}/evaluation/run", payload=payload)
        run_id = started.get("run_id")
        last_status_line = None
        while True:
            status = _json_request("GET", f"{base_url}/evaluation/status", timeout=600)
            run_id = run_id or status.get("run_id")
            if status.get("error"):
                print(f"evaluation failed: {status['error']}", file=sys.stderr)
                return 1
            if status.get("running"):
                status_line = _status_line(status)
                if status_line != last_status_line:
                    print(status_line, flush=True)
                    last_status_line = status_line
            if not status.get("running"):
                break
            time.sleep(args.poll_interval_seconds)
        csv_text = _text_request(f"{base_url}/evaluation/download")
        results = _json_request("GET", f"{base_url}/evaluation/results")
    except (HTTPError, URLError, TimeoutError, OSError) as error:
        print(f"evaluation request failed: {error}", file=sys.stderr)
        return 1

    finished_at = _utc_now()
    if not run_id:
        run_id = f"eval-{int(time.time())}"

    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"{run_id}.csv"
    json_path = out_dir / f"{run_id}.json"
    results_path = out_dir / f"{run_id}-results.json"
    csv_path.write_text(csv_text, encoding="utf-8")
    results_path.write_text(
        json.dumps(results, indent=2, sort_keys=True) + "\n",
        encoding="utf-8",
    )
    result_rows = results.get("results", [])
    resource_assignments = [
        {
            "repetition": row.get("repetition"),
            "node_count": row.get("node_count"),
            "assignments": json.loads(row.get("node_resource_assignments") or "[]"),
            "resource_limit_verified": row.get("resource_limit_verified"),
        }
        for row in result_rows
    ]

    manifest = {
        "run_id": run_id,
        **expected_manifest,
        "started_at": started_at,
        "finished_at": finished_at,
        "csv_path": str(csv_path),
        "results_path": str(results_path),
        "resource_assignments": resource_assignments,
    }
    json_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(csv_path)
    print(json_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
