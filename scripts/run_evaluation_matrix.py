#!/usr/bin/env python3
import argparse
import json
import sys
import time
from datetime import datetime, timezone
from pathlib import Path
from urllib.error import HTTPError, URLError
from urllib.request import Request, urlopen


def _csv_list_int(value: str) -> list[int]:
    return [int(part.strip()) for part in value.split(",") if part.strip()]


def _csv_list_float(value: str) -> list[float]:
    return [float(part.strip()) for part in value.split(",") if part.strip()]


def _bool_arg(value: str) -> bool:
    normalized = value.strip().lower()
    if normalized == "true":
        return True
    if normalized == "false":
        return False
    raise argparse.ArgumentTypeError("expected true or false")


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


def _utc_now() -> str:
    return datetime.now(timezone.utc).isoformat()


def main() -> int:
    parser = argparse.ArgumentParser(
        description="Run an evaluation matrix and save CSV output."
    )
    parser.add_argument("--base-url", default="http://localhost:8000")
    parser.add_argument("--implementation-label", required=True)
    parser.add_argument("--sync-mode", required=True)
    parser.add_argument("--state-growth-fix", choices=["true", "false"], required=True)
    parser.add_argument("--node-counts", default="5,10,15")
    parser.add_argument("--events-per-node", type=int, default=15)
    parser.add_argument("--partition-splits", default="0.5,0.7,0.8")
    parser.add_argument("--convergence-wait-seconds", type=float, default=25)
    parser.add_argument("--partition-wait-seconds", type=float, default=8)
    parser.add_argument("--poll-interval-seconds", type=float, default=2)
    parser.add_argument("--network-profile", default="hard_partition")
    parser.add_argument(
        "--dashboard-observability", default="light", choices=["off", "light", "full"]
    )
    parser.add_argument("--out", default="results/evaluation")
    args = parser.parse_args()

    base_url = args.base_url.rstrip("/")
    node_counts = _csv_list_int(args.node_counts)
    partition_splits = _csv_list_float(args.partition_splits)
    state_growth_fix = _bool_arg(args.state_growth_fix)
    payload = {
        "node_counts": node_counts,
        "events_per_node": args.events_per_node,
        "partition_splits": partition_splits,
        "convergence_wait_seconds": args.convergence_wait_seconds,
        "partition_wait_seconds": args.partition_wait_seconds,
        "poll_interval_seconds": args.poll_interval_seconds,
        "implementation_label": args.implementation_label,
        "sync_mode": args.sync_mode,
        "state_growth_fix": state_growth_fix,
        "network_profile": args.network_profile,
        "dashboard_observability": args.dashboard_observability,
    }

    started_at = _utc_now()
    try:
        started = _json_request("POST", f"{base_url}/evaluation/run", payload=payload)
        run_id = started.get("run_id")
        while True:
            status = _json_request("GET", f"{base_url}/evaluation/status")
            run_id = run_id or status.get("run_id")
            if status.get("error"):
                print(f"evaluation failed: {status['error']}", file=sys.stderr)
                return 1
            if not status.get("running"):
                break
            time.sleep(args.poll_interval_seconds)
        csv_text = _text_request(f"{base_url}/evaluation/download")
    except (HTTPError, URLError, TimeoutError, OSError) as error:
        print(f"evaluation request failed: {error}", file=sys.stderr)
        return 1

    finished_at = _utc_now()
    if not run_id:
        run_id = f"eval-{int(time.time())}"

    out_dir = Path(args.out) / args.implementation_label
    out_dir.mkdir(parents=True, exist_ok=True)
    csv_path = out_dir / f"{run_id}.csv"
    json_path = out_dir / f"{run_id}.json"
    csv_path.write_text(csv_text, encoding="utf-8")

    manifest = {
        "run_id": run_id,
        "implementation_label": args.implementation_label,
        "sync_mode": args.sync_mode,
        "state_growth_fix": state_growth_fix,
        "node_counts": node_counts,
        "events_per_node": args.events_per_node,
        "partition_splits": partition_splits,
        "convergence_wait_seconds": args.convergence_wait_seconds,
        "partition_wait_seconds": args.partition_wait_seconds,
        "poll_interval_seconds": args.poll_interval_seconds,
        "network_profile": args.network_profile,
        "started_at": started_at,
        "finished_at": finished_at,
        "csv_path": str(csv_path),
    }
    json_path.write_text(json.dumps(manifest, indent=2) + "\n", encoding="utf-8")
    print(csv_path)
    print(json_path)
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
