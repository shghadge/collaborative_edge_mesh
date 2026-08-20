#!/usr/bin/env python3
import argparse
import csv
import json
import math
import statistics
from collections import defaultdict
from pathlib import Path

COMMON_FIELDS = (
    "experiment_group",
    "sync_mode",
    "scenario",
    "node_count",
    "partition_split",
    "network_profile",
    "target_offered_rate_events_s",
    "docker_cpus",
    "docker_memory",
    "event_id_retention_limit",
    "delta_log_limit",
    "runs",
    "attempted_events_total",
    "accepted_events_total",
    "rejected_events_total",
    "fully_observed_events_total",
    "missing_events_total",
    "duplicate_submissions_total",
    "converged_runs",
    "semantic_converged_runs",
    "write_success_rate_median",
    "write_latency_p95_ms_median",
    "failed_request_latency_p95_ms_median",
    "accepted_throughput_events_s_median",
    "time_to_last_node_s_median",
    "convergence_time_s_median_converged",
    "recovery_time_s_median",
    "bytes_per_accepted_event_median",
    "sync_bytes_total_median",
    "full_state_repair_bytes_median",
    "delta_repair_bytes_median",
    "delta_pulls_gap_total",
    "cpu_mean_percent_median",
    "cpu_peak_percent_median",
    "memory_mean_mib_median",
    "memory_peak_mib_median",
)


def _number(row: dict[str, str], field: str) -> float | None:
    value = row.get(field)
    if value is None or value in {"", "None"}:
        return None
    if value == "True":
        return 1.0
    if value == "False":
        return 0.0
    return float(value)


def _normalize_timeout(row: dict[str, str]) -> dict[str, str]:
    convergence_time = _number(row, "convergence_time_s")
    wait_seconds = _number(row, "convergence_wait_seconds")
    if (
        row.get("converged_within_timeout") == "True"
        and convergence_time is not None
        and wait_seconds is not None
        and convergence_time > wait_seconds
    ):
        row["_over_budget_convergence_time_s"] = row["convergence_time_s"]
        row["converged_within_timeout"] = "False"
        row["convergence_time_s"] = ""
    return row


def _integer_total(rows: list[dict[str, str]], field: str) -> int:
    return int(sum(_number(row, field) or 0 for row in rows))


def _median(rows: list[dict[str, str]], field: str) -> float | str:
    values = [value for row in rows if (value := _number(row, field)) is not None]
    return round(statistics.median(values), 6) if values else ""


def _quantile(values: list[float], q: float) -> float:
    ordered = sorted(values)
    if len(ordered) == 1:
        return ordered[0]
    position = (len(ordered) - 1) * q
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return ordered[lower]
    weight = position - lower
    return ordered[lower] * (1 - weight) + ordered[upper] * weight


def _experiment_group(case_id: str) -> str:
    return case_id.split("-", 1)[0]


def _group_key(row: dict[str, str]) -> tuple[str, ...]:
    return (
        _experiment_group(row.get("case_id", "unknown")),
        row.get("sync_mode", ""),
        row.get("scenario", ""),
        row.get("node_count", ""),
        row.get("partition_split", ""),
        row.get("network_profile", ""),
        row.get("target_offered_rate_events_s", ""),
        row.get("docker_cpus", ""),
        row.get("docker_memory", ""),
        row.get("event_id_retention_limit", ""),
        row.get("delta_log_limit", ""),
    )


def _summarize(key: tuple[str, ...], rows: list[dict[str, str]]) -> dict:
    converged = [row for row in rows if row.get("converged_within_timeout") == "True"]
    summary: dict[str, object] = dict(zip(COMMON_FIELDS[:11], key))
    summary.update(
        {
            "runs": len(rows),
            "attempted_events_total": _integer_total(rows, "attempted_events"),
            "accepted_events_total": _integer_total(rows, "accepted_events"),
            "rejected_events_total": _integer_total(rows, "rejected_events"),
            "fully_observed_events_total": _integer_total(
                rows, "fully_observed_tracking_ids_count"
            ),
            "missing_events_total": _integer_total(rows, "missing_tracking_ids_count"),
            "duplicate_submissions_total": _integer_total(
                rows, "duplicate_submissions"
            ),
            "converged_runs": len(converged),
            "semantic_converged_runs": sum(
                row.get("semantic_state_converged") == "True" for row in rows
            ),
            "write_success_rate_median": _median(rows, "write_success_rate"),
            "write_latency_p95_ms_median": _median(rows, "write_latency_p95_ms"),
            "failed_request_latency_p95_ms_median": _median(
                rows, "failed_request_latency_p95_ms"
            ),
            "accepted_throughput_events_s_median": _median(
                rows, "accepted_throughput_events_s"
            ),
            "time_to_last_node_s_median": _median(rows, "time_to_last_node_s"),
            "convergence_time_s_median_converged": _median(
                converged, "convergence_time_s"
            ),
            "recovery_time_s_median": _median(rows, "recovery_time_s"),
            "bytes_per_accepted_event_median": _median(
                rows, "bytes_per_accepted_event"
            ),
            "sync_bytes_total_median": _median(rows, "sync_bytes_total"),
            "full_state_repair_bytes_median": _median(rows, "full_state_repair_bytes"),
            "delta_repair_bytes_median": _median(rows, "delta_repair_bytes"),
            "delta_pulls_gap_total": _integer_total(rows, "delta_pulls_gap"),
            "cpu_mean_percent_median": _median(rows, "cpu_mean_percent"),
            "cpu_peak_percent_median": _median(rows, "cpu_peak_percent"),
            "memory_mean_mib_median": _median(rows, "memory_mean_mib"),
            "memory_peak_mib_median": _median(rows, "memory_peak_mib"),
        }
    )
    return summary


def _load_rows(results_dir: Path) -> list[dict[str, str]]:
    rows = []
    for path in sorted(results_dir.glob("*/*/eval-*.csv")):
        with path.open(newline="", encoding="utf-8") as handle:
            rows.extend(_normalize_timeout(row) for row in csv.DictReader(handle))
    return rows


def _write_csv(path: Path, rows: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=COMMON_FIELDS)
        writer.writeheader()
        writer.writerows(rows)


def _overall(rows: list[dict[str, str]]) -> dict:
    convergence_values = [
        value
        for row in rows
        if (value := _number(row, "convergence_time_s")) is not None
    ]
    return {
        "run_count": len(rows),
        "attempted_events_total": _integer_total(rows, "attempted_events"),
        "accepted_events_total": _integer_total(rows, "accepted_events"),
        "rejected_events_total": _integer_total(rows, "rejected_events"),
        "fully_observed_events_total": _integer_total(
            rows, "fully_observed_tracking_ids_count"
        ),
        "missing_events_total": _integer_total(rows, "missing_tracking_ids_count"),
        "duplicate_submissions_total": _integer_total(rows, "duplicate_submissions"),
        "converged_run_count": sum(
            row.get("converged_within_timeout") == "True" for row in rows
        ),
        "over_budget_convergence_reclassified_count": sum(
            "_over_budget_convergence_time_s" in row for row in rows
        ),
        "semantic_converged_run_count": sum(
            row.get("semantic_state_converged") == "True" for row in rows
        ),
        "convergence_time_s_q1": round(_quantile(convergence_values, 0.25), 6)
        if convergence_values
        else None,
        "convergence_time_s_median": round(statistics.median(convergence_values), 6)
        if convergence_values
        else None,
        "convergence_time_s_q3": round(_quantile(convergence_values, 0.75), 6)
        if convergence_values
        else None,
    }


def main() -> int:
    parser = argparse.ArgumentParser(description="Export paper-facing evidence tables.")
    parser.add_argument("results_dir", type=Path)
    parser.add_argument("--out-dir", type=Path)
    args = parser.parse_args()

    rows = _load_rows(args.results_dir)
    if not rows:
        raise SystemExit("no evaluation CSV rows found")
    grouped = defaultdict(list)
    for row in rows:
        grouped[_group_key(row)].append(row)
    summaries = [_summarize(key, group) for key, group in sorted(grouped.items())]

    out_dir = args.out_dir or args.results_dir / "paper_evidence"
    out_dir.mkdir(parents=True, exist_ok=True)
    _write_csv(out_dir / "paper_results_table.csv", summaries)
    by_experiment_group = {
        experiment_group: _overall(
            [
                row
                for row in rows
                if _experiment_group(row.get("case_id", "unknown")) == experiment_group
            ]
        )
        for experiment_group in sorted(
            {_experiment_group(row.get("case_id", "unknown")) for row in rows}
        )
    }
    payload = {
        "overall": _overall(rows),
        "by_experiment_group": by_experiment_group,
        "groups": summaries,
    }
    (out_dir / "paper_evidence.json").write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    print(out_dir / "paper_results_table.csv")
    print(out_dir / "paper_evidence.json")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
