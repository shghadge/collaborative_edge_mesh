#!/usr/bin/env python3
import argparse
import csv
import hashlib
import json
import math
import random
import statistics
from collections import defaultdict
from pathlib import Path

GROUP_FIELDS = (
    "sync_mode",
    "scenario",
    "node_count",
    "partition_split",
    "network_profile",
    "target_offered_rate_events_s",
    "docker_cpus",
    "event_id_retention_limit",
    "delta_log_limit",
    "docker_memory",
)
METRICS = (
    "write_success_rate",
    "accepted_throughput_events_s",
    "write_latency_p50_ms",
    "write_latency_p95_ms",
    "failed_request_latency_p95_ms",
    "convergence_time_s",
    "time_to_first_remote_node_p50_s",
    "time_to_last_node_s",
    "tracking_coverage_percent",
    "missing_at_cutoff_count",
    "recovery_time_s",
    "sync_bytes_total",
    "gossip_summary_sent_bytes",
    "gossip_summary_received_bytes",
    "full_state_repair_bytes",
    "delta_repair_bytes",
    "central_request_bytes",
    "central_response_bytes",
    "bytes_per_accepted_event",
    "semantic_state_bytes",
    "evaluation_metadata_state_bytes",
    "nonsemantic_state_metadata_bytes",
    "cpu_mean_percent",
    "cpu_peak_percent",
    "memory_mean_mib",
    "memory_peak_mib",
    "network_rx_bytes",
    "network_tx_bytes",
    "block_read_bytes",
    "block_write_bytes",
    "duplicate_submissions",
    "converged_within_timeout",
    "semantic_state_converged",
)
PAIRED_METRICS = (
    "write_success_rate",
    "accepted_throughput_events_s",
    "write_latency_p95_ms",
    "convergence_time_s",
    "time_to_last_node_s",
    "sync_bytes_total",
    "bytes_per_accepted_event",
    "cpu_mean_percent",
    "memory_mean_mib",
)
SUMMARY_FIELDS = (
    *GROUP_FIELDS,
    "metric",
    "n",
    "mean",
    "sd",
    "median",
    "q1",
    "q3",
    "ci95_low",
    "ci95_high",
)
PAIR_FIELDS = (
    "comparison",
    "scenario",
    "node_count",
    "partition_split",
    "network_profile",
    "target_offered_rate_events_s",
    "docker_cpus",
    "docker_memory",
    "event_id_retention_limit",
    "delta_log_limit",
    "metric",
    "paired_n",
    "mean_difference",
    "median_difference",
    "ci95_low",
    "ci95_high",
    "permutation_p_two_sided",
    "paired_cohens_dz",
)


def _numeric(row: dict[str, str], field: str) -> float | None:
    raw = row.get(field)
    if raw is None or raw in {"", "None"}:
        return None
    if raw == "True":
        return 1.0
    if raw == "False":
        return 0.0
    try:
        return float(raw)
    except ValueError:
        return None


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


def _bootstrap_mean_ci(values: list[float], label: str) -> tuple[float, float]:
    if len(values) == 1:
        return values[0], values[0]
    seed = int.from_bytes(hashlib.sha256(label.encode()).digest()[:8], "big")
    rng = random.Random(seed)
    size = len(values)
    means = [
        sum(values[rng.randrange(size)] for _ in range(size)) / size
        for _ in range(5000)
    ]
    return _quantile(means, 0.025), _quantile(means, 0.975)


def _describe(values: list[float], label: str) -> dict[str, int | float]:
    ci_low, ci_high = _bootstrap_mean_ci(values, label)
    return {
        "n": len(values),
        "mean": statistics.fmean(values),
        "sd": statistics.stdev(values) if len(values) > 1 else 0.0,
        "median": statistics.median(values),
        "q1": _quantile(values, 0.25),
        "q3": _quantile(values, 0.75),
        "ci95_low": ci_low,
        "ci95_high": ci_high,
    }


def _paired_test(values: list[float]) -> tuple[float, float]:
    observed = abs(statistics.fmean(values))
    permutations = 1 << len(values)
    extreme = 0
    for mask in range(permutations):
        permuted_mean = statistics.fmean(
            value if mask & (1 << index) else -value
            for index, value in enumerate(values)
        )
        if abs(permuted_mean) >= observed - 1e-12:
            extreme += 1
    standard_deviation = statistics.stdev(values) if len(values) > 1 else 0.0
    effect_size = (
        statistics.fmean(values) / standard_deviation if standard_deviation > 0 else 0.0
    )
    return extreme / permutations, effect_size


def _format(value: float | int | str) -> str:
    if isinstance(value, float):
        return f"{value:.6g}"
    return str(value)


def _write_csv(path: Path, fields: tuple[str, ...], rows: list[dict]) -> None:
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fields)
        writer.writeheader()
        writer.writerows(rows)


def _normalize_timeout(row: dict[str, str]) -> dict[str, str]:
    convergence_time = _numeric(row, "convergence_time_s")
    wait_seconds = _numeric(row, "convergence_wait_seconds")
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


def _load_rows(results_dir: Path) -> tuple[list[dict[str, str]], list[Path]]:
    csv_paths = sorted(results_dir.glob("*/*/eval-*.csv"))
    rows = []
    for path in csv_paths:
        with path.open(newline="", encoding="utf-8") as handle:
            rows.extend(_normalize_timeout(row) for row in csv.DictReader(handle))
    return rows, csv_paths


def _audit(matrix: Path, results_dir: Path, rows: list[dict[str, str]]) -> dict:
    with matrix.open(newline="", encoding="utf-8") as handle:
        expected = [
            row["case_id"].strip()
            for row in csv.DictReader(handle)
            if row.get("enabled", "").strip().lower() in {"1", "true", "yes", "on"}
        ]
    manifests = {}
    for path in sorted(results_dir.glob("*/*/eval-*.json")):
        if path.name.endswith("-results.json"):
            continue
        payload = json.loads(path.read_text(encoding="utf-8"))
        case_id = payload.get("case_id")
        if case_id:
            manifests.setdefault(case_id, []).append(str(path))

    result_cases = defaultdict(int)
    for row in rows:
        if case_id := row.get("case_id", "").strip():
            result_cases[case_id] += 1

    expected_set = set(expected)
    completed = expected_set & set(manifests) & set(result_cases)
    return {
        "expected_case_count": len(expected),
        "completed_case_count": len(completed),
        "result_row_count": len(rows),
        "missing_case_ids": sorted(expected_set - set(manifests)),
        "missing_result_case_ids": sorted(expected_set - set(result_cases)),
        "unexpected_case_ids": sorted(set(manifests) - expected_set),
        "unexpected_result_case_ids": sorted(set(result_cases) - expected_set),
        "duplicate_case_ids": sorted(
            case_id for case_id, paths in manifests.items() if len(paths) > 1
        ),
        "duplicate_result_case_ids": sorted(
            case_id for case_id, count in result_cases.items() if count > 1
        ),
        "nonconverged_row_count": sum(
            row.get("converged_within_timeout") != "True" for row in rows
        ),
        "over_budget_convergence_row_count": sum(
            "_over_budget_convergence_time_s" in row for row in rows
        ),
        "over_budget_convergence_case_ids": sorted(
            row["case_id"]
            for row in rows
            if "_over_budget_convergence_time_s" in row
        ),
        "semantic_divergence_row_count": sum(
            row.get("semantic_state_converged") != "True" for row in rows
        ),
    }


def _summaries(rows: list[dict[str, str]]) -> list[dict]:
    grouped = defaultdict(list)
    for row in rows:
        grouped[tuple(row.get(field, "") for field in GROUP_FIELDS)].append(row)

    output = []
    for key, group in sorted(grouped.items()):
        base = dict(zip(GROUP_FIELDS, key))
        for metric in METRICS:
            values = [
                value for row in group if (value := _numeric(row, metric)) is not None
            ]
            if not values:
                continue
            stats = _describe(values, f"{key}:{metric}")
            output.append({**base, "metric": metric, **stats})
    return output


def _paired_summaries(rows: list[dict[str, str]]) -> list[dict]:
    by_case = defaultdict(dict)
    for row in rows:
        key = (
            row.get("scenario", ""),
            row.get("node_count", ""),
            row.get("partition_split", ""),
            row.get("network_profile", ""),
            row.get("target_offered_rate_events_s", ""),
            row.get("docker_cpus", ""),
            row.get("docker_memory", ""),
            row.get("event_id_retention_limit", ""),
            row.get("delta_log_limit", ""),
            row.get("scenario_seed", ""),
        )
        by_case[key][row.get("sync_mode", "")] = row

    comparisons = (
        ("true_delta_minus_full_state", "full_state", "true_delta"),
        ("true_delta_minus_centralized", "centralized", "true_delta"),
    )
    differences = defaultdict(list)
    for key, methods in by_case.items():
        (
            scenario,
            node_count,
            split,
            profile,
            rate,
            docker_cpus,
            docker_memory,
            event_retention,
            delta_log,
            _seed,
        ) = key
        for label, reference, candidate in comparisons:
            if reference not in methods or candidate not in methods:
                continue
            for metric in PAIRED_METRICS:
                reference_value = _numeric(methods[reference], metric)
                candidate_value = _numeric(methods[candidate], metric)
                if reference_value is None or candidate_value is None:
                    continue
                group_key = (
                    label,
                    scenario,
                    node_count,
                    split,
                    profile,
                    rate,
                    docker_cpus,
                    docker_memory,
                    event_retention,
                    delta_log,
                    metric,
                )
                differences[group_key].append(candidate_value - reference_value)

    output = []
    for key, values in sorted(differences.items()):
        (
            label,
            scenario,
            node_count,
            split,
            profile,
            rate,
            docker_cpus,
            docker_memory,
            event_retention,
            delta_log,
            metric,
        ) = key
        stats = _describe(values, f"paired:{key}")
        permutation_p, effect_size = _paired_test(values)
        output.append(
            {
                "comparison": label,
                "scenario": scenario,
                "node_count": node_count,
                "partition_split": split,
                "network_profile": profile,
                "target_offered_rate_events_s": rate,
                "docker_cpus": docker_cpus,
                "docker_memory": docker_memory,
                "event_id_retention_limit": event_retention,
                "delta_log_limit": delta_log,
                "metric": metric,
                "paired_n": stats["n"],
                "mean_difference": stats["mean"],
                "median_difference": stats["median"],
                "ci95_low": stats["ci95_low"],
                "ci95_high": stats["ci95_high"],
                "permutation_p_two_sided": permutation_p,
                "paired_cohens_dz": effect_size,
            }
        )
    return output


def _lookup(summary_rows: list[dict], metric: str) -> dict[tuple[str, ...], dict]:
    return {
        tuple(str(row[field]) for field in GROUP_FIELDS): row
        for row in summary_rows
        if row["metric"] == metric
    }


def _median_iqr(row: dict | None) -> str:
    if row is None:
        return "—"
    return f"{row['median']:.3g} [{row['q1']:.3g}, {row['q3']:.3g}]"


def _write_markdown(path: Path, audit: dict, summary_rows: list[dict]) -> None:
    success = _lookup(summary_rows, "write_success_rate")
    latency = _lookup(summary_rows, "write_latency_p95_ms")
    convergence = _lookup(summary_rows, "convergence_time_s")
    bytes_per_event = _lookup(summary_rows, "bytes_per_accepted_event")
    cpu = _lookup(summary_rows, "cpu_mean_percent")
    memory = _lookup(summary_rows, "memory_mean_mib")
    keys = sorted(success)

    lines = [
        "# Reviewer Experiment Summary",
        "",
        f"Completed cases: {audit['completed_case_count']}/{audit['expected_case_count']}",
        f"Result rows: {audit['result_row_count']}",
        f"Non-converged rows: {audit['nonconverged_row_count']}",
        f"Semantic-divergence rows: {audit['semantic_divergence_row_count']}",
        "",
        "Values below are median [Q1, Q3] across independent seeded runs.",
        "",
        "| Method | Scenario | Nodes | Split | Network | Target rate | Success | p95 write ms | Convergence s | Bytes/event | CPU % | Memory MiB |",
        "|---|---|---:|---|---|---:|---:|---:|---:|---:|---:|---:|",
    ]
    for key in keys:
        (
            method,
            scenario,
            nodes,
            split,
            profile,
            rate,
            _cpus,
            _event_retention,
            _delta_log,
            _memory,
        ) = key
        lines.append(
            "| "
            + " | ".join(
                [
                    method,
                    scenario,
                    nodes,
                    split or "—",
                    profile,
                    rate or "—",
                    _median_iqr(success.get(key)),
                    _median_iqr(latency.get(key)),
                    _median_iqr(convergence.get(key)),
                    _median_iqr(bytes_per_event.get(key)),
                    _median_iqr(cpu.get(key)),
                    _median_iqr(memory.get(key)),
                ]
            )
            + " |"
        )
    path.write_text("\n".join(lines) + "\n", encoding="utf-8")


def main() -> int:
    parser = argparse.ArgumentParser(description="Summarize reviewer experiment runs.")
    parser.add_argument("results_dir", type=Path)
    parser.add_argument("--matrix", type=Path, required=True)
    parser.add_argument("--out-dir", type=Path)
    args = parser.parse_args()

    out_dir = args.out_dir or args.results_dir / "summary"
    out_dir.mkdir(parents=True, exist_ok=True)
    rows, csv_paths = _load_rows(args.results_dir)
    if not rows:
        raise SystemExit("no evaluation CSV rows found")

    audit = _audit(args.matrix, args.results_dir, rows)
    audit["csv_files"] = [str(path) for path in csv_paths]
    summary_rows = _summaries(rows)
    paired_rows = _paired_summaries(rows)

    _write_csv(out_dir / "summary_statistics.csv", SUMMARY_FIELDS, summary_rows)
    _write_csv(out_dir / "paired_comparisons.csv", PAIR_FIELDS, paired_rows)
    (out_dir / "run_audit.json").write_text(
        json.dumps(audit, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )
    _write_markdown(out_dir / "summary.md", audit, summary_rows)

    print(out_dir / "run_audit.json")
    print(out_dir / "summary_statistics.csv")
    print(out_dir / "paired_comparisons.csv")
    print(out_dir / "summary.md")
    incomplete = (
        audit["missing_case_ids"]
        or audit["missing_result_case_ids"]
        or audit["duplicate_case_ids"]
        or audit["duplicate_result_case_ids"]
    )
    return 2 if incomplete else 0


if __name__ == "__main__":
    raise SystemExit(main())
