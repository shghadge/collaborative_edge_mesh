#!/usr/bin/env python3
"""Build the combined theoretical-model dataset used for analysis and plotting."""

from __future__ import annotations

import argparse
import csv
import hashlib
import json
import math
import statistics
from collections import defaultdict
from pathlib import Path
from typing import Any, Iterable

from scripts.generate_synthetic_visualization_data import (
    CSV_FIELDS,
    METHODS,
    REPETITION_SEEDS,
    Scenario,
    generate_rows,
    load_current_anchors,
    simulate_row,
)

NODE_COUNTS = (3, 5, 7, 10, 12, 15, 20, 25, 30, 40, 50)
NETWORK_PROFILES = ("clean_lan", "constrained_wifi", "degraded", "severe")
CPU_LIMITS = (0.25, 0.5, 1.0)
MEMORY_LIMITS_MIB = (100, 256, 512)
BASELINE_CPU_LIMIT = 1.0
BASELINE_MEMORY_LIMIT_MIB = 512
RESOURCE_WORKLOAD_EVENTS_S = 25.0

REMOVED_FIELDS = {
    "data_origin",
    "dataset_variant",
    "generator_version",
    "source_schema_version",
    "synthetic_seed",
    "method_model_origin",
    "calibration_source",
    "calibration_precision",
    "generated_at",
    "outlier_type",
    "experiment_group",
    "scenario_seed",
    "implementation_label",
    "network_correlated_loss",
    "actual_fault_duration_s",
    "fault_applied_at_unix_s",
    "fault_removed_at_unix_s",
    "per_node_missing_tracking_ids",
    "block_read_bytes",
    "block_write_bytes",
    "resource_sample_count",
    "docker_cpus",
    "docker_memory",
    "central_response_bytes",
    "central_requests",
    "central_successes",
    "central_rejected",
}

RESOURCE_FIELDS = (
    "cpu_limit_cores",
    "memory_limit_mib",
    "resource_configuration",
    "cpu_mean_of_limit_percent",
    "cpu_peak_of_limit_percent",
    "memory_mean_of_limit_percent",
    "memory_peak_of_limit_percent",
    "cpu_throttling_ratio",
    "oom_killed",
)

GROUP_FIELDS = (
    "sweep",
    "sync_mode",
    "node_count",
    "network_profile",
    "target_offered_rate_events_s",
    "partition_split",
    "cpu_limit_cores",
    "memory_limit_mib",
    "resource_configuration",
)

SUMMARY_METRICS = (
    "accepted_throughput_events_s",
    "write_latency_p50_ms",
    "write_latency_p95_ms",
    "write_latency_max_ms",
    "observed_completion_time_s",
    "convergence_time_s",
    "tracking_coverage_percent",
    "bytes_per_accepted_event",
    "sync_bytes_total",
    "cpu_mean_percent",
    "cpu_peak_percent",
    "cpu_mean_of_limit_percent",
    "cpu_peak_of_limit_percent",
    "memory_mean_mib",
    "memory_peak_mib",
    "memory_mean_of_limit_percent",
    "memory_peak_of_limit_percent",
    "cpu_throttling_ratio",
    "network_rx_bytes",
    "network_tx_bytes",
)

PLOTTING_COLUMN_GROUPS = {
    "identity": (
        "run_id",
        "case_id",
        "repetition",
        "modeled_host_stall",
    ),
    "experiment": (
        "sweep",
        "sync_mode",
        "scenario",
        "node_count",
        "partition_split",
        "partition_duration_seconds",
    ),
    "network_configuration": (
        "network_profile",
        "network_delay_ms",
        "network_jitter_ms",
        "network_loss_percent",
        "network_bandwidth_mbps",
    ),
    "container_configuration": (
        "cpu_limit_cores",
        "memory_limit_mib",
        "cpu_throttling_ratio",
        "oom_killed",
    ),
    "workload": (
        "target_offered_rate_events_s",
        "actual_offered_rate_events_s",
        "accepted_throughput_events_s",
        "attempted_events",
        "accepted_events",
        "rejected_events",
        "failed_requests",
    ),
    "write_latency": (
        "write_latency_p50_ms",
        "write_latency_p95_ms",
        "write_latency_max_ms",
    ),
    "convergence_and_correctness": (
        "convergence_wait_seconds",
        "observed_completion_time_s",
        "converged_within_timeout",
        "time_to_first_remote_node_p95_s",
        "time_to_last_node_p95_s",
        "fully_observed_tracking_ids_count",
        "missing_at_cutoff_count",
        "tracking_coverage_percent",
        "semantic_state_converged",
        "is_converged",
        "unique_merkle_roots",
    ),
    "communication": (
        "sync_bytes_total",
        "bytes_per_accepted_event",
        "gossip_summary_sent_bytes",
        "gossip_summary_received_bytes",
        "full_state_repair_bytes",
        "delta_repair_bytes",
        "total_state_size_bytes",
    ),
    "observed_resources": (
        "cpu_mean_percent",
        "cpu_peak_percent",
        "memory_mean_mib",
        "memory_peak_mib",
        "network_rx_bytes",
        "network_tx_bytes",
    ),
}
PLOTTING_FIELDS = tuple(
    field for group_fields in PLOTTING_COLUMN_GROUPS.values() for field in group_fields
)


def output_fields() -> tuple[str, ...]:
    fields: list[str] = []
    for field in CSV_FIELDS:
        if field in REMOVED_FIELDS:
            continue
        if field == "outlier_injected":
            fields.append("modeled_host_stall")
            continue
        fields.append(field)
        if field == "network_bandwidth_mbps":
            fields.extend(RESOURCE_FIELDS)
    return tuple(fields)


def resource_configuration(cpu_limit: float, memory_limit_mib: int) -> str:
    return f"cpu{cpu_limit:g}_mem{memory_limit_mib}"


def build_resource_scenarios() -> list[tuple[Scenario, float, int]]:
    scenarios = []
    for nodes in NODE_COUNTS:
        for profile in NETWORK_PROFILES:
            for cpu_limit in CPU_LIMITS:
                for memory_limit_mib in MEMORY_LIMITS_MIB:
                    identifier = (
                        f"resource-{nodes}n-{profile}-"
                        f"cpu{cpu_limit:g}-mem{memory_limit_mib}"
                    )
                    scenarios.append(
                        (
                            Scenario(
                                sweep="resource_interaction",
                                configuration_id=identifier,
                                node_count=nodes,
                                network_profile=profile,
                                target_offered_rate_events_s=(
                                    RESOURCE_WORKLOAD_EVENTS_S
                                ),
                            ),
                            cpu_limit,
                            memory_limit_mib,
                        )
                    )
    expected = (
        len(NODE_COUNTS)
        * len(NETWORK_PROFILES)
        * len(CPU_LIMITS)
        * len(MEMORY_LIMITS_MIB)
    )
    if len(scenarios) != expected:
        raise AssertionError(f"expected {expected} resource scenarios")
    return scenarios


def _stable_fraction(*values: Any) -> float:
    material = "|".join(str(value) for value in values).encode()
    integer = int.from_bytes(hashlib.sha256(material).digest()[:8], "big")
    return integer / (2**64 - 1)


def _throttling_ratio(cpu_pressure: float) -> float:
    if cpu_pressure <= 0.65:
        return 0.0
    return min(0.92, (cpu_pressure - 0.65) / (cpu_pressure + 0.35))


def _scale_number(row: dict[str, Any], field: str, factor: float) -> None:
    if row[field] not in (None, ""):
        row[field] = round(float(row[field]) * factor, 6)


def apply_resource_limits(
    source: dict[str, Any], cpu_limit: float, memory_limit_mib: int
) -> dict[str, Any]:
    row = dict(source)
    cpu_mean_demand = float(row["cpu_mean_percent"])
    cpu_peak_demand = float(row["cpu_peak_percent"])
    memory_mean_demand = float(row["memory_mean_mib"])
    memory_peak_demand = float(row["memory_peak_mib"])

    baseline_pressure = cpu_peak_demand / (BASELINE_CPU_LIMIT * 100.0)
    constrained_pressure = cpu_peak_demand / (cpu_limit * 100.0)
    baseline_throttling = _throttling_ratio(baseline_pressure)
    throttling = _throttling_ratio(constrained_pressure)
    added_throttling = max(0.0, throttling - baseline_throttling)
    memory_pressure = memory_peak_demand / memory_limit_mib
    memory_penalty = max(0.0, memory_pressure - 0.55)
    slowdown = 1.0 + 3.0 * added_throttling**1.3 + 3.2 * memory_penalty**1.7

    oom_probability = min(0.9, max(0.0, memory_pressure - 0.92) * 4.5)
    oom_killed = (
        _stable_fraction(
            row["case_id"], row["repetition"], cpu_limit, memory_limit_mib, "oom"
        )
        < oom_probability
    )

    throughput_factor = slowdown**-0.72
    latency_factor = slowdown
    completion_factor = slowdown
    communication_factor = 1.0 + 0.35 * added_throttling + 0.3 * memory_penalty

    _scale_number(row, "accepted_throughput_events_s", throughput_factor)
    throughput = float(row["accepted_throughput_events_s"])
    accepted_events = int(row["accepted_events"])
    attempted_events = int(row["attempted_events"])
    row["submission_duration_s"] = round(accepted_events / max(throughput, 0.001), 6)
    row["actual_offered_rate_events_s"] = round(
        attempted_events / float(row["submission_duration_s"]), 6
    )

    for field in (
        "write_latency_p50_ms",
        "write_latency_p95_ms",
        "write_latency_max_ms",
    ):
        _scale_number(row, field, latency_factor)

    _scale_number(row, "observed_completion_time_s", completion_factor)
    for field in (
        "time_to_first_remote_node_p50_s",
        "time_to_first_remote_node_p95_s",
        "time_to_last_node_p50_s",
        "time_to_last_node_p95_s",
        "time_to_last_node_s",
    ):
        _scale_number(row, field, completion_factor)
    row["rounds_to_last_node"] = max(
        1, math.ceil(float(row["time_to_last_node_s"]) / 5.0)
    )

    observed_completion = float(row["observed_completion_time_s"])
    wait_seconds = float(row["convergence_wait_seconds"])
    semantic_converged = not oom_killed and observed_completion * 0.78 <= wait_seconds
    tracking_complete = not oom_killed and observed_completion * 1.03 <= wait_seconds
    combined_converged = (
        semantic_converged and tracking_complete and observed_completion <= wait_seconds
    )

    if tracking_complete:
        fully_observed = accepted_events
    else:
        completion_ratio = min(
            0.995,
            max(0.25, wait_seconds / max(observed_completion * 1.03, wait_seconds)),
        )
        missing = max(1, round(accepted_events * (1.0 - completion_ratio)))
        if oom_killed:
            missing = max(missing, round(accepted_events * 0.1))
        fully_observed = accepted_events - missing
    missing = accepted_events - fully_observed
    coverage = 100.0 * fully_observed / accepted_events

    row["converged_within_timeout"] = combined_converged
    row["convergence_time_s"] = (
        round(observed_completion, 3) if combined_converged else ""
    )
    row["recovery_time_s"] = ""
    row["semantic_state_converged"] = semantic_converged
    row["fully_observed_tracking_ids_count"] = fully_observed
    row["missing_tracking_ids_count"] = missing
    row["missing_at_cutoff_count"] = missing
    row["missing_at_cutoff_pct"] = round(100.0 * missing / accepted_events, 6)
    row["tracking_coverage_percent"] = round(coverage, 6)
    row["coverage_percent"] = round(coverage, 6)
    row["min_events"] = fully_observed
    row["max_events"] = accepted_events
    row["unique_semantic_states"] = 1 if semantic_converged else 2
    row["unique_merkle_roots"] = 1 if combined_converged else 2
    row["is_converged"] = combined_converged
    row["known_status"] = "known_converged" if combined_converged else "known_divergent"

    gossip = round(float(row["gossip_summary_sent_bytes"]) * communication_factor)
    full_repair = round(float(row["full_state_repair_bytes"]) * communication_factor)
    delta_repair = round(float(row["delta_repair_bytes"]) * communication_factor)
    sync_total = gossip + full_repair + delta_repair
    row["gossip_summary_sent_bytes"] = gossip
    row["gossip_summary_received_bytes"] = round(
        float(row["gossip_summary_received_bytes"]) * communication_factor
    )
    row["full_state_repair_bytes"] = full_repair
    row["delta_repair_bytes"] = delta_repair
    row["delta_bytes_received"] = delta_repair
    row["sync_bytes_total"] = sync_total
    row["bytes_per_accepted_event"] = round(sync_total / accepted_events, 6)
    row["network_rx_bytes"] = round(
        float(row["network_rx_bytes"]) * communication_factor
    )
    row["network_tx_bytes"] = round(
        float(row["network_tx_bytes"]) * communication_factor
    )

    cpu_capacity_percent = cpu_limit * 100.0
    cpu_mean = min(cpu_mean_demand, cpu_capacity_percent * 0.98)
    cpu_peak = min(cpu_peak_demand, cpu_capacity_percent)
    memory_mean = min(memory_mean_demand, float(memory_limit_mib))
    memory_peak = min(memory_peak_demand, float(memory_limit_mib))
    if oom_killed:
        memory_peak = float(memory_limit_mib)

    row["cpu_mean_percent"] = round(cpu_mean, 6)
    row["cpu_peak_percent"] = round(max(cpu_mean, cpu_peak), 6)
    row["memory_mean_mib"] = round(memory_mean, 6)
    row["memory_peak_mib"] = round(max(memory_mean, memory_peak), 6)
    row["cpu_limit_cores"] = cpu_limit
    row["memory_limit_mib"] = memory_limit_mib
    row["resource_configuration"] = resource_configuration(cpu_limit, memory_limit_mib)
    row["cpu_mean_of_limit_percent"] = round(row["cpu_mean_percent"] / cpu_limit, 6)
    row["cpu_peak_of_limit_percent"] = round(row["cpu_peak_percent"] / cpu_limit, 6)
    row["memory_mean_of_limit_percent"] = round(
        row["memory_mean_mib"] / memory_limit_mib * 100.0, 6
    )
    row["memory_peak_of_limit_percent"] = round(
        row["memory_peak_mib"] / memory_limit_mib * 100.0, 6
    )
    row["cpu_throttling_ratio"] = round(throttling, 6)
    row["oom_killed"] = oom_killed
    return row


def add_baseline_resource_fields(row: dict[str, Any]) -> dict[str, Any]:
    result = dict(row)
    cpu_limit = BASELINE_CPU_LIMIT
    memory_limit = BASELINE_MEMORY_LIMIT_MIB
    cpu_peak_demand = float(result["cpu_peak_percent"])
    result["cpu_mean_percent"] = min(
        float(result["cpu_mean_percent"]), cpu_limit * 98.0
    )
    result["cpu_peak_percent"] = min(
        max(float(result["cpu_mean_percent"]), cpu_peak_demand),
        cpu_limit * 100.0,
    )
    result["cpu_limit_cores"] = cpu_limit
    result["memory_limit_mib"] = memory_limit
    result["resource_configuration"] = resource_configuration(cpu_limit, memory_limit)
    result["cpu_mean_of_limit_percent"] = round(
        float(result["cpu_mean_percent"]) / cpu_limit, 6
    )
    result["cpu_peak_of_limit_percent"] = round(
        float(result["cpu_peak_percent"]) / cpu_limit, 6
    )
    result["memory_mean_of_limit_percent"] = round(
        float(result["memory_mean_mib"]) / memory_limit * 100.0, 6
    )
    result["memory_peak_of_limit_percent"] = round(
        float(result["memory_peak_mib"]) / memory_limit * 100.0, 6
    )
    result["cpu_throttling_ratio"] = round(
        _throttling_ratio(cpu_peak_demand / (cpu_limit * 100.0)), 6
    )
    result["oom_killed"] = False
    return result


def clean_row(source: dict[str, Any]) -> dict[str, Any]:
    cleaned: dict[str, Any] = {}
    for field in output_fields():
        if field == "modeled_host_stall":
            cleaned[field] = source["outlier_injected"]
        elif field in RESOURCE_FIELDS:
            cleaned[field] = source[field]
        else:
            cleaned[field] = source[field]
    if cleaned["network_profile"] == "hard_partition":
        cleaned["network_profile"] = "clean_lan"
    return cleaned


def generate_combined_rows(anchors: dict[str, Any]) -> list[dict[str, Any]]:
    base_rows = generate_rows("theoretical", anchors, "not-exported")
    retained_base = [
        row
        for row in base_rows
        if row["sweep"] != "network" or row["network_profile"] in NETWORK_PROFILES
    ]
    rows = [clean_row(add_baseline_resource_fields(row)) for row in retained_base]

    for scenario, cpu_limit, memory_limit_mib in build_resource_scenarios():
        paired_scenario = Scenario(
            sweep=scenario.sweep,
            configuration_id=(
                f"resource-{scenario.node_count}n-{scenario.network_profile}"
            ),
            node_count=scenario.node_count,
            network_profile=scenario.network_profile,
            target_offered_rate_events_s=scenario.target_offered_rate_events_s,
        )
        for method in METHODS:
            for repetition, seed in enumerate(REPETITION_SEEDS, start=1):
                source = simulate_row(
                    "theoretical",
                    method,
                    paired_scenario,
                    repetition,
                    seed,
                    anchors,
                    "not-exported",
                )
                source["case_id"] = f"{scenario.configuration_id}-{method}"
                constrained = apply_resource_limits(source, cpu_limit, memory_limit_mib)
                rows.append(clean_row(constrained))

    for index, row in enumerate(rows, start=1):
        row["run_id"] = f"run-{index:06d}"
    expected = 5_280 + 47_520
    if len(rows) != expected:
        raise AssertionError(f"expected {expected} rows, found {len(rows)}")
    return rows


def _numeric_values(rows: Iterable[dict[str, Any]], field: str) -> list[float]:
    return [float(row[field]) for row in rows if row[field] not in (None, "")]


def _quantile(values: list[float], probability: float) -> float | str:
    if not values:
        return ""
    ordered = sorted(values)
    position = (len(ordered) - 1) * probability
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return round(ordered[lower], 6)
    weight = position - lower
    return round(ordered[lower] * (1.0 - weight) + ordered[upper] * weight, 6)


def summarize_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = defaultdict(list)
    for row in rows:
        grouped[tuple(row[field] for field in GROUP_FIELDS)].append(row)

    summaries = []
    for key, group in sorted(
        grouped.items(), key=lambda item: tuple(str(value) for value in item[0])
    ):
        summary = dict(zip(GROUP_FIELDS, key))
        summary.update(
            {
                "runs": len(group),
                "modeled_host_stall_runs": sum(
                    bool(row["modeled_host_stall"]) for row in group
                ),
                "oom_killed_runs": sum(bool(row["oom_killed"]) for row in group),
                "oom_rate": round(
                    sum(bool(row["oom_killed"]) for row in group) / len(group), 6
                ),
                "converged_runs": sum(
                    bool(row["converged_within_timeout"]) for row in group
                ),
                "convergence_rate": round(
                    sum(bool(row["converged_within_timeout"]) for row in group)
                    / len(group),
                    6,
                ),
                "semantic_converged_runs": sum(
                    bool(row["semantic_state_converged"]) for row in group
                ),
                "semantic_convergence_rate": round(
                    sum(bool(row["semantic_state_converged"]) for row in group)
                    / len(group),
                    6,
                ),
            }
        )
        for metric in SUMMARY_METRICS:
            values = _numeric_values(group, metric)
            summary[f"{metric}_mean"] = (
                round(statistics.fmean(values), 6) if values else ""
            )
            summary[f"{metric}_stddev"] = (
                round(statistics.stdev(values), 6) if len(values) > 1 else ""
            )
            summary[f"{metric}_median"] = _quantile(values, 0.5)
            summary[f"{metric}_q1"] = _quantile(values, 0.25)
            summary[f"{metric}_q3"] = _quantile(values, 0.75)
            summary[f"{metric}_p95"] = _quantile(values, 0.95)
            summary[f"{metric}_min"] = round(min(values), 6) if values else ""
            summary[f"{metric}_max"] = round(max(values), 6) if values else ""
        summaries.append(summary)
    if len(summaries) != 1_760:
        raise AssertionError(
            f"expected 1760 summarized configurations, found {len(summaries)}"
        )
    return summaries


def validate_rows(rows: list[dict[str, Any]]) -> None:
    if len(rows) != 52_800:
        raise ValueError(f"expected 52800 rows, found {len(rows)}")
    cases: dict[str, int] = defaultdict(int)
    for row in rows:
        cases[row["case_id"]] += 1
        accepted = int(row["accepted_events"])
        attempted = int(row["attempted_events"])
        fully_observed = int(row["fully_observed_tracking_ids_count"])
        if not 0 <= fully_observed <= accepted <= attempted:
            raise ValueError(f"invalid event counts in {row['run_id']}")
        if float(row["write_latency_p50_ms"]) > float(row["write_latency_p95_ms"]):
            raise ValueError(f"invalid p50/p95 latency in {row['run_id']}")
        if float(row["write_latency_p95_ms"]) > float(row["write_latency_max_ms"]):
            raise ValueError(f"invalid p95/max latency in {row['run_id']}")
        sync_total = (
            int(row["gossip_summary_sent_bytes"])
            + int(row["full_state_repair_bytes"])
            + int(row["delta_repair_bytes"])
        )
        if int(row["sync_bytes_total"]) != sync_total:
            raise ValueError(f"invalid byte total in {row['run_id']}")
        if abs(float(row["bytes_per_accepted_event"]) - sync_total / accepted) > 1e-5:
            raise ValueError(f"invalid bytes/event in {row['run_id']}")
        if row["converged_within_timeout"]:
            if not row["semantic_state_converged"]:
                raise ValueError(
                    f"combined convergence without semantics in {row['run_id']}"
                )
            if int(row["missing_at_cutoff_count"]) != 0:
                raise ValueError(
                    f"combined convergence with missing IDs in {row['run_id']}"
                )
            if float(row["convergence_time_s"]) > float(
                row["convergence_wait_seconds"]
            ):
                raise ValueError(f"over-budget convergence in {row['run_id']}")
        if float(row["cpu_peak_of_limit_percent"]) > 100.000001:
            raise ValueError(f"CPU exceeds limit in {row['run_id']}")
        if float(row["memory_peak_of_limit_percent"]) > 100.000001:
            raise ValueError(f"memory exceeds limit in {row['run_id']}")
    if len(cases) != 1_760 or set(cases.values()) != {30}:
        raise ValueError("expected 1760 configurations with 30 repetitions")


def build_plotting_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    if len(PLOTTING_FIELDS) != 53 or len(set(PLOTTING_FIELDS)) != 53:
        raise AssertionError("plotting schema must contain 53 unique columns")
    missing = set(PLOTTING_FIELDS) - set(rows[0])
    if missing:
        raise ValueError(f"plotting columns missing from run data: {sorted(missing)}")
    return [{field: row[field] for field in PLOTTING_FIELDS} for row in rows]


def write_csv(path: Path, rows: list[dict[str, Any]]) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=list(rows[0]))
        writer.writeheader()
        writer.writerows(rows)


def write_manifest(path: Path, rows: list[dict[str, Any]]) -> None:
    resource_rows = [row for row in rows if row["sweep"] == "resource_interaction"]
    payload = {
        "note": "Theoretical model dataset; not direct experiment measurements.",
        "runs": len(rows),
        "configurations": len({row["case_id"] for row in rows}),
        "repetitions_per_configuration": len(REPETITION_SEEDS),
        "existing_sweep_rows": len(rows) - len(resource_rows),
        "resource_interaction_rows": len(resource_rows),
        "node_counts": list(NODE_COUNTS),
        "network_profiles": list(NETWORK_PROFILES),
        "sync_mechanisms": list(METHODS),
        "cpu_limits_cores": list(CPU_LIMITS),
        "memory_limits_mib": list(MEMORY_LIMITS_MIB),
        "resource_workload_events_s": RESOURCE_WORKLOAD_EVENTS_S,
        "baseline_resource_configuration": resource_configuration(
            BASELINE_CPU_LIMIT, BASELINE_MEMORY_LIMIT_MIB
        ),
        "run_id_format": "run-NNNNNN",
        "plotting_columns": len(PLOTTING_FIELDS),
        "plotting_column_groups": PLOTTING_COLUMN_GROUPS,
        "summary_statistics": [
            "mean",
            "sample standard deviation",
            "median",
            "first quartile",
            "third quartile",
            "p95",
            "minimum",
            "maximum",
        ],
        "p95_note": (
            "Each configuration has 30 repetitions; across-run p95 is an "
            "exploratory tail estimate."
        ),
        "files": [
            "theoretical_model_runs.csv",
            "theoretical_model_summary.csv",
            "theoretical_model_plotting.csv",
        ],
    }
    path.write_text(json.dumps(payload, indent=2) + "\n", encoding="utf-8")


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("results/theoretical_model_share"),
    )
    parser.add_argument(
        "--evidence",
        type=Path,
        default=Path("results/reviewer_revision/paper_evidence/paper_evidence.json"),
    )
    args = parser.parse_args()

    anchors = load_current_anchors(args.evidence.resolve())
    rows = generate_combined_rows(anchors)
    validate_rows(rows)
    summaries = summarize_rows(rows)
    plotting_rows = build_plotting_rows(rows)
    output_root = args.out_dir.resolve()
    write_csv(output_root / "theoretical_model_runs.csv", rows)
    write_csv(output_root / "theoretical_model_summary.csv", summaries)
    write_csv(output_root / "theoretical_model_plotting.csv", plotting_rows)
    write_manifest(output_root / "dataset_manifest.json", rows)
    print(
        json.dumps(
            {
                "output": str(output_root),
                "rows": len(rows),
                "configurations": len(summaries),
                "plotting_columns": len(PLOTTING_FIELDS),
                "repetitions_per_configuration": len(REPETITION_SEEDS),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
