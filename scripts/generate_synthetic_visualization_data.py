#!/usr/bin/env python3
"""Generate deterministic synthetic datasets for visualization and model exploration."""

from __future__ import annotations

import argparse
import csv
import hashlib
import importlib
import json
import math
import random
import os
import statistics
from dataclasses import asdict, dataclass
from datetime import datetime, timezone
from pathlib import Path
from typing import Any, Iterable

GENERATOR_VERSION = "1.0.0"
SCHEMA_VERSION = "synthetic-visualization-v1"
REPETITION_SEEDS = tuple(range(10001, 10031))
METHODS = ("full_state", "optimized_full_state", "naive_key_delta", "true_delta")
VARIANTS = ("theoretical", "old_figure_calibrated")
CURRENT_METHODS = {"full_state", "true_delta"}
HISTORICAL_METHODS = {"optimized_full_state", "naive_key_delta"}
MIB = 1024 * 1024


@dataclass(frozen=True)
class NetworkProfile:
    delay_ms: float
    jitter_ms: float
    loss_percent: float
    bandwidth_mbps: float
    correlated_loss: bool = False


NETWORK_PROFILES = {
    "clean_lan": NetworkProfile(1.0, 0.2, 0.0, 1000.0),
    "normal_wifi": NetworkProfile(20.0, 5.0, 0.5, 50.0),
    "constrained_wifi": NetworkProfile(50.0, 15.0, 2.0, 10.0),
    "disaster_moderate": NetworkProfile(100.0, 30.0, 5.0, 5.0),
    "degraded": NetworkProfile(300.0, 100.0, 10.0, 2.0, True),
    "severe": NetworkProfile(600.0, 200.0, 20.0, 0.5, True),
    "hard_partition": NetworkProfile(1.0, 0.2, 0.0, 1000.0),
}

# Approximate values read from Figure 3 in AIBThings2026-draft-2.pdf.
# They are model anchors, not recovered measurements.
OLD_FIGURE_CONVERGENCE_S = {
    "50/50": {
        "optimized_full_state": {5: 1.8, 10: 6.5, 15: 9.0},
        "naive_key_delta": {5: 3.2, 10: 5.5, 15: 13.0},
    },
    "70/30": {
        "optimized_full_state": {5: 4.0, 10: 10.2, 15: 10.2},
        "naive_key_delta": {5: 3.2, 10: 2.0, 15: 12.5},
    },
    "80/20": {
        "optimized_full_state": {5: 9.5, 10: 9.5, 15: 3.5},
        "naive_key_delta": {5: 2.0, 10: 6.8, 15: 17.0},
    },
}
OLD_FIGURE_SYNC_MIB = {
    "50/50": {
        "optimized_full_state": {5: 0.2, 10: 2.0, 15: 8.0},
        "naive_key_delta": {5: 0.8, 10: 9.0, 15: 27.0},
    },
    "70/30": {
        "optimized_full_state": {5: 0.2, 10: 3.0, 15: 11.0},
        "naive_key_delta": {5: 0.7, 10: 8.0, 15: 30.0},
    },
    "80/20": {
        "optimized_full_state": {5: 0.3, 10: 2.0, 15: 9.0},
        "naive_key_delta": {5: 1.0, 10: 8.0, 15: 28.0},
    },
}


@dataclass(frozen=True)
class Scenario:
    sweep: str
    configuration_id: str
    node_count: int
    network_profile: str
    target_offered_rate_events_s: float | None = None
    partition_split: str | None = None
    partition_duration_s: float = 0.0


CSV_FIELDS = (
    "data_origin",
    "dataset_variant",
    "generator_version",
    "source_schema_version",
    "synthetic_seed",
    "method_model_origin",
    "calibration_source",
    "calibration_precision",
    "generated_at",
    "outlier_injected",
    "outlier_type",
    "run_id",
    "case_id",
    "repetition",
    "scenario_seed",
    "experiment_group",
    "sweep",
    "implementation_label",
    "sync_mode",
    "scenario",
    "node_count",
    "partition_split",
    "partition_group_a",
    "partition_group_b",
    "network_profile",
    "network_delay_ms",
    "network_jitter_ms",
    "network_loss_percent",
    "network_bandwidth_mbps",
    "network_correlated_loss",
    "partition_duration_seconds",
    "actual_fault_duration_s",
    "fault_applied_at_unix_s",
    "fault_removed_at_unix_s",
    "submission_duration_s",
    "target_offered_rate_events_s",
    "actual_offered_rate_events_s",
    "accepted_throughput_events_s",
    "attempted_events",
    "accepted_events",
    "rejected_events",
    "failed_requests",
    "write_success_rate",
    "write_latency_p50_ms",
    "write_latency_p95_ms",
    "write_latency_max_ms",
    "failed_request_latency_p95_ms",
    "convergence_wait_seconds",
    "observed_completion_time_s",
    "convergence_time_s",
    "converged_within_timeout",
    "recovery_time_s",
    "time_to_first_remote_node_p50_s",
    "time_to_first_remote_node_p95_s",
    "time_to_last_node_p50_s",
    "time_to_last_node_p95_s",
    "time_to_last_node_s",
    "rounds_to_last_node",
    "accepted_tracking_ids_count",
    "fully_observed_tracking_ids_count",
    "missing_tracking_ids_count",
    "missing_at_cutoff_count",
    "missing_at_cutoff_pct",
    "tracking_coverage_percent",
    "per_node_missing_tracking_ids",
    "semantic_state_converged",
    "unique_semantic_states",
    "is_converged",
    "unique_merkle_roots",
    "known_status",
    "coverage_percent",
    "max_events",
    "min_events",
    "duplicate_submissions",
    "sync_bytes_total",
    "bytes_per_accepted_event",
    "gossip_summary_sent_bytes",
    "gossip_summary_received_bytes",
    "full_state_repair_bytes",
    "delta_repair_bytes",
    "semantic_state_bytes",
    "evaluation_metadata_state_bytes",
    "nonsemantic_state_metadata_bytes",
    "total_state_size_bytes",
    "avg_state_size_bytes",
    "avg_state_bytes_per_known_event",
    "repair_pull_count",
    "gossip_messages_sent",
    "delta_pulls_started",
    "delta_pulls_success",
    "delta_pulls_gap",
    "delta_deltas_applied",
    "delta_bytes_received",
    "cpu_mean_percent",
    "cpu_peak_percent",
    "memory_mean_mib",
    "memory_peak_mib",
    "network_rx_bytes",
    "network_tx_bytes",
    "block_read_bytes",
    "block_write_bytes",
    "resource_sample_count",
    "docker_cpus",
    "docker_memory",
    "gateway_staleness_seconds",
    "central_response_bytes",
    "central_requests",
    "central_successes",
    "central_rejected",
)

SUMMARY_METRICS = (
    "accepted_throughput_events_s",
    "write_latency_p95_ms",
    "observed_completion_time_s",
    "convergence_time_s",
    "recovery_time_s",
    "tracking_coverage_percent",
    "bytes_per_accepted_event",
    "sync_bytes_total",
    "cpu_mean_percent",
    "cpu_peak_percent",
    "memory_mean_mib",
    "memory_peak_mib",
)


def build_scenarios() -> list[Scenario]:
    scenarios: list[Scenario] = []
    for nodes in (3, 5, 7, 10, 12, 15, 20, 25, 30, 40, 50):
        scenarios.append(Scenario("scale", f"scale-{nodes}n", nodes, "clean_lan"))
    for nodes in (10, 25, 50):
        for profile in (
            "clean_lan",
            "normal_wifi",
            "constrained_wifi",
            "disaster_moderate",
            "degraded",
            "severe",
        ):
            scenarios.append(
                Scenario("network", f"network-{nodes}n-{profile}", nodes, profile)
            )
    for nodes in (5, 15, 30):
        for rate in (5.0, 10.0, 25.0, 50.0, 100.0):
            scenarios.append(
                Scenario(
                    "load",
                    f"load-{nodes}n-{rate:g}r",
                    nodes,
                    "clean_lan",
                    target_offered_rate_events_s=rate,
                )
            )
    for nodes in (15, 30):
        for split in ("50/50", "70/30", "80/20"):
            scenarios.append(
                Scenario(
                    "partition_shape",
                    f"partition-{nodes}n-{split.replace('/', '-')}",
                    nodes,
                    "hard_partition",
                    partition_split=split,
                    partition_duration_s=30.0,
                )
            )
    if len(scenarios) != 50:
        raise AssertionError(f"expected 50 scenarios, found {len(scenarios)}")
    return scenarios


def _number(value: Any) -> float | None:
    if value in (None, ""):
        return None
    return float(value)


def load_current_anchors(evidence_path: Path) -> dict[str, Any]:
    evidence = json.loads(evidence_path.read_text(encoding="utf-8"))
    metrics = (
        "convergence_time_s_median_converged",
        "write_latency_p95_ms_median",
        "accepted_throughput_events_s_median",
        "bytes_per_accepted_event_median",
        "cpu_mean_percent_median",
        "cpu_peak_percent_median",
        "memory_mean_mib_median",
        "memory_peak_mib_median",
    )
    anchors: dict[str, Any] = {
        method: {"baseline": {}, "partition": {}} for method in CURRENT_METHODS
    }
    for group in evidence["groups"]:
        method = group.get("sync_mode")
        if method not in CURRENT_METHODS or group.get("experiment_group") != "core":
            continue
        nodes = int(group["node_count"])
        values = {metric: _number(group.get(metric)) for metric in metrics}
        if group.get("scenario") == "baseline":
            anchors[method]["baseline"][nodes] = values
        elif (
            group.get("scenario") == "partition_heal"
            and group.get("partition_split") == "50/50"
        ):
            anchors[method]["partition"][nodes] = values
    for method in CURRENT_METHODS:
        if set(anchors[method]["baseline"]) != {5, 10, 15}:
            raise ValueError(f"missing current baseline anchors for {method}")
        if set(anchors[method]["partition"]) != {5, 10, 15}:
            raise ValueError(f"missing current partition anchors for {method}")
    return anchors


def _interpolate(
    nodes: int, anchors: dict[int, float], max_exponent: float = 2.0
) -> float:
    points = sorted(
        (int(node_count), float(value)) for node_count, value in anchors.items()
    )
    if nodes in anchors:
        return float(anchors[nodes])
    if nodes < points[0][0]:
        left, right = points[0], points[1]
    elif nodes > points[-1][0]:
        left, right = points[-2], points[-1]
    else:
        left, right = next(
            (left, right)
            for left, right in zip(points, points[1:])
            if left[0] < nodes < right[0]
        )
    x1, y1 = left
    x2, y2 = right
    if y1 <= 0 or y2 <= 0:
        fraction = (nodes - x1) / (x2 - x1)
        return max(0.0, y1 + fraction * (y2 - y1))
    exponent = math.log(y2 / y1) / math.log(x2 / x1)
    exponent = max(-max_exponent, min(max_exponent, exponent))
    return y1 * (nodes / x1) ** exponent


def _anchor_metric(
    anchors: dict[str, Any], method: str, scenario: str, metric: str, nodes: int
) -> float:
    values = {
        node_count: row[metric]
        for node_count, row in anchors[method][scenario].items()
        if row[metric] is not None
    }
    return _interpolate(nodes, values)


def _old_figure_anchor(
    table: dict[str, dict[str, dict[int, float]]],
    method: str,
    nodes: int,
    split: str | None,
) -> float:
    if split:
        return _interpolate(nodes, table[split][method], max_exponent=2.4)
    values = {
        node_count: statistics.median(
            table[partition][method][node_count] for partition in table
        )
        for node_count in (5, 10, 15)
    }
    return _interpolate(nodes, values, max_exponent=2.4)


def _stable_rng(seed: int, scenario: Scenario, method: str) -> random.Random:
    material = f"{seed}|{scenario.configuration_id}|{method}".encode()
    derived = int.from_bytes(hashlib.sha256(material).digest()[:8], "big")
    return random.Random(derived)


def _profile_factors(profile: NetworkProfile) -> tuple[float, float, float]:
    loss = profile.loss_percent / 100.0
    convergence = (
        1.0
        + profile.delay_ms / 180.0
        + loss * 8.0
        + max(0.0, math.sqrt(100.0 / profile.bandwidth_mbps) - 0.32) * 0.18
    )
    communication = (
        1.0 + loss * 5.0 + max(0.0, 20.0 / profile.bandwidth_mbps - 1.0) * 0.15
    )
    variability = 0.08 + profile.delay_ms / 2400.0 + loss * 0.7
    return convergence, communication, min(0.55, variability)


def _partition_multiplier(split: str | None) -> float:
    return {None: 1.0, "50/50": 1.0, "70/30": 1.08, "80/20": 1.18}[split]


def _event_count(scenario: Scenario) -> int:
    per_node = 90 if scenario.sweep in {"load", "partition_shape"} else 15
    return scenario.node_count * per_node


def _base_values(
    variant: str,
    method: str,
    scenario: Scenario,
    accepted_events: int,
    anchors: dict[str, Any],
) -> dict[str, float]:
    nodes = scenario.node_count
    profile = NETWORK_PROFILES[scenario.network_profile]
    network_convergence, network_communication, _ = _profile_factors(profile)
    partition_multiplier = _partition_multiplier(scenario.partition_split)
    is_partition = scenario.sweep == "partition_shape"
    anchor_scenario = "partition" if is_partition else "baseline"

    full = {
        metric: _anchor_metric(anchors, "full_state", anchor_scenario, metric, nodes)
        for metric in (
            "convergence_time_s_median_converged",
            "write_latency_p95_ms_median",
            "accepted_throughput_events_s_median",
            "bytes_per_accepted_event_median",
            "cpu_mean_percent_median",
            "cpu_peak_percent_median",
            "memory_mean_mib_median",
            "memory_peak_mib_median",
        )
    }
    if method in CURRENT_METHODS:
        base = {
            metric: _anchor_metric(anchors, method, anchor_scenario, metric, nodes)
            for metric in full
        }
    elif variant == "theoretical":
        if method == "optimized_full_state":
            communication_ratio = min(0.92, 0.62 + 0.006 * nodes)
            base = {
                **full,
                "convergence_time_s_median_converged": full[
                    "convergence_time_s_median_converged"
                ]
                * (0.78 + 0.002 * nodes),
                "write_latency_p95_ms_median": full["write_latency_p95_ms_median"]
                * 0.88,
                "accepted_throughput_events_s_median": full[
                    "accepted_throughput_events_s_median"
                ]
                * 1.08,
                "bytes_per_accepted_event_median": full[
                    "bytes_per_accepted_event_median"
                ]
                * communication_ratio,
                "cpu_mean_percent_median": full["cpu_mean_percent_median"] * 1.18,
                "cpu_peak_percent_median": full["cpu_peak_percent_median"] * 1.15,
                "memory_mean_mib_median": full["memory_mean_mib_median"] * 0.96,
                "memory_peak_mib_median": full["memory_peak_mib_median"] * 0.97,
            }
        else:
            amplification = 0.9 + 0.08 * nodes
            base = {
                **full,
                "convergence_time_s_median_converged": full[
                    "convergence_time_s_median_converged"
                ]
                * (0.72 + 0.0028 * nodes**1.45),
                "write_latency_p95_ms_median": full["write_latency_p95_ms_median"]
                * (0.92 + 0.008 * nodes),
                "accepted_throughput_events_s_median": full[
                    "accepted_throughput_events_s_median"
                ]
                / (1.0 + 0.008 * nodes),
                "bytes_per_accepted_event_median": full[
                    "bytes_per_accepted_event_median"
                ]
                * amplification,
                "cpu_mean_percent_median": full["cpu_mean_percent_median"]
                * (1.6 + 0.025 * nodes),
                "cpu_peak_percent_median": full["cpu_peak_percent_median"]
                * (1.5 + 0.02 * nodes),
                "memory_mean_mib_median": full["memory_mean_mib_median"]
                * (1.0 + 0.006 * nodes),
                "memory_peak_mib_median": full["memory_peak_mib_median"]
                * (1.0 + 0.008 * nodes),
            }
    else:
        convergence = _old_figure_anchor(
            OLD_FIGURE_CONVERGENCE_S, method, nodes, scenario.partition_split
        )
        sync_mib = _old_figure_anchor(
            OLD_FIGURE_SYNC_MIB, method, nodes, scenario.partition_split
        )
        base = {
            **full,
            "convergence_time_s_median_converged": convergence,
            "write_latency_p95_ms_median": full["write_latency_p95_ms_median"]
            * (0.9 if method == "optimized_full_state" else 1.05),
            "accepted_throughput_events_s_median": full[
                "accepted_throughput_events_s_median"
            ]
            * (1.05 if method == "optimized_full_state" else 0.9),
            "bytes_per_accepted_event_median": sync_mib * MIB / accepted_events,
            "cpu_mean_percent_median": full["cpu_mean_percent_median"]
            * (1.2 if method == "optimized_full_state" else 2.2),
            "cpu_peak_percent_median": full["cpu_peak_percent_median"]
            * (1.15 if method == "optimized_full_state" else 2.0),
            "memory_mean_mib_median": full["memory_mean_mib_median"]
            * (0.96 if method == "optimized_full_state" else 1.12),
            "memory_peak_mib_median": full["memory_peak_mib_median"]
            * (0.97 if method == "optimized_full_state" else 1.15),
        }

    load_factor: float
    if scenario.target_offered_rate_events_s is not None:
        rate = scenario.target_offered_rate_events_s
        capacity = {
            "full_state": 126.0,
            "optimized_full_state": 138.0,
            "naive_key_delta": 112.0,
            "true_delta": 130.0,
        }[method] * (5.0 / nodes) ** 0.2
        utilization = min(0.985, rate / capacity)
        load_factor = 1.0 + 3.8 * utilization**3 / max(0.12, 1.0 - utilization)
        base["accepted_throughput_events_s_median"] = min(rate, capacity)
        base["write_latency_p95_ms_median"] *= load_factor
        base["convergence_time_s_median_converged"] *= 1.0 + 1.4 * utilization**2
        base["bytes_per_accepted_event_median"] *= 1.0 + 0.22 * utilization
        base["cpu_mean_percent_median"] *= 1.0 + 2.0 * utilization
        base["cpu_peak_percent_median"] *= 1.0 + 1.6 * utilization

    if not is_partition:
        base["convergence_time_s_median_converged"] *= network_convergence
    else:
        base["convergence_time_s_median_converged"] *= partition_multiplier
    base["bytes_per_accepted_event_median"] *= network_communication * (
        partition_multiplier if is_partition else 1.0
    )
    base["write_latency_p95_ms_median"] += profile.delay_ms * 0.35
    base["accepted_throughput_events_s_median"] /= max(1.0, network_convergence**0.28)
    base["cpu_mean_percent_median"] *= 1.0 + 0.12 * (network_communication - 1.0)
    base["cpu_peak_percent_median"] *= 1.0 + 0.08 * (network_communication - 1.0)
    return base


def _provenance(variant: str, method: str) -> tuple[str, str, str]:
    if method in CURRENT_METHODS:
        return (
            "measured_calibrated",
            "reviewer_revision/paper_evidence/paper_evidence.json",
            "repeated-run grouped medians",
        )
    if variant == "theoretical":
        return (
            "historical_theoretical",
            "documented algorithmic assumptions",
            "model assumption",
        )
    return (
        "historical_figure_calibrated",
        "AIBThings2026-draft-2.pdf Figure 3",
        "approximate plot digitization",
    )


def simulate_row(
    variant: str,
    method: str,
    scenario: Scenario,
    repetition: int,
    seed: int,
    anchors: dict[str, Any],
    generated_at: str,
) -> dict[str, Any]:
    rng = _stable_rng(seed, scenario, method)
    profile = NETWORK_PROFILES[scenario.network_profile]
    _, communication_factor, variability = _profile_factors(profile)
    attempted = _event_count(scenario)
    accepted = attempted
    rejected = 0
    failed_requests = 0
    base = _base_values(variant, method, scenario, accepted, anchors)

    environment_noise = rng.lognormvariate(-0.5 * variability**2, variability)
    independent_noise = rng.lognormvariate(-0.5 * 0.07**2, 0.07)
    outlier = rng.random() < 0.01
    outlier_factor = rng.uniform(5.0, 10.0) if outlier else 1.0

    observed_completion = (
        base["convergence_time_s_median_converged"]
        * environment_noise
        * independent_noise
        * outlier_factor
    )
    wait_seconds = 45.0
    semantic_time = observed_completion * rng.uniform(0.68, 0.88)
    tracking_time = observed_completion * rng.uniform(0.96, 1.12)
    reliability_penalty = min(
        0.42,
        profile.loss_percent / 100.0 * 1.2
        + max(0.0, profile.delay_ms - 100.0) / 1800.0,
    )
    semantic = semantic_time <= wait_seconds and rng.random() >= reliability_penalty
    tracking_complete = tracking_time <= wait_seconds
    combined = semantic and tracking_complete and observed_completion <= wait_seconds

    if tracking_complete:
        fully_observed = accepted
    else:
        completion_ratio = max(0.35, min(0.995, wait_seconds / tracking_time))
        missing = max(1, round(accepted * (1.0 - completion_ratio)))
        fully_observed = accepted - missing
    missing = accepted - fully_observed
    tracking_coverage = 100.0 * fully_observed / accepted

    convergence_time: float | str = round(observed_completion, 3) if combined else ""
    recovery_time: float | str = (
        round(observed_completion, 3)
        if scenario.sweep == "partition_shape" and combined
        else ""
    )
    time_to_last = observed_completion * rng.uniform(0.86, 1.0)
    first_remote_p50 = max(
        0.001,
        min(
            time_to_last,
            profile.delay_ms / 1000.0 + observed_completion * rng.uniform(0.04, 0.10),
        ),
    )
    first_remote_p95 = min(time_to_last, first_remote_p50 * rng.uniform(1.25, 1.8))
    last_p50 = time_to_last * rng.uniform(0.65, 0.82)
    last_p95 = min(time_to_last, last_p50 * rng.uniform(1.08, 1.28))
    rounds = max(1, math.ceil(time_to_last / 5.0))

    bytes_per_event = max(
        1.0,
        base["bytes_per_accepted_event_median"]
        * environment_noise**0.22
        * rng.lognormvariate(-0.5 * 0.11**2, 0.11),
    )
    sync_bytes_total = round(bytes_per_event * accepted)
    gossip_share = min(
        0.52,
        0.13
        + 0.012 * math.sqrt(scenario.node_count)
        + 0.04 * (communication_factor - 1.0),
    )
    gossip_sent = round(sync_bytes_total * gossip_share)
    repair_bytes = sync_bytes_total - gossip_sent
    uses_delta = method in {"naive_key_delta", "true_delta"}
    full_repair = 0 if uses_delta else repair_bytes
    delta_repair = repair_bytes if uses_delta else 0
    gossip_received = round(
        gossip_sent * max(1, scenario.node_count - 1) * rng.uniform(0.78, 0.98)
    )
    repair_pulls = max(1, round(scenario.node_count * rounds * rng.uniform(0.7, 1.3)))
    gossip_messages = max(
        scenario.node_count,
        round(scenario.node_count * rounds * 3 * rng.uniform(0.85, 1.15)),
    )
    delta_started = repair_pulls if uses_delta else 0
    gap_probability = min(
        0.35,
        profile.loss_percent / 100.0
        + (0.04 if scenario.sweep == "partition_shape" else 0.0),
    )
    delta_gaps = (
        round(delta_started * gap_probability * rng.uniform(0.5, 1.5))
        if uses_delta
        else 0
    )
    delta_success = max(0, delta_started - delta_gaps) if uses_delta else 0
    delta_applied = round(delta_success * rng.uniform(1.0, 2.6)) if uses_delta else 0

    p95_latency = (
        base["write_latency_p95_ms_median"]
        * environment_noise**0.45
        * rng.lognormvariate(-0.5 * 0.09**2, 0.09)
        * (rng.uniform(3.2, 5.0) if outlier else 1.0)
    )
    p50_latency = p95_latency * rng.uniform(0.28, 0.48)
    max_latency = p95_latency * rng.uniform(1.3, 2.5)
    throughput = max(
        0.01,
        base["accepted_throughput_events_s_median"]
        / environment_noise**0.18
        * rng.lognormvariate(-0.5 * 0.04**2, 0.04),
    )
    if scenario.target_offered_rate_events_s is not None:
        throughput = min(scenario.target_offered_rate_events_s * 1.015, throughput)
    submission_duration = accepted / throughput
    actual_offered = attempted / submission_duration

    cpu_mean = min(
        390.0,
        base["cpu_mean_percent_median"]
        * environment_noise**0.28
        * rng.lognormvariate(-0.5 * 0.08**2, 0.08),
    )
    cpu_peak = min(
        400.0,
        max(
            cpu_mean,
            base["cpu_peak_percent_median"]
            * environment_noise**0.35
            * rng.lognormvariate(-0.5 * 0.1**2, 0.1)
            * (rng.uniform(1.15, 1.55) if outlier else 1.0),
        ),
    )
    memory_mean = max(
        16.0,
        base["memory_mean_mib_median"] * rng.lognormvariate(-0.5 * 0.035**2, 0.035),
    )
    memory_peak = max(
        memory_mean,
        base["memory_peak_mib_median"] * rng.lognormvariate(-0.5 * 0.045**2, 0.045),
    )

    semantic_state_bytes = round(96 * accepted + 320 * scenario.node_count)
    evaluation_metadata = round(140 * accepted * scenario.node_count)
    metadata_factor = {
        "full_state": 0.65,
        "optimized_full_state": 0.32,
        "naive_key_delta": 1.3 + scenario.node_count / 30.0,
        "true_delta": 0.9,
    }[method]
    nonsemantic_metadata = round(semantic_state_bytes * metadata_factor)
    total_state_size = semantic_state_bytes + evaluation_metadata + nonsemantic_metadata
    avg_state_size = round(total_state_size / scenario.node_count)
    avg_state_per_event = avg_state_size / max(1, accepted)
    payload_and_repair_rx = gossip_received + repair_bytes
    network_tx = round((gossip_sent + repair_bytes) * rng.uniform(1.08, 1.22))
    network_rx = round(payload_and_repair_rx * rng.uniform(1.08, 1.22))

    if combined:
        unique_roots = 1
        unique_semantic_states = 1
        known_status = "known_converged"
    else:
        unique_roots = rng.randint(2, min(6, scenario.node_count))
        unique_semantic_states = (
            1 if semantic else rng.randint(2, min(5, scenario.node_count))
        )
        known_status = "known_divergent"

    group_a = ""
    group_b = ""
    if scenario.partition_split:
        left_percent = int(scenario.partition_split.split("/")[0])
        left_count = max(1, round(scenario.node_count * left_percent / 100.0))
        left_count = min(scenario.node_count - 1, left_count)
        group_a = left_count
        group_b = scenario.node_count - left_count

    method_origin, calibration_source, calibration_precision = _provenance(
        variant, method
    )
    synthetic_time = (
        1_800_000_000
        + int(
            hashlib.sha256(scenario.configuration_id.encode()).hexdigest()[:6],
            16,
        )
        + repetition * 100
    )
    if scenario.partition_split:
        fault_applied: int | str = synthetic_time + 10
        fault_removed: float | str = fault_applied + scenario.partition_duration_s
    else:
        fault_applied = ""
        fault_removed = ""
    per_node_missing = (
        "{}"
        if missing == 0
        else json.dumps(
            {
                f"edge-{index + 1}": max(1, round(missing / scenario.node_count))
                for index in range(min(scenario.node_count, max(1, missing)))
            },
            separators=(",", ":"),
            sort_keys=True,
        )
    )

    row = {
        "data_origin": "synthetic",
        "dataset_variant": variant,
        "generator_version": GENERATOR_VERSION,
        "source_schema_version": SCHEMA_VERSION,
        "synthetic_seed": seed,
        "method_model_origin": method_origin,
        "calibration_source": calibration_source,
        "calibration_precision": calibration_precision,
        "generated_at": generated_at,
        "outlier_injected": outlier,
        "outlier_type": "synthetic_host_stall" if outlier else "",
        "run_id": f"synthetic-{variant}-{scenario.configuration_id}-{method}-s{seed}",
        "case_id": f"{scenario.configuration_id}-{method}",
        "repetition": repetition,
        "scenario_seed": seed,
        "experiment_group": "synthetic_visualization",
        "sweep": scenario.sweep,
        "implementation_label": method,
        "sync_mode": method,
        "scenario": "partition_heal" if scenario.partition_split else "baseline",
        "node_count": scenario.node_count,
        "partition_split": scenario.partition_split or "",
        "partition_group_a": group_a,
        "partition_group_b": group_b,
        "network_profile": scenario.network_profile,
        "network_delay_ms": profile.delay_ms,
        "network_jitter_ms": profile.jitter_ms,
        "network_loss_percent": profile.loss_percent,
        "network_bandwidth_mbps": profile.bandwidth_mbps,
        "network_correlated_loss": profile.correlated_loss,
        "partition_duration_seconds": scenario.partition_duration_s,
        "actual_fault_duration_s": scenario.partition_duration_s
        if scenario.partition_split
        else "",
        "fault_applied_at_unix_s": fault_applied,
        "fault_removed_at_unix_s": fault_removed,
        "submission_duration_s": round(submission_duration, 6),
        "target_offered_rate_events_s": scenario.target_offered_rate_events_s or "",
        "actual_offered_rate_events_s": round(actual_offered, 6),
        "accepted_throughput_events_s": round(throughput, 6),
        "attempted_events": attempted,
        "accepted_events": accepted,
        "rejected_events": rejected,
        "failed_requests": failed_requests,
        "write_success_rate": round(accepted / attempted, 6),
        "write_latency_p50_ms": round(p50_latency, 6),
        "write_latency_p95_ms": round(p95_latency, 6),
        "write_latency_max_ms": round(max_latency, 6),
        "failed_request_latency_p95_ms": "",
        "convergence_wait_seconds": wait_seconds,
        "observed_completion_time_s": round(observed_completion, 3),
        "convergence_time_s": convergence_time,
        "converged_within_timeout": combined,
        "recovery_time_s": recovery_time,
        "time_to_first_remote_node_p50_s": round(first_remote_p50, 3),
        "time_to_first_remote_node_p95_s": round(first_remote_p95, 3),
        "time_to_last_node_p50_s": round(last_p50, 3),
        "time_to_last_node_p95_s": round(last_p95, 3),
        "time_to_last_node_s": round(time_to_last, 3),
        "rounds_to_last_node": rounds,
        "accepted_tracking_ids_count": accepted,
        "fully_observed_tracking_ids_count": fully_observed,
        "missing_tracking_ids_count": missing,
        "missing_at_cutoff_count": missing,
        "missing_at_cutoff_pct": round(100.0 * missing / accepted, 6),
        "tracking_coverage_percent": round(tracking_coverage, 6),
        "per_node_missing_tracking_ids": per_node_missing,
        "semantic_state_converged": semantic,
        "unique_semantic_states": unique_semantic_states,
        "is_converged": unique_roots == 1,
        "unique_merkle_roots": unique_roots,
        "known_status": known_status,
        "coverage_percent": round(tracking_coverage, 6),
        "max_events": accepted,
        "min_events": fully_observed,
        "duplicate_submissions": 0,
        "sync_bytes_total": sync_bytes_total,
        "bytes_per_accepted_event": round(sync_bytes_total / accepted, 6),
        "gossip_summary_sent_bytes": gossip_sent,
        "gossip_summary_received_bytes": gossip_received,
        "full_state_repair_bytes": full_repair,
        "delta_repair_bytes": delta_repair,
        "semantic_state_bytes": semantic_state_bytes,
        "evaluation_metadata_state_bytes": evaluation_metadata,
        "nonsemantic_state_metadata_bytes": nonsemantic_metadata,
        "total_state_size_bytes": total_state_size,
        "avg_state_size_bytes": avg_state_size,
        "avg_state_bytes_per_known_event": round(avg_state_per_event, 6),
        "repair_pull_count": repair_pulls,
        "gossip_messages_sent": gossip_messages,
        "delta_pulls_started": delta_started,
        "delta_pulls_success": delta_success,
        "delta_pulls_gap": delta_gaps,
        "delta_deltas_applied": delta_applied,
        "delta_bytes_received": delta_repair,
        "cpu_mean_percent": round(cpu_mean, 6),
        "cpu_peak_percent": round(cpu_peak, 6),
        "memory_mean_mib": round(memory_mean, 6),
        "memory_peak_mib": round(memory_peak, 6),
        "network_rx_bytes": network_rx,
        "network_tx_bytes": network_tx,
        "block_read_bytes": round(rng.uniform(0.0, 0.04) * MIB),
        "block_write_bytes": round(rng.uniform(0.01, 0.18) * MIB),
        "resource_sample_count": max(
            3, round((submission_duration + wait_seconds) / 2)
        ),
        "docker_cpus": "",
        "docker_memory": "",
        "gateway_staleness_seconds": round(
            min(wait_seconds, observed_completion) * rng.uniform(0.0, 0.3), 3
        ),
        "central_response_bytes": 0,
        "central_requests": 0,
        "central_successes": 0,
        "central_rejected": 0,
    }
    return {field: row[field] for field in CSV_FIELDS}


def generate_rows(
    variant: str,
    anchors: dict[str, Any],
    generated_at: str,
) -> list[dict[str, Any]]:
    if variant not in VARIANTS:
        raise ValueError(f"unknown dataset variant: {variant}")
    rows = [
        simulate_row(variant, method, scenario, repetition, seed, anchors, generated_at)
        for scenario in build_scenarios()
        for method in METHODS
        for repetition, seed in enumerate(REPETITION_SEEDS, start=1)
    ]
    if len(rows) != 6000:
        raise AssertionError(f"expected 6000 rows for {variant}, found {len(rows)}")
    return rows


def _quantile(values: list[float], q: float) -> float | str:
    if not values:
        return ""
    ordered = sorted(values)
    position = (len(ordered) - 1) * q
    lower = math.floor(position)
    upper = math.ceil(position)
    if lower == upper:
        return round(ordered[lower], 6)
    weight = position - lower
    return round(ordered[lower] * (1 - weight) + ordered[upper] * weight, 6)


def _float_values(rows: Iterable[dict[str, Any]], field: str) -> list[float]:
    return [float(row[field]) for row in rows if row[field] not in (None, "")]


def summarize_rows(rows: list[dict[str, Any]]) -> list[dict[str, Any]]:
    grouped: dict[tuple[Any, ...], list[dict[str, Any]]] = {}
    key_fields = (
        "dataset_variant",
        "sweep",
        "sync_mode",
        "node_count",
        "network_profile",
        "target_offered_rate_events_s",
        "partition_split",
    )
    for row in rows:
        key = tuple(row[field] for field in key_fields)
        grouped.setdefault(key, []).append(row)
    summaries: list[dict[str, Any]] = []
    for key, group in sorted(
        grouped.items(), key=lambda item: tuple(str(value) for value in item[0])
    ):
        summary = dict(zip(key_fields, key))
        summary.update(
            {
                "runs": len(group),
                "outlier_runs": sum(bool(row["outlier_injected"]) for row in group),
                "converged_runs": sum(
                    bool(row["converged_within_timeout"]) for row in group
                ),
                "semantic_converged_runs": sum(
                    bool(row["semantic_state_converged"]) for row in group
                ),
                "convergence_rate": round(
                    sum(bool(row["converged_within_timeout"]) for row in group)
                    / len(group),
                    6,
                ),
                "semantic_convergence_rate": round(
                    sum(bool(row["semantic_state_converged"]) for row in group)
                    / len(group),
                    6,
                ),
            }
        )
        for metric in SUMMARY_METRICS:
            values = _float_values(group, metric)
            summary[f"{metric}_median"] = _quantile(values, 0.5)
            summary[f"{metric}_q1"] = _quantile(values, 0.25)
            summary[f"{metric}_q3"] = _quantile(values, 0.75)
        summaries.append(summary)
    if len(summaries) != 200:
        raise AssertionError(f"expected 200 summary rows, found {len(summaries)}")
    return summaries


def write_csv(
    path: Path, rows: list[dict[str, Any]], fields: tuple[str, ...] | None = None
) -> None:
    path.parent.mkdir(parents=True, exist_ok=True)
    fieldnames = list(fields or tuple(rows[0]))
    with path.open("w", newline="", encoding="utf-8") as handle:
        writer = csv.DictWriter(handle, fieldnames=fieldnames)
        writer.writeheader()
        writer.writerows(rows)


def _file_sha256(path: Path) -> str:
    digest = hashlib.sha256()
    with path.open("rb") as handle:
        for chunk in iter(lambda: handle.read(1024 * 1024), b""):
            digest.update(chunk)
    return digest.hexdigest()


def _write_model_parameters(path: Path, variant: str, anchors: dict[str, Any]) -> None:
    payload = {
        "generator_version": GENERATOR_VERSION,
        "schema_version": SCHEMA_VERSION,
        "dataset_variant": variant,
        "repetitions": len(REPETITION_SEEDS),
        "seeds": list(REPETITION_SEEDS),
        "methods": list(METHODS),
        "network_profiles": {
            name: asdict(profile) for name, profile in NETWORK_PROFILES.items()
        },
        "current_method_anchors": anchors,
        "historical_model": (
            {
                "optimized_full_state": "materialized state without full event history",
                "naive_key_delta": "changed-key payload with redundant value accumulation",
            }
            if variant == "theoretical"
            else {
                "source": "AIBThings2026-draft-2.pdf Figure 3",
                "precision": "approximate plot digitization",
                "convergence_seconds": OLD_FIGURE_CONVERGENCE_S,
                "sync_mib": OLD_FIGURE_SYNC_MIB,
            }
        ),
        "failure_model": {
            "combined_convergence_budget_seconds": 45,
            "semantic_completion_fraction_of_observed_completion": [0.68, 0.88],
            "tracking_completion_fraction_of_observed_completion": [0.96, 1.12],
            "host_stall_probability": 0.01,
            "host_stall_multiplier": [5.0, 10.0],
        },
    }
    path.write_text(
        json.dumps(payload, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def _plot_outputs(output_root: Path, all_rows: dict[str, list[dict[str, Any]]]) -> None:
    try:
        matplotlib = importlib.import_module("matplotlib")
        matplotlib.use("Agg")
        plt = importlib.import_module("matplotlib.pyplot")
        pd = importlib.import_module("pandas")
    except ImportError as exc:
        raise RuntimeError("plotting requires the project report dependencies") from exc

    labels = {
        "full_state": "Full state",
        "optimized_full_state": "Optimized full state",
        "naive_key_delta": "Naïve key delta",
        "true_delta": "Corrected true delta",
    }
    colors = {
        "full_state": "#C44E52",
        "optimized_full_state": "#4C9FBF",
        "naive_key_delta": "#E39C37",
        "true_delta": "#4C9A50",
    }

    def save(fig: Any, path: Path) -> None:
        path.parent.mkdir(parents=True, exist_ok=True)
        if not fig.get_constrained_layout():
            fig.tight_layout()
        fig.savefig(path, dpi=220, bbox_inches="tight")
        fig.savefig(path.with_suffix(".pdf"), bbox_inches="tight")
        plt.close(fig)

    for variant, rows in all_rows.items():
        frame = pd.DataFrame(rows)
        figures = output_root / variant / "figures"
        scale = frame[frame["sweep"] == "scale"].copy()
        for field, title, ylabel, filename in (
            (
                "observed_completion_time_s",
                "Modeled convergence time by scale",
                "Seconds",
                "scale_convergence",
            ),
            (
                "bytes_per_accepted_event",
                "Modeled synchronization cost by scale",
                "Bytes per accepted event",
                "scale_communication",
            ),
            (
                "cpu_peak_percent",
                "Modeled peak CPU by scale",
                "Peak CPU (%)",
                "scale_cpu",
            ),
            (
                "memory_peak_mib",
                "Modeled peak memory by scale",
                "Peak RSS (MiB)",
                "scale_memory",
            ),
        ):
            fig, ax = plt.subplots(figsize=(8, 4.8))
            for method in METHODS:
                subset = scale[scale["sync_mode"] == method]
                grouped = subset.groupby("node_count")[field]
                median = grouped.median()
                q1 = grouped.quantile(0.25)
                q3 = grouped.quantile(0.75)
                x = median.index.to_numpy(dtype=float)
                ax.plot(
                    x,
                    median.to_numpy(dtype=float),
                    marker="o",
                    label=labels[method],
                    color=colors[method],
                )
                ax.fill_between(
                    x,
                    q1.to_numpy(dtype=float),
                    q3.to_numpy(dtype=float),
                    color=colors[method],
                    alpha=0.14,
                )
            ax.set(title=title, xlabel="Node count", ylabel=ylabel)
            ax.grid(alpha=0.25)
            ax.legend(fontsize=8)
            save(fig, figures / f"{filename}.png")

        success = (
            scale.groupby(["node_count", "sync_mode"])["converged_within_timeout"]
            .mean()
            .reset_index()
        )
        fig, ax = plt.subplots(figsize=(8, 4.8))
        for method in METHODS:
            subset = success[success["sync_mode"] == method]
            ax.plot(
                subset["node_count"],
                subset["converged_within_timeout"],
                marker="o",
                label=labels[method],
                color=colors[method],
            )
        ax.set(
            title="Modeled in-budget convergence probability",
            xlabel="Node count",
            ylabel="Probability",
            ylim=(-0.03, 1.03),
        )
        ax.grid(alpha=0.25)
        ax.legend(fontsize=8)
        save(fig, figures / "scale_convergence_probability.png")

        load = frame[frame["sweep"] == "load"].copy()
        for nodes in (5, 15, 30):
            subset_nodes = load[load["node_count"] == nodes]
            fig, axes = plt.subplots(1, 2, figsize=(11, 4.4))
            for method in METHODS:
                subset = subset_nodes[subset_nodes["sync_mode"] == method]
                grouped = subset.groupby("target_offered_rate_events_s")
                throughput = grouped["accepted_throughput_events_s"].median()
                latency = grouped["write_latency_p95_ms"].median()
                axes[0].plot(
                    throughput.index,
                    throughput.values,
                    marker="o",
                    label=labels[method],
                    color=colors[method],
                )
                axes[1].plot(
                    latency.index,
                    latency.values,
                    marker="o",
                    label=labels[method],
                    color=colors[method],
                )
            axes[0].plot(
                [5, 100],
                [5, 100],
                linestyle="--",
                color="#777777",
                linewidth=1,
                label="Ideal",
            )
            axes[0].set(
                title="Accepted throughput",
                xlabel="Offered events/s",
                ylabel="Accepted events/s",
            )
            axes[1].set(
                title="Write tail latency",
                xlabel="Offered events/s",
                ylabel="Median p95 latency (ms)",
            )
            for ax in axes:
                ax.grid(alpha=0.25)
            axes[0].legend(fontsize=7)
            axes[1].legend(fontsize=7)
            fig.suptitle(f"Modeled load response — {nodes} nodes")
            save(fig, figures / f"load_response_{nodes}n.png")

        network = frame[frame["sweep"] == "network"].copy()
        profile_order = [
            "clean_lan",
            "normal_wifi",
            "constrained_wifi",
            "disaster_moderate",
            "degraded",
            "severe",
        ]
        fig, axes = plt.subplots(
            2,
            2,
            figsize=(11, 8),
            sharex=True,
            sharey=True,
            constrained_layout=True,
        )
        for ax, method in zip(axes.flat, METHODS):
            subset = network[network["sync_mode"] == method]
            pivot = subset.pivot_table(
                index="node_count",
                columns="network_profile",
                values="converged_within_timeout",
                aggfunc="mean",
            ).reindex(index=[10, 25, 50], columns=profile_order)
            image = ax.imshow(
                pivot.to_numpy(dtype=float),
                vmin=0,
                vmax=1,
                aspect="auto",
                cmap="viridis",
            )
            ax.set_title(labels[method])
            ax.set_xticks(
                range(len(profile_order)),
                [name.replace("_", " ") for name in profile_order],
                rotation=35,
                ha="right",
                fontsize=7,
            )
            ax.set_yticks(range(3), [10, 25, 50])
            for y in range(3):
                for x in range(len(profile_order)):
                    ax.text(
                        x,
                        y,
                        f"{pivot.iloc[y, x]:.2f}",
                        ha="center",
                        va="center",
                        color="white" if pivot.iloc[y, x] < 0.55 else "black",
                        fontsize=7,
                    )
        fig.colorbar(
            image, ax=axes.ravel().tolist(), label="In-budget probability", shrink=0.78
        )
        fig.suptitle("Modeled convergence probability by network profile")
        fig.supxlabel("Network profile")
        fig.supylabel("Node count")
        save(fig, figures / "network_convergence_heatmap.png")

        partition = frame[frame["sweep"] == "partition_shape"].copy()
        fig, axes = plt.subplots(1, 2, figsize=(11, 4.5))
        for method in METHODS:
            subset = partition[partition["sync_mode"] == method]
            for nodes, linestyle in ((15, "-"), (30, "--")):
                values = (
                    subset[subset["node_count"] == nodes]
                    .groupby("partition_split")["observed_completion_time_s"]
                    .median()
                    .reindex(["50/50", "70/30", "80/20"])
                )
                axes[0].plot(
                    values.index,
                    values.values,
                    marker="o",
                    linestyle=linestyle,
                    color=colors[method],
                    label=f"{labels[method]} — {nodes}n",
                )
            success_values = (
                subset.groupby("partition_split")["converged_within_timeout"]
                .mean()
                .reindex(["50/50", "70/30", "80/20"])
            )
            axes[1].plot(
                success_values.index,
                success_values.values,
                marker="o",
                color=colors[method],
                label=labels[method],
            )
        axes[0].set(
            title="Post-heal completion time",
            xlabel="Partition shape",
            ylabel="Seconds",
        )
        axes[1].set(
            title="In-budget convergence probability",
            xlabel="Partition shape",
            ylabel="Probability",
            ylim=(-0.03, 1.03),
        )
        for ax in axes:
            ax.grid(alpha=0.25)
            ax.legend(fontsize=6)
        save(fig, figures / "partition_shape_recovery.png")

    comparison = output_root / "comparison_figures"
    combined = pd.concat(
        [pd.DataFrame(rows) for rows in all_rows.values()], ignore_index=True
    )
    historical = combined[
        (combined["sweep"] == "scale") & combined["sync_mode"].isin(HISTORICAL_METHODS)
    ]
    for field, title, ylabel, filename in (
        (
            "observed_completion_time_s",
            "Historical model assumption: convergence",
            "Seconds",
            "historical_model_convergence",
        ),
        (
            "bytes_per_accepted_event",
            "Historical model assumption: communication",
            "Bytes per accepted event",
            "historical_model_communication",
        ),
    ):
        fig, ax = plt.subplots(figsize=(8, 4.8))
        for (variant, method), subset in historical.groupby(
            ["dataset_variant", "sync_mode"]
        ):
            median = subset.groupby("node_count")[field].median()
            linestyle = "-" if variant == "theoretical" else "--"
            ax.plot(
                median.index,
                median.values,
                marker="o",
                linestyle=linestyle,
                color=colors[method],
                label=f"{labels[method]} — {variant.replace('_', ' ')}",
            )
        ax.set(title=title, xlabel="Node count", ylabel=ylabel)
        ax.grid(alpha=0.25)
        ax.legend(fontsize=7)
        save(fig, comparison / f"{filename}.png")


def validate_rows(rows: list[dict[str, Any]], variant: str) -> None:
    if len(rows) != 6000:
        raise ValueError(f"{variant}: expected 6000 rows, found {len(rows)}")
    case_counts: dict[str, int] = {}
    for row in rows:
        case_counts[row["case_id"]] = case_counts.get(row["case_id"], 0) + 1
        attempted = int(row["attempted_events"])
        accepted = int(row["accepted_events"])
        fully_observed = int(row["fully_observed_tracking_ids_count"])
        if not (0 <= fully_observed <= accepted <= attempted):
            raise ValueError(f"invalid event counts in {row['run_id']}")
        if float(row["write_latency_p50_ms"]) > float(row["write_latency_p95_ms"]):
            raise ValueError(f"invalid p50/p95 latency in {row['run_id']}")
        if float(row["write_latency_p95_ms"]) > float(row["write_latency_max_ms"]):
            raise ValueError(f"invalid p95/max latency in {row['run_id']}")
        expected_sync = (
            int(row["gossip_summary_sent_bytes"])
            + int(row["full_state_repair_bytes"])
            + int(row["delta_repair_bytes"])
        )
        if int(row["sync_bytes_total"]) != expected_sync:
            raise ValueError(f"invalid synchronization byte total in {row['run_id']}")
        if (
            abs(float(row["bytes_per_accepted_event"]) - expected_sync / accepted)
            > 1e-5
        ):
            raise ValueError(f"invalid bytes/event in {row['run_id']}")
        if row["converged_within_timeout"]:
            if (
                not row["semantic_state_converged"]
                or int(row["missing_at_cutoff_count"]) != 0
            ):
                raise ValueError(f"invalid combined convergence in {row['run_id']}")
            if (
                int(row["unique_merkle_roots"]) != 1
                or int(row["unique_semantic_states"]) != 1
            ):
                raise ValueError(f"invalid converged cardinality in {row['run_id']}")
            if float(row["convergence_time_s"]) > float(
                row["convergence_wait_seconds"]
            ):
                raise ValueError(f"over-budget convergence in {row['run_id']}")
    if len(case_counts) != 200 or set(case_counts.values()) != {30}:
        raise ValueError(f"{variant}: expected 200 cases with 30 repetitions")


def _write_manifest(
    path: Path,
    output_root: Path,
    evidence_path: Path,
    old_paper_path: Path,
    all_rows: dict[str, list[dict[str, Any]]],
    generated_at: str,
) -> None:
    manifest = {
        "data_origin": "synthetic",
        "generator_version": GENERATOR_VERSION,
        "schema_version": SCHEMA_VERSION,
        "generated_at": generated_at,
        "warning": "Model-based synthetic visualization data; not experimental evidence.",
        "dataset_variants": {
            variant: {
                "rows": len(rows),
                "configurations": len({row["case_id"] for row in rows}),
                "repetitions_per_configuration": 30,
                "outlier_rows": sum(bool(row["outlier_injected"]) for row in rows),
                "converged_rows": sum(
                    bool(row["converged_within_timeout"]) for row in rows
                ),
                "semantic_converged_rows": sum(
                    bool(row["semantic_state_converged"]) for row in rows
                ),
                "csv": str(
                    (output_root / variant / "synthetic_runs.csv").relative_to(
                        output_root
                    )
                ),
                "summary_csv": str(
                    (output_root / variant / "synthetic_summary.csv").relative_to(
                        output_root
                    )
                ),
            }
            for variant, rows in all_rows.items()
        },
        "shared_matrix": {
            "scenarios": len(build_scenarios()),
            "methods": list(METHODS),
            "seeds": list(REPETITION_SEEDS),
            "rows_per_variant": 6000,
            "total_rows": sum(len(rows) for rows in all_rows.values()),
        },
        "calibration_sources": {
            "paths_relative_to": "generation_manifest.json directory",
            "current_evidence": {
                "path": Path(
                    os.path.relpath(evidence_path, start=output_root)
                ).as_posix(),
                "sha256": _file_sha256(evidence_path),
            },
            "historical_figure": {
                "path": Path(
                    os.path.relpath(old_paper_path, start=output_root)
                ).as_posix(),
                "sha256": _file_sha256(old_paper_path),
                "figure": 3,
                "precision": "approximate plot digitization",
            },
        },
    }
    path.write_text(
        json.dumps(manifest, indent=2, sort_keys=True) + "\n", encoding="utf-8"
    )


def main() -> None:
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument(
        "--out-dir",
        type=Path,
        default=Path("results/synthetic_visualization"),
    )
    parser.add_argument(
        "--evidence",
        type=Path,
        default=Path("results/reviewer_revision/paper_evidence/paper_evidence.json"),
    )
    parser.add_argument(
        "--old-paper",
        type=Path,
        default=Path("../Paper Submission/AIBThings2026-draft-2.pdf"),
    )
    parser.add_argument("--skip-plots", action="store_true")
    args = parser.parse_args()

    output_root = args.out_dir.resolve()
    evidence_path = args.evidence.resolve()
    old_paper_path = args.old_paper.resolve()
    if not evidence_path.is_file():
        raise SystemExit(f"current evidence not found: {evidence_path}")
    if not old_paper_path.is_file():
        raise SystemExit(f"historical paper not found: {old_paper_path}")

    generated_at = datetime.now(timezone.utc).isoformat()
    anchors = load_current_anchors(evidence_path)
    scenarios = [asdict(scenario) for scenario in build_scenarios()]
    write_csv(output_root / "shared_matrix.csv", scenarios)

    all_rows: dict[str, list[dict[str, Any]]] = {}
    for variant in VARIANTS:
        rows = generate_rows(variant, anchors, generated_at)
        validate_rows(rows, variant)
        summaries = summarize_rows(rows)
        variant_dir = output_root / variant
        write_csv(variant_dir / "synthetic_runs.csv", rows, CSV_FIELDS)
        write_csv(variant_dir / "synthetic_summary.csv", summaries)
        _write_model_parameters(variant_dir / "model_parameters.json", variant, anchors)
        all_rows[variant] = rows

    if not args.skip_plots:
        _plot_outputs(output_root, all_rows)
    _write_manifest(
        output_root / "generation_manifest.json",
        output_root,
        evidence_path,
        old_paper_path,
        all_rows,
        generated_at,
    )
    print(
        json.dumps(
            {
                "output": str(output_root),
                "variants": {variant: len(rows) for variant, rows in all_rows.items()},
                "total_rows": sum(len(rows) for rows in all_rows.values()),
            },
            indent=2,
        )
    )


if __name__ == "__main__":
    main()
