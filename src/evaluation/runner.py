"""Evaluation runner that drives scenarios against the edge mesh and records metrics.

It recreates node containers, submits events, applies network profiles and
partitions, polls nodes and gateways until convergence or timeout, samples
per-container CPU and memory, and builds one CSV row per scenario step.
"""

import asyncio
import csv
import io
import json
import math
import random
import time
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

import aiohttp
import structlog
from pydantic import BaseModel, Field

from src.services.docker_manager import NETWORK_PROFILE_SETTINGS
from src.evaluation.resources import (
    DEFAULT_RESOURCE_PROFILE,
    RESOURCE_MODES,
    RESOURCE_PROFILES,
    build_resource_assignments,
    resource_profile_block_size,
    resource_totals,
)

log = structlog.get_logger()

NETWORK_PROFILES = set(NETWORK_PROFILE_SETTINGS)
SYNC_MODES = {
    "full_state",
    "optimized_full_state",
    "naive_key_delta",
    "true_delta",
}
SHARED_WORKLOADS = {
    "shared_key": "sensor",
    "shared_counter": "resource",
    "shared_set": "infrastructure",
}
SHARED_WARMUP_EVENTS_PER_NODE = {
    "shared_key": 1,
    "shared_counter": 60,
    "shared_set": 60,
}
SCENARIOS = {"baseline", "partition_heal", "high_load", *SHARED_WORKLOADS}
DEFAULT_EVENT_ID_RETENTION_LIMIT = 100
DEFAULT_DELTA_LOG_LIMIT = 1000

CSV_HEADER = [
    "run_id",
    "condition_id",
    "sweep",
    "case_id",
    "repetition",
    "scenario_seed",
    "repetition_seed",
    "implementation",
    "sync_mode",
    "event_id_retention_limit",
    "delta_log_limit",
    "network_profile",
    "network_delay_ms",
    "network_jitter_ms",
    "network_loss_percent",
    "network_bandwidth_mbps",
    "network_rules_verified",
    "docker_cpus",
    "docker_memory",
    "resource_mode",
    "resource_profile",
    "resource_total_cpus",
    "resource_total_memory_mib",
    "resource_limit_verified",
    "node_resource_assignments",
    "scenario",
    "node_count",
    "partition_split",
    "partition_group_a",
    "partition_group_b",
    "fault_applied_at_unix_s",
    "fault_removed_at_unix_s",
    "actual_fault_duration_s",
    "attempted_events",
    "accepted_events",
    "write_success_rate",
    "target_offered_rate_events_s",
    "duplicate_submissions",
    "rejected_events",
    "request_error_count",
    "submission_duration_s",
    "offered_rate_events_s",
    "accepted_throughput_events_s",
    "write_latency_p50_ms",
    "write_latency_p95_ms",
    "failed_request_latency_p95_ms",
    "write_latency_max_ms",
    "convergence_time_s",
    "converged_within_timeout",
    "convergence_wait_seconds",
    "missing_at_cutoff_count",
    "time_to_last_node_s",
    "recovery_after_heal_s",
    "time_to_first_remote_node_p50_s",
    "time_to_first_remote_node_p95_s",
    "time_to_last_node_p50_s",
    "time_to_last_node_p95_s",
    "rounds_to_last_node",
    "tracking_coverage_percent",
    "accepted_tracking_ids_count",
    "attempted_tracking_ids_count",
    "fully_observed_tracking_ids_count",
    "missing_tracking_ids_count",
    "per_node_missing_tracking_ids",
    "missing_at_cutoff_pct",
    "last_node_to_converge",
    "last_node_resource_tier",
    "low_event_arrival_p50_s",
    "low_event_arrival_p95_s",
    "medium_event_arrival_p50_s",
    "medium_event_arrival_p95_s",
    "high_event_arrival_p50_s",
    "high_event_arrival_p95_s",
    "gateway_staleness_seconds",
    "sync_bytes_total",
    "tracking_bytes_received",
    "protocol_sync_bytes_total",
    "gossip_summary_sent_bytes",
    "gossip_summary_received_bytes",
    "full_state_repair_bytes",
    "delta_repair_bytes",
    "delta_request_bytes",
    "semantic_state_bytes",
    "evaluation_metadata_state_bytes",
    "nonsemantic_state_metadata_bytes",
    "bytes_per_accepted_event",
    "protocol_bytes_per_accepted_event",
    "bytes_per_attempted_event",
    "avg_state_bytes_per_known_event",
    "repair_pull_count",
    "repair_pulls_failed",
    "gossip_messages_sent",
    "total_state_size_bytes",
    "avg_state_size_bytes",
    "known_status",
    "coverage_percent",
    "resource_sample_count",
    "cpu_mean_percent",
    "cpu_peak_percent",
    "cpu_periods",
    "cpu_throttled_periods",
    "cpu_throttled_period_percent",
    "cpu_throttled_time_s",
    "memory_mean_mib",
    "memory_peak_mib",
    "per_node_resource_metrics",
    "resource_tier_metrics",
    "low_cpu_limit_mean_percent",
    "low_cpu_limit_peak_percent",
    "low_memory_limit_mean_percent",
    "low_memory_limit_peak_percent",
    "medium_cpu_limit_mean_percent",
    "medium_cpu_limit_peak_percent",
    "medium_memory_limit_mean_percent",
    "medium_memory_limit_peak_percent",
    "high_cpu_limit_mean_percent",
    "high_cpu_limit_peak_percent",
    "high_memory_limit_mean_percent",
    "high_memory_limit_peak_percent",
    "container_failure_count",
    "container_oom_count",
    "network_rx_bytes",
    "network_tx_bytes",
    "block_read_bytes",
    "block_write_bytes",
    "is_converged",
    "unique_merkle_roots",
    "semantic_state_converged",
    "unique_semantic_states",
    "max_events",
    "min_events",
    "delta_pulls_started",
    "delta_pulls_success",
    "delta_pulls_gap",
    "delta_deltas_applied",
    "delta_bytes_received",
]


class EvalScenarioConfig(BaseModel):
    """Configuration for one evaluation run: topology, workload, sync mode, network and resources."""

    case_id: str | None = None
    condition_id: str | None = None
    sweep: str | None = None
    node_counts: list[int] = Field(default=[5, 10])
    events_per_node: int = Field(default=5)
    partition_splits: list[float] = Field(default=[0.5, 0.7, 0.8])
    partition_wait_seconds: float = Field(default=8.0)
    convergence_wait_seconds: float = Field(default=15.0)
    poll_interval_seconds: float = Field(default=2.0)
    sync_mode: str = "true_delta"
    event_id_retention_limit: int = DEFAULT_EVENT_ID_RETENTION_LIMIT
    delta_log_limit: int = DEFAULT_DELTA_LOG_LIMIT
    network_profile: str = "clean_lan"
    scenario_seed: int | None = None
    event_concurrency: int = Field(default=25)
    offered_rate_per_second: float | None = Field(default=None, gt=0)
    docker_cpus: float | None = None
    docker_memory: str | None = None
    resource_mode: str = "uniform"
    resource_profile: str | None = None
    repetitions: int = Field(default=1, ge=1)
    scenarios: list[str] = Field(default=["baseline", "partition_heal", "high_load"])
    warm_topology: bool = True
    node_readiness_timeout_seconds: float = Field(default=30.0, ge=1.0)
    partition_events_per_node: int = Field(default=3, ge=0)

    def effective_event_id_retention_limit(self) -> int:
        """Return the event ID retention limit, forced to 0 (unbounded) for full_state mode."""
        if self.sync_mode == "full_state":
            return 0
        return self.event_id_retention_limit

    def seed_for_repetition(self, repetition: int) -> int | None:
        """Return the RNG seed for a 1-based repetition, or None when no scenario seed is set."""
        if self.scenario_seed is None:
            return None
        return self.scenario_seed + repetition - 1

    def implementation_label(self) -> str:
        """Build a label naming the sync mode, retention, network profile and resource settings."""
        retention_limit = self.effective_event_id_retention_limit()
        retention = (
            "unbounded_retention"
            if retention_limit <= 0
            else f"retention_{retention_limit}"
        )
        base = f"{self.sync_mode}_{retention}_{self.network_profile}"
        if self.delta_log_limit != DEFAULT_DELTA_LOG_LIMIT:
            delta_retention = (
                "unbounded_delta_log"
                if self.delta_log_limit <= 0
                else f"delta_log_{self.delta_log_limit}"
            )
            base = f"{base}_{delta_retention}"
        if self.resource_mode == "heterogeneous":
            base = f"{base}_{self.resource_profile or DEFAULT_RESOURCE_PROFILE}"
        if self.docker_cpus:
            return (
                f"{base}_cpu_{self.docker_cpus:g}_mem_{self.docker_memory or 'default'}"
            )
        if self.docker_memory:
            return f"{base}_mem_{self.docker_memory}"
        return base


def initial_eval_state() -> dict[str, Any]:
    """Return a fresh evaluation state dict with no run in progress."""
    state = {
        "running": False,
        "run_id": None,
        "started_at": None,
        "progress": 0,
        "total_steps": 0,
        "current_step": "",
        "results": [],
        "error": None,
        "csv_data": None,
    }
    state.update(_progress_defaults())
    return state


def _progress_defaults() -> dict[str, Any]:
    """Return the default values for the per-step progress fields."""
    return {
        "completed_steps": 0,
        "steps_remaining": 0,
        "current_repetition": None,
        "total_repetitions": 0,
        "repetitions_remaining": 0,
        "current_node_count": None,
        "current_scenario": None,
        "current_partition_split": None,
    }


def _percentile(values: list[float], percentile: float) -> float | None:
    """Return the nearest-rank percentile (0 to 1) of values, or None when empty."""
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, math.ceil(percentile * len(ordered)) - 1)
    return ordered[min(index, len(ordered) - 1)]


def _semantic_state(state: dict[str, Any]) -> dict[str, Any]:
    """Reduce a node /state payload to its CRDT values, dropping metadata."""
    return {
        "counters": {
            key: value.get("total_value")
            for key, value in state.get("counters", {}).items()
        },
        "registers": {
            key: value.get("value") for key, value in state.get("registers", {}).items()
        },
        "pn_counters": {
            key: value.get("total_value")
            for key, value in state.get("pn_counters", {}).items()
        },
        "sets": {
            key: value.get("active_elements", [])
            for key, value in state.get("sets", {}).items()
        },
    }


class EvaluationRunner:
    """Runs evaluation scenarios against Docker-managed nodes and collects results.

    Docker access goes through docker_mgr; the callables locate service URLs,
    discover gateways and resync gateways with the current node set.
    """

    def __init__(
        self,
        *,
        docker_mgr,
        candidate_urls: Callable[..., list[str]],
        discover_gateways: Callable[[], Awaitable[list[dict]]],
        sync_gateways_with_nodes: Callable[..., Awaitable[Any]],
    ):
        self.docker_mgr = docker_mgr
        self.candidate_urls = candidate_urls
        self.discover_gateways = discover_gateways
        self.sync_gateways_with_nodes = sync_gateways_with_nodes
        self.state = initial_eval_state()
        self._resource_samples = []
        self._resource_stop = None
        self._resource_task = None
        self._network_profile_receipt = {}
        self._resource_assignments: dict[str, dict[str, Any]] = {}
        self._resource_allocation_totals: dict[str, float | int | None] = {}
        self._resource_limit_verified = False
        self._netem_profile: str | None = None
        self._partition_applied = False

    async def _collect_resource_sample(self) -> None:
        """Take one Docker resource snapshot in a thread and store it if non-empty."""
        if not hasattr(self.docker_mgr, "resource_snapshot"):
            return
        sample = await asyncio.to_thread(self.docker_mgr.resource_snapshot)
        if sample:
            self._resource_samples.append(sample)

    async def _resource_sample_loop(self) -> None:
        """Collect a resource sample about once per second until the stop event is set."""
        while self._resource_stop and not self._resource_stop.is_set():
            await self._collect_resource_sample()
            try:
                await asyncio.wait_for(self._resource_stop.wait(), timeout=1.0)
            except TimeoutError:
                continue

    async def _start_resource_sampling(self) -> None:
        """Stop any running sampler, clear samples, and start a new background sampling task."""
        await self._stop_resource_sampling()
        self._resource_samples = []
        if not hasattr(self.docker_mgr, "resource_snapshot"):
            return
        self._resource_stop = asyncio.Event()
        self._resource_task = asyncio.create_task(self._resource_sample_loop())

    async def _stop_resource_sampling(self) -> None:
        """Stop the sampling task, take one final sample, and reset the sampler handles."""
        if self._resource_stop:
            self._resource_stop.set()
        if self._resource_task:
            await self._resource_task
            await self._collect_resource_sample()
        self._resource_stop = None
        self._resource_task = None

    def _resource_summary(self) -> dict[str, Any]:
        """Summarize collected samples into CPU, memory, throttling, I/O and per-tier metrics.

        CPU percent is derived from deltas between consecutive cumulative CPU
        counters per node; counters such as throttling and network bytes are
        reported as growth between each node's first and last sample. Per-tier
        values are relative to the assigned CPU and memory limits.
        """
        samples = [item for sample in self._resource_samples for item in sample]
        empty_tier_fields = {
            f"{tier}_{metric}": 0.0
            for tier in ("low", "medium", "high")
            for metric in (
                "cpu_limit_mean_percent",
                "cpu_limit_peak_percent",
                "memory_limit_mean_percent",
                "memory_limit_peak_percent",
            )
        }
        if not samples:
            return {
                "resource_sample_count": 0,
                "cpu_mean_percent": 0.0,
                "cpu_peak_percent": 0.0,
                "memory_mean_mib": 0.0,
                "memory_peak_mib": 0.0,
                "cpu_periods": 0,
                "cpu_throttled_periods": 0,
                "cpu_throttled_period_percent": 0.0,
                "cpu_throttled_time_s": 0.0,
                "per_node_resource_metrics": "{}",
                "resource_tier_metrics": "{}",
                "network_rx_bytes": 0,
                "network_tx_bytes": 0,
                "block_read_bytes": 0,
                "block_write_bytes": 0,
                **empty_tier_fields,
            }

        cpu_by_node: dict[str, list[float]] = {}
        previous_cpu_by_node: dict[str, tuple[int, int]] = {}
        for resource_sample in self._resource_samples:
            for sample in resource_sample:
                node = str(sample["node"])
                cpu_by_node.setdefault(node, [])
                if "cpu_usage_ns" not in sample or "system_cpu_usage_ns" not in sample:
                    if "cpu_percent" in sample:
                        cpu_by_node[node].append(float(sample["cpu_percent"]))
                    continue
                current = (
                    int(sample["cpu_usage_ns"]),
                    int(sample["system_cpu_usage_ns"]),
                )
                previous = previous_cpu_by_node.get(node)
                previous_cpu_by_node[node] = current
                if previous is None:
                    continue
                cpu_delta = current[0] - previous[0]
                system_delta = current[1] - previous[1]
                if cpu_delta > 0 and system_delta > 0:
                    cpu_by_node[node].append(
                        (cpu_delta / system_delta)
                        * max(int(sample.get("online_cpus", 1)), 1)
                        * 100
                    )

        memory_by_node: dict[str, list[int]] = {}
        first_by_node: dict[str, dict[str, Any]] = {}
        last_by_node: dict[str, dict[str, Any]] = {}
        for sample in samples:
            node = str(sample["node"])
            memory_by_node.setdefault(node, []).append(int(sample["memory_bytes"]))
            first_by_node.setdefault(node, sample)
            last_by_node[node] = sample

        def counter_growth(key: str) -> int:
            """Sum the non-negative growth of a cumulative counter across all nodes."""
            return sum(
                max(
                    int(last_by_node[node].get(key, 0))
                    - int(first_by_node[node].get(key, 0)),
                    0,
                )
                for node in last_by_node
            )

        mib = 1024 * 1024
        per_node_metrics: dict[str, dict[str, Any]] = {}
        tier_cpu_utilization: dict[str, list[float]] = {}
        tier_memory_utilization: dict[str, list[float]] = {}
        tier_nodes: dict[str, set[str]] = {}
        for node in sorted(memory_by_node):
            assignment = self._resource_assignments.get(node, {})
            tier = str(assignment.get("tier") or "uniform")
            cpu_limit = assignment.get("cpus")
            memory_limit_bytes = (
                int(assignment["memory_mib"]) * mib
                if assignment.get("memory_mib") is not None
                else int(last_by_node[node].get("memory_limit_bytes") or 0)
            )
            cpu_values = cpu_by_node.get(node, [])
            memory_values = memory_by_node[node]
            cpu_limit_values = (
                [value / float(cpu_limit) for value in cpu_values] if cpu_limit else []
            )
            memory_limit_values = (
                [(value / memory_limit_bytes) * 100 for value in memory_values]
                if memory_limit_bytes > 0
                else []
            )
            tier_nodes.setdefault(tier, set()).add(node)
            tier_cpu_utilization.setdefault(tier, []).extend(cpu_limit_values)
            tier_memory_utilization.setdefault(tier, []).extend(memory_limit_values)
            cpu_periods = max(
                int(last_by_node[node].get("cpu_periods", 0))
                - int(first_by_node[node].get("cpu_periods", 0)),
                0,
            )
            cpu_throttled_periods = max(
                int(last_by_node[node].get("cpu_throttled_periods", 0))
                - int(first_by_node[node].get("cpu_throttled_periods", 0)),
                0,
            )
            cpu_throttled_time_ns = max(
                int(last_by_node[node].get("cpu_throttled_time_ns", 0))
                - int(first_by_node[node].get("cpu_throttled_time_ns", 0)),
                0,
            )
            per_node_metrics[node] = {
                "tier": tier,
                "cpu_limit": cpu_limit,
                "cpu_limit_mean_percent": round(
                    sum(cpu_limit_values) / len(cpu_limit_values), 3
                )
                if cpu_limit_values
                else 0.0,
                "cpu_limit_peak_percent": round(max(cpu_limit_values), 3)
                if cpu_limit_values
                else 0.0,
                "memory_limit_mib": round(memory_limit_bytes / mib, 3)
                if memory_limit_bytes
                else None,
                "memory_limit_mean_percent": round(
                    sum(memory_limit_values) / len(memory_limit_values), 3
                )
                if memory_limit_values
                else 0.0,
                "memory_limit_peak_percent": round(max(memory_limit_values), 3)
                if memory_limit_values
                else 0.0,
                "cpu_periods": cpu_periods,
                "cpu_throttled_periods": cpu_throttled_periods,
                "cpu_throttled_period_percent": round(
                    (cpu_throttled_periods / cpu_periods) * 100, 3
                )
                if cpu_periods
                else 0.0,
                "cpu_throttled_time_s": round(cpu_throttled_time_ns / 1e9, 6),
            }

        tier_metrics: dict[str, dict[str, float | int]] = {}
        tier_fields: dict[str, float] = {
            str(key): float(value) for key, value in empty_tier_fields.items()
        }
        for tier in sorted(tier_nodes):
            cpu_values = tier_cpu_utilization.get(tier, [])
            memory_values = tier_memory_utilization.get(tier, [])
            summary = {
                "node_count": len(tier_nodes[tier]),
                "cpu_limit_mean_percent": round(sum(cpu_values) / len(cpu_values), 3)
                if cpu_values
                else 0.0,
                "cpu_limit_peak_percent": round(max(cpu_values), 3)
                if cpu_values
                else 0.0,
                "memory_limit_mean_percent": round(
                    sum(memory_values) / len(memory_values), 3
                )
                if memory_values
                else 0.0,
                "memory_limit_peak_percent": round(max(memory_values), 3)
                if memory_values
                else 0.0,
            }
            tier_metrics[tier] = summary
            if tier in {"low", "medium", "high"}:
                for metric in (
                    "cpu_limit_mean_percent",
                    "cpu_limit_peak_percent",
                    "memory_limit_mean_percent",
                    "memory_limit_peak_percent",
                ):
                    tier_fields[f"{tier}_{metric}"] = float(summary[metric])

        cpu_values = [value for values in cpu_by_node.values() for value in values]
        memory_values = [
            value for values in memory_by_node.values() for value in values
        ]
        cpu_periods = counter_growth("cpu_periods")
        cpu_throttled_periods = counter_growth("cpu_throttled_periods")
        cpu_throttled_time_ns = counter_growth("cpu_throttled_time_ns")
        return {
            "resource_sample_count": len(self._resource_samples),
            "cpu_mean_percent": (
                round(sum(cpu_values) / len(cpu_values), 3) if cpu_values else 0.0
            ),
            "cpu_peak_percent": round(max(cpu_values), 3) if cpu_values else 0.0,
            "memory_mean_mib": round(
                (sum(memory_values) / len(memory_values)) / mib, 3
            ),
            "memory_peak_mib": round(max(memory_values) / mib, 3),
            "cpu_periods": cpu_periods,
            "cpu_throttled_periods": cpu_throttled_periods,
            "cpu_throttled_period_percent": round(
                (cpu_throttled_periods / cpu_periods) * 100, 3
            )
            if cpu_periods
            else 0.0,
            "cpu_throttled_time_s": round(cpu_throttled_time_ns / 1e9, 6),
            "per_node_resource_metrics": json.dumps(
                per_node_metrics, sort_keys=True, separators=(",", ":")
            ),
            "resource_tier_metrics": json.dumps(
                tier_metrics, sort_keys=True, separators=(",", ":")
            ),
            "network_rx_bytes": counter_growth("network_rx_bytes"),
            "network_tx_bytes": counter_growth("network_tx_bytes"),
            "block_read_bytes": counter_growth("block_read_bytes"),
            "block_write_bytes": counter_growth("block_write_bytes"),
            **tier_fields,
        }

    def validate_config(self, cfg: EvalScenarioConfig) -> None:
        """Raise ValueError if the config names unknown modes, profiles or scenarios, or is inconsistent."""
        if cfg.sync_mode not in SYNC_MODES:
            supported = ", ".join(sorted(SYNC_MODES))
            raise ValueError(f"sync_mode must be one of: {supported}")
        if cfg.network_profile not in NETWORK_PROFILES:
            supported = ", ".join(sorted(NETWORK_PROFILES))
            raise ValueError(f"network_profile must be one of: {supported}")
        if cfg.resource_mode not in RESOURCE_MODES:
            raise ValueError("resource_mode must be uniform or heterogeneous")
        if cfg.resource_mode == "heterogeneous":
            profile_name = cfg.resource_profile or DEFAULT_RESOURCE_PROFILE
            if profile_name not in RESOURCE_PROFILES:
                supported = ", ".join(sorted(RESOURCE_PROFILES))
                raise ValueError(f"resource_profile must be one of: {supported}")
            block_size = resource_profile_block_size(profile_name)
            invalid_counts = [
                node_count
                for node_count in cfg.node_counts
                if node_count % block_size != 0
            ]
            if invalid_counts:
                raise ValueError(
                    f"{profile_name} requires node counts divisible by {block_size}"
                )
            uniform_control = RESOURCE_PROFILES[profile_name]["uniform_control"]
            if cfg.docker_cpus is not None and not math.isclose(
                cfg.docker_cpus, float(uniform_control["cpus"])
            ):
                raise ValueError(
                    f"{profile_name} uses {uniform_control['cpus']} CPU as its "
                    "equal-budget uniform control"
                )
            if cfg.docker_memory is not None and cfg.docker_memory.lower() != str(
                uniform_control["memory"]
            ):
                raise ValueError(
                    f"{profile_name} uses {uniform_control['memory']} memory as its "
                    "equal-budget uniform control"
                )
        if (
            cfg.sync_mode == "optimized_full_state"
            and cfg.event_id_retention_limit <= 0
        ):
            raise ValueError(
                "optimized_full_state requires a positive event_id_retention_limit"
            )
        if cfg.event_id_retention_limit < 0:
            raise ValueError("event_id_retention_limit must be zero or greater")
        if cfg.delta_log_limit < 0:
            raise ValueError("delta_log_limit must be zero or greater")
        if cfg.repetitions < 1:
            raise ValueError("repetitions must be at least 1")
        unknown_scenarios = sorted(set(cfg.scenarios) - SCENARIOS)
        if unknown_scenarios:
            raise ValueError(
                f"scenarios must contain only {', '.join(sorted(SCENARIOS))}"
            )
        if self._scenario_step_count(cfg) == 0:
            raise ValueError("selected scenarios produce no evaluation steps")

    def _enabled_scenarios(self, cfg: EvalScenarioConfig) -> set[str]:
        """Return the set of scenarios selected in the config."""
        return set(cfg.scenarios)

    def active_partition_splits(self, cfg: EvalScenarioConfig) -> list[float]:
        """Return the partition splits to run, or an empty list when partition_heal is disabled."""
        if "partition_heal" in self._enabled_scenarios(cfg):
            return cfg.partition_splits
        return []

    def _scenario_step_count(self, cfg: EvalScenarioConfig) -> int:
        """Count the scenario steps run per node count and repetition."""
        scenarios = self._enabled_scenarios(cfg)
        count = 0
        if "baseline" in scenarios:
            count += 1
        if "high_load" in scenarios:
            count += 1
        count += sum(scenario in scenarios for scenario in SHARED_WORKLOADS)
        count += len(self.active_partition_splits(cfg))
        return count

    def planned_steps(self, cfg: EvalScenarioConfig) -> int:
        """Return the total number of scenario steps the run will execute."""
        return cfg.repetitions * len(cfg.node_counts) * self._scenario_step_count(cfg)

    def status(self) -> dict[str, Any]:
        """Return a public snapshot of run progress, without the result rows or CSV."""
        return {
            "running": self.state["running"],
            "run_id": self.state["run_id"],
            "started_at": self.state["started_at"],
            "progress": self.state["progress"],
            "completed_steps": self.state["completed_steps"],
            "total_steps": self.state["total_steps"],
            "steps_remaining": self.state["steps_remaining"],
            "current_step": self.state["current_step"],
            "current_repetition": self.state["current_repetition"],
            "total_repetitions": self.state["total_repetitions"],
            "repetitions_remaining": self.state["repetitions_remaining"],
            "current_node_count": self.state["current_node_count"],
            "current_scenario": self.state["current_scenario"],
            "current_partition_split": self.state["current_partition_split"],
            "error": self.state["error"],
            "results_count": len(self.state["results"]),
            "has_csv": self.state["csv_data"] is not None,
        }

    def _set_progress(
        self,
        *,
        current_step: str,
        completed_steps: int,
        cfg: EvalScenarioConfig,
        repetition: int,
        node_count: int,
        scenario: str,
        partition_split: str | None = None,
    ) -> None:
        """Update the progress fields in the run state for the current step."""
        self.state["current_step"] = current_step
        self.state["progress"] = completed_steps
        self.state["completed_steps"] = completed_steps
        self.state["steps_remaining"] = max(
            self.state["total_steps"] - completed_steps, 0
        )
        self.state["current_repetition"] = repetition
        self.state["total_repetitions"] = cfg.repetitions
        self.state["repetitions_remaining"] = max(cfg.repetitions - repetition, 0)
        self.state["current_node_count"] = node_count
        self.state["current_scenario"] = scenario
        self.state["current_partition_split"] = partition_split

    async def send_events(
        self,
        session,
        nodes,
        count,
        *,
        category="sensor",
        concurrency=25,
        rng: random.Random,
        offered_rate: float | None = None,
        shared_workload: str | None = None,
    ):
        """POST count events to each node and summarize the outcomes.

        Events are spread round-robin over nodes with bounded concurrency and,
        when offered_rate is set, paced to that many events per second. Tracking
        IDs come from rng so runs are reproducible. shared_workload makes every
        event target the same key of the given category. Returns counts of
        accepted, rejected, duplicate and failed requests, tracking IDs, latencies
        and rates.
        """
        started_at = time.perf_counter()
        result = {
            "attempted": count * len(nodes),
            "accepted": 0,
            "rejected": 0,
            "duplicates": 0,
            "request_errors": 0,
            "tracking_ids": [],
            "attempted_tracking_ids": [],
            "failed_request_latencies_ms": [],
            "request_latencies_ms": [],
            "first_submitted_at": None,
            "last_submitted_at": None,
        }
        event_types = [
            "water_level",
            "temperature",
            "shelter_occupancy",
            "road_status",
            "wind_speed",
            "air_quality",
        ]
        semaphore = asyncio.Semaphore(max(1, int(concurrency or 1)))

        async def _send_one(i, target):
            """Submit event i to target and return (outcome, tracking_id, submitted_at, latency_ms)."""
            if offered_rate is not None:
                delay = (i / offered_rate) - (time.perf_counter() - started_at)
                if delay > 0:
                    await asyncio.sleep(delay)
            node_url = target.get("internal_url") or target.get("url")
            submitted_at = time.time()
            if not node_url:
                return "error", None, submitted_at, None

            if shared_workload is None:
                event_type = event_types[i % len(event_types)]
                event_category = category
                value = round(rng.uniform(1.0, 100.0), 2)
                location = f"{category}_{i}"
            else:
                event_category = shared_workload
                event_type = {
                    "sensor": "water_level",
                    "resource": "shelter_occupancy",
                    "infrastructure": "road_block",
                }[shared_workload]
                if shared_workload == "resource":
                    value = 1
                elif shared_workload == "infrastructure":
                    value = "active"
                else:
                    value = round(rng.uniform(1.0, 100.0), 2)
                location = "shared_site"
            tracking_id = str(uuid.UUID(int=rng.getrandbits(128)))
            payload = {
                "id": tracking_id,
                "type": event_type,
                "value": value,
                "location": location,
                "category": event_category,
                "metadata": {
                    "tracking_id": tracking_id,
                    "source": "evaluation",
                    "evaluation_submitted_at": submitted_at,
                },
            }

            request_started = time.perf_counter()
            try:
                async with semaphore:
                    async with session.post(
                        f"{node_url}/event", json=payload
                    ) as response:
                        try:
                            response_data = await response.json()
                        except Exception:
                            response_data = {}
                        latency_ms = (time.perf_counter() - request_started) * 1000
                        status = response_data.get("status")
                        if response.status < 400 and status != "duplicate":
                            return "accepted", tracking_id, submitted_at, latency_ms
                        outcome = "duplicate" if status == "duplicate" else "rejected"
                        return outcome, tracking_id, submitted_at, latency_ms
            except Exception:
                return "error", tracking_id, submitted_at, None

        submissions = [
            _send_one(i, nodes[i % len(nodes)]) for i in range(count * len(nodes))
        ]
        for status, tracking_id, submitted_at, latency_ms in await asyncio.gather(
            *submissions
        ):
            if (
                result["first_submitted_at"] is None
                or submitted_at < result["first_submitted_at"]
            ):
                result["first_submitted_at"] = submitted_at
            if (
                result["last_submitted_at"] is None
                or submitted_at > result["last_submitted_at"]
            ):
                result["last_submitted_at"] = submitted_at

            if tracking_id is not None:
                result["attempted_tracking_ids"].append(tracking_id)

            if status == "duplicate":
                if latency_ms is not None:
                    result["failed_request_latencies_ms"].append(latency_ms)
                result["duplicates"] += 1
                continue
            if status == "rejected":
                if latency_ms is not None:
                    result["failed_request_latencies_ms"].append(latency_ms)
                result["rejected"] += 1
                continue
            if status == "error":
                result["request_errors"] += 1
                continue

            if latency_ms is not None:
                result["request_latencies_ms"].append(latency_ms)
            result["accepted"] += 1
            result["tracking_ids"].append(tracking_id)

        duration = max(time.perf_counter() - started_at, 0.000001)
        result["submission_duration_s"] = duration
        submission_span = (
            result["last_submitted_at"] - result["first_submitted_at"]
            if result["first_submitted_at"] is not None
            and result["last_submitted_at"] is not None
            else 0.0
        )
        result["offered_rate_events_s"] = (
            (result["attempted"] - 1) / submission_span
            if result["attempted"] > 1 and submission_span > 0
            else 0.0
        )
        result["accepted_throughput_events_s"] = result["accepted"] / duration
        return result

    async def poll_all_gateways(self, session, gateways):
        """Ask each gateway to poll its nodes now, trying each candidate URL; log failures."""
        for gw in gateways:
            polled = False
            for base_url in self.candidate_urls(gw.get("url"), gw.get("external_url")):
                try:
                    async with session.post(f"{base_url}/gateway/poll") as resp:
                        if resp.status < 400:
                            await resp.json()
                            polled = True
                            break
                except Exception:
                    continue
            if not polled:
                log.warning("evaluation_gateway_poll_failed", gateway=gw.get("name"))

    async def collect_metrics(
        self,
        session,
        nodes,
        gateways,
        accepted_events=None,
        tracking_ids=None,
        attempted_tracking_ids=None,
        *,
        include_state: bool,
    ):
        """Gather gateway and node metrics into one flat result dict.

        Reads gateway runtime metrics, node gossip stats and Merkle roots. When
        include_state is True it also fetches each node's full /state to compute
        state sizes, semantic convergence and per-event arrival times from the
        first_seen_tracking metadata (time_to_last_node_s, percentiles, missing
        events). protocol_sync_bytes_total excludes evaluation tracking bytes.
        """
        last_convergence = None
        known_statuses = []
        coverage_values = []
        gateway_staleness_values = []
        for gw in gateways:
            gw_metrics = {}
            gw_status = None
            gw_coverage = None
            gw_visibility = {}
            for base_url in self.candidate_urls(gw.get("url"), gw.get("external_url")):
                try:
                    async with session.get(
                        f"{base_url}/gateway/runtime-metrics"
                    ) as resp:
                        if resp.status >= 400:
                            continue
                        gw_data = await resp.json()
                        gw_metrics = gw_data.get("runtime_metrics", {})
                        gw_status = gw_data.get("known_status")
                        gw_coverage = gw_data.get("coverage")
                        gw_visibility = gw_data.get("visibility") or {}
                        break
                except Exception:
                    continue

            convergence_value = gw_metrics.get("last_convergence_seconds")
            if convergence_value is not None:
                if last_convergence is None or convergence_value > last_convergence:
                    last_convergence = convergence_value
            if isinstance(gw_status, str) and gw_status:
                known_statuses.append(gw_status)
            if isinstance(gw_coverage, (int, float)):
                coverage_values.append(gw_coverage)
            staleness = gw_visibility.get("max_observation_age_seconds")
            if isinstance(staleness, (int, float)):
                gateway_staleness_values.append(max(0.0, staleness))

        if any(status == "known_diverged" for status in known_statuses):
            known_status = "known_diverged"
        elif any(status == "unknown_partial" for status in known_statuses):
            known_status = "unknown_partial"
        elif known_statuses:
            known_status = "known_converged"
        else:
            known_status = "unknown_partial"

        coverage_percent = (
            round((min(coverage_values) * 100.0), 1) if coverage_values else 0.0
        )
        total_gossip = 0
        total_state_size = 0
        gossip_summary_sent_bytes = 0
        gossip_summary_received_bytes = 0
        full_state_repair_bytes = 0
        semantic_state_bytes = 0
        evaluation_metadata_state_bytes = 0
        nonsemantic_state_metadata_bytes = 0
        repair_pull_count = 0
        repair_pulls_failed = 0
        delta_pulls_started = 0
        delta_pulls_success = 0
        delta_pulls_gap = 0
        delta_deltas_applied = 0
        delta_bytes_received = 0
        delta_request_bytes = 0
        tracking_bytes_received = 0
        event_counts = []
        merkle_roots = []
        semantic_states = []
        per_node_tracking_ids: dict[str, set[str]] = {}
        accepted_tracking_ids = set(tracking_ids or [])
        attempted_tracking_ids = set(
            attempted_tracking_ids
            if attempted_tracking_ids is not None
            else accepted_tracking_ids
        )
        tracking_seen_counts = {
            tracking_id: 0 for tracking_id in attempted_tracking_ids
        }
        tracking_seen_latencies = {
            tracking_id: [] for tracking_id in attempted_tracking_ids
        }
        tracking_last_seen_latency = {}
        tracking_last_seen_round = {}
        arrival_latencies_by_tier: dict[str, list[float]] = {}
        latest_arrival_by_node: dict[str, float] = {}
        latest_first_seen_at: float | None = None

        for node in nodes:
            node_url = node.get("internal_url") or node.get("url")
            if not node_url:
                continue
            try:
                async with session.get(f"{node_url}/gossip/stats") as resp:
                    gdata = await resp.json()
                    stats = gdata.get("stats", {})
                    total_gossip += stats.get("sent", 0)
                    node_delta_bytes = stats.get("delta_bytes_received", 0)
                    gossip_summary_sent_bytes += stats.get("sent_bytes", 0)
                    gossip_summary_received_bytes += stats.get("received_bytes", 0)
                    full_state_repair_bytes += stats.get("repair_bytes_received", 0)
                    repair_pull_count += stats.get("repair_pulls_started", 0)
                    repair_pulls_failed += stats.get("repair_pulls_failed", 0)
                    delta_pulls_started += stats.get("delta_pulls_started", 0)
                    delta_pulls_success += stats.get("delta_pulls_success", 0)
                    delta_pulls_gap += stats.get("delta_pulls_gap", 0)
                    delta_deltas_applied += stats.get("delta_deltas_applied", 0)
                    delta_bytes_received += node_delta_bytes
                    delta_request_bytes += stats.get("delta_request_bytes_sent", 0)
                    tracking_bytes_received += stats.get("tracking_bytes_received", 0)
            except Exception:
                pass

            if include_state:
                try:
                    async with session.get(f"{node_url}/state") as resp:
                        sdata = await resp.json()
                        state_size = len(json.dumps(sdata))
                        semantic = _semantic_state(sdata)
                        semantic_size = len(json.dumps(semantic))
                        evaluation_metadata_size = len(
                            json.dumps(sdata.get("first_seen_tracking") or {})
                        )
                        total_state_size += state_size
                        semantic_state_bytes += semantic_size
                        evaluation_metadata_state_bytes += evaluation_metadata_size
                        nonsemantic_state_metadata_bytes += max(
                            state_size - semantic_size - evaluation_metadata_size,
                            0,
                        )
                        summary = sdata.get("state_summary", {})
                        event_count = summary.get("total_events", 0)
                        if event_count == 0:
                            event_ids = sdata.get(
                                "recent_event_ids", sdata.get("event_ids", [])
                            )
                            event_count = (
                                len(event_ids) if isinstance(event_ids, list) else 0
                            )
                        event_counts.append(event_count)
                        semantic_states.append(semantic)

                        node_id = (
                            sdata.get("node_id") or node.get("node_id") or node_url
                        )
                        seen_tracking = sdata.get("first_seen_tracking") or {}
                        per_node_tracking_ids[str(node_id)] = set(seen_tracking)
                        node_key = str(node_id)
                        container_name = (
                            node_key
                            if node_key.startswith("edge-node-")
                            else f"edge-node-{node_key.replace('node-', '')}"
                        )
                        resource_tier = str(
                            self._resource_assignments.get(container_name, {}).get(
                                "tier", "uniform"
                            )
                        )
                        for tracking_id, seen in seen_tracking.items():
                            if (
                                attempted_tracking_ids
                                and tracking_id not in attempted_tracking_ids
                            ):
                                continue
                            if tracking_id in tracking_seen_counts:
                                tracking_seen_counts[tracking_id] += 1
                            submitted_at = seen.get("submitted_at")
                            first_seen_at = seen.get("first_seen_at")
                            if isinstance(submitted_at, (int, float)) and isinstance(
                                first_seen_at, (int, float)
                            ):
                                latency = max(0.0, first_seen_at - submitted_at)
                                tracking_seen_latencies[tracking_id].append(latency)
                                tracking_last_seen_latency[tracking_id] = max(
                                    tracking_last_seen_latency.get(tracking_id, 0.0),
                                    latency,
                                )
                                arrival_latencies_by_tier.setdefault(
                                    resource_tier, []
                                ).append(latency)
                                latest_arrival_by_node[node_key] = max(
                                    latest_arrival_by_node.get(node_key, 0.0),
                                    latency,
                                )
                                latest_first_seen_at = max(
                                    latest_first_seen_at or first_seen_at, first_seen_at
                                )
                            round_number = seen.get("gossip_round")
                            if isinstance(round_number, int):
                                tracking_last_seen_round[tracking_id] = max(
                                    tracking_last_seen_round.get(tracking_id, 0),
                                    round_number,
                                )
                except Exception:
                    pass

            try:
                async with session.get(f"{node_url}/state/merkle") as resp:
                    mdata = await resp.json()
                    merkle_roots.append(mdata.get("merkle_root", ""))
            except Exception:
                pass

        max_ev = max(event_counts) if event_counts else 0
        min_ev = min(event_counts) if event_counts else 0
        expected_events = (
            len(attempted_tracking_ids)
            if attempted_tracking_ids
            else accepted_events
            if accepted_events is not None
            else max_ev
        )
        fully_seen_tracking = [
            tracking_id
            for tracking_id, count in tracking_seen_counts.items()
            if count >= len(nodes)
        ]
        missing_tracking_ids = attempted_tracking_ids - set(fully_seen_tracking)
        if include_state and attempted_tracking_ids:
            missing_at_cutoff_count = len(missing_tracking_ids)
        else:
            missing_at_cutoff_count = (
                max(expected_events - min_ev, 0) if include_state else expected_events
            )
        missing_at_cutoff_pct = (
            round((missing_at_cutoff_count / expected_events) * 100, 2)
            if expected_events > 0
            else 0.0
        )
        per_node_missing_tracking_ids = {
            node_id: len(attempted_tracking_ids - seen_ids)
            for node_id, seen_ids in per_node_tracking_ids.items()
        }
        tracking_coverage_percent = (
            round((len(fully_seen_tracking) / len(tracking_seen_counts)) * 100, 1)
            if tracking_seen_counts
            else 0.0
        )
        time_to_last_node_s = (
            round(
                max(
                    tracking_last_seen_latency.get(tracking_id, 0.0)
                    for tracking_id in fully_seen_tracking
                ),
                3,
            )
            if fully_seen_tracking
            else None
        )
        rounds_to_last_node = (
            max(
                tracking_last_seen_round.get(tracking_id, 0)
                for tracking_id in fully_seen_tracking
            )
            if fully_seen_tracking
            else None
        )
        first_remote_latencies = []
        last_node_latencies = []
        for tracking_id in fully_seen_tracking:
            latencies = sorted(tracking_seen_latencies.get(tracking_id, []))
            if len(latencies) > 1:
                first_remote_latencies.append(latencies[1])
            if latencies:
                last_node_latencies.append(latencies[-1])
        last_node_to_converge = None
        last_node_resource_tier = None
        if (
            attempted_tracking_ids
            and not missing_tracking_ids
            and len(per_node_tracking_ids) == len(nodes)
            and latest_arrival_by_node
        ):
            last_node_to_converge = max(
                latest_arrival_by_node,
                key=lambda node_id: latest_arrival_by_node[node_id],
            )
            last_container = (
                last_node_to_converge
                if last_node_to_converge.startswith("edge-node-")
                else f"edge-node-{last_node_to_converge.replace('node-', '')}"
            )
            last_node_resource_tier = self._resource_assignments.get(
                last_container, {}
            ).get("tier")

        tier_arrival_metrics = {
            f"{tier}_event_arrival_{percentile_name}_s": _percentile(
                arrival_latencies_by_tier.get(tier, []), percentile
            )
            for tier in ("low", "medium", "high")
            for percentile_name, percentile in (("p50", 0.5), ("p95", 0.95))
        }

        sync_bytes_total = (
            gossip_summary_sent_bytes
            + full_state_repair_bytes
            + delta_bytes_received
            + delta_request_bytes
        )
        unique_roots = len(set(root for root in merkle_roots if root))
        direct_roots_converged = len(merkle_roots) == len(nodes) and unique_roots == 1
        semantic_fingerprints = {
            json.dumps(state, sort_keys=True, separators=(",", ":"))
            for state in semantic_states
        }
        unique_semantic_states = len(semantic_fingerprints)
        semantic_state_converged = (
            len(semantic_states) == len(nodes) and unique_semantic_states == 1
        )
        if direct_roots_converged:
            known_status = "known_converged"
        avg_state_size_bytes = (
            round(total_state_size / len(nodes), 0) if include_state and nodes else 0
        )

        return {
            "convergence_time_s": last_convergence,
            "missing_at_cutoff_count": missing_at_cutoff_count,
            "missing_at_cutoff_pct": missing_at_cutoff_pct,
            "gateway_staleness_seconds": max(gateway_staleness_values)
            if gateway_staleness_values
            else None,
            "time_to_last_node_s": time_to_last_node_s,
            "latest_first_seen_at_unix_s": latest_first_seen_at,
            "rounds_to_last_node": rounds_to_last_node,
            "last_node_to_converge": last_node_to_converge,
            "last_node_resource_tier": last_node_resource_tier,
            **tier_arrival_metrics,
            "tracking_coverage_percent": tracking_coverage_percent,
            "time_to_first_remote_node_p50_s": _percentile(first_remote_latencies, 0.5),
            "time_to_first_remote_node_p95_s": _percentile(
                first_remote_latencies, 0.95
            ),
            "time_to_last_node_p50_s": _percentile(last_node_latencies, 0.5),
            "time_to_last_node_p95_s": _percentile(last_node_latencies, 0.95),
            "accepted_tracking_ids_count": len(accepted_tracking_ids),
            "attempted_tracking_ids_count": len(attempted_tracking_ids),
            "fully_observed_tracking_ids_count": len(fully_seen_tracking),
            "missing_tracking_ids_count": len(missing_tracking_ids),
            "per_node_missing_tracking_ids": json.dumps(
                per_node_missing_tracking_ids, sort_keys=True
            ),
            "sync_bytes_total": sync_bytes_total,
            "tracking_bytes_received": tracking_bytes_received,
            "protocol_sync_bytes_total": sync_bytes_total - tracking_bytes_received,
            "gossip_summary_sent_bytes": gossip_summary_sent_bytes,
            "gossip_summary_received_bytes": gossip_summary_received_bytes,
            "full_state_repair_bytes": full_state_repair_bytes,
            "delta_repair_bytes": delta_bytes_received,
            "delta_request_bytes": delta_request_bytes,
            "semantic_state_bytes": semantic_state_bytes,
            "evaluation_metadata_state_bytes": evaluation_metadata_state_bytes,
            "nonsemantic_state_metadata_bytes": (nonsemantic_state_metadata_bytes),
            "repair_pull_count": repair_pull_count,
            "repair_pulls_failed": repair_pulls_failed,
            "gossip_messages_sent": total_gossip,
            "total_state_size_bytes": total_state_size,
            "avg_state_size_bytes": avg_state_size_bytes,
            "avg_state_bytes_per_known_event": round(avg_state_size_bytes / max_ev, 2)
            if max_ev > 0
            else "",
            "known_status": known_status,
            "coverage_percent": coverage_percent,
            "is_converged": direct_roots_converged,
            "unique_merkle_roots": unique_roots,
            "semantic_state_converged": semantic_state_converged,
            "unique_semantic_states": unique_semantic_states,
            "node_count": len(nodes),
            "max_events": max_ev,
            "min_events": min_ev,
            "delta_pulls_started": delta_pulls_started,
            "delta_pulls_success": delta_pulls_success,
            "delta_pulls_gap": delta_pulls_gap,
            "delta_deltas_applied": delta_deltas_applied,
            "delta_bytes_received": delta_bytes_received,
        }

    async def wait_for_metrics(
        self,
        session,
        nodes,
        gateways,
        cfg,
        accepted_events,
        tracking_ids=None,
        attempted_tracking_ids=None,
    ):
        """Poll metrics until all tracked events reach every node or the wait deadline passes.

        Cheap polls skip /state; once Merkle roots agree, or at the deadline, a
        full collection runs after forcing a gateway poll. Sets
        converged_within_timeout and convergence_time_s, which is
        time_to_last_node_s when tracking IDs are given.
        """
        started = time.time()
        deadline = started + cfg.convergence_wait_seconds
        metrics = None
        gateways_polled = False

        async def _collect_final_metrics():
            """Poll gateways once, then collect metrics including full node state."""
            nonlocal gateways_polled
            if not gateways_polled:
                await self.poll_all_gateways(session, gateways)
                gateways_polled = True
            return await self.collect_metrics(
                session,
                nodes,
                gateways,
                accepted_events=accepted_events,
                tracking_ids=tracking_ids,
                include_state=True,
                attempted_tracking_ids=attempted_tracking_ids,
            )

        def _mark_converged_within_timeout(
            collected_metrics: dict[str, Any], observed_at: float
        ) -> bool:
            """Mark metrics as converged if roots agree, nothing is missing, and the time fits the deadline."""
            if (
                not collected_metrics["is_converged"]
                or collected_metrics["missing_at_cutoff_count"] != 0
            ):
                return False
            convergence_time = (
                collected_metrics.get("time_to_last_node_s")
                if attempted_tracking_ids
                else round(observed_at - started, 3)
            )
            if (
                convergence_time is None
                or float(convergence_time) > cfg.convergence_wait_seconds
            ):
                return False
            collected_metrics["convergence_time_s"] = round(float(convergence_time), 3)
            collected_metrics["converged_within_timeout"] = True
            collected_metrics["convergence_wait_seconds"] = cfg.convergence_wait_seconds
            return True

        while True:
            metrics = await self.collect_metrics(
                session,
                nodes,
                gateways,
                accepted_events=accepted_events,
                tracking_ids=tracking_ids,
                include_state=False,
                attempted_tracking_ids=attempted_tracking_ids,
            )
            observed_at = time.time()
            full_metrics_collected = False
            if metrics["is_converged"]:
                metrics = await _collect_final_metrics()
                full_metrics_collected = True
                observed_at = time.time()
                if _mark_converged_within_timeout(metrics, observed_at):
                    await self._collect_resource_sample()
                    return metrics
            if observed_at >= deadline:
                if not full_metrics_collected:
                    metrics = await _collect_final_metrics()
                    observed_at = time.time()
                if _mark_converged_within_timeout(metrics, observed_at):
                    await self._collect_resource_sample()
                    return metrics
                metrics["convergence_time_s"] = None
                metrics["converged_within_timeout"] = False
                metrics["convergence_wait_seconds"] = cfg.convergence_wait_seconds
                await self._collect_resource_sample()
                return metrics
            await asyncio.sleep(cfg.poll_interval_seconds)

    async def wait_for_nodes_ready(self, session, nodes, timeout_seconds: float):
        """Probe node /health endpoints until all respond; raise RuntimeError after timeout_seconds."""
        pending = {
            node["node_id"]: self.candidate_urls(
                node.get("internal_url"),
                node.get("url"),
                node.get("external_url"),
            )
            for node in nodes
        }
        deadline = time.time() + timeout_seconds

        async def _probe(node_id, urls):
            """Return node_id if any of its URLs answers /health successfully, else None."""
            for base_url in urls:
                try:
                    async with session.get(f"{base_url}/health") as response:
                        if response.status < 400:
                            return node_id
                except Exception:
                    continue
            return None

        while pending and time.time() < deadline:
            probes = [_probe(node_id, urls) for node_id, urls in pending.items()]
            for ready_node_id in await asyncio.gather(*probes):
                if ready_node_id:
                    pending.pop(ready_node_id, None)
            if pending:
                await asyncio.sleep(0.25)

        if pending:
            missing = ", ".join(sorted(pending))
            raise RuntimeError(f"nodes did not become ready: {missing}")

    async def _inspect_resource_limits(self) -> dict[str, dict[str, Any]]:
        """Read the actual Docker resource limits and state of the assigned nodes."""
        if not hasattr(self.docker_mgr, "inspect_node_resource_limits"):
            return {}
        node_ids = [
            str(assignment["node_id"])
            for assignment in self._resource_assignments.values()
        ]
        return await asyncio.to_thread(
            self.docker_mgr.inspect_node_resource_limits, node_ids
        )

    async def _container_health_summary(self) -> dict[str, int]:
        """Count assigned containers that are not running or were OOM-killed."""
        inspected = await self._inspect_resource_limits()
        return {
            "container_failure_count": sum(
                1 for state in inspected.values() if not state.get("running")
            ),
            "container_oom_count": sum(
                1 for state in inspected.values() if state.get("oom_killed")
            ),
        }

    def _assign_node_resources(
        self, node_count: int, cfg: EvalScenarioConfig, repetition: int
    ) -> list[dict[str, Any]]:
        """Compute and store CPU and memory assignments for the nodes of one repetition."""
        assignments = build_resource_assignments(
            node_count=node_count,
            resource_mode=cfg.resource_mode,
            resource_profile=cfg.resource_profile,
            uniform_cpus=cfg.docker_cpus,
            uniform_memory=cfg.docker_memory,
            repetition_seed=cfg.seed_for_repetition(repetition),
        )
        self._resource_assignments = {
            str(assignment["container"]): assignment for assignment in assignments
        }
        self._resource_allocation_totals = resource_totals(assignments)
        self._resource_limit_verified = False
        return assignments

    async def _verify_resource_limits(self) -> None:
        """Check the Docker limits match the assignments; raise RuntimeError on mismatch."""
        inspected = await self._inspect_resource_limits()
        if not inspected:
            return
        mismatches = []
        for container_name, assignment in self._resource_assignments.items():
            actual = inspected.get(container_name)
            expected_cpus = assignment.get("cpus")
            expected_memory_mib = assignment.get("memory_mib")
            if actual is None:
                mismatches.append(f"{container_name}: not found")
                continue
            if expected_cpus is not None and not math.isclose(
                float(actual.get("cpus") or 0),
                float(expected_cpus),
                rel_tol=0,
                abs_tol=1e-9,
            ):
                mismatches.append(f"{container_name}: CPU limit")
            if expected_memory_mib is not None and int(
                actual.get("memory_bytes") or 0
            ) != int(float(expected_memory_mib) * 1024 * 1024):
                mismatches.append(f"{container_name}: memory limit")
        if mismatches:
            raise RuntimeError(
                "node resource limit verification failed: " + ", ".join(mismatches)
            )
        self._resource_limit_verified = True

    async def _apply_repetition_resources(
        self, node_count: int, cfg: EvalScenarioConfig, repetition: int
    ) -> None:
        """Update running containers to this repetition's resource assignments and verify them."""
        assignments = self._assign_node_resources(node_count, cfg, repetition)
        receipt = await asyncio.to_thread(
            self.docker_mgr.update_node_resources, assignments
        )
        if receipt.get("status") != "updated":
            raise RuntimeError(
                receipt.get("message")
                or f"failed to update node resources for repetition {repetition}"
            )
        await self._verify_resource_limits()
        log.info(
            "eval_node_resources_updated",
            repetition=repetition,
            node_count=node_count,
            seed=cfg.seed_for_repetition(repetition),
        )

    async def _heal_partition(self) -> bool:
        """Heal partition rules this runner applied; return whether a heal ran."""
        if not self._partition_applied:
            return False
        try:
            await asyncio.to_thread(self.docker_mgr.heal_all)
        except Exception as error:
            log.warning("eval_partition_heal_failed", error=str(error))
        else:
            self._partition_applied = False
        return True

    async def _clear_network_profile(self) -> bool:
        """Clear the netem profile this runner applied; return whether a clear ran."""
        if self._netem_profile is None:
            return False
        try:
            await asyncio.to_thread(self.docker_mgr.clear_netem)
        except Exception as error:
            log.warning(
                "eval_netem_clear_failed",
                profile=self._netem_profile,
                error=str(error),
            )
        else:
            self._netem_profile = None
            self._network_profile_receipt = {}
        return True

    async def recreate_nodes(
        self,
        session,
        node_count: int,
        cfg: EvalScenarioConfig,
        repetition: int = 1,
    ):
        """Replace all node containers with node_count fresh ones and wait until they are ready.

        Heals partitions, clears netem, removes existing nodes, creates new ones
        with the configured sync mode and resource limits, verifies limits, and
        resyncs gateways. Returns (nodes, gateways).
        """
        assignments = self._assign_node_resources(node_count, cfg, repetition)

        await self._heal_partition()
        await self._clear_network_profile()
        existing = await asyncio.to_thread(self.docker_mgr.list_nodes)
        remove_tasks = [
            asyncio.to_thread(self.docker_mgr.remove_node, node["node_id"])
            for node in existing
        ]
        if remove_tasks:
            await asyncio.gather(*remove_tasks, return_exceptions=True)

        create_tasks = [
            asyncio.to_thread(
                self.docker_mgr.create_node,
                node_id=assignment["node_id"],
                sync_mode=cfg.sync_mode,
                event_id_retention_limit=cfg.effective_event_id_retention_limit(),
                delta_log_limit=cfg.delta_log_limit,
                cpu_limit=assignment["cpus"],
                memory_limit=assignment["memory"],
            )
            for assignment in assignments
        ]
        for result in await asyncio.gather(*create_tasks, return_exceptions=True):
            if isinstance(result, Exception):
                log.warning("eval_node_create_failed", error=str(result))

        nodes = await asyncio.to_thread(self.docker_mgr.list_nodes)
        if len(nodes) < node_count:
            raise RuntimeError(f"expected {node_count} nodes, found {len(nodes)}")

        await self._verify_resource_limits()

        await self.wait_for_nodes_ready(
            session, nodes, cfg.node_readiness_timeout_seconds
        )
        await self.sync_gateways_with_nodes(force=True)
        return nodes, await self.discover_gateways()

    async def reset_runtime(self, session, nodes, gateways):
        """Resync gateways, then reset evaluation state on every node and gateway."""

        async def _post_first_available(urls, path):
            """POST path to the first URL that succeeds; return whether any did."""
            for base_url in urls:
                try:
                    async with session.post(f"{base_url}{path}") as resp:
                        if resp.status < 400:
                            await resp.json()
                            return True
                except Exception:
                    continue
            return False

        await self.sync_gateways_with_nodes(force=True)

        node_tasks = []
        for node in nodes:
            urls = self.candidate_urls(
                node.get("internal_url"),
                node.get("external_url"),
                node.get("url"),
            )
            node_tasks.append(_post_first_available(urls, "/evaluation/reset"))
        if node_tasks:
            await asyncio.gather(*node_tasks, return_exceptions=True)

        gateway_tasks = []
        for gateway in gateways:
            urls = self.candidate_urls(gateway.get("url"), gateway.get("external_url"))
            gateway_tasks.append(
                _post_first_available(urls, "/gateway/evaluation/reset")
            )
        if gateway_tasks:
            await asyncio.gather(*gateway_tasks, return_exceptions=True)

    async def reset_measurement_metrics(self, session, nodes, gateways):
        """Reset scenario counters while preserving the synchronized node state."""

        async def _post_first_available(urls, path):
            """POST path to the first URL that succeeds; return whether any did."""
            for base_url in urls:
                try:
                    async with session.post(f"{base_url}{path}") as resp:
                        if resp.status < 400:
                            await resp.json()
                            return True
                except Exception:
                    continue
            return False

        node_tasks = []
        for node in nodes:
            urls = self.candidate_urls(
                node.get("internal_url"),
                node.get("external_url"),
                node.get("url"),
            )
            node_tasks.append(_post_first_available(urls, "/evaluation/reset-metrics"))
        node_results = (
            await asyncio.gather(*node_tasks, return_exceptions=True)
            if node_tasks
            else []
        )
        if not node_results or not all(result is True for result in node_results):
            raise RuntimeError("failed to reset node measurement metrics")

        gateway_tasks = []
        for gateway in gateways:
            urls = self.candidate_urls(gateway.get("url"), gateway.get("external_url"))
            gateway_tasks.append(
                _post_first_available(urls, "/gateway/evaluation/reset")
            )
        if gateway_tasks:
            await asyncio.gather(*gateway_tasks, return_exceptions=True)

    def _row_base(
        self,
        cfg,
        run_id,
        repetition,
        scenario,
        node_count,
        event_result,
        partition_split=None,
    ):
        """Build the config, network, resource and submission columns of one result row."""
        attempted = event_result.get("attempted", 0)
        accepted = event_result.get("accepted", 0)
        return {
            "run_id": run_id,
            "case_id": cfg.case_id or "",
            "condition_id": cfg.condition_id or "",
            "sweep": cfg.sweep or "",
            "repetition": repetition,
            "scenario_seed": cfg.scenario_seed,
            "repetition_seed": cfg.seed_for_repetition(repetition),
            "implementation": cfg.implementation_label(),
            "sync_mode": cfg.sync_mode,
            "event_id_retention_limit": cfg.effective_event_id_retention_limit(),
            "delta_log_limit": cfg.delta_log_limit,
            "network_profile": cfg.network_profile,
            "network_delay_ms": (
                NETWORK_PROFILE_SETTINGS.get(cfg.network_profile, {}).get(
                    "delay_ms", ""
                )
            ),
            "network_jitter_ms": (
                NETWORK_PROFILE_SETTINGS.get(cfg.network_profile, {}).get(
                    "jitter_ms", ""
                )
            ),
            "network_loss_percent": (
                NETWORK_PROFILE_SETTINGS.get(cfg.network_profile, {}).get(
                    "loss_percent", ""
                )
            ),
            "network_bandwidth_mbps": (
                NETWORK_PROFILE_SETTINGS.get(cfg.network_profile, {}).get(
                    "bandwidth_mbps", ""
                )
            ),
            "network_rules_verified": event_result.get(
                "network_rules_verified",
                self._network_profile_receipt.get("status") == "applied",
            ),
            "docker_cpus": cfg.docker_cpus or "",
            "docker_memory": cfg.docker_memory or "",
            "resource_mode": cfg.resource_mode,
            "resource_profile": (
                cfg.resource_profile or DEFAULT_RESOURCE_PROFILE
                if cfg.resource_mode == "heterogeneous"
                else ""
            ),
            "resource_total_cpus": self._resource_allocation_totals.get("total_cpus")
            or "",
            "resource_total_memory_mib": self._resource_allocation_totals.get(
                "total_memory_mib"
            )
            or "",
            "resource_limit_verified": self._resource_limit_verified,
            "node_resource_assignments": json.dumps(
                list(self._resource_assignments.values()),
                sort_keys=True,
                separators=(",", ":"),
            ),
            "scenario": scenario,
            "node_count": node_count,
            "partition_split": partition_split,
            "partition_group_a": json.dumps(event_result.get("partition_group_a", [])),
            "partition_group_b": json.dumps(event_result.get("partition_group_b", [])),
            "fault_applied_at_unix_s": event_result.get("fault_applied_at_unix_s", ""),
            "fault_removed_at_unix_s": event_result.get("fault_removed_at_unix_s", ""),
            "actual_fault_duration_s": event_result.get("actual_fault_duration_s", ""),
            "attempted_events": attempted,
            "accepted_events": accepted,
            "write_success_rate": round(accepted / attempted, 4) if attempted else 0.0,
            "duplicate_submissions": event_result.get("duplicates", 0),
            "rejected_events": event_result.get("rejected", 0),
            "request_error_count": event_result.get("request_errors", 0),
            "submission_duration_s": round(
                event_result.get("submission_duration_s", 0.0), 3
            ),
            "offered_rate_events_s": round(
                event_result.get("offered_rate_events_s", 0.0), 2
            ),
            "target_offered_rate_events_s": cfg.offered_rate_per_second or "",
            "accepted_throughput_events_s": round(
                event_result.get("accepted_throughput_events_s", 0.0), 2
            ),
            "write_latency_p50_ms": _percentile(
                event_result.get("request_latencies_ms", []), 0.5
            ),
            "write_latency_p95_ms": _percentile(
                event_result.get("request_latencies_ms", []), 0.95
            ),
            "write_latency_max_ms": max(
                event_result.get("request_latencies_ms", []) or [0.0]
            ),
            "failed_request_latency_p95_ms": _percentile(
                event_result.get("failed_request_latencies_ms", []), 0.95
            ),
        }

    def _append_result(self, row, metrics):
        """Merge a row with metrics and resource summary, add per-event byte ratios, and store it."""
        combined = {**row, **metrics, **self._resource_summary()}
        accepted = int(combined.get("accepted_events", 0) or 0)
        sync_bytes = int(combined.get("sync_bytes_total", 0) or 0)
        combined["bytes_per_accepted_event"] = (
            round(sync_bytes / accepted, 2) if accepted else ""
        )
        protocol_bytes = int(combined.get("protocol_sync_bytes_total", 0) or 0)
        combined["protocol_bytes_per_accepted_event"] = (
            round(protocol_bytes / accepted, 2) if accepted else ""
        )
        attempted = int(combined.get("attempted_events", 0) or 0)
        combined["bytes_per_attempted_event"] = (
            round(sync_bytes / attempted, 2) if attempted else ""
        )
        self.state["results"].append(combined)

    def reserve_run(self) -> str:
        """Mark a run as started and return its new run ID."""
        run_id = f"eval-{uuid.uuid4().hex[:8]}"
        self.state["running"] = True
        self.state["run_id"] = run_id
        self.state["started_at"] = time.time()
        self.state["error"] = None
        self.state["current_step"] = "Starting"
        return run_id

    async def run(self, cfg: EvalScenarioConfig, run_id: str | None = None):
        """Execute every configured scenario and store the results and CSV in the run state.

        For each node count and repetition it runs baseline, partition_heal per
        split, high_load and the shared workloads. Errors are recorded in the
        state instead of raised; sampling and netem are always cleaned up.
        """
        run_id = run_id or self.reserve_run()

        self.state["running"] = True
        self.state["run_id"] = run_id
        self.state["started_at"] = time.time()
        self.state["results"] = []
        self.state["error"] = None
        self.state["csv_data"] = None
        self.state.update(_progress_defaults())

        scenarios = self._enabled_scenarios(cfg)
        partition_splits = self.active_partition_splits(cfg)
        self.state["total_steps"] = self.planned_steps(cfg)
        self.state["steps_remaining"] = self.state["total_steps"]
        self.state["total_repetitions"] = cfg.repetitions
        step = 0

        target_netem_profile = (
            cfg.network_profile
            if hasattr(self.docker_mgr, "apply_netem_profile")
            else None
        )

        async def _prepare_scenario(session, nodes, gateways):
            """Restore a clean network, reset node state, apply the netem profile, and start sampling."""
            network_changed = await self._heal_partition()
            if self._netem_profile != target_netem_profile:
                network_changed = await self._clear_network_profile() or network_changed
            if network_changed:
                await self.wait_for_nodes_ready(
                    session, nodes, cfg.node_readiness_timeout_seconds
                )
            await self.reset_runtime(session, nodes, gateways)
            if (
                target_netem_profile is not None
                and self._netem_profile != target_netem_profile
            ):
                receipt = await asyncio.to_thread(
                    self.docker_mgr.apply_netem_profile, target_netem_profile
                )
                if receipt.get("status") != "applied":
                    raise RuntimeError(
                        receipt.get("message")
                        or f"failed to apply network profile {target_netem_profile}"
                    )
                self._network_profile_receipt = receipt
                self._netem_profile = target_netem_profile
            await self._start_resource_sampling()
            return nodes, gateways

        async def _finish_scenario(session, nodes):
            """Heal any partition, wait for nodes if one was healed, and stop sampling."""
            if await self._heal_partition():
                await self.wait_for_nodes_ready(
                    session, nodes, cfg.node_readiness_timeout_seconds
                )
            await self._stop_resource_sampling()

        try:
            timeout = aiohttp.ClientTimeout(total=30)
            async with aiohttp.ClientSession(timeout=timeout) as session:
                for node_count in cfg.node_counts:
                    warm_nodes = None
                    warm_gateways = None
                    if cfg.warm_topology:
                        warm_nodes, warm_gateways = await self.recreate_nodes(
                            session, node_count, cfg, repetition=1
                        )

                    for repetition in range(1, cfg.repetitions + 1):
                        rng = random.Random(cfg.seed_for_repetition(repetition))
                        if cfg.warm_topology:
                            nodes, gateways = warm_nodes, warm_gateways
                            if cfg.resource_mode == "heterogeneous" and repetition > 1:
                                await self._apply_repetition_resources(
                                    node_count, cfg, repetition
                                )
                        else:
                            nodes, gateways = await self.recreate_nodes(
                                session, node_count, cfg, repetition=repetition
                            )

                        if "baseline" in scenarios:
                            self._set_progress(
                                current_step=(
                                    f"[rep {repetition}/{cfg.repetitions}] "
                                    f"[{node_count}N] Baseline"
                                ),
                                completed_steps=step,
                                cfg=cfg,
                                repetition=repetition,
                                node_count=node_count,
                                scenario="baseline",
                            )
                            nodes, gateways = await _prepare_scenario(
                                session, nodes, gateways
                            )
                            baseline_events = await self.send_events(
                                session,
                                nodes,
                                cfg.events_per_node,
                                concurrency=cfg.event_concurrency,
                                rng=rng,
                                offered_rate=cfg.offered_rate_per_second,
                            )
                            metrics = await self.wait_for_metrics(
                                session,
                                nodes,
                                gateways,
                                cfg,
                                baseline_events["accepted"],
                                tracking_ids=baseline_events["tracking_ids"],
                                attempted_tracking_ids=baseline_events[
                                    "attempted_tracking_ids"
                                ],
                            )
                            metrics.update(await self._container_health_summary())
                            self._append_result(
                                self._row_base(
                                    cfg,
                                    run_id,
                                    repetition,
                                    "baseline",
                                    node_count,
                                    baseline_events,
                                ),
                                metrics,
                            )
                            await _finish_scenario(session, nodes)
                            step += 1

                        for split in partition_splits:
                            split_label = (
                                f"{round(split * 100)}/{round((1 - split) * 100)}"
                            )
                            self._set_progress(
                                current_step=(
                                    f"[rep {repetition}/{cfg.repetitions}] "
                                    f"[{node_count}N] Partition {split_label}"
                                ),
                                completed_steps=step,
                                cfg=cfg,
                                repetition=repetition,
                                node_count=node_count,
                                scenario="partition_heal",
                                partition_split=split_label,
                            )
                            nodes, gateways = await _prepare_scenario(
                                session, nodes, gateways
                            )
                            pre_events = await self.send_events(
                                session,
                                nodes,
                                cfg.events_per_node,
                                concurrency=cfg.event_concurrency,
                                rng=rng,
                                offered_rate=cfg.offered_rate_per_second,
                            )
                            fault_receipt = {}
                            await self.poll_all_gateways(session, gateways)
                            fault_applied_at = time.time()
                            try:
                                fault_receipt = await asyncio.to_thread(
                                    self.docker_mgr.create_split_brain, split
                                )
                            except Exception:
                                fault_receipt = {}
                            if fault_receipt.get("status") not in {
                                "split_brain",
                                "partitioned",
                            }:
                                await asyncio.to_thread(self.docker_mgr.heal_all)
                                raise RuntimeError(
                                    fault_receipt.get("message")
                                    or "failed to apply partition rules"
                                )
                            self._partition_applied = True
                            partition_events = await self.send_events(
                                session,
                                nodes,
                                cfg.partition_events_per_node,
                                concurrency=cfg.event_concurrency,
                                rng=rng,
                                offered_rate=cfg.offered_rate_per_second,
                            )
                            await asyncio.sleep(cfg.partition_wait_seconds)
                            await self._heal_partition()
                            fault_removed_at = time.time()
                            await self.wait_for_nodes_ready(
                                session, nodes, cfg.node_readiness_timeout_seconds
                            )
                            combined_events = {
                                "attempted": pre_events["attempted"]
                                + partition_events["attempted"],
                                "accepted": pre_events["accepted"]
                                + partition_events["accepted"],
                                "tracking_ids": pre_events["tracking_ids"]
                                + partition_events["tracking_ids"],
                                "attempted_tracking_ids": pre_events[
                                    "attempted_tracking_ids"
                                ]
                                + partition_events["attempted_tracking_ids"],
                            }
                            combined_events["partition_group_a"] = (
                                fault_receipt.get("group_a") or []
                            )
                            combined_events["partition_group_b"] = (
                                fault_receipt.get("group_b") or []
                            )
                            combined_events["network_rules_verified"] = True
                            combined_events["fault_applied_at_unix_s"] = round(
                                fault_applied_at, 6
                            )
                            combined_events["fault_removed_at_unix_s"] = round(
                                fault_removed_at, 6
                            )
                            combined_events["actual_fault_duration_s"] = round(
                                fault_removed_at - fault_applied_at, 3
                            )
                            combined_events["duplicates"] = (
                                pre_events["duplicates"]
                                + partition_events["duplicates"]
                            )
                            combined_events["rejected"] = (
                                pre_events["rejected"] + partition_events["rejected"]
                            )
                            combined_events["request_errors"] = (
                                pre_events["request_errors"]
                                + partition_events["request_errors"]
                            )
                            combined_events["failed_request_latencies_ms"] = (
                                pre_events["failed_request_latencies_ms"]
                                + partition_events["failed_request_latencies_ms"]
                            )
                            combined_events["request_latencies_ms"] = (
                                pre_events["request_latencies_ms"]
                                + partition_events["request_latencies_ms"]
                            )
                            submission_duration = (
                                pre_events["submission_duration_s"]
                                + partition_events["submission_duration_s"]
                            )
                            combined_events["submission_duration_s"] = (
                                submission_duration
                            )
                            combined_events["offered_rate_events_s"] = (
                                combined_events["attempted"] / submission_duration
                            )
                            combined_events["accepted_throughput_events_s"] = (
                                combined_events["accepted"] / submission_duration
                            )
                            expected_events = combined_events["accepted"]
                            metrics = await self.wait_for_metrics(
                                session,
                                nodes,
                                gateways,
                                cfg,
                                expected_events,
                                tracking_ids=combined_events["tracking_ids"],
                                attempted_tracking_ids=combined_events[
                                    "attempted_tracking_ids"
                                ],
                            )
                            metrics.update(await self._container_health_summary())
                            latest_arrival = metrics.get("latest_first_seen_at_unix_s")
                            metrics["recovery_after_heal_s"] = (
                                round(max(latest_arrival - fault_removed_at, 0.0), 3)
                                if metrics.get("converged_within_timeout")
                                and latest_arrival is not None
                                else ""
                            )
                            self._append_result(
                                self._row_base(
                                    cfg,
                                    run_id,
                                    repetition,
                                    "partition_heal",
                                    node_count,
                                    combined_events,
                                    partition_split=split_label,
                                ),
                                metrics,
                            )
                            await _finish_scenario(session, nodes)
                            step += 1

                        if "high_load" in scenarios:
                            self._set_progress(
                                current_step=(
                                    f"[rep {repetition}/{cfg.repetitions}] "
                                    f"[{node_count}N] High load"
                                ),
                                completed_steps=step,
                                cfg=cfg,
                                repetition=repetition,
                                node_count=node_count,
                                scenario="high_load",
                            )
                            nodes, gateways = await _prepare_scenario(
                                session, nodes, gateways
                            )
                            heavy_events = await self.send_events(
                                session,
                                nodes,
                                cfg.events_per_node * 3,
                                category="resource",
                                concurrency=cfg.event_concurrency,
                                rng=rng,
                                offered_rate=cfg.offered_rate_per_second,
                            )
                            metrics = await self.wait_for_metrics(
                                session,
                                nodes,
                                gateways,
                                cfg,
                                heavy_events["accepted"],
                                tracking_ids=heavy_events["tracking_ids"],
                                attempted_tracking_ids=heavy_events[
                                    "attempted_tracking_ids"
                                ],
                            )
                            metrics.update(await self._container_health_summary())
                            self._append_result(
                                self._row_base(
                                    cfg,
                                    run_id,
                                    repetition,
                                    "high_load",
                                    node_count,
                                    heavy_events,
                                ),
                                metrics,
                            )
                            await _finish_scenario(session, nodes)
                            step += 1

                        for (
                            shared_scenario,
                            shared_workload,
                        ) in SHARED_WORKLOADS.items():
                            if shared_scenario not in scenarios:
                                continue
                            scenario_label = shared_scenario.replace("_", " ").title()
                            self._set_progress(
                                current_step=(
                                    f"[rep {repetition}/{cfg.repetitions}] "
                                    f"[{node_count}N] {scenario_label} workload"
                                ),
                                completed_steps=step,
                                cfg=cfg,
                                repetition=repetition,
                                node_count=node_count,
                                scenario=shared_scenario,
                            )
                            nodes, gateways = await _prepare_scenario(
                                session, nodes, gateways
                            )
                            warmup_events = await self.send_events(
                                session,
                                nodes,
                                SHARED_WARMUP_EVENTS_PER_NODE[shared_scenario],
                                concurrency=cfg.event_concurrency,
                                rng=rng,
                                shared_workload=shared_workload,
                            )
                            warmup_metrics = await self.wait_for_metrics(
                                session,
                                nodes,
                                gateways,
                                cfg,
                                warmup_events["accepted"],
                                tracking_ids=warmup_events["tracking_ids"],
                                attempted_tracking_ids=warmup_events[
                                    "attempted_tracking_ids"
                                ],
                            )
                            if not warmup_metrics["converged_within_timeout"]:
                                raise RuntimeError(
                                    f"{shared_scenario} warmup did not converge within "
                                    "the configured deadline"
                                )

                            await self._stop_resource_sampling()
                            await self.reset_measurement_metrics(
                                session, nodes, gateways
                            )
                            await self._start_resource_sampling()

                            shared_events = await self.send_events(
                                session,
                                nodes,
                                cfg.events_per_node,
                                concurrency=cfg.event_concurrency,
                                rng=rng,
                                offered_rate=cfg.offered_rate_per_second,
                                shared_workload=shared_workload,
                            )
                            metrics = await self.wait_for_metrics(
                                session,
                                nodes,
                                gateways,
                                cfg,
                                shared_events["accepted"],
                                tracking_ids=shared_events["tracking_ids"],
                                attempted_tracking_ids=shared_events[
                                    "attempted_tracking_ids"
                                ],
                            )
                            metrics.update(await self._container_health_summary())
                            self._append_result(
                                self._row_base(
                                    cfg,
                                    run_id,
                                    repetition,
                                    shared_scenario,
                                    node_count,
                                    shared_events,
                                ),
                                metrics,
                            )
                            await _finish_scenario(session, nodes)
                            step += 1
            self.state["current_step"] = "Generating CSV..."
            self.state["progress"] = step
            self.state["completed_steps"] = step
            self.state["steps_remaining"] = 0
            output = io.StringIO()
            writer = csv.DictWriter(
                output, fieldnames=CSV_HEADER, extrasaction="ignore"
            )
            writer.writeheader()
            for row in self.state["results"]:
                writer.writerow(row)
            self.state["csv_data"] = output.getvalue()
            self.state["current_step"] = "Done!"
            self.state["progress"] = self.state["total_steps"]
        except Exception as error:
            self.state["error"] = str(error)
            self.state["current_step"] = f"Error: {error}"
            log.error("evaluation_failed", error=str(error))
        finally:
            await self._stop_resource_sampling()
            try:
                if hasattr(self.docker_mgr, "clear_netem"):
                    await asyncio.to_thread(self.docker_mgr.clear_netem)
                    self._netem_profile = None
                    self._network_profile_receipt = {}
            except Exception:
                pass
            self.state["running"] = False
