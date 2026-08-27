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

log = structlog.get_logger()

NETWORK_PROFILES = {"hard_partition", *NETWORK_PROFILE_SETTINGS}
OBSERVABILITY_MODES = {"off", "light", "full"}
SYNC_MODES = {
    "full_state",
    "optimized_full_state",
    "naive_key_delta",
    "true_delta",
    "centralized",
}
SCENARIOS = {"baseline", "partition_heal", "high_load"}
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
    "dashboard_observability",
    "docker_cpus",
    "docker_memory",
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
    "time_to_first_remote_node_p50_s",
    "time_to_first_remote_node_p95_s",
    "time_to_last_node_p50_s",
    "time_to_last_node_p95_s",
    "rounds_to_last_node",
    "tracking_coverage_percent",
    "accepted_tracking_ids_count",
    "fully_observed_tracking_ids_count",
    "missing_tracking_ids_count",
    "per_node_missing_tracking_ids",
    "missing_at_cutoff_pct",
    "gateway_staleness_seconds",
    "recovery_time_s",
    "sync_bytes_total",
    "gossip_summary_sent_bytes",
    "gossip_summary_received_bytes",
    "full_state_repair_bytes",
    "delta_repair_bytes",
    "semantic_state_bytes",
    "evaluation_metadata_state_bytes",
    "nonsemantic_state_metadata_bytes",
    "central_request_bytes",
    "central_response_bytes",
    "bytes_per_accepted_event",
    "avg_state_bytes_per_known_event",
    "repair_pull_count",
    "gossip_messages_sent",
    "total_state_size_bytes",
    "avg_state_size_bytes",
    "known_status",
    "coverage_percent",
    "resource_sample_count",
    "cpu_mean_percent",
    "cpu_peak_percent",
    "memory_mean_mib",
    "memory_peak_mib",
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
    network_profile: str = "hard_partition"
    scenario_seed: int | None = None
    dashboard_observability: str = "off"
    event_concurrency: int = Field(default=25)
    offered_rate_per_second: float | None = Field(default=None, gt=0)
    docker_cpus: float | None = None
    docker_memory: str | None = None
    repetitions: int = Field(default=1, ge=1)
    scenarios: list[str] = Field(default=["baseline", "partition_heal", "high_load"])
    warm_topology: bool = True
    node_readiness_timeout_seconds: float = Field(default=30.0, ge=1.0)
    partition_events_per_node: int = Field(default=3, ge=0)

    def effective_event_id_retention_limit(self) -> int:
        if self.sync_mode == "full_state":
            return 0
        return self.event_id_retention_limit

    def seed_for_repetition(self, repetition: int) -> int | None:
        if self.scenario_seed is None:
            return None
        return self.scenario_seed + repetition - 1

    def implementation_label(self) -> str:
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
        if self.docker_cpus:
            return (
                f"{base}_cpu_{self.docker_cpus:g}_mem_{self.docker_memory or 'default'}"
            )
        if self.docker_memory:
            return f"{base}_mem_{self.docker_memory}"
        return base


def initial_eval_state() -> dict[str, Any]:
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
    if not values:
        return None
    ordered = sorted(values)
    index = max(0, math.ceil(percentile * len(ordered)) - 1)
    return ordered[min(index, len(ordered) - 1)]


def _semantic_state(state: dict[str, Any]) -> dict[str, Any]:
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

    async def _collect_resource_sample(self) -> None:
        if not hasattr(self.docker_mgr, "resource_snapshot"):
            return
        sample = await asyncio.to_thread(self.docker_mgr.resource_snapshot)
        if sample:
            self._resource_samples.append(sample)

    async def _resource_sample_loop(self) -> None:
        while self._resource_stop and not self._resource_stop.is_set():
            await self._collect_resource_sample()
            try:
                await asyncio.wait_for(self._resource_stop.wait(), timeout=1.0)
            except TimeoutError:
                continue

    async def _start_resource_sampling(self) -> None:
        await self._stop_resource_sampling()
        self._resource_samples = []
        if not hasattr(self.docker_mgr, "resource_snapshot"):
            return
        self._resource_stop = asyncio.Event()
        self._resource_task = asyncio.create_task(self._resource_sample_loop())

    async def _stop_resource_sampling(self) -> None:
        if self._resource_stop:
            self._resource_stop.set()
        if self._resource_task:
            await self._resource_task
            await self._collect_resource_sample()
        self._resource_stop = None
        self._resource_task = None

    def _resource_summary(self) -> dict[str, float | int]:
        samples = [item for sample in self._resource_samples for item in sample]
        if not samples:
            return {
                "resource_sample_count": 0,
                "cpu_mean_percent": 0.0,
                "cpu_peak_percent": 0.0,
                "memory_mean_mib": 0.0,
                "memory_peak_mib": 0.0,
                "network_rx_bytes": 0,
                "network_tx_bytes": 0,
                "block_read_bytes": 0,
                "block_write_bytes": 0,
            }

        cpu_values = []
        previous_cpu_by_node = {}
        for resource_sample in self._resource_samples:
            for sample in resource_sample:
                node = str(sample["node"])
                if "cpu_usage_ns" not in sample or "system_cpu_usage_ns" not in sample:
                    if "cpu_percent" in sample:
                        cpu_values.append(float(sample["cpu_percent"]))
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
                    cpu_values.append(
                        (cpu_delta / system_delta)
                        * max(int(sample.get("online_cpus", 1)), 1)
                        * 100
                    )
        memory_values = [int(sample["memory_bytes"]) for sample in samples]
        first_by_node = {}
        last_by_node = {}
        for sample in samples:
            node = str(sample["node"])
            first_by_node.setdefault(node, sample)
            last_by_node[node] = sample

        def counter_growth(key: str) -> int:
            return sum(
                max(
                    int(last_by_node[node][key]) - int(first_by_node[node][key]),
                    0,
                )
                for node in last_by_node
            )

        mib = 1024 * 1024
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
            "network_rx_bytes": counter_growth("network_rx_bytes"),
            "network_tx_bytes": counter_growth("network_tx_bytes"),
            "block_read_bytes": counter_growth("block_read_bytes"),
            "block_write_bytes": counter_growth("block_write_bytes"),
        }

    def validate_config(self, cfg: EvalScenarioConfig) -> None:
        if cfg.dashboard_observability not in OBSERVABILITY_MODES:
            raise ValueError("dashboard_observability must be off, light, or full")
        if cfg.sync_mode not in SYNC_MODES:
            supported = ", ".join(sorted(SYNC_MODES))
            raise ValueError(f"sync_mode must be one of: {supported}")
        if cfg.network_profile not in NETWORK_PROFILES:
            supported = ", ".join(sorted(NETWORK_PROFILES))
            raise ValueError(f"network_profile must be one of: {supported}")
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
                "scenarios must contain only baseline, partition_heal, or high_load"
            )
        if self._scenario_step_count(cfg) == 0:
            raise ValueError("selected scenarios produce no evaluation steps")

    def _enabled_scenarios(self, cfg: EvalScenarioConfig) -> set[str]:
        return set(cfg.scenarios)

    def _active_partition_splits(self, cfg: EvalScenarioConfig) -> list[float]:
        if (
            cfg.network_profile == "hard_partition"
            and "partition_heal" in self._enabled_scenarios(cfg)
        ):
            return cfg.partition_splits
        return []

    def _scenario_step_count(self, cfg: EvalScenarioConfig) -> int:
        scenarios = self._enabled_scenarios(cfg)
        count = 0
        if "baseline" in scenarios:
            count += 1
        if "high_load" in scenarios:
            count += 1
        count += len(self._active_partition_splits(cfg))
        return count

    def planned_steps(self, cfg: EvalScenarioConfig) -> int:
        return cfg.repetitions * len(cfg.node_counts) * self._scenario_step_count(cfg)

    def status(self) -> dict[str, Any]:
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
        central_url: str | None = None,
        offered_rate: float | None = None,
    ):
        started_at = time.perf_counter()
        result = {
            "attempted": count * len(nodes),
            "accepted": 0,
            "rejected": 0,
            "duplicates": 0,
            "request_errors": 0,
            "tracking_ids": [],
            "failed_request_latencies_ms": [],
            "request_latencies_ms": [],
            "central_request_bytes": 0,
            "central_response_bytes": 0,
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
            if offered_rate is not None:
                delay = (i / offered_rate) - (time.perf_counter() - started_at)
                if delay > 0:
                    await asyncio.sleep(delay)
            node_url = target.get("internal_url") or target.get("url")
            submitted_at = time.time()
            if not node_url:
                return "error", None, submitted_at, None, 0, 0

            event_type = event_types[i % len(event_types)]
            tracking_id = str(uuid.UUID(int=rng.getrandbits(128)))
            payload = {
                "id": tracking_id,
                "type": event_type,
                "value": round(rng.uniform(1.0, 100.0), 2),
                "location": f"{category}_{i}",
                "category": category,
                "metadata": {
                    "tracking_id": tracking_id,
                    "source": "evaluation",
                    "evaluation_submitted_at": submitted_at,
                },
            }
            request_url = f"{node_url}/event"
            request_payload = payload
            if central_url:
                request_url = f"{node_url}/evaluation/central-submit"
                request_payload = {"central_url": central_url, "event": payload}

            request_started = time.perf_counter()
            try:
                async with semaphore:
                    async with session.post(
                        request_url, json=request_payload
                    ) as response:
                        try:
                            response_data = await response.json()
                        except Exception:
                            response_data = {}
                        latency_ms = (time.perf_counter() - request_started) * 1000
                        central_request_bytes = int(
                            response_data.get("central_request_bytes", 0)
                        )
                        central_response_bytes = int(
                            response_data.get("central_response_bytes", 0)
                        )
                        status = response_data.get("status")
                        if response.status < 400 and status != "duplicate":
                            return (
                                "accepted",
                                tracking_id,
                                submitted_at,
                                latency_ms,
                                central_request_bytes,
                                central_response_bytes,
                            )
                        if status == "duplicate":
                            outcome = "duplicate"
                        else:
                            outcome = "rejected"
                        return (
                            outcome,
                            tracking_id,
                            submitted_at,
                            latency_ms,
                            central_request_bytes,
                            central_response_bytes,
                        )
            except Exception:
                return "error", tracking_id, submitted_at, None, 0, 0

        submissions = [
            _send_one(i, nodes[i % len(nodes)]) for i in range(count * len(nodes))
        ]
        for (
            status,
            tracking_id,
            submitted_at,
            latency_ms,
            central_request_bytes,
            central_response_bytes,
        ) in await asyncio.gather(*submissions):
            result["central_request_bytes"] += central_request_bytes
            result["central_response_bytes"] += central_response_bytes
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

    @staticmethod
    def _central_gateway_url(gateways) -> str:
        if not gateways:
            raise RuntimeError("centralized baseline requires one gateway")
        gateway = gateways[0]
        central_url = gateway.get("url") or gateway.get("external_url")
        if not central_url:
            raise RuntimeError("centralized baseline gateway has no URL")
        return str(central_url).rstrip("/")

    async def wait_for_central_metrics(
        self,
        session,
        gateways,
        cfg,
        event_result,
    ):
        central_url = self._central_gateway_url(gateways)
        accepted_ids = set(event_result.get("tracking_ids") or [])
        started = time.time()
        deadline = started + cfg.convergence_wait_seconds
        central_state = {}
        while True:
            try:
                async with session.get(
                    f"{central_url}/gateway/evaluation/central/state"
                ) as response:
                    if response.status < 400:
                        central_state = await response.json()
            except Exception:
                central_state = {}

            stored_ids = set(central_state.get("event_ids") or [])
            missing_ids = accepted_ids - stored_ids
            if not missing_ids or time.time() >= deadline:
                break
            await asyncio.sleep(cfg.poll_interval_seconds)

        elapsed = round(time.time() - started, 3)
        stored_ids = set(central_state.get("event_ids") or [])
        missing_ids = accepted_ids - stored_ids
        accepted_count = len(accepted_ids)
        fully_stored_count = accepted_count - len(missing_ids)
        coverage = (
            round((fully_stored_count / accepted_count) * 100, 1)
            if accepted_count
            else 100.0
        )
        state_size = int(central_state.get("state_size_bytes", 0))
        central_request_bytes = int(event_result.get("central_request_bytes", 0))
        central_response_bytes = int(event_result.get("central_response_bytes", 0))
        converged = not missing_ids
        await self._collect_resource_sample()
        return {
            "convergence_time_s": elapsed if converged else None,
            "converged_within_timeout": converged,
            "convergence_wait_seconds": cfg.convergence_wait_seconds,
            "missing_at_cutoff_count": len(missing_ids),
            "missing_at_cutoff_pct": round((len(missing_ids) / accepted_count) * 100, 2)
            if accepted_count
            else 0.0,
            "gateway_staleness_seconds": 0.0,
            "time_to_last_node_s": None,
            "rounds_to_last_node": None,
            "tracking_coverage_percent": coverage,
            "accepted_tracking_ids_count": accepted_count,
            "fully_observed_tracking_ids_count": fully_stored_count,
            "missing_tracking_ids_count": len(missing_ids),
            "per_node_missing_tracking_ids": "{}",
            "sync_bytes_total": central_request_bytes + central_response_bytes,
            "gossip_summary_sent_bytes": 0,
            "gossip_summary_received_bytes": 0,
            "full_state_repair_bytes": 0,
            "delta_repair_bytes": 0,
            "semantic_state_bytes": state_size,
            "evaluation_metadata_state_bytes": 0,
            "nonsemantic_state_metadata_bytes": 0,
            "central_request_bytes": central_request_bytes,
            "central_response_bytes": central_response_bytes,
            "repair_pull_count": 0,
            "gossip_messages_sent": 0,
            "total_state_size_bytes": state_size,
            "avg_state_size_bytes": state_size,
            "avg_state_bytes_per_known_event": round(
                state_size / max(accepted_count, 1), 2
            ),
            "known_status": "known_converged" if converged else "known_diverged",
            "coverage_percent": coverage,
            "is_converged": converged,
            "unique_merkle_roots": 0,
            "semantic_state_converged": converged,
            "unique_semantic_states": 1 if central_state else 0,
            "max_events": len(stored_ids),
            "min_events": len(stored_ids),
            "delta_pulls_started": 0,
            "delta_pulls_success": 0,
            "delta_pulls_gap": 0,
            "delta_deltas_applied": 0,
            "delta_bytes_received": 0,
        }

    async def wait_for_central_access(
        self,
        session,
        nodes,
        central_url: str,
        timeout_seconds: float,
    ) -> float | None:
        pending = {
            node.get("node_id"): node.get("internal_url") or node.get("url")
            for node in nodes
        }
        pending = {
            node_id: node_url
            for node_id, node_url in pending.items()
            if node_id and node_url
        }
        started = time.time()
        deadline = started + timeout_seconds
        while pending and time.time() < deadline:

            async def probe(node_id, node_url):
                try:
                    async with session.get(
                        f"{node_url}/evaluation/central-health",
                        params={"central_url": central_url},
                    ) as response:
                        if response.status < 400:
                            return node_id
                except Exception:
                    return None
                return None

            ready = await asyncio.gather(
                *(probe(node_id, node_url) for node_id, node_url in pending.items())
            )
            for node_id in ready:
                if node_id:
                    pending.pop(node_id, None)
            if pending:
                await asyncio.sleep(0.25)
        if pending:
            return None
        return round(time.time() - started, 3)

    async def collect_metrics(
        self,
        session,
        nodes,
        gateways,
        accepted_events=None,
        tracking_ids=None,
        *,
        include_state: bool,
    ):
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
        delta_pulls_started = 0
        delta_pulls_success = 0
        delta_pulls_gap = 0
        delta_deltas_applied = 0
        delta_bytes_received = 0
        event_counts = []
        merkle_roots = []
        semantic_states = []
        per_node_tracking_ids = {}
        tracking_ids = set(tracking_ids or [])
        tracking_seen_counts = {tracking_id: 0 for tracking_id in tracking_ids}
        tracking_seen_latencies = {tracking_id: [] for tracking_id in tracking_ids}
        tracking_last_seen_latency = {}
        tracking_last_seen_round = {}

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
                    delta_pulls_started += stats.get("delta_pulls_started", 0)
                    delta_pulls_success += stats.get("delta_pulls_success", 0)
                    delta_pulls_gap += stats.get("delta_pulls_gap", 0)
                    delta_deltas_applied += stats.get("delta_deltas_applied", 0)
                    delta_bytes_received += node_delta_bytes
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
                        for tracking_id, seen in seen_tracking.items():
                            if tracking_ids and tracking_id not in tracking_ids:
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
        expected_events = accepted_events if accepted_events is not None else max_ev
        fully_seen_tracking = [
            tracking_id
            for tracking_id, count in tracking_seen_counts.items()
            if count >= len(nodes)
        ]
        missing_tracking_ids = tracking_ids - set(fully_seen_tracking)
        if include_state and tracking_ids:
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
            node_id: len(tracking_ids - seen_ids)
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

        sync_bytes_total = (
            gossip_summary_sent_bytes + full_state_repair_bytes + delta_bytes_received
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
            "rounds_to_last_node": rounds_to_last_node,
            "tracking_coverage_percent": tracking_coverage_percent,
            "time_to_first_remote_node_p50_s": _percentile(first_remote_latencies, 0.5),
            "time_to_first_remote_node_p95_s": _percentile(
                first_remote_latencies, 0.95
            ),
            "time_to_last_node_p50_s": _percentile(last_node_latencies, 0.5),
            "time_to_last_node_p95_s": _percentile(last_node_latencies, 0.95),
            "accepted_tracking_ids_count": len(tracking_ids),
            "fully_observed_tracking_ids_count": len(fully_seen_tracking),
            "missing_tracking_ids_count": len(missing_tracking_ids),
            "per_node_missing_tracking_ids": json.dumps(
                per_node_missing_tracking_ids, sort_keys=True
            ),
            "sync_bytes_total": sync_bytes_total,
            "gossip_summary_sent_bytes": gossip_summary_sent_bytes,
            "gossip_summary_received_bytes": gossip_summary_received_bytes,
            "full_state_repair_bytes": full_state_repair_bytes,
            "delta_repair_bytes": delta_bytes_received,
            "semantic_state_bytes": semantic_state_bytes,
            "evaluation_metadata_state_bytes": evaluation_metadata_state_bytes,
            "nonsemantic_state_metadata_bytes": (nonsemantic_state_metadata_bytes),
            "repair_pull_count": repair_pull_count,
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
        self, session, nodes, gateways, cfg, accepted_events, tracking_ids=None
    ):
        started = time.time()
        deadline = started + cfg.convergence_wait_seconds
        metrics = None

        while True:
            await self.poll_all_gateways(session, gateways)
            metrics = await self.collect_metrics(
                session,
                nodes,
                gateways,
                accepted_events=accepted_events,
                tracking_ids=tracking_ids,
                include_state=False,
            )
            observed_at = time.time()
            if metrics["is_converged"] and observed_at <= deadline:
                metrics = await self.collect_metrics(
                    session,
                    nodes,
                    gateways,
                    accepted_events=accepted_events,
                    tracking_ids=tracking_ids,
                    include_state=True,
                )
                observed_at = time.time()
                if (
                    metrics["is_converged"]
                    and metrics["missing_at_cutoff_count"] == 0
                    and observed_at <= deadline
                ):
                    metrics["convergence_time_s"] = round(observed_at - started, 3)
                    metrics["converged_within_timeout"] = True
                    metrics["convergence_wait_seconds"] = cfg.convergence_wait_seconds
                    await self._collect_resource_sample()
                    return metrics
            if time.time() >= deadline:
                metrics = await self.collect_metrics(
                    session,
                    nodes,
                    gateways,
                    accepted_events=accepted_events,
                    tracking_ids=tracking_ids,
                    include_state=True,
                )
                metrics["convergence_time_s"] = None
                metrics["converged_within_timeout"] = False
                metrics["convergence_wait_seconds"] = cfg.convergence_wait_seconds
                await self._collect_resource_sample()
                return metrics
            await asyncio.sleep(cfg.poll_interval_seconds)

    async def wait_for_nodes_ready(self, session, nodes, timeout_seconds: float):
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

    async def recreate_nodes(self, session, node_count: int, cfg: EvalScenarioConfig):
        if hasattr(self.docker_mgr, "clear_netem"):
            try:
                await asyncio.to_thread(self.docker_mgr.clear_netem)
            except Exception:
                pass
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
                node_id=f"node-{index + 1}",
                sync_mode=cfg.sync_mode,
                event_id_retention_limit=cfg.effective_event_id_retention_limit(),
                delta_log_limit=cfg.delta_log_limit,
                observability_mode=cfg.dashboard_observability,
                cpu_limit=cfg.docker_cpus,
                memory_limit=cfg.docker_memory,
            )
            for index in range(node_count)
        ]
        for result in await asyncio.gather(*create_tasks, return_exceptions=True):
            if isinstance(result, Exception):
                log.warning("eval_node_create_failed", error=str(result))

        nodes = await asyncio.to_thread(self.docker_mgr.list_nodes)
        if len(nodes) < node_count:
            raise RuntimeError(f"expected {node_count} nodes, found {len(nodes)}")
        await self.wait_for_nodes_ready(
            session, nodes, cfg.node_readiness_timeout_seconds
        )
        await self.sync_gateways_with_nodes(force=True)
        return nodes, await self.discover_gateways()

    async def reset_runtime(self, session, nodes, gateways):
        async def _post_first_available(urls, path):
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
            "dashboard_observability": cfg.dashboard_observability,
            "docker_cpus": cfg.docker_cpus or "",
            "docker_memory": cfg.docker_memory or "",
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
        combined = {**row, **metrics, **self._resource_summary()}
        accepted = combined.get("accepted_events", 0)
        sync_bytes = combined.get("sync_bytes_total", 0)
        combined["bytes_per_accepted_event"] = (
            round(sync_bytes / accepted, 2) if accepted else ""
        )
        self.state["results"].append(combined)

    def reserve_run(self) -> str:
        run_id = f"eval-{uuid.uuid4().hex[:8]}"
        self.state["running"] = True
        self.state["run_id"] = run_id
        self.state["started_at"] = time.time()
        self.state["error"] = None
        self.state["current_step"] = "Starting"
        return run_id

    async def run(self, cfg: EvalScenarioConfig, run_id: str | None = None):
        run_id = run_id or self.reserve_run()

        self.state["running"] = True
        self.state["run_id"] = run_id
        self.state["started_at"] = time.time()
        self.state["results"] = []
        self.state["error"] = None
        self.state["csv_data"] = None
        self.state.update(_progress_defaults())

        scenarios = self._enabled_scenarios(cfg)
        partition_splits = self._active_partition_splits(cfg)
        self.state["total_steps"] = self.planned_steps(cfg)
        self.state["steps_remaining"] = self.state["total_steps"]
        self.state["total_repetitions"] = cfg.repetitions
        step = 0

        async def _prepare_scenario(session, nodes, gateways):
            try:
                await asyncio.to_thread(self.docker_mgr.heal_all)
            except Exception:
                pass
            if hasattr(self.docker_mgr, "clear_netem"):
                try:
                    await asyncio.to_thread(self.docker_mgr.clear_netem)
                except Exception:
                    pass
            await self.wait_for_nodes_ready(
                session, nodes, cfg.node_readiness_timeout_seconds
            )
            await self.reset_runtime(session, nodes, gateways)
            self._network_profile_receipt = {}
            if cfg.network_profile != "hard_partition" and hasattr(
                self.docker_mgr, "apply_netem_profile"
            ):
                receipt = await asyncio.to_thread(
                    self.docker_mgr.apply_netem_profile, cfg.network_profile
                )
                if receipt.get("status") != "applied":
                    raise RuntimeError(
                        receipt.get("message")
                        or f"failed to apply network profile {cfg.network_profile}"
                    )
                self._network_profile_receipt = receipt
            await self._start_resource_sampling()
            return nodes, gateways

        async def _finish_scenario(session, nodes):
            try:
                await asyncio.to_thread(self.docker_mgr.heal_all)
            except Exception:
                pass
            if hasattr(self.docker_mgr, "clear_netem"):
                try:
                    await asyncio.to_thread(self.docker_mgr.clear_netem)
                except Exception:
                    pass
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
                            session, node_count, cfg
                        )

                    for repetition in range(1, cfg.repetitions + 1):
                        rng = random.Random(cfg.seed_for_repetition(repetition))
                        if cfg.warm_topology:
                            nodes, gateways = warm_nodes, warm_gateways
                        else:
                            nodes, gateways = await self.recreate_nodes(
                                session, node_count, cfg
                            )
                        central_url = (
                            self._central_gateway_url(gateways)
                            if cfg.sync_mode == "centralized"
                            else None
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
                                central_url=central_url,
                                offered_rate=cfg.offered_rate_per_second,
                            )
                            if central_url:
                                metrics = await self.wait_for_central_metrics(
                                    session,
                                    gateways,
                                    cfg,
                                    baseline_events,
                                )
                            else:
                                metrics = await self.wait_for_metrics(
                                    session,
                                    nodes,
                                    gateways,
                                    cfg,
                                    baseline_events["accepted"],
                                    tracking_ids=baseline_events["tracking_ids"],
                                )
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
                                central_url=central_url,
                            )
                            fault_receipt = {}
                            if central_url:
                                fault_applied_at = time.time()
                                fault_receipt = await asyncio.to_thread(
                                    self.docker_mgr.partition_from_gateway,
                                    split,
                                    central_url,
                                )
                            else:
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
                            partition_events = await self.send_events(
                                session,
                                nodes,
                                cfg.partition_events_per_node,
                                concurrency=cfg.event_concurrency,
                                rng=rng,
                                central_url=central_url,
                            )
                            await asyncio.sleep(cfg.partition_wait_seconds)
                            try:
                                await asyncio.to_thread(self.docker_mgr.heal_all)
                            except Exception:
                                pass
                            fault_removed_at = time.time()
                            await self.wait_for_nodes_ready(
                                session, nodes, cfg.node_readiness_timeout_seconds
                            )
                            recovery_time_s = None
                            if central_url:
                                recovery_time_s = await self.wait_for_central_access(
                                    session,
                                    nodes,
                                    central_url,
                                    cfg.node_readiness_timeout_seconds,
                                )
                            combined_events = {
                                "attempted": pre_events["attempted"]
                                + partition_events["attempted"],
                                "accepted": pre_events["accepted"]
                                + partition_events["accepted"],
                                "tracking_ids": pre_events["tracking_ids"]
                                + partition_events["tracking_ids"],
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
                            combined_events["central_request_bytes"] = (
                                pre_events["central_request_bytes"]
                                + partition_events["central_request_bytes"]
                            )
                            combined_events["central_response_bytes"] = (
                                pre_events["central_response_bytes"]
                                + partition_events["central_response_bytes"]
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
                            if central_url:
                                metrics = await self.wait_for_central_metrics(
                                    session,
                                    gateways,
                                    cfg,
                                    combined_events,
                                )
                                metrics["recovery_time_s"] = recovery_time_s
                            else:
                                expected_events = combined_events["accepted"]
                                metrics = await self.wait_for_metrics(
                                    session,
                                    nodes,
                                    gateways,
                                    cfg,
                                    expected_events,
                                    tracking_ids=combined_events["tracking_ids"],
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
                                central_url=central_url,
                                offered_rate=cfg.offered_rate_per_second,
                            )
                            if central_url:
                                metrics = await self.wait_for_central_metrics(
                                    session,
                                    gateways,
                                    cfg,
                                    heavy_events,
                                )
                            else:
                                metrics = await self.wait_for_metrics(
                                    session,
                                    nodes,
                                    gateways,
                                    cfg,
                                    heavy_events["accepted"],
                                    tracking_ids=heavy_events["tracking_ids"],
                                )
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
            except Exception:
                pass
            self.state["running"] = False
