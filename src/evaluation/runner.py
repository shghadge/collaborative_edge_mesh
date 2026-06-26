import asyncio
import csv
import io
import json
import random
import time
import uuid
from collections.abc import Awaitable, Callable
from typing import Any

import aiohttp
import structlog
from pydantic import BaseModel, Field

log = structlog.get_logger()

NETWORK_PROFILES = {"hard_partition", "degraded_300ms_10loss"}
OBSERVABILITY_MODES = {"off", "light", "full"}
SYNC_MODES = {"full_state", "true_delta"}
DEFAULT_EVENT_ID_RETENTION_LIMIT = 100

CSV_HEADER = [
    "run_id",
    "repetition",
    "implementation",
    "sync_mode",
    "event_id_retention_limit",
    "network_profile",
    "dashboard_observability",
    "docker_cpus",
    "docker_memory",
    "scenario",
    "node_count",
    "partition_split",
    "attempted_events",
    "accepted_events",
    "write_success_rate",
    "convergence_time_s",
    "converged_within_timeout",
    "convergence_wait_seconds",
    "missing_at_cutoff_count",
    "time_to_last_node_s",
    "rounds_to_last_node",
    "tracking_coverage_percent",
    "missing_at_cutoff_pct",
    "gateway_staleness_seconds",
    "sync_bytes_total",
    "bytes_per_accepted_event",
    "avg_state_bytes_per_known_event",
    "repair_pull_count",
    "gossip_messages_sent",
    "total_state_size_bytes",
    "avg_state_size_bytes",
    "known_status",
    "coverage_percent",
    "is_converged",
    "unique_merkle_roots",
    "max_events",
    "min_events",
    "delta_pulls_started",
    "delta_pulls_success",
    "delta_pulls_gap",
    "delta_deltas_applied",
    "delta_bytes_received",
]


class EvalScenarioConfig(BaseModel):
    node_counts: list[int] = Field(default=[5, 10])
    events_per_node: int = Field(default=5)
    partition_splits: list[float] = Field(default=[0.5, 0.7, 0.8])
    partition_wait_seconds: float = Field(default=8.0)
    convergence_wait_seconds: float = Field(default=15.0)
    poll_interval_seconds: float = Field(default=2.0)
    sync_mode: str = "true_delta"
    event_id_retention_limit: int = DEFAULT_EVENT_ID_RETENTION_LIMIT
    network_profile: str = "hard_partition"
    scenario_seed: int | None = None
    dashboard_observability: str = "light"
    event_concurrency: int = Field(default=25)
    docker_cpus: float | None = None
    docker_memory: str | None = None
    repetitions: int = Field(default=1, ge=1)

    def implementation_label(self) -> str:
        retention = (
            "unbounded_retention"
            if self.event_id_retention_limit <= 0
            else f"retention_{self.event_id_retention_limit}"
        )
        base = f"{self.sync_mode}_{retention}_{self.network_profile}"
        if self.docker_cpus:
            return (
                f"{base}_cpu_{self.docker_cpus:g}_mem_{self.docker_memory or 'default'}"
            )
        if self.docker_memory:
            return f"{base}_mem_{self.docker_memory}"
        return base


def initial_eval_state() -> dict[str, Any]:
    return {
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

    def validate_config(self, cfg: EvalScenarioConfig) -> None:
        if cfg.dashboard_observability not in OBSERVABILITY_MODES:
            raise ValueError("dashboard_observability must be off, light, or full")
        if cfg.sync_mode not in SYNC_MODES:
            raise ValueError("sync_mode must be full_state or true_delta")
        if cfg.network_profile not in NETWORK_PROFILES:
            raise ValueError(
                "network_profile must be hard_partition or degraded_300ms_10loss"
            )
        if cfg.event_id_retention_limit < 0:
            raise ValueError("event_id_retention_limit must be zero or greater")
        if cfg.repetitions < 1:
            raise ValueError("repetitions must be at least 1")

    def planned_steps(self, cfg: EvalScenarioConfig) -> int:
        partition_splits = (
            cfg.partition_splits if cfg.network_profile == "hard_partition" else []
        )
        return cfg.repetitions * len(cfg.node_counts) * (2 + len(partition_splits))

    def status(self) -> dict[str, Any]:
        return {
            "running": self.state["running"],
            "run_id": self.state["run_id"],
            "started_at": self.state["started_at"],
            "progress": self.state["progress"],
            "total_steps": self.state["total_steps"],
            "current_step": self.state["current_step"],
            "error": self.state["error"],
            "results_count": len(self.state["results"]),
            "has_csv": self.state["csv_data"] is not None,
        }

    async def send_events(
        self,
        session,
        nodes,
        count,
        *,
        category="sensor",
        concurrency=25,
        rng: random.Random,
    ):
        result = {
            "attempted": count * len(nodes),
            "accepted": 0,
            "tracking_ids": [],
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
            node_url = target.get("internal_url") or target.get("url")
            submitted_at = time.time()
            if not node_url:
                return None

            etype = event_types[i % len(event_types)]
            tid = str(uuid.uuid4())
            payload = {
                "type": etype,
                "value": round(rng.uniform(1.0, 100.0), 2),
                "location": f"{category}_{i}",
                "category": category,
                "metadata": {
                    "tracking_id": tid,
                    "source": "evaluation",
                    "evaluation_submitted_at": submitted_at,
                },
            }
            try:
                async with semaphore:
                    async with session.post(f"{node_url}/event", json=payload) as resp:
                        await resp.json()
                        if resp.status < 400:
                            return tid, submitted_at
            except Exception:
                return None
            return None

        submissions = [
            _send_one(i, nodes[i % len(nodes)]) for i in range(count * len(nodes))
        ]
        for item in await asyncio.gather(*submissions):
            if item is None:
                continue
            tid, submitted_at = item
            result["accepted"] += 1
            result["tracking_ids"].append(tid)
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
        sync_bytes_total = 0
        repair_pull_count = 0
        delta_pulls_started = 0
        delta_pulls_success = 0
        delta_pulls_gap = 0
        delta_deltas_applied = 0
        delta_bytes_received = 0
        event_counts = []
        merkle_roots = []
        tracking_ids = set(tracking_ids or [])
        tracking_seen_counts = {tracking_id: 0 for tracking_id in tracking_ids}
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
                    sync_bytes_total += (
                        stats.get("sent_bytes", 0)
                        + stats.get("received_bytes", 0)
                        + stats.get("repair_bytes_received", 0)
                        + node_delta_bytes
                    )
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
                        size = len(json.dumps(sdata))
                        total_state_size += size
                        summary = sdata.get("state_summary", {})
                        ev_count = summary.get("total_events", 0)
                        if ev_count == 0:
                            ev_ids = sdata.get(
                                "recent_event_ids", sdata.get("event_ids", [])
                            )
                            ev_count = len(ev_ids) if isinstance(ev_ids, list) else 0
                        event_counts.append(ev_count)
                        for tracking_id, seen in (
                            sdata.get("first_seen_tracking") or {}
                        ).items():
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
        missing_at_cutoff_count = (
            max(expected_events - min_ev, 0) if include_state else expected_events
        )
        missing_at_cutoff_pct = (
            round((missing_at_cutoff_count / expected_events) * 100, 2)
            if expected_events > 0
            else 0.0
        )
        fully_seen_tracking = [
            tracking_id
            for tracking_id, count in tracking_seen_counts.items()
            if count >= len(nodes)
        ]
        tracking_coverage_percent = (
            round((len(fully_seen_tracking) / len(tracking_seen_counts)) * 100, 1)
            if tracking_seen_counts
            else 0.0
        )
        time_to_last_node_s = (
            round(
                max(
                    tracking_last_seen_latency.get(tid, 0.0)
                    for tid in fully_seen_tracking
                ),
                3,
            )
            if fully_seen_tracking
            else None
        )
        rounds_to_last_node = (
            max(tracking_last_seen_round.get(tid, 0) for tid in fully_seen_tracking)
            if fully_seen_tracking
            else None
        )

        unique_roots = len(set(r for r in merkle_roots if r))
        direct_roots_converged = len(merkle_roots) == len(nodes) and unique_roots == 1
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
            "sync_bytes_total": sync_bytes_total,
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
            if metrics["is_converged"]:
                metrics = await self.collect_metrics(
                    session,
                    nodes,
                    gateways,
                    accepted_events=accepted_events,
                    tracking_ids=tracking_ids,
                    include_state=True,
                )
                if metrics["is_converged"] and metrics["missing_at_cutoff_count"] == 0:
                    metrics["convergence_time_s"] = round(time.time() - started, 3)
                    metrics["converged_within_timeout"] = True
                    metrics["convergence_wait_seconds"] = cfg.convergence_wait_seconds
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
                return metrics
            await asyncio.sleep(cfg.poll_interval_seconds)

    async def recreate_nodes(self, node_count: int, cfg: EvalScenarioConfig):
        if hasattr(self.docker_mgr, "clear_netem"):
            try:
                await asyncio.to_thread(self.docker_mgr.clear_netem)
            except Exception:
                pass
        existing = await asyncio.to_thread(self.docker_mgr.list_nodes)
        for node in existing:
            try:
                await asyncio.to_thread(self.docker_mgr.remove_node, node["node_id"])
            except Exception:
                pass
        await asyncio.sleep(2)
        for _ in range(node_count):
            try:
                await asyncio.to_thread(
                    self.docker_mgr.create_node,
                    sync_mode=cfg.sync_mode,
                    event_id_retention_limit=cfg.event_id_retention_limit,
                    observability_mode=cfg.dashboard_observability,
                    cpu_limit=cfg.docker_cpus,
                    memory_limit=cfg.docker_memory,
                )
            except Exception as error:
                log.warning("eval_node_create_failed", error=str(error))
        await self.sync_gateways_with_nodes(force=True)
        await asyncio.sleep(3)
        return await asyncio.to_thread(
            self.docker_mgr.list_nodes
        ), await self.discover_gateways()

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
            "repetition": repetition,
            "implementation": cfg.implementation_label(),
            "sync_mode": cfg.sync_mode,
            "event_id_retention_limit": cfg.event_id_retention_limit,
            "network_profile": cfg.network_profile,
            "dashboard_observability": cfg.dashboard_observability,
            "docker_cpus": cfg.docker_cpus or "",
            "docker_memory": cfg.docker_memory or "",
            "scenario": scenario,
            "node_count": node_count,
            "partition_split": partition_split,
            "attempted_events": attempted,
            "accepted_events": accepted,
            "write_success_rate": round(accepted / attempted, 4) if attempted else 0.0,
        }

    def _append_result(self, row, metrics):
        combined = {**row, **metrics}
        accepted = combined.get("accepted_events", 0)
        sync_bytes = combined.get("sync_bytes_total", 0)
        combined["bytes_per_accepted_event"] = (
            round(sync_bytes / accepted, 2) if accepted else ""
        )
        self.state["results"].append(combined)

    async def run(self, cfg: EvalScenarioConfig):
        run_id = f"eval-{uuid.uuid4().hex[:8]}"
        rng = random.Random(cfg.scenario_seed)

        self.state["running"] = True
        self.state["run_id"] = run_id
        self.state["started_at"] = time.time()
        self.state["results"] = []
        self.state["error"] = None
        self.state["csv_data"] = None

        partition_splits = (
            cfg.partition_splits if cfg.network_profile == "hard_partition" else []
        )
        self.state["total_steps"] = self.planned_steps(cfg)
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
            await self.reset_runtime(session, nodes, gateways)
            if cfg.network_profile != "hard_partition" and hasattr(
                self.docker_mgr, "apply_netem_profile"
            ):
                await asyncio.to_thread(
                    self.docker_mgr.apply_netem_profile, cfg.network_profile
                )
            return nodes, gateways

        async def _finish_scenario():
            try:
                await asyncio.to_thread(self.docker_mgr.heal_all)
            except Exception:
                pass
            if hasattr(self.docker_mgr, "clear_netem"):
                try:
                    await asyncio.to_thread(self.docker_mgr.clear_netem)
                except Exception:
                    pass

        try:
            timeout = aiohttp.ClientTimeout(total=30)
            for repetition in range(1, cfg.repetitions + 1):
                for node_count in cfg.node_counts:
                    async with aiohttp.ClientSession(timeout=timeout) as session:
                        nodes, gateways = await self.recreate_nodes(node_count, cfg)
                        self.state["current_step"] = (
                            f"[rep {repetition}/{cfg.repetitions}] "
                            f"[{node_count}N] Baseline"
                        )
                        self.state["progress"] = step
                        nodes, gateways = await _prepare_scenario(
                            session, nodes, gateways
                        )
                        baseline_events = await self.send_events(
                            session,
                            nodes,
                            cfg.events_per_node,
                            concurrency=cfg.event_concurrency,
                            rng=rng,
                        )
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
                        await _finish_scenario()
                        step += 1

                        for split in partition_splits:
                            split_label = (
                                f"{round(split * 100)}/{round((1 - split) * 100)}"
                            )
                            self.state["current_step"] = (
                                f"[rep {repetition}/{cfg.repetitions}] "
                                f"[{node_count}N] Partition {split_label}"
                            )
                            self.state["progress"] = step
                            nodes, gateways = await _prepare_scenario(
                                session, nodes, gateways
                            )
                            pre_events = await self.send_events(
                                session,
                                nodes,
                                cfg.events_per_node,
                                concurrency=cfg.event_concurrency,
                                rng=rng,
                            )
                            await self.poll_all_gateways(session, gateways)
                            try:
                                await asyncio.to_thread(
                                    self.docker_mgr.create_split_brain, split
                                )
                            except Exception:
                                pass
                            await asyncio.sleep(1)
                            partition_events = await self.send_events(
                                session,
                                nodes,
                                3,
                                concurrency=cfg.event_concurrency,
                                rng=rng,
                            )
                            await asyncio.sleep(cfg.partition_wait_seconds)
                            try:
                                await asyncio.to_thread(self.docker_mgr.heal_all)
                            except Exception:
                                pass
                            await asyncio.sleep(2)
                            combined_events = {
                                "attempted": pre_events["attempted"]
                                + partition_events["attempted"],
                                "accepted": pre_events["accepted"]
                                + partition_events["accepted"],
                                "tracking_ids": pre_events["tracking_ids"]
                                + partition_events["tracking_ids"],
                            }
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
                            await _finish_scenario()
                            step += 1

                        self.state["current_step"] = (
                            f"[rep {repetition}/{cfg.repetitions}] "
                            f"[{node_count}N] High load"
                        )
                        self.state["progress"] = step
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
                        )
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
                        await _finish_scenario()
                        step += 1

            self.state["current_step"] = "Generating CSV..."
            self.state["progress"] = step
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
            try:
                if hasattr(self.docker_mgr, "clear_netem"):
                    await asyncio.to_thread(self.docker_mgr.clear_netem)
            except Exception:
                pass
            self.state["running"] = False
