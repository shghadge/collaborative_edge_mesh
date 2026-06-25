import asyncio
import time
from datetime import datetime
import structlog
import aiohttp

from ..config import Config
from ..crdt import NodeState
from ..storage import SQLiteStore

log = structlog.get_logger()


class GatewayService:
    """
    Polls edge nodes, detects state divergence via Merkle roots,
    merges states, and stores snapshots to SQLite.
    """

    def __init__(self, config: Config, store: SQLiteStore):
        self.config = config
        self.store = store
        self.edge_nodes = {}  # node_id -> {"url": "http://...", "last_merkle": "..."}
        self.merged_state = None
        self.is_divergent = False
        self.last_poll = None
        self.poll_count = 0
        self.running = False
        self.node_health = {}
        self.relay_observations = {}
        self.runtime_metrics = self._new_runtime_metrics()

        self.visibility_freshness_seconds = max(
            config.gateway_visibility_freshness_seconds,
            config.gateway_poll_interval * 3,
        )
        self.coverage_confidence_threshold = (
            config.gateway_coverage_confidence_threshold
        )

        # parse EDGE_NODES env: "edge-node-1:8000,edge-node-2:8000"
        for entry in config.edge_nodes:
            if not entry:
                continue
            host, port = entry.rsplit(":", 1)
            node_id = host  # use hostname as node id
            self.edge_nodes[node_id] = {
                "url": f"http://{host}:{port}",
                "last_merkle": None,
                "last_version": None,
            }
            self._ensure_node_health(node_id)

    @staticmethod
    def _new_runtime_metrics():
        return {
            "polls_started": 0,
            "polls_completed": 0,
            "polls_failed": 0,
            "total_http_requests": 0,
            "total_http_success": 0,
            "total_http_failures": 0,
            "http_retries": 0,
            "state_merges_successful": 0,
            "state_merges_failed": 0,
            "stale_state_skips": 0,
            "consecutive_divergent_polls": 0,
            "divergence_started_at": None,
            "divergence_duration_seconds": 0.0,
            "total_convergence_events": 0,
            "last_convergence_seconds": None,
            "last_poll_duration_ms": 0.0,
            "last_merge_duration_ms": 0.0,
            "last_reachable_nodes": 0,
            "relay_reports_received": 0,
            "relay_observations_accepted": 0,
        }

    def register_node(self, node_id, url):
        """Register a new edge node (used by Docker manager when creating nodes)."""
        self.edge_nodes[node_id] = {
            "url": url,
            "last_merkle": None,
            "last_version": None,
        }
        self._ensure_node_health(node_id)
        log.info("node_registered", node_id=node_id, url=url)

    def unregister_node(self, node_id):
        """Remove an edge node."""
        self.edge_nodes.pop(node_id, None)
        self.node_health.pop(node_id, None)
        log.info("node_unregistered", node_id=node_id)

    def sync_nodes(self, nodes):
        """Replace current edge node map with discovered nodes."""
        desired = {}
        for node in nodes:
            node_id = node.get("node_id")
            url = node.get("url")
            if not node_id or not url:
                continue

            existing = self.edge_nodes.get(node_id, {})
            desired[node_id] = {
                "url": url,
                "last_merkle": existing.get("last_merkle"),
                "last_version": existing.get("last_version"),
            }

        removed = [node_id for node_id in self.edge_nodes if node_id not in desired]
        self.edge_nodes = desired

        for node_id in desired:
            self._ensure_node_health(node_id)

        for node_id in removed:
            self.node_health.pop(node_id, None)
            self.relay_observations.pop(node_id, None)

        log.info(
            "nodes_synced",
            total=len(self.edge_nodes),
            removed=removed,
        )

    def reset_runtime(self):
        """Clear scenario-scoped gateway state while preserving registered nodes."""
        self.merged_state = None
        self.is_divergent = False
        self.last_poll = None
        self.poll_count = 0
        self.relay_observations.clear()
        self.runtime_metrics = self._new_runtime_metrics()
        for info in self.edge_nodes.values():
            info["last_merkle"] = None
            info["last_version"] = None
        self.node_health = {}
        for node_id in self.edge_nodes:
            self._ensure_node_health(node_id)

    def _ensure_node_health(self, node_id):
        if node_id not in self.node_health:
            self.node_health[node_id] = {
                "consecutive_failures": 0,
                "last_error": None,
                "last_success_at": None,
                "last_latency_ms": None,
                "backoff_until": 0.0,
            }

    def _mark_node_failure(self, node_id, error):
        self._ensure_node_health(node_id)
        health = self.node_health[node_id]
        health["consecutive_failures"] += 1
        health["last_error"] = str(error)
        # Backoff disabled: keep field for backward-compatible metrics shape.
        health["backoff_until"] = 0.0

    def _mark_node_success(self, node_id, latency_ms):
        self._ensure_node_health(node_id)
        health = self.node_health[node_id]
        health["consecutive_failures"] = 0
        health["last_error"] = None
        health["last_success_at"] = time.time()
        health["last_latency_ms"] = round(latency_ms, 2)
        health["backoff_until"] = 0.0

    async def _get_json_with_retry(self, session, node_id, url):
        self._ensure_node_health(node_id)
        self.runtime_metrics["total_http_requests"] += 1
        started = time.time()
        try:
            async with session.get(url) as resp:
                data = await resp.json()
            latency_ms = (time.time() - started) * 1000
            self.runtime_metrics["total_http_success"] += 1
            self._mark_node_success(node_id, latency_ms)
            return data
        except Exception as error:
            self.runtime_metrics["total_http_failures"] += 1
            self._mark_node_failure(node_id, error)
            log.warning("http_request_failed", node=node_id, url=url, error=str(error))
            return None

    def _update_divergence_metrics(self):
        now = time.time()
        started_at = self.runtime_metrics["divergence_started_at"]

        if self.is_divergent:
            self.runtime_metrics["consecutive_divergent_polls"] += 1
            if started_at is None:
                self.runtime_metrics["divergence_started_at"] = now
                self.runtime_metrics["divergence_duration_seconds"] = 0.0
            else:
                self.runtime_metrics["divergence_duration_seconds"] = round(
                    now - started_at, 3
                )
        else:
            self.runtime_metrics["consecutive_divergent_polls"] = 0
            if started_at is not None:
                duration = now - started_at
                self.runtime_metrics["total_convergence_events"] += 1
                self.runtime_metrics["last_convergence_seconds"] = round(duration, 3)
                self.runtime_metrics["divergence_started_at"] = None
                self.runtime_metrics["divergence_duration_seconds"] = 0.0

    def _parse_observed_time(self, value):
        if value is None:
            return time.time()
        if isinstance(value, (int, float)):
            return float(value)
        if isinstance(value, str):
            try:
                return datetime.fromisoformat(value.replace("Z", "+00:00")).timestamp()
            except Exception:
                return time.time()
        return time.time()

    def _upsert_observation(self, source_node, observation, direct=False):
        node_id = observation.get("node_id")
        if not node_id:
            return False

        incoming_last_seen = self._parse_observed_time(observation.get("last_seen_at"))
        incoming_observed_at = time.time()
        observed_monotonic_at = time.monotonic()
        incoming_mode = (
            "direct" if direct else observation.get("reachable_via", "relayed")
        )
        existing = self.relay_observations.get(node_id)
        if existing is not None:
            existing_last_seen = self._parse_observed_time(existing.get("last_seen_at"))
            if incoming_last_seen < existing_last_seen:
                return False
            if (
                incoming_last_seen == existing_last_seen
                and existing.get("reachable_via") == "direct"
                and incoming_mode != "direct"
            ):
                return False

        self.relay_observations[node_id] = {
            "node_id": node_id,
            "merkle_root": observation.get("merkle_root", ""),
            "state_version": observation.get("state_version"),
            "last_seen_at": incoming_last_seen,
            "observed_at": incoming_observed_at,
            "observed_monotonic_at": observed_monotonic_at,
            "source_node": source_node,
            "reachable_via": incoming_mode,
            "hops": 0 if direct else int(observation.get("hops", 1) or 1),
        }
        return True

    def ingest_relay_observations(self, source_node: str, observations: list[dict]):
        self.runtime_metrics["relay_reports_received"] += 1
        accepted = 0
        for observation in observations:
            if not isinstance(observation, dict):
                continue
            if self._upsert_observation(source_node, observation, direct=False):
                accepted += 1
        self.runtime_metrics["relay_observations_accepted"] += accepted

        visibility = self._build_visibility_snapshot()
        return {
            "status": "accepted",
            "source_node": source_node,
            "received": len(observations),
            "accepted": accepted,
            "known_status": visibility["known_status"],
            "coverage": visibility["coverage"],
        }

    def _build_visibility_snapshot(self):
        now = time.monotonic()
        expected_nodes = len(self.edge_nodes)
        freshness = self.visibility_freshness_seconds

        def observation_age(obs):
            return max(0.0, now - obs.get("observed_monotonic_at", now))

        fresh = [
            obs
            for obs in self.relay_observations.values()
            if observation_age(obs) <= freshness
        ]
        fresh_roots = {
            obs.get("merkle_root") for obs in fresh if obs.get("merkle_root")
        }
        fresh_observed_nodes = len(fresh)
        observation_ages = [observation_age(obs) for obs in fresh]
        if expected_nodes <= 0:
            # No registered edge nodes means there is no visibility basis yet.
            # Report 0 coverage to avoid a misleading "100% + insufficient" mix.
            coverage = 0.0
        else:
            coverage = min(fresh_observed_nodes / expected_nodes, 1.0)

        if coverage < self.coverage_confidence_threshold:
            known_status = "unknown_partial"
        elif len(fresh_roots) > 1:
            known_status = "known_diverged"
        elif len(fresh_roots) == 1:
            known_status = "known_converged"
        else:
            known_status = "unknown_partial"

        return {
            "known_status": known_status,
            "coverage": round(coverage, 3),
            "fresh_observed_nodes": fresh_observed_nodes,
            "expected_nodes": expected_nodes,
            "fresh_merkle_roots": len(fresh_roots),
            "confidence_threshold": self.coverage_confidence_threshold,
            "freshness_window_seconds": freshness,
            "max_observation_age_seconds": round(max(observation_ages), 3)
            if observation_ages
            else None,
        }

    async def poll_once(self):
        """Poll all edge nodes, check divergence, merge if needed."""
        if not self.edge_nodes:
            return

        poll_started = time.time()
        self.runtime_metrics["polls_started"] += 1

        merkle_roots = {}

        try:
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=5),
                connector=aiohttp.TCPConnector(limit=50),
            ) as session:
                # Fetch merkle roots from all nodes CONCURRENTLY
                async def _fetch_merkle(node_id, info):
                    data = await self._get_json_with_retry(
                        session,
                        node_id,
                        f"{info['url']}/state/merkle",
                    )
                    if not data:
                        return node_id, "unreachable"
                    root = data.get("merkle_root", "unreachable")
                    info["last_merkle"] = root
                    return node_id, root

                merkle_results = await asyncio.gather(
                    *[
                        _fetch_merkle(nid, info)
                        for nid, info in self.edge_nodes.items()
                    ],
                    return_exceptions=True,
                )
                for result in merkle_results:
                    if isinstance(result, Exception):
                        continue
                    node_id, root = result
                    merkle_roots[node_id] = root

                # check divergence
                reachable_roots = {
                    k: v for k, v in merkle_roots.items() if v != "unreachable"
                }
                unique_roots = set(reachable_roots.values())
                self.is_divergent = len(unique_roots) > 1
                self.runtime_metrics["last_reachable_nodes"] = len(reachable_roots)
                self._update_divergence_metrics()

                # Register direct observations for visibility/confidence model
                for node_id, root in reachable_roots.items():
                    self._upsert_observation(
                        source_node=node_id,
                        observation={
                            "node_id": node_id,
                            "merkle_root": root,
                            "state_version": self.edge_nodes.get(node_id, {}).get(
                                "last_version"
                            ),
                            "last_seen_at": time.time(),
                            "hops": 0,
                            "reachable_via": "direct",
                        },
                        direct=True,
                    )

                self.store.log_divergence(self.is_divergent, merkle_roots)

                if self.is_divergent:
                    log.warning("divergence_detected", roots=merkle_roots)

                # Fetch full state from all reachable nodes CONCURRENTLY and merge
                merge_start = time.time()

                nodes_to_fetch = list(reachable_roots.keys())
                if not self.is_divergent and nodes_to_fetch:
                    # When roots already agree, fetch one representative state only.
                    nodes_to_fetch = [nodes_to_fetch[0]]

                async def _fetch_state(node_id):
                    node_info = self.edge_nodes.get(node_id)
                    if not node_info:
                        return node_id, None
                    url = node_info["url"]
                    data = await self._get_json_with_retry(
                        session, node_id, f"{url}/state"
                    )
                    return node_id, data

                state_results = await asyncio.gather(
                    *[_fetch_state(nid) for nid in nodes_to_fetch],
                    return_exceptions=True,
                )

                async def _fetch_relay(node_id):
                    node_info = self.edge_nodes.get(node_id)
                    if not node_info:
                        return node_id, None
                    url = node_info["url"]
                    data = await self._get_json_with_retry(
                        session, node_id, f"{url}/relay/observations"
                    )
                    return node_id, data

                relay_results = await asyncio.gather(
                    *[_fetch_relay(nid) for nid in reachable_roots],
                    return_exceptions=True,
                )

                for relay_result in relay_results:
                    if isinstance(relay_result, Exception):
                        continue
                    node_id, relay_data = relay_result
                    if not isinstance(relay_data, dict):
                        continue
                    observations = relay_data.get("observations", [])
                    source_node = relay_data.get("source_node") or node_id
                    if isinstance(observations, list) and observations:
                        self.ingest_relay_observations(source_node, observations)

                for result in state_results:
                    if isinstance(result, Exception):
                        continue
                    node_id, data = result
                    if not data:
                        continue
                    try:
                        incoming = NodeState.from_dict(data)
                    except Exception as error:
                        self.runtime_metrics["state_merges_failed"] += 1
                        log.warning(
                            "state_decode_failed", node=node_id, error=str(error)
                        )
                        continue

                    last_version = self.edge_nodes.get(node_id, {}).get("last_version")
                    incoming_version = getattr(incoming, "version", None)
                    if (
                        last_version is not None
                        and incoming_version is not None
                        and incoming_version < last_version
                    ):
                        self.runtime_metrics["stale_state_skips"] += 1
                        log.warning(
                            "stale_state_skipped",
                            node=node_id,
                            incoming_version=incoming_version,
                            last_version=last_version,
                        )
                        continue

                    if self.merged_state is None:
                        self.merged_state = NodeState("gateway")
                        self.merged_state.counters = {}

                    before = self.merged_state.merkle_root()
                    self.merged_state.merge(incoming)
                    after = self.merged_state.merkle_root()
                    if node_id in self.edge_nodes:
                        self.edge_nodes[node_id]["last_version"] = incoming_version
                    if before != after:
                        self.runtime_metrics["state_merges_successful"] += 1

                merge_time = time.time() - merge_start
                self.runtime_metrics["last_merge_duration_ms"] = round(
                    merge_time * 1000, 2
                )

                # save snapshot
                if self.merged_state:
                    import json as _json

                    root = self.merged_state.merkle_root()
                    state_dict = self.merged_state.to_dict()
                    state_size = len(_json.dumps(state_dict))
                    self.store.save_snapshot(
                        merkle_root=root,
                        node_count=len(reachable_roots),
                        source_nodes=list(reachable_roots.keys()),
                        state_dict=state_dict,
                        retention_limit=self.config.gateway_snapshot_retention_limit,
                    )
                    self.store.save_metric("merge_time_ms", merge_time * 1000)
                    self.store.save_metric("node_count", len(reachable_roots))
                    self.store.save_metric(
                        "is_divergent", 1 if self.is_divergent else 0
                    )

                    # Save time-series for dashboard charts
                    self.store.save_timeseries_metric(
                        "state_size_bytes",
                        state_size,
                    )
                    convergence_value = self.runtime_metrics.get(
                        "last_convergence_seconds"
                    )
                    if convergence_value is None:
                        convergence_value = 0.0
                    self.store.save_timeseries_metric(
                        "convergence_time",
                        convergence_value,
                    )
                    self.store.save_timeseries_metric(
                        "node_count",
                        len(reachable_roots),
                    )
                    self.store.save_timeseries_metric(
                        "is_divergent",
                        1.0 if self.is_divergent else 0.0,
                    )

            self.last_poll = time.time()
            self.poll_count += 1
            self.runtime_metrics["polls_completed"] += 1
            self.runtime_metrics["last_poll_duration_ms"] = round(
                (time.time() - poll_started) * 1000, 2
            )

            log.info(
                "poll_complete",
                nodes=self.runtime_metrics["last_reachable_nodes"],
                divergent=self.is_divergent,
                merge_ms=round(self.runtime_metrics["last_merge_duration_ms"], 1),
                retries=self.runtime_metrics["http_retries"],
            )
        except Exception as error:
            self.runtime_metrics["polls_failed"] += 1
            self.runtime_metrics["last_poll_duration_ms"] = round(
                (time.time() - poll_started) * 1000, 2
            )
            log.error("poll_cycle_failed", error=str(error))
            raise

    async def start_polling(self, interval=10):
        """Poll in a loop."""
        self.running = True
        log.info(
            "gateway_polling_started",
            interval=interval,
            nodes=list(self.edge_nodes.keys()),
        )
        while self.running:
            await self.poll_once()
            await asyncio.sleep(interval)

    def stop(self):
        self.running = False

    def get_runtime_metrics(self):
        visibility = self._build_visibility_snapshot()
        return {
            "runtime_metrics": dict(self.runtime_metrics),
            "node_health": dict(self.node_health),
            "registered_node_count": len(self.edge_nodes),
            "is_divergent": self.is_divergent,
            "known_status": visibility["known_status"],
            "coverage": visibility["coverage"],
            "visibility": visibility,
            "poll_count": self.poll_count,
            "last_poll": self.last_poll,
        }

    def get_status(self):
        visibility = self._build_visibility_snapshot()
        return {
            "node_id": self.config.node_id,
            "registered_nodes": {
                nid: info["url"] for nid, info in self.edge_nodes.items()
            },
            "node_health": self.node_health,
            "is_divergent": self.is_divergent,
            "known_status": visibility["known_status"],
            "coverage": visibility["coverage"],
            "visibility": visibility,
            "last_poll": self.last_poll,
            "poll_count": self.poll_count,
            "runtime_metrics": dict(self.runtime_metrics),
            "merged_merkle": self.merged_state.merkle_root()
            if self.merged_state
            else None,
        }
