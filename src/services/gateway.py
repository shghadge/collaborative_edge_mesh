import asyncio
import os
import time
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
        self.runtime_metrics = {
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
        }

        self.http_retry_attempts = max(int(os.getenv("GATEWAY_HTTP_RETRIES", "2")), 1)
        self.http_retry_backoff_ms = max(int(os.getenv("GATEWAY_HTTP_RETRY_BACKOFF_MS", "150")), 0)
        self.node_failure_backoff_seconds = max(float(os.getenv("GATEWAY_NODE_FAILURE_BACKOFF", "2")), 0.0)

        # parse EDGE_NODES env: "edge-node-1:8000,edge-node-2:8000"
        raw = os.getenv("EDGE_NODES", "")
        for entry in raw.split(","):
            entry = entry.strip()
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

    def register_node(self, node_id, url):
        """Register a new edge node (used by Docker manager when creating nodes)."""
        self.edge_nodes[node_id] = {"url": url, "last_merkle": None, "last_version": None}
        self._ensure_node_health(node_id)
        log.info("node_registered", node_id=node_id, url=url)

    def unregister_node(self, node_id):
        """Remove an edge node."""
        self.edge_nodes.pop(node_id, None)
        self.node_health.pop(node_id, None)
        log.info("node_unregistered", node_id=node_id)

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
        backoff_seconds = self.node_failure_backoff_seconds * health["consecutive_failures"]
        health["backoff_until"] = time.time() + backoff_seconds

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
        now = time.time()
        if now < self.node_health[node_id]["backoff_until"]:
            return None

        last_error = None
        for attempt in range(1, self.http_retry_attempts + 1):
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
                last_error = error
                if attempt < self.http_retry_attempts:
                    self.runtime_metrics["http_retries"] += 1
                    await asyncio.sleep((self.http_retry_backoff_ms / 1000.0) * attempt)

        self._mark_node_failure(node_id, last_error)
        log.warning("http_request_failed", node=node_id, url=url, error=str(last_error))
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
                self.runtime_metrics["divergence_duration_seconds"] = round(now - started_at, 3)
        else:
            self.runtime_metrics["consecutive_divergent_polls"] = 0
            if started_at is not None:
                duration = now - started_at
                self.runtime_metrics["total_convergence_events"] += 1
                self.runtime_metrics["last_convergence_seconds"] = round(duration, 3)
                self.runtime_metrics["divergence_started_at"] = None
                self.runtime_metrics["divergence_duration_seconds"] = 0.0

    async def poll_once(self):
        """Poll all edge nodes, check divergence, merge if needed."""
        if not self.edge_nodes:
            return

        poll_started = time.time()
        self.runtime_metrics["polls_started"] += 1

        merkle_roots = {}

        try:
            async with aiohttp.ClientSession(
                timeout=aiohttp.ClientTimeout(total=5)
            ) as session:
                # fetch merkle roots from all nodes
                for node_id, info in self.edge_nodes.items():
                    data = await self._get_json_with_retry(
                        session,
                        node_id,
                        f"{info['url']}/state/merkle",
                    )
                    if not data:
                        merkle_roots[node_id] = "unreachable"
                        continue
                    merkle_roots[node_id] = data.get("merkle_root", "unreachable")
                    info["last_merkle"] = merkle_roots[node_id]

                # check divergence
                reachable_roots = {
                    k: v for k, v in merkle_roots.items() if v != "unreachable"
                }
                unique_roots = set(reachable_roots.values())
                self.is_divergent = len(unique_roots) > 1
                self.runtime_metrics["last_reachable_nodes"] = len(reachable_roots)
                self._update_divergence_metrics()

                self.store.log_divergence(self.is_divergent, merkle_roots)

                if self.is_divergent:
                    log.warning("divergence_detected", roots=merkle_roots)

                # fetch full state from all reachable nodes and merge
                merge_start = time.time()

                for node_id in reachable_roots:
                    url = self.edge_nodes[node_id]["url"]
                    data = await self._get_json_with_retry(session, node_id, f"{url}/state")
                    if not data:
                        continue
                    try:
                        incoming = NodeState.from_dict(data)
                    except Exception as error:
                        self.runtime_metrics["state_merges_failed"] += 1
                        log.warning("state_decode_failed", node=node_id, error=str(error))
                        continue

                    last_version = self.edge_nodes[node_id].get("last_version")
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
                    self.edge_nodes[node_id]["last_version"] = incoming_version
                    if before != after:
                        self.runtime_metrics["state_merges_successful"] += 1

                merge_time = time.time() - merge_start
                self.runtime_metrics["last_merge_duration_ms"] = round(merge_time * 1000, 2)

                # save snapshot
                if self.merged_state:
                    root = self.merged_state.merkle_root()
                    self.store.save_snapshot(
                        merkle_root=root,
                        node_count=len(reachable_roots),
                        source_nodes=list(reachable_roots.keys()),
                        state_dict=self.merged_state.to_dict(),
                    )
                    self.store.save_metric("merge_time_ms", merge_time * 1000)
                    self.store.save_metric("node_count", len(reachable_roots))
                    self.store.save_metric("is_divergent", 1 if self.is_divergent else 0)

            self.last_poll = time.time()
            self.poll_count += 1
            self.runtime_metrics["polls_completed"] += 1
            self.runtime_metrics["last_poll_duration_ms"] = round((time.time() - poll_started) * 1000, 2)

            log.info(
                "poll_complete",
                nodes=self.runtime_metrics["last_reachable_nodes"],
                divergent=self.is_divergent,
                merge_ms=round(self.runtime_metrics["last_merge_duration_ms"], 1),
                retries=self.runtime_metrics["http_retries"],
            )
        except Exception as error:
            self.runtime_metrics["polls_failed"] += 1
            self.runtime_metrics["last_poll_duration_ms"] = round((time.time() - poll_started) * 1000, 2)
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
        return {
            "runtime_metrics": dict(self.runtime_metrics),
            "node_health": dict(self.node_health),
            "registered_node_count": len(self.edge_nodes),
            "is_divergent": self.is_divergent,
            "poll_count": self.poll_count,
            "last_poll": self.last_poll,
        }

    def get_status(self):
        return {
            "node_id": self.config.node_id,
            "registered_nodes": {
                nid: info["url"] for nid, info in self.edge_nodes.items()
            },
            "node_health": self.node_health,
            "is_divergent": self.is_divergent,
            "last_poll": self.last_poll,
            "poll_count": self.poll_count,
            "runtime_metrics": dict(self.runtime_metrics),
            "merged_merkle": self.merged_state.merkle_root()
            if self.merged_state
            else None,
        }
