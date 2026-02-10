import asyncio
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

        # parse EDGE_NODES env: "edge-node-1:8000,edge-node-2:8000"
        import os

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
            }

    def register_node(self, node_id, url):
        """Register a new edge node (used by Docker manager when creating nodes)."""
        self.edge_nodes[node_id] = {"url": url, "last_merkle": None}
        log.info("node_registered", node_id=node_id, url=url)

    def unregister_node(self, node_id):
        """Remove an edge node."""
        self.edge_nodes.pop(node_id, None)
        log.info("node_unregistered", node_id=node_id)

    async def poll_once(self):
        """Poll all edge nodes, check divergence, merge if needed."""
        if not self.edge_nodes:
            return

        merkle_roots = {}
        node_states = {}

        async with aiohttp.ClientSession(
            timeout=aiohttp.ClientTimeout(total=5)
        ) as session:
            # fetch merkle roots from all nodes
            for node_id, info in self.edge_nodes.items():
                try:
                    async with session.get(f"{info['url']}/state/merkle") as resp:
                        data = await resp.json()
                        merkle_roots[node_id] = data["merkle_root"]
                        info["last_merkle"] = data["merkle_root"]
                except Exception as e:
                    log.warning("poll_failed", node=node_id, error=str(e))
                    merkle_roots[node_id] = "unreachable"

            # check divergence
            reachable_roots = {
                k: v for k, v in merkle_roots.items() if v != "unreachable"
            }
            unique_roots = set(reachable_roots.values())
            self.is_divergent = len(unique_roots) > 1

            self.store.log_divergence(self.is_divergent, merkle_roots)

            if self.is_divergent:
                log.warning("divergence_detected", roots=merkle_roots)

            # fetch full state from all reachable nodes and merge
            start = time.time()

            for node_id in reachable_roots:
                try:
                    url = self.edge_nodes[node_id]["url"]
                    async with session.get(f"{url}/state") as resp:
                        data = await resp.json()
                        incoming = NodeState.from_dict(data)
                        node_states[node_id] = incoming

                        if self.merged_state is None:
                            self.merged_state = NodeState("gateway")
                            self.merged_state.counters = {}

                        self.merged_state.merge(incoming)

                except Exception as e:
                    log.warning("state_fetch_failed", node=node_id, error=str(e))

            merge_time = time.time() - start

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
        log.info(
            "poll_complete",
            nodes=len(reachable_roots),
            divergent=self.is_divergent,
            merge_ms=round(merge_time * 1000, 1),
        )

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

    def get_status(self):
        return {
            "node_id": self.config.node_id,
            "registered_nodes": {
                nid: info["url"] for nid, info in self.edge_nodes.items()
            },
            "is_divergent": self.is_divergent,
            "last_poll": self.last_poll,
            "poll_count": self.poll_count,
            "merged_merkle": self.merged_state.merkle_root()
            if self.merged_state
            else None,
        }
