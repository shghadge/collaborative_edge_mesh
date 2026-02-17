import structlog
from datetime import datetime
from typing import Any, Dict

from fastapi import FastAPI, HTTPException

from ..config import Config
from ..models import Event, NodeStatus
from ..crdt import NodeState
from ..hash_chain import HashChainLog

log = structlog.get_logger()


class IntakeService:
    """HTTP API for receiving events and querying state."""

    def __init__(self, config: Config, state: NodeState, chain: HashChainLog):
        self.config = config
        self.state = state
        self.chain = chain
        self.start_time = datetime.utcnow()
        self.app = self._build_app()

    def _build_app(self):
        app = FastAPI(title=f"Edge Node {self.config.node_id}")

        @app.get("/")
        async def root():
            return {"node_id": self.config.node_id, "service": "edge-mesh"}

        @app.get("/health")
        async def health():
            return {"status": "ok"}

        @app.post("/event")
        async def receive_event(event: Event):
            """Receive an event, log it to the hash chain, record in CRDT state.

            The response tells you exactly where the data was stored:
            which counter, register, pn_counter, or set key was used.
            """
            try:
                entry = self.chain.append(
                    event.id, event.type, event.model_dump(mode="json")
                )

                # build the data dict from the event fields + metadata
                event_data = {
                    "value": event.value,
                    "location": event.location,
                    **event.metadata,
                }

                # route to correct CRDT based on category
                stored_in = self.state.record_event(
                    event.id,
                    event.type,
                    event_data,
                    category=event.category.value,
                    operation=event.operation,
                )

                log.info(
                    "event_received",
                    event_id=event.id,
                    type=event.type,
                    category=event.category.value,
                    stored_in=stored_in,
                )

                return {
                    "status": "accepted",
                    "event_id": event.id,
                    "category": event.category.value,
                    "log_sequence": entry["sequence"],
                    "version": self.state.version,
                    "stored_in": stored_in,
                }
            except Exception as e:
                log.error("event_failed", error=str(e))
                raise HTTPException(status_code=500, detail=str(e))

        @app.get("/state")
        async def get_state():
            return self.state.to_dict()

        @app.get("/state/merkle")
        async def get_merkle():
            return {
                "node_id": self.config.node_id,
                "merkle_root": self.state.merkle_root(),
                "version": self.state.version,
            }

        @app.get("/status")
        async def get_status():
            uptime = (datetime.utcnow() - self.start_time).total_seconds()
            return NodeStatus(
                node_id=self.config.node_id,
                version=self.state.version,
                merkle_root=self.state.merkle_root(),
                peer_count=len(self.config.peers),
                event_count=self.state.get_event_count(),
                uptime_seconds=uptime,
            ).model_dump()

        @app.get("/log")
        async def get_log(since: int = 0, limit: int = 100):
            entries = self.chain.get_entries(since)[:limit]
            return {
                "entries": entries,
                "total": len(self.chain.entries),
                "valid": self.chain.verify(),
                "latest_hash": self.chain.latest_hash(),
            }

        @app.post("/merge")
        async def merge_state(remote_state: Dict[str, Any]):
            """Merge a remote node's state into ours."""
            try:
                incoming = NodeState.from_dict(remote_state)
                old_root = self.state.merkle_root()
                self.state.merge(incoming)
                new_root = self.state.merkle_root()
                log.info(
                    "state_merged",
                    from_node=remote_state.get("node_id"),
                    old_root=old_root[:12],
                    new_root=new_root[:12],
                )
                return {
                    "status": "merged",
                    "version": self.state.version,
                    "merkle_root": new_root,
                }
            except Exception as e:
                log.error("merge_failed", error=str(e))
                raise HTTPException(status_code=400, detail=str(e))

        return app
