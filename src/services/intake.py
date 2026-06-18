import json
import uuid
import structlog
from datetime import datetime, timezone
from typing import Any, Dict

from fastapi import FastAPI, HTTPException
from fastapi.middleware.cors import CORSMiddleware

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
        app.add_middleware(
            CORSMiddleware,
            allow_origins=["*"],
            allow_credentials=True,
            allow_methods=["*"],
            allow_headers=["*"],
        )

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
            Also returns tracking metadata for per-message observability.
            """
            try:
                tracking_id = str(event.metadata.get("tracking_id") or uuid.uuid4())
                recorded_at = datetime.now(timezone.utc).isoformat()

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

                # Compute tracking metadata
                merkle_root_after = self.state.merkle_root()
                state_dict = self.state.to_dict()
                state_size_bytes = len(json.dumps(state_dict))

                log.info(
                    "event_received",
                    event_id=event.id,
                    type=event.type,
                    category=event.category.value,
                    stored_in=stored_in,
                    tracking_id=tracking_id,
                )

                return {
                    "status": "accepted",
                    "event_id": event.id,
                    "category": event.category.value,
                    "log_sequence": entry["sequence"],
                    "version": self.state.version,
                    "stored_in": stored_in,
                    # Per-message tracking metadata
                    "tracking_id": tracking_id,
                    "recorded_at": recorded_at,
                    "state_size_bytes": state_size_bytes,
                    "merkle_root_after": merkle_root_after,
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
                peer_count=0,  # updated dynamically via /peers endpoint
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

        @app.get("/metrics/summary")
        async def metrics_summary():
            """Node-level metric snapshot for dashboard per-node stats."""
            state_dict = self.state.to_dict()
            state_size = len(json.dumps(state_dict))
            uptime = (
                datetime.now(timezone.utc)
                - self.start_time.replace(tzinfo=timezone.utc)
            ).total_seconds()
            return {
                "node_id": self.config.node_id,
                "event_count": self.state.get_event_count(),
                "state_size_bytes": state_size,
                "merkle_root": self.state.merkle_root(),
                "version": self.state.version,
                "uptime_seconds": round(uptime, 2),
            }

        return app
