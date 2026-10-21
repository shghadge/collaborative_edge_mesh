"""
Gateway entry point. Runs the gateway polling loop and HTTP API.
Provides endpoints for state management and gateway node-sync control.
"""

import asyncio
import logging
import signal
import structlog
import uvicorn
from typing import List, Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.middleware.cors import CORSMiddleware
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from src.config import config
from src.storage import SQLiteStore
from src.services.gateway import GatewayService
from src.services.peer_discovery import PeerDiscovery

logging.basicConfig(
    format="%(message)s", level=getattr(logging, config.log_level, logging.INFO)
)

structlog.configure(
    processors=[
        structlog.stdlib.add_log_level,
        structlog.processors.TimeStamper(fmt="iso"),
        structlog.dev.ConsoleRenderer(),
    ],
    wrapper_class=structlog.stdlib.BoundLogger,
    logger_factory=structlog.stdlib.LoggerFactory(),
)

log = structlog.get_logger()

app = FastAPI(title="Edge Mesh Gateway")
app.add_middleware(
    CORSMiddleware,
    allow_origins=["*"],
    allow_credentials=True,
    allow_methods=["*"],
    allow_headers=["*"],
)

store = SQLiteStore(f"{config.data_dir}/gateway.db")
gateway = GatewayService(config, store)
discovery = PeerDiscovery(
    config.node_id,
    config.http_port,
    assigned_gateway="gateway",
    role="gateway",
)


class GatewayNode(BaseModel):
    """An edge node the gateway should poll, identified by id and base URL."""

    node_id: str
    url: str


class GatewayNodeSyncRequest(BaseModel):
    """Request body for replacing the gateway's full set of edge nodes."""

    nodes: List[GatewayNode]


class RelayObservationItem(BaseModel):
    """One node observation relayed by an edge node to the gateway."""

    node_id: str
    merkle_root: Optional[str] = None
    state_version: Optional[int] = None
    last_seen_at: Optional[str] = None
    reachable_via: Optional[str] = "relayed"
    hops: Optional[int] = 1


class RelayObservationRequest(BaseModel):
    """Request body for a batch of relayed observations from one edge node."""

    source_node: str
    observations: List[RelayObservationItem]


def _api_error(
    status_code: int, code: str, message: str, details=None
) -> HTTPException:
    """Build an HTTPException whose detail is a `{code, message, details}` payload."""
    payload = {"code": code, "message": message}
    if details is not None:
        payload["details"] = details
    return HTTPException(status_code=status_code, detail=payload)


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
    """Return HTTP errors as JSON with `code` and `message` fields."""
    if (
        isinstance(exc.detail, dict)
        and "code" in exc.detail
        and "message" in exc.detail
    ):
        payload = dict(exc.detail)
    else:
        payload = {
            "code": "HTTP_ERROR",
            "message": str(exc.detail),
        }
    return JSONResponse(status_code=exc.status_code, content=payload)


@app.exception_handler(Exception)
async def unhandled_exception_handler(request: Request, exc: Exception):
    """Log an unexpected error and return a generic JSON 500 response."""
    log.error("gateway_unhandled_exception", error=str(exc), path=str(request.url.path))
    return JSONResponse(
        status_code=500,
        content={
            "code": "INTERNAL_SERVER_ERROR",
            "message": "Unexpected gateway error",
        },
    )


# --- Gateway status & state ---


@app.get("/gateway/status")
async def gateway_status():
    """Return the gateway status, including registered nodes and visibility."""
    return gateway.get_status()


@app.get("/gateway/runtime-metrics")
async def gateway_runtime_metrics():
    """Return gateway runtime counters, node health, and visibility."""
    return gateway.get_runtime_metrics()


@app.post("/gateway/evaluation/reset")
async def gateway_evaluation_reset():
    """Clear scenario state and metrics while keeping registered nodes."""
    gateway.reset_runtime()
    return {
        "status": "reset",
        "registered_node_count": len(gateway.edge_nodes),
    }


@app.post("/gateway/nodes/register")
async def register_gateway_node(node: GatewayNode):
    """Register a single edge node for polling."""
    gateway.register_node(node.node_id, node.url)
    return {"status": "registered", "node_id": node.node_id, "url": node.url}


@app.delete("/gateway/nodes/{node_id}")
async def unregister_gateway_node(node_id: str):
    """Stop polling the given edge node."""
    gateway.unregister_node(node_id)
    return {"status": "unregistered", "node_id": node_id}


@app.post("/gateway/nodes/sync")
async def sync_gateway_nodes(payload: GatewayNodeSyncRequest):
    """Replace the registered edge nodes with the given list."""
    gateway.sync_nodes([node.model_dump() for node in payload.nodes])
    return {
        "status": "synced",
        "registered_count": len(gateway.edge_nodes),
        "registered_nodes": {
            node_id: info["url"] for node_id, info in gateway.edge_nodes.items()
        },
    }


@app.post("/gateway/relay-observations")
async def relay_observations(payload: RelayObservationRequest):
    """Ingest observations an edge node relays about other nodes."""
    return gateway.ingest_relay_observations(
        source_node=payload.source_node,
        observations=[item.model_dump() for item in payload.observations],
    )


@app.post("/gateway/poll")
async def trigger_poll():
    """Manually trigger a poll cycle."""
    await gateway.poll_once()
    return gateway.get_status()


@app.get("/gateway/merged-state")
async def merged_state():
    """Return the latest stored merged-state snapshot, if any."""
    snapshot = store.get_latest_snapshot()
    if not snapshot:
        return {"status": "no data yet, trigger a poll first"}
    return snapshot


@app.get("/gateway/peers")
async def discovered_peers():
    """Return peers found by mDNS discovery, with discovery stats."""
    return {
        "node_id": config.node_id,
        "discovery": "mdns",
        "peers": discovery.get_peers(),
        "peer_count": len(discovery.get_peers()),
        "stats": discovery.stats,
    }


@app.get("/gateway/history")
async def snapshot_history(limit: int = 20):
    """Return the most recent snapshot summaries, newest first."""
    return store.get_snapshot_history(limit)


@app.get("/gateway/divergence")
async def divergence():
    """Return the divergence flag, visibility status, and recent divergence log."""
    visibility = gateway.get_runtime_metrics().get("visibility", {})
    return {
        "is_divergent": gateway.is_divergent,
        "known_status": visibility.get("known_status", "unknown_partial"),
        "coverage": visibility.get("coverage", 0.0),
        "log": store.get_divergence_log(20),
    }


@app.get("/gateway/metrics")
async def metrics(name: Optional[str] = None, limit: int = 100):
    """Return stored metrics, optionally filtered by name, newest first."""
    return store.get_metrics(name, limit)


async def main():
    """Run mDNS discovery, the HTTP server, and the polling loop until shutdown.

    SIGTERM and SIGINT stop polling and end the run; discovery is always
    stopped on exit.
    """
    shutdown = asyncio.Event()
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown.set)

    poll_interval = config.gateway_poll_interval

    server = uvicorn.Server(
        uvicorn.Config(app, host="0.0.0.0", port=config.http_port, log_level="warning")
    )

    log.info(
        "gateway_starting",
        http=config.http_port,
        poll_interval=poll_interval,
        nodes=list(gateway.edge_nodes.keys()),
    )

    async def wait_shutdown():
        """Wait for a shutdown signal, stop polling, and cancel the gather."""
        await shutdown.wait()
        gateway.stop()
        raise asyncio.CancelledError()

    try:
        await discovery.start()
        await asyncio.gather(
            server.serve(),
            gateway.start_polling(interval=poll_interval),
            wait_shutdown(),
        )
    except asyncio.CancelledError:
        pass
    finally:
        await discovery.stop()
        log.info("gateway_stopped")


if __name__ == "__main__":
    asyncio.run(main())
