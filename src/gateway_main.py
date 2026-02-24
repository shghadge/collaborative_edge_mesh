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
from fastapi.responses import JSONResponse
from pydantic import BaseModel

from src.config import config
from src.storage import SQLiteStore
from src.services.gateway import GatewayService

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

store = SQLiteStore(f"{config.data_dir}/gateway.db")
gateway = GatewayService(config, store)


class GatewayNode(BaseModel):
    node_id: str
    url: str


class GatewayNodeSyncRequest(BaseModel):
    nodes: List[GatewayNode]


def _api_error(
    status_code: int, code: str, message: str, details=None
) -> HTTPException:
    payload = {"code": code, "message": message}
    if details is not None:
        payload["details"] = details
    return HTTPException(status_code=status_code, detail=payload)


@app.exception_handler(HTTPException)
async def http_exception_handler(request: Request, exc: HTTPException):
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
    log.error("gateway_unhandled_exception", error=str(exc), path=str(request.url.path))
    return JSONResponse(
        status_code=500,
        content={
            "code": "INTERNAL_SERVER_ERROR",
            "message": "Unexpected gateway error",
        },
    )


# --- Gateway status & state ---


# --- Gateway status & state ---


@app.get("/gateway/status")
async def gateway_status():
    return gateway.get_status()


@app.get("/gateway/runtime-metrics")
async def gateway_runtime_metrics():
    return gateway.get_runtime_metrics()


@app.post("/gateway/nodes/register")
async def register_gateway_node(node: GatewayNode):
    gateway.register_node(node.node_id, node.url)
    return {"status": "registered", "node_id": node.node_id, "url": node.url}


@app.delete("/gateway/nodes/{node_id}")
async def unregister_gateway_node(node_id: str):
    gateway.unregister_node(node_id)
    return {"status": "unregistered", "node_id": node_id}


@app.post("/gateway/nodes/sync")
async def sync_gateway_nodes(payload: GatewayNodeSyncRequest):
    gateway.sync_nodes([node.model_dump() for node in payload.nodes])
    return {
        "status": "synced",
        "registered_count": len(gateway.edge_nodes),
        "registered_nodes": {
            node_id: info["url"] for node_id, info in gateway.edge_nodes.items()
        },
    }


@app.post("/gateway/poll")
async def trigger_poll():
    """Manually trigger a poll cycle."""
    await gateway.poll_once()
    return gateway.get_status()


@app.get("/gateway/merged-state")
async def merged_state():
    snapshot = store.get_latest_snapshot()
    if not snapshot:
        return {"status": "no data yet, trigger a poll first"}
    return snapshot


@app.get("/gateway/history")
async def snapshot_history(limit: int = 20):
    return store.get_snapshot_history(limit)


@app.get("/gateway/divergence")
async def divergence():
    return {
        "is_divergent": gateway.is_divergent,
        "log": store.get_divergence_log(20),
    }


@app.get("/gateway/metrics")
async def metrics(name: Optional[str] = None, limit: int = 100):
    return store.get_metrics(name, limit)


# --- Start ---


async def main():
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
        await shutdown.wait()
        gateway.stop()
        raise asyncio.CancelledError()

    try:
        await asyncio.gather(
            server.serve(),
            gateway.start_polling(interval=poll_interval),
            wait_shutdown(),
        )
    except asyncio.CancelledError:
        pass
    finally:
        log.info("gateway_stopped")


if __name__ == "__main__":
    asyncio.run(main())
