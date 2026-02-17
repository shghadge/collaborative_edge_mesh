"""
Gateway entry point. Runs the gateway polling loop and HTTP API.
Provides endpoints for state management, Docker node control, and partition simulation.
"""

import asyncio
import logging
import signal
import structlog
import uvicorn
from typing import Optional

from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse

from src.config import config
from src.storage import SQLiteStore
from src.services.gateway import GatewayService
from src.services.docker_manager import DockerManager

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

# docker manager may fail if docker isn't available
try:
    docker_mgr = DockerManager(gateway)
except Exception as e:
    log.warning("docker_unavailable", error=str(e))
    docker_mgr = None


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


# --- Docker node management ---


@app.get("/nodes")
async def list_nodes():
    if not docker_mgr:
        raise _api_error(503, "DOCKER_UNAVAILABLE", "Docker not available")
    return docker_mgr.list_nodes()


@app.post("/nodes")
async def create_node(node_id: Optional[str] = None):
    if not docker_mgr:
        raise _api_error(503, "DOCKER_UNAVAILABLE", "Docker not available")
    try:
        return docker_mgr.create_node(node_id)
    except Exception as e:
        raise _api_error(500, "NODE_CREATE_FAILED", "Failed to create node", str(e))


@app.delete("/nodes/{node_id}")
async def remove_node(node_id: str):
    if not docker_mgr:
        raise _api_error(503, "DOCKER_UNAVAILABLE", "Docker not available")
    return docker_mgr.remove_node(node_id)


# --- Partition control ---


@app.post("/nodes/{node_id}/partition")
async def isolate_node(node_id: str):
    if not docker_mgr:
        raise _api_error(503, "DOCKER_UNAVAILABLE", "Docker not available")
    return docker_mgr.isolate_node(node_id)


@app.delete("/nodes/{node_id}/partition")
async def heal_node(node_id: str):
    if not docker_mgr:
        raise _api_error(503, "DOCKER_UNAVAILABLE", "Docker not available")
    return docker_mgr.heal_node(node_id)


@app.post("/partition/split-brain")
async def split_brain():
    if not docker_mgr:
        raise _api_error(503, "DOCKER_UNAVAILABLE", "Docker not available")
    return docker_mgr.create_split_brain()


@app.post("/partition/heal-all")
async def heal_all():
    if not docker_mgr:
        raise _api_error(503, "DOCKER_UNAVAILABLE", "Docker not available")
    return docker_mgr.heal_all()


# Mount static files for dashboard
from fastapi.staticfiles import StaticFiles
import os

static_dir = os.path.join(os.path.dirname(__file__), "static")
app.mount("/", StaticFiles(directory=static_dir, html=True), name="static")


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
