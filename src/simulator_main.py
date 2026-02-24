"""
Simulator entry point. Hosts dashboard/static UI and simulation control APIs.
Automatically discovers gateways and edge nodes via Docker (no registry).
"""

import asyncio
import logging
import os
import signal
from typing import Optional

import aiohttp
import structlog
import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles

from src.config import config
from src.services.docker_manager import DockerManager
from src.services.scenarios import (
    run_bootstrap_events_convergence,
    run_split_brain_then_heal,
)

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
app = FastAPI(title="Edge Mesh Simulator")


try:
    docker_mgr = DockerManager()
except Exception as error:
    log.warning("docker_unavailable", error=str(error))
    docker_mgr = None


class RemoteGatewayService:
    """Adapter exposing GatewayService-like interface for scenario functions."""

    def __init__(self):
        self.is_divergent = False
        self.poll_count = 0

    async def poll_once(self):
        status = await _proxy_gateway_request("POST", "/gateway/poll")
        self.is_divergent = bool(status.get("is_divergent", False))
        self.poll_count = int(status.get("poll_count", self.poll_count + 1))


remote_gateway = RemoteGatewayService()


def _api_error(
    status_code: int,
    code: str,
    message: str,
    details=None,
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
    log.error(
        "simulator_unhandled_exception",
        error=str(exc),
        path=str(request.url.path),
    )
    return JSONResponse(
        status_code=500,
        content={
            "code": "INTERNAL_SERVER_ERROR",
            "message": "Unexpected simulator error",
        },
    )


def _require_docker():
    if docker_mgr is None:
        raise _api_error(503, "DOCKER_UNAVAILABLE", "Docker not available")


async def _discover_gateways():
    _require_docker()
    gateways = await asyncio.to_thread(docker_mgr.list_gateways)
    if not gateways:
        raise _api_error(503, "NO_GATEWAY", "No running gateway containers discovered")
    return gateways


async def _sync_gateways_with_nodes():
    _require_docker()
    nodes = await asyncio.to_thread(docker_mgr.list_nodes)
    payload = {
        "nodes": [
            {
                "node_id": node.get("node_id"),
                "url": node.get("internal_url") or node.get("url"),
            }
            for node in nodes
            if node.get("node_id") and (node.get("internal_url") or node.get("url"))
        ]
    }

    gateways = await _discover_gateways()

    timeout = aiohttp.ClientTimeout(total=6)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        for gateway in gateways:
            url = f"{gateway['url']}/gateway/nodes/sync"
            try:
                async with session.post(url, json=payload) as response:
                    if response.status >= 400:
                        body = await response.text()
                        log.warning(
                            "gateway_sync_failed",
                            gateway=gateway["name"],
                            status=response.status,
                            body=body,
                        )
            except Exception as error:
                log.warning(
                    "gateway_sync_exception",
                    gateway=gateway["name"],
                    error=str(error),
                )


async def _proxy_gateway_request(method: str, path: str, params: Optional[dict] = None):
    await _sync_gateways_with_nodes()
    gateways = await _discover_gateways()
    primary = gateways[0]

    timeout = aiohttp.ClientTimeout(total=8)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        try:
            async with session.request(
                method,
                f"{primary['url']}{path}",
                params=params,
            ) as response:
                if response.status >= 400:
                    body = await response.text()
                    raise _api_error(
                        response.status,
                        "GATEWAY_REQUEST_FAILED",
                        f"Gateway request failed for {path}",
                        {"gateway": primary["name"], "body": body},
                    )
                return await response.json()
        except HTTPException:
            raise
        except Exception as error:
            raise _api_error(
                503,
                "GATEWAY_UNREACHABLE",
                "Unable to reach discovered gateway",
                {"gateway": primary["name"], "error": str(error)},
            )


@app.get("/nodes")
async def list_nodes():
    _require_docker()
    return await asyncio.to_thread(docker_mgr.list_nodes)


@app.post("/nodes")
async def create_node(node_id: Optional[str] = None):
    _require_docker()
    try:
        result = await asyncio.to_thread(docker_mgr.create_node, node_id)
        await _sync_gateways_with_nodes()
        return result
    except Exception as error:
        raise _api_error(500, "NODE_CREATE_FAILED", "Failed to create node", str(error))


@app.post("/nodes/batch")
async def create_nodes_batch(count: int = 1):
    _require_docker()
    if count < 1 or count > 20:
        raise _api_error(400, "INVALID_COUNT", "count must be between 1 and 20")

    created = []
    failed = []
    for _ in range(count):
        try:
            result = await asyncio.to_thread(docker_mgr.create_node)
            created.append(result)
        except Exception as error:
            failed.append(str(error))

    await _sync_gateways_with_nodes()

    status = "completed" if not failed else "partial"
    return {
        "action": "create_nodes_batch",
        "status": status,
        "requested": count,
        "created_count": len(created),
        "failed_count": len(failed),
        "created": created,
        "failures": failed,
    }


@app.delete("/nodes/{node_id}")
async def remove_node(node_id: str):
    _require_docker()
    result = await asyncio.to_thread(docker_mgr.remove_node, node_id)
    await _sync_gateways_with_nodes()
    return result


@app.post("/nodes/{node_id}/partition")
async def isolate_node(node_id: str):
    _require_docker()
    return await asyncio.to_thread(docker_mgr.isolate_node, node_id)


@app.delete("/nodes/{node_id}/partition")
async def heal_node(node_id: str):
    _require_docker()
    return await asyncio.to_thread(docker_mgr.heal_node, node_id)


@app.post("/partition/split-brain")
async def split_brain():
    _require_docker()
    return await asyncio.to_thread(docker_mgr.create_split_brain)


@app.post("/partition/heal-all")
async def heal_all():
    _require_docker()
    return await asyncio.to_thread(docker_mgr.heal_all)


@app.post("/scenarios/split-brain-heal")
async def scenario_split_brain_heal(
    isolate_seconds: float = 8.0,
    verify_polls: int = 2,
):
    _require_docker()
    if isolate_seconds < 0:
        raise _api_error(400, "INVALID_ISOLATE_SECONDS", "isolate_seconds must be >= 0")
    if verify_polls < 1 or verify_polls > 20:
        raise _api_error(
            400,
            "INVALID_VERIFY_POLLS",
            "verify_polls must be between 1 and 20",
        )

    return await run_split_brain_then_heal(
        docker_manager=docker_mgr,
        gateway_service=remote_gateway,
        isolate_seconds=isolate_seconds,
        verify_polls=verify_polls,
    )


@app.post("/scenarios/bootstrap-converge")
async def scenario_bootstrap_converge(
    create_nodes: int = 0,
    events_per_node: int = 1,
    verify_polls: int = 3,
):
    _require_docker()
    if create_nodes < 0 or create_nodes > 20:
        raise _api_error(400, "INVALID_CREATE_NODES", "create_nodes must be between 0 and 20")
    if events_per_node < 1 or events_per_node > 10:
        raise _api_error(
            400,
            "INVALID_EVENTS_PER_NODE",
            "events_per_node must be between 1 and 10",
        )
    if verify_polls < 1 or verify_polls > 20:
        raise _api_error(
            400,
            "INVALID_VERIFY_POLLS",
            "verify_polls must be between 1 and 20",
        )

    return await run_bootstrap_events_convergence(
        docker_manager=docker_mgr,
        gateway_service=remote_gateway,
        create_nodes=create_nodes,
        events_per_node=events_per_node,
        verify_polls=verify_polls,
    )


@app.get("/gateway/status")
async def gateway_status():
    return await _proxy_gateway_request("GET", "/gateway/status")


@app.get("/gateway/divergence")
async def gateway_divergence():
    return await _proxy_gateway_request("GET", "/gateway/divergence")


@app.get("/gateway/metrics")
async def gateway_metrics(name: Optional[str] = None, limit: int = 100):
    return await _proxy_gateway_request(
        "GET",
        "/gateway/metrics",
        params={"name": name, "limit": limit},
    )


@app.get("/gateway/runtime-metrics")
async def gateway_runtime_metrics():
    return await _proxy_gateway_request("GET", "/gateway/runtime-metrics")


@app.post("/gateway/poll")
async def trigger_gateway_poll():
    return await _proxy_gateway_request("POST", "/gateway/poll")


static_dir = os.path.join(os.path.dirname(__file__), "static")
app.mount("/", StaticFiles(directory=static_dir, html=True), name="static")


async def main():
    shutdown = asyncio.Event()
    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown.set)

    server = uvicorn.Server(
        uvicorn.Config(app, host="0.0.0.0", port=config.http_port, log_level="warning")
    )

    log.info("simulator_starting", http=config.http_port)

    async def wait_shutdown():
        await shutdown.wait()
        raise asyncio.CancelledError()

    try:
        await asyncio.gather(server.serve(), wait_shutdown())
    except asyncio.CancelledError:
        pass
    finally:
        log.info("simulator_stopped")


if __name__ == "__main__":
    asyncio.run(main())
