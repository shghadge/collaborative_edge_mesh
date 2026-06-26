"""
Simulator entry point. Hosts dashboard/static UI and simulation control APIs.
Automatically discovers gateways and edge nodes via Docker (no registry).
"""

import asyncio
import json
import logging
import os
import random
import signal
import uuid
from typing import Optional, Any, Dict, Set

import aiohttp
import structlog
import uvicorn
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import HTMLResponse, JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from src.config import config
from src.evaluation.runner import EvalScenarioConfig, EvaluationRunner
from src.services.docker_manager import DockerManager
from src.services.observability import SimulatorObservabilityService
from src.services.simulator_control import SimulatorControlService

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
app = FastAPI(title="Edge Mesh Simulator", docs_url=None)


try:
    docker_mgr = DockerManager()
except Exception as error:
    log.warning("docker_unavailable", error=str(error))
    docker_mgr = None

control_service = None

# --- WebSocket state ---
_ws_clients: Set[WebSocket] = set()

observability = None


# --- Models ---


class SendEventRequest(BaseModel):
    type: str
    value: Any
    location: Optional[str] = None
    category: str = "general"
    operation: Optional[str] = None
    metadata: Dict[str, Any] = Field(default_factory=dict)


# --- Error Handling ---


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


if docker_mgr is not None:
    control_service = SimulatorControlService(docker_mgr, _api_error, log)


@app.get("/docs", include_in_schema=False)
async def custom_swagger_ui_html():
    urls = [{"url": "/openapi.json", "name": "Simulator Engine"}]

    if docker_mgr is not None:
        try:
            gateways = await _discover_gateways()
            for gw in gateways:
                gw_url = gw.get("external_url") or gw.get("url")
                if gw_url:
                    urls.append(
                        {
                            "url": f"{gw_url}/openapi.json",
                            "name": f"Gateway ({gw['name']})",
                        }
                    )

            nodes = await asyncio.to_thread(docker_mgr.list_nodes)
            for node in nodes:
                node_url = node.get("url")  # external facing URL
                if node_url:
                    urls.append(
                        {
                            "url": f"{node_url}/openapi.json",
                            "name": f"Node ({node['name']})",
                        }
                    )
        except Exception as e:
            log.warning("swagger_discovery_failed", error=str(e))

    # FastAPI's built-in get_swagger_ui_html does not include the standalone JS required for the topbar/URL dropdown.
    # Therefore, we yield a custom HTMLResponse containing the required JS and CSS to render the Topbar.
    html = f"""
    <!DOCTYPE html>
    <html>
    <head>
    <link type="text/css" rel="stylesheet" href="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui.css">
    <title>Edge Mesh APIs</title>
    </head>
    <body>
    <div id="swagger-ui"></div>
    <script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-bundle.js"></script>
    <script src="https://cdn.jsdelivr.net/npm/swagger-ui-dist@5/swagger-ui-standalone-preset.js"></script>
    <script>
    const ui = SwaggerUIBundle({{
        urls: {json.dumps(urls)},
        dom_id: '#swagger-ui',
        deepLinking: true,
        presets: [
            SwaggerUIBundle.presets.apis,
            SwaggerUIStandalonePreset
        ],
        plugins: [
            SwaggerUIBundle.plugins.DownloadUrl
        ],
        layout: "StandaloneLayout"
    }})
    </script>
    </body>
    </html>
    """
    return HTMLResponse(html)


# --- Docker / Gateway helpers ---


def _normalize_node_id(node_id: str) -> str:
    if control_service is None:
        if node_id.startswith("edge-node-"):
            suffix = node_id.replace("edge-node-", "", 1)
            return f"node-{suffix}"
        return node_id
    return control_service.normalize_node_id(node_id)


def _candidate_urls(*urls: Optional[str]) -> list[str]:
    if control_service is None:
        ordered: list[str] = []
        for url in urls:
            if not url:
                continue
            if url not in ordered:
                ordered.append(url)
        return ordered
    return control_service.candidate_urls(*urls)


def _build_node_gateway_map(
    valid_nodes: list[dict],
    gateways: list[dict],
    *,
    persist_auto_assignments: bool,
) -> dict[str, str]:
    _require_docker()
    return control_service.build_node_gateway_map(
        valid_nodes,
        gateways,
        persist_auto_assignments=persist_auto_assignments,
    )


def _require_docker():
    if control_service is None:
        raise _api_error(503, "DOCKER_UNAVAILABLE", "Docker not available")
    control_service.require_docker()


async def _discover_gateways():
    return await control_service.discover_gateways()


async def _sync_gateways_with_nodes(force: bool = False):
    await control_service.sync_gateways_with_nodes(force=force)


async def _proxy_gateway_request(method: str, path: str, params: Optional[dict] = None):
    return await control_service.proxy_gateway_request(method, path, params=params)


async def _post_gateway_event_tracking(record: dict) -> None:
    try:
        gateways = await _discover_gateways()
    except Exception as error:
        log.debug("event_tracking_gateway_discovery_failed", error=str(error))
        return

    timeout = aiohttp.ClientTimeout(total=3)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        for gateway in gateways:
            for base_url in _candidate_urls(
                gateway.get("url"),
                f"http://{gateway.get('name')}:8000" if gateway.get("name") else None,
                gateway.get("external_url"),
            ):
                try:
                    async with session.post(
                        f"{base_url}/gateway/event-tracking",
                        json=record,
                    ) as response:
                        if response.status < 400:
                            return
                except Exception as error:
                    log.debug(
                        "event_tracking_post_failed",
                        gateway=gateway.get("name"),
                        url=base_url,
                        error=str(error),
                    )


def _get_observability() -> SimulatorObservabilityService:
    global observability
    if observability is None:
        observability = SimulatorObservabilityService(
            config=config,
            discover_gateways=_discover_gateways,
            list_nodes=list_nodes,
            list_gateways=list_gateways,
            proxy_gateway_request=_proxy_gateway_request,
        )
    return observability


# --- Gateway Management ---


@app.get("/gateways")
async def list_gateways():
    _require_docker()
    try:
        gateways = await _discover_gateways()
    except Exception:
        gateways = []

    nodes = await asyncio.to_thread(docker_mgr.list_nodes)
    valid_nodes = [n for n in nodes if n.get("node_id")]
    valid_nodes.sort(key=lambda x: x["name"])

    node_to_gw = _build_node_gateway_map(
        valid_nodes,
        gateways,
        persist_auto_assignments=False,
    )

    for node in valid_nodes:
        node["assigned_gateway"] = node_to_gw.get(node.get("node_id"))

    for gw in gateways:
        gw["assigned_nodes"] = [
            n["name"] for n in valid_nodes if n.get("assigned_gateway") == gw["name"]
        ]

    return gateways


@app.post("/gateways")
async def create_gateway(gateway_id: Optional[str] = None):
    _require_docker()
    try:
        result = await asyncio.to_thread(docker_mgr.create_gateway, gateway_id)
        # Give gateway container a moment to spin up the HTTP server before syncing
        await asyncio.sleep(2)
        await _sync_gateways_with_nodes(force=True)
        return result
    except Exception as error:
        raise _api_error(
            500, "GATEWAY_CREATE_FAILED", "Failed to create gateway", str(error)
        )


@app.delete("/gateways/{gateway_id}")
async def remove_gateway(gateway_id: str):
    _require_docker()
    if gateway_id == "gateway":
        # Block deleting the primary gateway so the system always has at least one
        raise _api_error(400, "INVALID_ACTION", "Cannot delete the primary 'gateway'")
    result = await asyncio.to_thread(docker_mgr.remove_gateway, gateway_id)
    await _sync_gateways_with_nodes(force=True)
    return result


# --- Node Management ---


@app.get("/nodes")
async def list_nodes():
    _require_docker()
    nodes = await asyncio.to_thread(docker_mgr.list_nodes)
    valid_nodes = [n for n in nodes if n.get("node_id")]
    valid_nodes.sort(key=lambda x: x["name"])

    try:
        gateways = await _discover_gateways()
    except Exception:
        gateways = []

    # Map assigned gateways
    assigned_nodes = []
    node_to_gw = _build_node_gateway_map(
        valid_nodes,
        gateways,
        persist_auto_assignments=False,
    )

    for node in valid_nodes:
        node["assigned_gateway"] = node_to_gw.get(node.get("node_id"))
        assigned_nodes.append(node)

    # Append any isolated/broken nodes not in valid_nodes
    valid_ids = {n["name"] for n in valid_nodes}
    for node in nodes:
        if node["name"] not in valid_ids:
            node["assigned_gateway"] = None
            assigned_nodes.append(node)

    return assigned_nodes


@app.post("/nodes")
async def create_node(
    node_id: Optional[str] = None, assigned_gateway: Optional[str] = None
):
    _require_docker()
    try:
        result = await asyncio.to_thread(
            docker_mgr.create_node, node_id, assigned_gateway=assigned_gateway or ""
        )
        if assigned_gateway and result.get("node_id"):
            control_service.user_gateway_assignments[result.get("node_id")] = (
                assigned_gateway
            )
        await _sync_gateways_with_nodes(force=True)
        return result
    except Exception as error:
        raise _api_error(500, "NODE_CREATE_FAILED", "Failed to create node", str(error))


@app.post("/nodes/batch")
async def create_nodes_batch(count: int = 1, assigned_gateway: Optional[str] = None):
    _require_docker()
    if count < 1:
        raise _api_error(400, "INVALID_COUNT", "count must be at least 1")

    async def _create_one():
        try:
            result = await asyncio.to_thread(
                docker_mgr.create_node, assigned_gateway=assigned_gateway or ""
            )
            if assigned_gateway and result.get("node_id"):
                control_service.user_gateway_assignments[result.get("node_id")] = (
                    assigned_gateway
                )
            return {"success": True, "result": result}
        except Exception as error:
            return {"success": False, "error": str(error)}

    results = await asyncio.gather(*[_create_one() for _ in range(count)])

    created = [r["result"] for r in results if r["success"]]
    failed = [r["error"] for r in results if not r["success"]]

    await _sync_gateways_with_nodes(force=True)

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


class NodeGatewayUpdate(BaseModel):
    assigned_gateway: Optional[str]


@app.put("/nodes/{node_id}/gateway")
async def update_node_gateway(node_id: str, request: NodeGatewayUpdate):
    _require_docker()
    node_id = _normalize_node_id(node_id)
    if request.assigned_gateway:
        control_service.user_gateway_assignments[node_id] = request.assigned_gateway
    else:
        control_service.user_gateway_assignments.pop(node_id, None)
    await _sync_gateways_with_nodes(force=True)
    return {
        "status": "success",
        "node_id": node_id,
        "assigned_gateway": request.assigned_gateway,
    }


@app.delete("/nodes/{node_id}")
async def remove_node(node_id: str):
    _require_docker()
    node_id = _normalize_node_id(node_id)
    control_service.user_gateway_assignments.pop(node_id, None)
    result = await asyncio.to_thread(docker_mgr.remove_node, node_id)
    await _sync_gateways_with_nodes(force=True)
    return result


# --- Partition Control ---


async def _push_merged_state_to_node(node_id: str):
    """Push the gateway's merged state to a healed node via its /merge endpoint.

    This gives the node the full converged state immediately after healing,
    bypassing the mDNS re-discovery + gossip cycle which can take 10-30s.
    """
    if not config.enable_heal_state_push:
        log.info("heal_state_push_skipped", node_id=node_id, reason="disabled")
        return

    try:
        # Allow iptables rules to settle briefly before connecting
        await asyncio.sleep(1)

        # Get the healed node's URL
        nodes = await asyncio.to_thread(docker_mgr.list_nodes)
        target = next((n for n in nodes if n.get("node_id") == node_id), None)
        if not target:
            return

        node_url = target.get("internal_url") or target.get("url")
        if not node_url:
            return

        # Fetch merged state from the gateway
        gateways = await _discover_gateways()
        if not gateways:
            return
        gw = gateways[0]
        gw_url = gw.get("url") or gw.get("external_url")
        if not gw_url:
            return

        timeout = aiohttp.ClientTimeout(total=5)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            async with session.get(f"{gw_url}/gateway/merged-state") as resp:
                if resp.status >= 400:
                    return
                merged = await resp.json()

            # merged-state wraps the state dict under "state" key
            state_dict = merged.get("state") or merged
            if not isinstance(state_dict, dict) or "node_id" not in state_dict:
                return

            async with session.post(f"{node_url}/merge", json=state_dict) as resp:
                if resp.status < 400:
                    log.info(
                        "heal_state_pushed",
                        node_id=node_id,
                        url=node_url,
                    )
    except Exception as e:
        log.warning("heal_state_push_failed", node_id=node_id, error=str(e))


@app.post("/nodes/{node_id}/partition")
async def isolate_node(node_id: str):
    _require_docker()
    result = await asyncio.to_thread(docker_mgr.isolate_node, node_id)
    await _sync_gateways_with_nodes(force=True)
    return result


@app.delete("/nodes/{node_id}/partition")
async def heal_node(node_id: str):
    _require_docker()
    result = await asyncio.to_thread(docker_mgr.heal_node, node_id)
    await _sync_gateways_with_nodes(force=True)
    if config.enable_heal_state_push:
        asyncio.create_task(_push_merged_state_to_node(node_id))
    return result


@app.post("/partition/split-brain")
async def split_brain(majority_ratio: float = 0.5):
    _require_docker()
    result = await asyncio.to_thread(docker_mgr.create_split_brain, majority_ratio)
    await _sync_gateways_with_nodes(force=True)
    return result


@app.post("/partition/heal-all")
async def heal_all():
    _require_docker()
    result = await asyncio.to_thread(docker_mgr.heal_all)
    await _sync_gateways_with_nodes(force=True)
    if config.enable_heal_state_push:
        nodes = await asyncio.to_thread(docker_mgr.list_nodes)
        for node in nodes:
            node_id = node.get("node_id")
            if node_id:
                asyncio.create_task(_push_merged_state_to_node(node_id))
    return result


# --- Event Sending ---


@app.post("/event/send")
async def send_event(req: SendEventRequest, target_node: Optional[str] = None):
    """Send an event to a specific node or a random available node."""
    _require_docker()
    nodes = await asyncio.to_thread(docker_mgr.list_nodes)
    if not nodes:
        raise _api_error(404, "NO_NODES", "No edge nodes available")

    # find target node
    target = None
    if target_node:
        for node in nodes:
            node_name = node.get("name", "").replace("edge-", "")
            if (
                node_name == target_node
                or node.get("node_id") == target_node
                or node.get("name") == target_node
            ):
                target = node
                break
        if not target:
            raise _api_error(404, "NODE_NOT_FOUND", f"Node '{target_node}' not found")
    else:
        target = random.choice(nodes)

    node_url = target.get("internal_url") or target.get("url")
    if not node_url:
        raise _api_error(500, "NO_URL", "Node has no reachable URL")

    tracking_id = str(uuid.uuid4())
    payload = {
        "type": req.type,
        "value": req.value,
        "location": req.location,
        "category": req.category,
        "operation": req.operation,
        "metadata": {
            **req.metadata,
            "tracking_id": tracking_id,
            "source": "dashboard",
        },
    }

    timeout = aiohttp.ClientTimeout(total=5)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        try:
            async with session.post(f"{node_url}/event", json=payload) as response:
                body = await response.json()
                if response.status >= 400:
                    raise _api_error(
                        response.status,
                        "EVENT_REJECTED",
                        f"Node rejected event for {target.get('name')}",
                        body,
                    )

                # Also get current state size and merkle root
                state_info = {}
                try:
                    async with session.get(f"{node_url}/state/merkle") as merkle_resp:
                        state_info = await merkle_resp.json()
                except Exception:
                    pass

                try:
                    async with session.get(f"{node_url}/state") as state_resp:
                        state_data = await state_resp.json()
                        state_info["state_size_bytes"] = len(json.dumps(state_data))
                except Exception:
                    pass

                event_tracking_id = body.get("tracking_id") or tracking_id
                await _post_gateway_event_tracking(
                    {
                        "tracking_id": event_tracking_id,
                        "node_id": target.get("node_id") or target.get("name"),
                        "event_type": req.type,
                        "category": req.category,
                        "state_size_bytes": state_info.get("state_size_bytes")
                        or body.get("state_size_bytes"),
                        "merkle_root_after": state_info.get("merkle_root")
                        or body.get("merkle_root_after"),
                        "metadata": {
                            "target_node": target.get("name"),
                            "location": req.location,
                            "operation": req.operation,
                            "source": "dashboard",
                        },
                    }
                )

                return {
                    "status": "sent",
                    "tracking_id": event_tracking_id,
                    "target_node": target.get("name"),
                    "node_id": target.get("node_id"),
                    "event_response": body,
                    "state_size_bytes": state_info.get("state_size_bytes"),
                    "merkle_root_after": state_info.get("merkle_root"),
                }
        except HTTPException:
            raise
        except Exception as error:
            raise _api_error(
                503,
                "EVENT_SEND_FAILED",
                f"Failed to send event to {target.get('name')}",
                str(error),
            )


# --- Metrics APIs ---


@app.get("/metrics/live")
async def live_metrics():
    """Aggregate dashboard metrics with mode-aware observability cost."""
    _require_docker()
    return await _get_observability().get_live_metrics(force_refresh=False)


@app.get("/metrics/timeseries")
async def metrics_timeseries(metric: str = "state_size_bytes", limit: int = 100):
    """Get time-series chart data from the gateway."""
    return await _get_observability().get_timeseries(metric=metric, limit=limit)


# --- Gateway Proxy ---


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


# --- Evaluation / Data Collection ---

evaluation_runner = EvaluationRunner(
    docker_mgr=docker_mgr,
    candidate_urls=_candidate_urls,
    discover_gateways=_discover_gateways,
    sync_gateways_with_nodes=_sync_gateways_with_nodes,
)


@app.post("/evaluation/run")
async def run_evaluation(cfg: EvalScenarioConfig = None):
    """Start a structured evaluation run. Returns immediately; poll /evaluation/status."""
    _require_docker()
    if evaluation_runner.state["running"]:
        raise _api_error(409, "EVAL_RUNNING", "An evaluation is already in progress")

    if cfg is None:
        cfg = EvalScenarioConfig()

    try:
        evaluation_runner.validate_config(cfg)
    except ValueError as error:
        raise _api_error(400, "INVALID_EVALUATION_CONFIG", str(error))

    partition_splits = (
        cfg.partition_splits if cfg.network_profile == "hard_partition" else []
    )
    total_steps = evaluation_runner.planned_steps(cfg)
    asyncio.create_task(evaluation_runner.run(cfg))
    return {
        "status": "started",
        "total_steps": total_steps,
        "node_counts": cfg.node_counts,
        "events_per_node": cfg.events_per_node,
        "repetitions": cfg.repetitions,
        "partition_splits": partition_splits,
        "implementation": cfg.implementation_label(),
        "sync_mode": cfg.sync_mode,
        "event_id_retention_limit": cfg.event_id_retention_limit,
        "network_profile": cfg.network_profile,
        "dashboard_observability": cfg.dashboard_observability,
        "docker_cpus": cfg.docker_cpus,
        "docker_memory": cfg.docker_memory,
    }


@app.get("/evaluation/status")
async def evaluation_status():
    """Poll evaluation progress."""
    return evaluation_runner.status()


@app.get("/evaluation/download")
async def download_evaluation():
    """Download evaluation results as CSV."""
    csv_data = evaluation_runner.state["csv_data"]
    if csv_data is None:
        raise _api_error(
            404, "NO_DATA", "No evaluation data available. Run an evaluation first."
        )
    from fastapi.responses import Response

    return Response(
        content=csv_data,
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=evaluation_results.csv"},
    )


@app.get("/evaluation/results")
async def evaluation_results():
    """Return evaluation results as JSON for the UI table."""
    return {
        "results": evaluation_runner.state["results"],
        "has_csv": evaluation_runner.state["csv_data"] is not None,
    }


# --- WebSocket ---


@app.websocket("/ws")
async def websocket_endpoint(ws: WebSocket):
    await ws.accept()
    _ws_clients.add(ws)
    log.info("ws_client_connected", total=len(_ws_clients))
    try:
        while True:
            # Keep connection alive; ignore client messages
            await ws.receive_text()
    except WebSocketDisconnect:
        pass
    finally:
        _ws_clients.discard(ws)
        log.info("ws_client_disconnected", total=len(_ws_clients))


async def _ws_broadcast(data: dict):
    """Broadcast JSON data to all connected WebSocket clients."""
    if not _ws_clients:
        return
    msg = json.dumps(data)
    dead = []
    for ws in _ws_clients:
        try:
            await ws.send_text(msg)
        except Exception:
            dead.append(ws)
    for ws in dead:
        _ws_clients.discard(ws)


async def _ws_broadcast_loop():
    """Background loop: push metrics to WebSocket clients periodically."""
    while True:
        await asyncio.sleep(config.observability_push_interval_seconds)
        if not _ws_clients:
            continue
        try:
            payload = {}

            if docker_mgr:
                payload = await _get_observability().build_ws_payload()

            if payload:
                payload["type"] = "update"
                await _ws_broadcast(payload)
        except Exception as e:
            log.debug("ws_broadcast_error", error=str(e))


static_dir = os.path.join(os.path.dirname(__file__), "static")
app.mount("/", StaticFiles(directory=static_dir, html=True), name="static")


# --- Start ---


async def main():
    shutdown = asyncio.Event()
    loop = asyncio.get_event_loop()
    _get_observability()
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
        await asyncio.gather(server.serve(), _ws_broadcast_loop(), wait_shutdown())
    except asyncio.CancelledError:
        pass
    finally:
        log.info("simulator_stopped")


if __name__ == "__main__":
    asyncio.run(main())
