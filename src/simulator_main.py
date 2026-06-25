"""
Simulator entry point. Hosts dashboard/static UI and simulation control APIs.
Automatically discovers gateways and edge nodes via Docker (no registry).
"""

import asyncio
import time
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
    # Push gateway's merged state to the healed node so it converges immediately
    # instead of waiting for mDNS re-discovery + gossip cycles.
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
    # Push merged state to every previously-isolated node
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
    force_refresh = False
    if not _eval_state.get("running", False):
        force_refresh = False
    return await _get_observability().get_live_metrics(force_refresh=force_refresh)


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


# Evaluation state (module-level, single-run)
_eval_state = {
    "running": False,
    "run_id": None,
    "started_at": None,
    "progress": 0,
    "total_steps": 0,
    "current_step": "",
    "results": [],
    "error": None,
    "csv_data": None,
}


class EvalScenarioConfig(BaseModel):
    node_counts: list[int] = Field(default=[5, 10])
    events_per_node: int = Field(default=5)
    partition_splits: list[float] = Field(default=[0.5, 0.7, 0.8])
    partition_wait_seconds: float = Field(default=8.0)
    convergence_wait_seconds: float = Field(default=15.0)
    poll_interval_seconds: float = Field(default=2.0)
    implementation_label: str = "current_full_state_repair"
    sync_mode: str = "full_state"
    state_growth_fix: bool = False
    network_profile: str = "hard_partition"
    scenario_seed: int | None = None
    dashboard_observability: str = "light"
    event_concurrency: int = Field(default=25)


async def _eval_send_events(session, nodes, count, category="sensor", concurrency=25):
    """Send `count` events per node and return simple availability metrics."""
    result = {
        "attempted": count * len(nodes),
        "accepted": 0,
        "tracking_ids": [],
        "first_submitted_at": None,
        "last_submitted_at": None,
    }
    event_types = [
        "water_level",
        "temperature",
        "shelter_occupancy",
        "road_status",
        "wind_speed",
        "air_quality",
    ]
    semaphore = asyncio.Semaphore(max(1, int(concurrency or 1)))

    async def _send_one(i, target):
        node_url = target.get("internal_url") or target.get("url")
        submitted_at = time.time()
        if not node_url:
            return None

        etype = event_types[i % len(event_types)]
        tid = str(uuid.uuid4())
        payload = {
            "type": etype,
            "value": round(random.uniform(1.0, 100.0), 2),
            "location": f"{category}_{i}",
            "category": category,
            "metadata": {
                "tracking_id": tid,
                "source": "evaluation",
                "evaluation_submitted_at": submitted_at,
            },
        }
        try:
            async with semaphore:
                async with session.post(f"{node_url}/event", json=payload) as resp:
                    await resp.json()
                    if resp.status < 400:
                        return tid, submitted_at
        except Exception:
            return None
        return None

    submissions = [
        _send_one(i, nodes[i % len(nodes)]) for i in range(count * len(nodes))
    ]
    for item in await asyncio.gather(*submissions):
        if item is None:
            continue
        tid, submitted_at = item
        result["accepted"] += 1
        result["tracking_ids"].append(tid)
        if (
            result["first_submitted_at"] is None
            or submitted_at < result["first_submitted_at"]
        ):
            result["first_submitted_at"] = submitted_at
        if (
            result["last_submitted_at"] is None
            or submitted_at > result["last_submitted_at"]
        ):
            result["last_submitted_at"] = submitted_at
    return result


async def _eval_poll_all_gateways(session, gateways):
    """Trigger a poll on each gateway once, using URL fallback per gateway."""
    for gw in gateways:
        polled = False
        for base_url in _candidate_urls(gw.get("url"), gw.get("external_url")):
            try:
                async with session.post(f"{base_url}/gateway/poll") as resp:
                    if resp.status < 400:
                        await resp.json()
                        polled = True
                        break
            except Exception:
                continue
        if not polled:
            log.warning("evaluation_gateway_poll_failed", gateway=gw.get("name"))


async def _eval_collect_metrics(session, nodes, gateways, accepted_events=None):
    """Collect the minimal thesis metrics for the current run state."""
    last_convergence = None
    known_statuses = []
    coverage_values = []
    gateway_staleness_values = []
    for gw in gateways:
        gw_metrics = {}
        gw_status = None
        gw_coverage = None
        gw_visibility = {}
        for base_url in _candidate_urls(gw.get("url"), gw.get("external_url")):
            try:
                async with session.get(f"{base_url}/gateway/runtime-metrics") as resp:
                    if resp.status >= 400:
                        continue
                    gw_data = await resp.json()
                    gw_metrics = gw_data.get("runtime_metrics", {})
                    gw_status = gw_data.get("known_status")
                    gw_coverage = gw_data.get("coverage")
                    gw_visibility = gw_data.get("visibility") or {}
                    break
            except Exception:
                continue

        convergence_value = gw_metrics.get("last_convergence_seconds")
        if convergence_value is not None:
            if last_convergence is None or convergence_value > last_convergence:
                last_convergence = convergence_value
        if isinstance(gw_status, str) and gw_status:
            known_statuses.append(gw_status)
        if isinstance(gw_coverage, (int, float)):
            coverage_values.append(gw_coverage)
        staleness = gw_visibility.get("max_observation_age_seconds")
        if isinstance(staleness, (int, float)):
            gateway_staleness_values.append(max(0.0, staleness))

    if any(status == "known_diverged" for status in known_statuses):
        known_status = "known_diverged"
    elif any(status == "unknown_partial" for status in known_statuses):
        known_status = "unknown_partial"
    elif known_statuses:
        known_status = "known_converged"
    else:
        known_status = "unknown_partial"

    coverage_percent = (
        round((min(coverage_values) * 100.0), 1) if coverage_values else 0.0
    )

    total_gossip = 0
    total_state_size = 0
    sync_bytes_total = 0
    repair_pull_count = 0
    delta_pulls_started = 0
    delta_pulls_success = 0
    delta_pulls_gap = 0
    delta_deltas_applied = 0
    delta_bytes_received = 0
    event_counts = []
    merkle_roots = []

    for node in nodes:
        node_url = node.get("internal_url") or node.get("url")
        if not node_url:
            continue
        try:
            async with session.get(f"{node_url}/gossip/stats") as resp:
                gdata = await resp.json()
                stats = gdata.get("stats", {})
                total_gossip += stats.get("sent", 0)
                node_delta_bytes = stats.get("delta_bytes_received", 0)
                sync_bytes_total += (
                    stats.get("sent_bytes", 0)
                    + stats.get("received_bytes", 0)
                    + stats.get("repair_bytes_received", 0)
                    + node_delta_bytes
                )
                repair_pull_count += stats.get("repair_pulls_started", 0)
                delta_pulls_started += stats.get("delta_pulls_started", 0)
                delta_pulls_success += stats.get("delta_pulls_success", 0)
                delta_pulls_gap += stats.get("delta_pulls_gap", 0)
                delta_deltas_applied += stats.get("delta_deltas_applied", 0)
                delta_bytes_received += node_delta_bytes
        except Exception:
            pass
        try:
            async with session.get(f"{node_url}/state") as resp:
                sdata = await resp.json()
                size = len(json.dumps(sdata))
                total_state_size += size
                summary = sdata.get("state_summary", {})
                ev_count = summary.get("total_events", 0)
                if ev_count == 0:
                    ev_ids = sdata.get("recent_event_ids", sdata.get("event_ids", []))
                    ev_count = len(ev_ids) if isinstance(ev_ids, list) else 0
                event_counts.append(ev_count)
        except Exception:
            pass
        try:
            async with session.get(f"{node_url}/state/merkle") as resp:
                mdata = await resp.json()
                merkle_roots.append(mdata.get("merkle_root", ""))
        except Exception:
            pass

    max_ev = max(event_counts) if event_counts else 0
    min_ev = min(event_counts) if event_counts else 0
    expected_events = accepted_events if accepted_events is not None else max_ev
    missing_at_cutoff_count = max(expected_events - min_ev, 0)
    missing_at_cutoff_pct = (
        round((missing_at_cutoff_count / expected_events) * 100, 2)
        if expected_events > 0
        else 0.0
    )
    unique_roots = len(set(r for r in merkle_roots if r))
    direct_roots_converged = len(merkle_roots) == len(nodes) and unique_roots == 1
    if direct_roots_converged:
        known_status = "known_converged"
    is_converged = direct_roots_converged
    avg_state_size_bytes = round(total_state_size / len(nodes), 0) if nodes else 0

    return {
        "convergence_time_s": last_convergence,
        "missing_at_cutoff_count": missing_at_cutoff_count,
        "missing_at_cutoff_pct": missing_at_cutoff_pct,
        "gateway_staleness_seconds": max(gateway_staleness_values)
        if gateway_staleness_values
        else None,
        "sync_bytes_total": sync_bytes_total,
        "repair_pull_count": repair_pull_count,
        "gossip_messages_sent": total_gossip,
        "total_state_size_bytes": total_state_size,
        "avg_state_size_bytes": avg_state_size_bytes,
        "avg_state_bytes_per_known_event": round(avg_state_size_bytes / max_ev, 2)
        if max_ev > 0
        else "",
        "known_status": known_status,
        "coverage_percent": coverage_percent,
        "is_converged": is_converged,
        "unique_merkle_roots": unique_roots,
        "node_count": len(nodes),
        "max_events": max_ev,
        "min_events": min_ev,
        "delta_pulls_started": delta_pulls_started,
        "delta_pulls_success": delta_pulls_success,
        "delta_pulls_gap": delta_pulls_gap,
        "delta_deltas_applied": delta_deltas_applied,
        "delta_bytes_received": delta_bytes_received,
    }


async def _eval_wait_for_metrics(session, nodes, gateways, cfg, accepted_events):
    """Poll until convergence or timeout, then return scenario-scoped metrics."""
    started = time.time()
    deadline = started + cfg.convergence_wait_seconds
    metrics = None

    while True:
        await _eval_poll_all_gateways(session, gateways)
        metrics = await _eval_collect_metrics(
            session,
            nodes,
            gateways,
            accepted_events=accepted_events,
        )
        if metrics["is_converged"] and metrics["missing_at_cutoff_count"] == 0:
            metrics["convergence_time_s"] = round(time.time() - started, 3)
            metrics["converged_within_timeout"] = True
            metrics["convergence_wait_seconds"] = cfg.convergence_wait_seconds
            return metrics
        if time.time() >= deadline:
            metrics["convergence_time_s"] = None
            metrics["converged_within_timeout"] = False
            metrics["convergence_wait_seconds"] = cfg.convergence_wait_seconds
            return metrics
        await asyncio.sleep(cfg.poll_interval_seconds)


async def _eval_recreate_nodes(node_count: int, cfg: EvalScenarioConfig):
    _require_docker()
    if hasattr(docker_mgr, "clear_netem"):
        try:
            await asyncio.to_thread(docker_mgr.clear_netem)
        except Exception:
            pass
    existing = await asyncio.to_thread(docker_mgr.list_nodes)
    for n in existing:
        try:
            await asyncio.to_thread(docker_mgr.remove_node, n["node_id"])
        except Exception:
            pass
    await asyncio.sleep(2)
    event_id_retention_limit = 100 if cfg.state_growth_fix else 0
    for _ in range(node_count):
        try:
            await asyncio.to_thread(
                docker_mgr.create_node,
                sync_mode=cfg.sync_mode,
                event_id_retention_limit=event_id_retention_limit,
                observability_mode=cfg.dashboard_observability,
            )
        except Exception as e:
            log.warning("eval_node_create_failed", error=str(e))
    await _sync_gateways_with_nodes(force=True)
    await asyncio.sleep(3)
    return await asyncio.to_thread(docker_mgr.list_nodes), await _discover_gateways()


async def _eval_reset_runtime(session, nodes, gateways):
    """Reset node/gateway scenario state without recreating containers."""

    async def _post_first_available(urls, path):
        for base_url in urls:
            try:
                async with session.post(f"{base_url}{path}") as resp:
                    if resp.status < 400:
                        await resp.json()
                        return True
            except Exception:
                continue
        return False

    await _sync_gateways_with_nodes(force=True)

    node_tasks = []
    for node in nodes:
        urls = _candidate_urls(
            node.get("internal_url"),
            node.get("external_url"),
            node.get("url"),
        )
        node_tasks.append(_post_first_available(urls, "/evaluation/reset"))
    if node_tasks:
        await asyncio.gather(*node_tasks, return_exceptions=True)

    gateway_tasks = []
    for gateway in gateways:
        urls = _candidate_urls(gateway.get("url"), gateway.get("external_url"))
        gateway_tasks.append(_post_first_available(urls, "/gateway/evaluation/reset"))
    if gateway_tasks:
        await asyncio.gather(*gateway_tasks, return_exceptions=True)


async def _run_evaluation(cfg: EvalScenarioConfig):
    """Run a repeatable evaluation matrix."""
    global _eval_state
    run_id = f"eval-{uuid.uuid4().hex[:8]}"
    if cfg.scenario_seed is not None:
        random.seed(cfg.scenario_seed)

    _eval_state["running"] = True
    _eval_state["run_id"] = run_id
    _eval_state["started_at"] = time.time()
    _eval_state["results"] = []
    _eval_state["error"] = None
    _eval_state["csv_data"] = None

    partition_splits = (
        cfg.partition_splits if cfg.network_profile == "hard_partition" else []
    )
    steps_per_node_count = 2 + len(partition_splits)
    _eval_state["total_steps"] = len(cfg.node_counts) * steps_per_node_count
    step = 0

    def _row_base(scenario, node_count, event_result, partition_split=None):
        attempted = event_result.get("attempted", 0)
        accepted = event_result.get("accepted", 0)
        return {
            "run_id": run_id,
            "implementation": cfg.implementation_label,
            "sync_mode": cfg.sync_mode,
            "state_growth_fix": cfg.state_growth_fix,
            "network_profile": cfg.network_profile,
            "dashboard_observability": cfg.dashboard_observability,
            "scenario": scenario,
            "node_count": node_count,
            "partition_split": partition_split,
            "attempted_events": attempted,
            "accepted_events": accepted,
            "write_success_rate": round(accepted / attempted, 4) if attempted else 0.0,
        }

    def _append_result(row, metrics):
        combined = {**row, **metrics}
        accepted = combined.get("accepted_events", 0)
        sync_bytes = combined.get("sync_bytes_total", 0)
        combined["bytes_per_accepted_event"] = (
            round(sync_bytes / accepted, 2) if accepted else ""
        )
        _eval_state["results"].append(combined)

    async def _prepare_scenario(session, nodes, gateways):
        try:
            await asyncio.to_thread(docker_mgr.heal_all)
        except Exception:
            pass
        if hasattr(docker_mgr, "clear_netem"):
            try:
                await asyncio.to_thread(docker_mgr.clear_netem)
            except Exception:
                pass
        await _eval_reset_runtime(session, nodes, gateways)
        if cfg.network_profile != "hard_partition" and hasattr(
            docker_mgr, "apply_netem_profile"
        ):
            await asyncio.to_thread(docker_mgr.apply_netem_profile, cfg.network_profile)
        return nodes, gateways

    async def _finish_scenario():
        try:
            await asyncio.to_thread(docker_mgr.heal_all)
        except Exception:
            pass
        if hasattr(docker_mgr, "clear_netem"):
            try:
                await asyncio.to_thread(docker_mgr.clear_netem)
            except Exception:
                pass

    try:
        timeout = aiohttp.ClientTimeout(total=30)

        for node_count in cfg.node_counts:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                nodes, gateways = await _eval_recreate_nodes(node_count, cfg)
                _eval_state["current_step"] = f"[{node_count}N] Baseline"
                _eval_state["progress"] = step
                nodes, gateways = await _prepare_scenario(session, nodes, gateways)
                baseline_events = await _eval_send_events(
                    session,
                    nodes,
                    cfg.events_per_node,
                    concurrency=cfg.event_concurrency,
                )
                metrics = await _eval_wait_for_metrics(
                    session,
                    nodes,
                    gateways,
                    cfg,
                    baseline_events["accepted"],
                )
                _append_result(
                    _row_base("baseline", node_count, baseline_events),
                    metrics,
                )
                await _finish_scenario()
                step += 1

                for split in partition_splits:
                    split_label = f"{round(split * 100)}/{round((1 - split) * 100)}"
                    _eval_state["current_step"] = (
                        f"[{node_count}N] Partition {split_label}"
                    )
                    _eval_state["progress"] = step
                    nodes, gateways = await _prepare_scenario(session, nodes, gateways)

                    pre_events = await _eval_send_events(
                        session,
                        nodes,
                        cfg.events_per_node,
                        concurrency=cfg.event_concurrency,
                    )
                    await _eval_poll_all_gateways(session, gateways)

                    try:
                        await asyncio.to_thread(docker_mgr.create_split_brain, split)
                    except Exception:
                        pass
                    await asyncio.sleep(1)

                    partition_events = await _eval_send_events(
                        session,
                        nodes,
                        3,
                        concurrency=cfg.event_concurrency,
                    )
                    await asyncio.sleep(cfg.partition_wait_seconds)

                    try:
                        await asyncio.to_thread(docker_mgr.heal_all)
                    except Exception:
                        pass
                    await asyncio.sleep(2)

                    combined_events = {
                        "attempted": pre_events["attempted"]
                        + partition_events["attempted"],
                        "accepted": pre_events["accepted"]
                        + partition_events["accepted"],
                    }
                    expected_events = (
                        pre_events["accepted"] + partition_events["accepted"]
                    )
                    metrics = await _eval_wait_for_metrics(
                        session,
                        nodes,
                        gateways,
                        cfg,
                        expected_events,
                    )
                    _append_result(
                        _row_base(
                            "partition_heal",
                            node_count,
                            combined_events,
                            partition_split=split_label,
                        ),
                        metrics,
                    )
                    await _finish_scenario()
                    step += 1

                _eval_state["current_step"] = f"[{node_count}N] High load"
                _eval_state["progress"] = step
                nodes, gateways = await _prepare_scenario(session, nodes, gateways)
                heavy_events = await _eval_send_events(
                    session,
                    nodes,
                    cfg.events_per_node * 3,
                    category="resource",
                    concurrency=cfg.event_concurrency,
                )
                metrics = await _eval_wait_for_metrics(
                    session,
                    nodes,
                    gateways,
                    cfg,
                    heavy_events["accepted"],
                )
                _append_result(
                    _row_base("high_load", node_count, heavy_events),
                    metrics,
                )
                await _finish_scenario()
                step += 1

        _eval_state["current_step"] = "Generating CSV..."
        _eval_state["progress"] = step

        csv_header = [
            "run_id",
            "implementation",
            "sync_mode",
            "state_growth_fix",
            "network_profile",
            "dashboard_observability",
            "scenario",
            "node_count",
            "partition_split",
            "attempted_events",
            "accepted_events",
            "write_success_rate",
            "convergence_time_s",
            "converged_within_timeout",
            "convergence_wait_seconds",
            "missing_at_cutoff_count",
            "missing_at_cutoff_pct",
            "gateway_staleness_seconds",
            "sync_bytes_total",
            "bytes_per_accepted_event",
            "avg_state_bytes_per_known_event",
            "repair_pull_count",
            "gossip_messages_sent",
            "total_state_size_bytes",
            "avg_state_size_bytes",
            "known_status",
            "coverage_percent",
            "is_converged",
            "unique_merkle_roots",
            "max_events",
            "min_events",
            "delta_pulls_started",
            "delta_pulls_success",
            "delta_pulls_gap",
            "delta_deltas_applied",
            "delta_bytes_received",
        ]
        csv_lines = [",".join(csv_header)]
        for row in _eval_state["results"]:
            csv_lines.append(",".join(str(row.get(col, "")) for col in csv_header))
        _eval_state["csv_data"] = "\n".join(csv_lines) + "\n"
        _eval_state["current_step"] = "Done!"
        _eval_state["progress"] = _eval_state["total_steps"]

    except Exception as e:
        _eval_state["error"] = str(e)
        _eval_state["current_step"] = f"Error: {e}"
        log.error("evaluation_failed", error=str(e))
    finally:
        try:
            if hasattr(docker_mgr, "clear_netem"):
                await asyncio.to_thread(docker_mgr.clear_netem)
        except Exception:
            pass
        _eval_state["running"] = False


@app.post("/evaluation/run")
async def run_evaluation(cfg: EvalScenarioConfig = None):
    """Start a structured evaluation run. Returns immediately; poll /evaluation/status."""
    if _eval_state["running"]:
        raise _api_error(409, "EVAL_RUNNING", "An evaluation is already in progress")

    if cfg is None:
        cfg = EvalScenarioConfig()

    if cfg.dashboard_observability not in {"off", "light", "full"}:
        raise _api_error(
            400,
            "INVALID_DASHBOARD_OBSERVABILITY",
            "dashboard_observability must be off, light, or full",
        )
    if cfg.sync_mode not in {"full_state", "true_delta"}:
        raise _api_error(
            400,
            "INVALID_SYNC_MODE",
            "sync_mode must be full_state or true_delta",
        )
    if cfg.network_profile not in {"hard_partition", "degraded_300ms_10loss"}:
        raise _api_error(
            400,
            "INVALID_NETWORK_PROFILE",
            "network_profile must be hard_partition or degraded_300ms_10loss",
        )

    partition_splits = (
        cfg.partition_splits if cfg.network_profile == "hard_partition" else []
    )
    total_steps = len(cfg.node_counts) * (2 + len(partition_splits))
    asyncio.create_task(_run_evaluation(cfg))
    return {
        "status": "started",
        "total_steps": total_steps,
        "node_counts": cfg.node_counts,
        "events_per_node": cfg.events_per_node,
        "partition_splits": partition_splits,
        "implementation": cfg.implementation_label,
        "sync_mode": cfg.sync_mode,
        "state_growth_fix": cfg.state_growth_fix,
        "network_profile": cfg.network_profile,
        "dashboard_observability": cfg.dashboard_observability,
    }


@app.get("/evaluation/status")
async def evaluation_status():
    """Poll evaluation progress."""
    return {
        "running": _eval_state["running"],
        "run_id": _eval_state["run_id"],
        "started_at": _eval_state["started_at"],
        "progress": _eval_state["progress"],
        "total_steps": _eval_state["total_steps"],
        "current_step": _eval_state["current_step"],
        "error": _eval_state["error"],
        "results_count": len(_eval_state["results"]),
        "has_csv": _eval_state["csv_data"] is not None,
    }


@app.get("/evaluation/download")
async def download_evaluation():
    """Download evaluation results as CSV."""
    if _eval_state["csv_data"] is None:
        raise _api_error(
            404, "NO_DATA", "No evaluation data available. Run an evaluation first."
        )
    from fastapi.responses import Response

    return Response(
        content=_eval_state["csv_data"],
        media_type="text/csv",
        headers={"Content-Disposition": "attachment; filename=evaluation_results.csv"},
    )


@app.get("/evaluation/results")
async def evaluation_results():
    """Return evaluation results as JSON for the UI table."""
    return {
        "results": _eval_state["results"],
        "has_csv": _eval_state["csv_data"] is not None,
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
