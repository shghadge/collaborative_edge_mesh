"""
Simulator entry point. Hosts dashboard/static UI and simulation control APIs.
Automatically discovers gateways and edge nodes via Docker (no registry).
"""

import asyncio
import json
import logging
import random
import os
import signal
import time
import uuid
from typing import Optional, Any, Dict, Set

import aiohttp
import structlog
import uvicorn
from fastapi import FastAPI, HTTPException, Request, WebSocket, WebSocketDisconnect
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field

from src.config import config
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

from fastapi.openapi.docs import get_swagger_ui_html
from fastapi.responses import HTMLResponse

log = structlog.get_logger()
app = FastAPI(title="Edge Mesh Simulator", docs_url=None)


try:
    docker_mgr = DockerManager()
except Exception as error:
    log.warning("docker_unavailable", error=str(error))
    docker_mgr = None

USER_GATEWAY_ASSIGNMENTS = {}  # node_id -> gateway_name

# --- WebSocket state ---
_ws_clients: Set[WebSocket] = set()

# --- Sync debounce ---
_last_sync_time: float = 0.0
_SYNC_DEBOUNCE_SECONDS = 5.0
_sync_lock = asyncio.Lock() if False else None  # created at startup
_last_sync_fingerprint: str = ""


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


@app.get("/docs", include_in_schema=False)
async def custom_swagger_ui_html():
    urls = [{"url": "/openapi.json", "name": "Simulator Engine"}]

    if docker_mgr is not None:
        try:
            gateways = await _discover_gateways()
            for gw in gateways:
                gw_url = gw.get("external_url") or gw.get("url")
                if gw_url:
                    urls.append({
                        "url": f"{gw_url}/openapi.json",
                        "name": f"Gateway ({gw['name']})"
                    })

            nodes = await asyncio.to_thread(docker_mgr.list_nodes)
            for node in nodes:
                node_url = node.get("url")  # external facing URL
                if node_url:
                    urls.append({
                        "url": f"{node_url}/openapi.json",
                        "name": f"Node ({node['name']})"
                    })
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
    if node_id.startswith("edge-node-"):
        suffix = node_id.replace("edge-node-", "", 1)
        return f"node-{suffix}"
    return node_id


def _candidate_urls(*urls: Optional[str]) -> list[str]:
    ordered: list[str] = []
    for url in urls:
        if not url:
            continue
        if url not in ordered:
            ordered.append(url)
    return ordered


def _build_node_gateway_map(
    valid_nodes: list[dict],
    gateways: list[dict],
    *,
    persist_auto_assignments: bool,
) -> dict[str, str]:
    """Resolve each node to a gateway using sticky assignments + RR fallback."""
    if not gateways:
        return {}

    gw_names = [gw["name"] for gw in gateways]
    node_to_gw: dict[str, str] = {}
    unassigned_nodes: list[dict] = []

    for node in valid_nodes:
        node_id = node.get("node_id")
        if not node_id:
            continue

        assigned_gw = USER_GATEWAY_ASSIGNMENTS.get(node_id)
        if assigned_gw and assigned_gw in gw_names:
            node_to_gw[node_id] = assigned_gw
        else:
            unassigned_nodes.append(node)

    for i, node in enumerate(unassigned_nodes):
        node_id = node.get("node_id")
        if not node_id:
            continue
        gw_name = gateways[i % len(gateways)]["name"]
        node_to_gw[node_id] = gw_name

    if persist_auto_assignments:
        valid_node_ids = {
            node.get("node_id") for node in valid_nodes if node.get("node_id")
        }
        for node_id in list(USER_GATEWAY_ASSIGNMENTS.keys()):
            if node_id not in valid_node_ids:
                USER_GATEWAY_ASSIGNMENTS.pop(node_id, None)
        USER_GATEWAY_ASSIGNMENTS.update(node_to_gw)

    return node_to_gw


def _require_docker():
    if docker_mgr is None:
        raise _api_error(503, "DOCKER_UNAVAILABLE", "Docker not available")


async def _discover_gateways():
    _require_docker()
    gateways = await asyncio.to_thread(docker_mgr.list_gateways)
    if not gateways:
        raise _api_error(503, "NO_GATEWAY", "No running gateway containers discovered")
    return gateways


async def _sync_gateways_with_nodes(force: bool = False):
    """Sync gateway-node assignments. Debounced to avoid hammering."""
    global _last_sync_time, _last_sync_fingerprint, _sync_lock
    _require_docker()

    if _sync_lock is None:
        _sync_lock = asyncio.Lock()

    # Debounce: skip if recently synced and not forced
    now = time.time()
    if not force and (now - _last_sync_time) < _SYNC_DEBOUNCE_SECONDS:
        return

    async with _sync_lock:
        # Double-check after acquiring lock
        if not force and (time.time() - _last_sync_time) < _SYNC_DEBOUNCE_SECONDS:
            return

        nodes = await asyncio.to_thread(docker_mgr.list_nodes)
        valid_nodes = [
            node for node in nodes
            if node.get("node_id") and (node.get("internal_url") or node.get("url"))
        ]
        valid_nodes.sort(key=lambda x: x["name"])

        try:
            gateways = await _discover_gateways()
        except Exception:
            gateways = []

        if not gateways:
            return

        # Build fingerprint to skip if nothing changed
        fp_parts = sorted(f"{n.get('node_id')}:{USER_GATEWAY_ASSIGNMENTS.get(n.get('node_id'), '')}" for n in valid_nodes)
        fingerprint = "|".join(fp_parts) + "||" + "|".join(g["name"] for g in gateways)
        if not force and fingerprint == _last_sync_fingerprint:
            _last_sync_time = time.time()
            return

        assignments = {gw["name"]: [] for gw in gateways}
        node_to_gw = _build_node_gateway_map(
            valid_nodes,
            gateways,
            persist_auto_assignments=True,
        )

        for node in valid_nodes:
            gw_name = node_to_gw.get(node.get("node_id"))
            if not gw_name:
                continue
            assignments[gw_name].append({
                "node_id": node.get("node_id"),
                "url": node.get("internal_url") or node.get("url"),
                "internal_url": node.get("internal_url"),
                "external_url": node.get("url"),
            })

        timeout = aiohttp.ClientTimeout(total=6)
        connector = aiohttp.TCPConnector(limit=50)
        async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
            # Sync gateways in parallel
            async def _sync_gateway(gateway):
                payload = {"nodes": assignments[gateway["name"]]}
                for base_url in _candidate_urls(gateway.get("url"), gateway.get("external_url")):
                    url = f"{base_url}/gateway/nodes/sync"
                    try:
                        async with session.post(url, json=payload) as response:
                            if response.status < 400:
                                return
                    except Exception:
                        pass
                log.debug("gateway_sync_unreachable", gateway=gateway["name"])

            await asyncio.gather(*[_sync_gateway(gw) for gw in gateways], return_exceptions=True)

            # Notify all nodes in parallel
            async def _notify_node(node_info, gw_name):
                node_urls = _candidate_urls(
                    node_info.get("internal_url"),
                    node_info.get("external_url"),
                    node_info.get("url"),
                )
                
                # Try all candidate URLs, with robust retries for newly booted nodes
                for _ in range(15):
                    for base_url in node_urls:
                        try:
                            async with session.post(
                                f"{base_url}/config/gateway",
                                json={"assigned_gateway": gw_name}
                            ) as response:
                                if response.status < 400:
                                    return
                        except Exception:
                            pass
                    await asyncio.sleep(1)
                log.warning("node_config_sync_failed", node=node_info.get("node_id"))

            tasks = []
            for gw_name, nodes_in_gw in assignments.items():
                for node_info in nodes_in_gw:
                    tasks.append(_notify_node(node_info, gw_name))
            await asyncio.gather(*tasks, return_exceptions=True)

        _last_sync_time = time.time()
        _last_sync_fingerprint = fingerprint

async def _proxy_gateway_request(method: str, path: str, params: Optional[dict] = None):
    # Don't sync on every proxy call — sync is debounced and triggered by topology changes
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
        raise _api_error(500, "GATEWAY_CREATE_FAILED", "Failed to create gateway", str(error))


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
async def create_node(node_id: Optional[str] = None, assigned_gateway: Optional[str] = None):
    _require_docker()
    try:
        result = await asyncio.to_thread(docker_mgr.create_node, node_id, assigned_gateway=assigned_gateway or "")
        if assigned_gateway and result.get("node_id"):
            USER_GATEWAY_ASSIGNMENTS[result.get("node_id")] = assigned_gateway
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
            result = await asyncio.to_thread(docker_mgr.create_node, assigned_gateway=assigned_gateway or "")
            if assigned_gateway and result.get("node_id"):
                USER_GATEWAY_ASSIGNMENTS[result.get("node_id")] = assigned_gateway
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
        USER_GATEWAY_ASSIGNMENTS[node_id] = request.assigned_gateway
    else:
        USER_GATEWAY_ASSIGNMENTS.pop(node_id, None)
    await _sync_gateways_with_nodes(force=True)
    return {"status": "success", "node_id": node_id, "assigned_gateway": request.assigned_gateway}


@app.delete("/nodes/{node_id}")
async def remove_node(node_id: str):
    _require_docker()
    node_id = _normalize_node_id(node_id)
    USER_GATEWAY_ASSIGNMENTS.pop(node_id, None)
    result = await asyncio.to_thread(docker_mgr.remove_node, node_id)
    await _sync_gateways_with_nodes(force=True)
    return result


# --- Partition Control ---


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
            if node_name == target_node or node.get("node_id") == target_node or node.get("name") == target_node:
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

                # Save tracking to gateway's store
                try:
                    await _discover_gateways()
                    # Event tracking persisted by gateway on its next poll
                except Exception:
                    pass

                return {
                    "status": "sent",
                    "tracking_id": tracking_id,
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
    """Aggregate all 5 report metrics from gateway + nodes."""
    _require_docker()

    # Fetch runtime metrics from all gateways concurrently
    gateways = await _discover_gateways()
    overall_divergent = False
    max_divergence_duration = 0.0
    max_convergence_time = 0.0
    total_polls_completed = 0

    timeout = aiohttp.ClientTimeout(total=3)
    connector = aiohttp.TCPConnector(limit=50)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:
        async def _fetch_gw_metrics(gw):
            try:
                async with session.get(f"{gw['url']}/gateway/runtime-metrics") as resp:
                    return await resp.json()
            except Exception:
                return None

        gw_results = await asyncio.gather(
            *[_fetch_gw_metrics(gw) for gw in gateways],
            return_exceptions=True
        )
        for gw_data in gw_results:
            if gw_data is None or isinstance(gw_data, Exception):
                continue
            rt = gw_data.get("runtime_metrics", {})
            if gw_data.get("is_divergent"):
                overall_divergent = True
            max_divergence_duration = max(max_divergence_duration, rt.get("divergence_duration_seconds", 0.0))
            max_convergence_time = max(max_convergence_time, rt.get("last_convergence_seconds", 0.0) or 0.0)
            total_polls_completed += rt.get("polls_completed", 0)

    # Get per-node stats concurrently
    nodes = await asyncio.to_thread(docker_mgr.list_nodes)
    per_node_stats = []
    total_gossip_sent = 0
    total_gossip_received = 0
    total_state_size = 0

    timeout = aiohttp.ClientTimeout(total=3)
    connector = aiohttp.TCPConnector(limit=50)
    async with aiohttp.ClientSession(timeout=timeout, connector=connector) as session:

        async def _fetch_node_stats(node):
            """Fetch gossip + state + merkle for one node concurrently."""
            node_url = node.get("internal_url") or node.get("url")
            if not node_url:
                return None

            node_info = {
                "node_id": node.get("node_id"),
                "name": node.get("name"),
                "isolated": node.get("isolated", False),
                "assigned_gateway": USER_GATEWAY_ASSIGNMENTS.get(node.get("node_id"), "unknown"),
            }

            # Fetch gossip, state, and merkle in parallel for this single node
            async def _get_gossip():
                try:
                    async with session.get(f"{node_url}/gossip/stats") as resp:
                        return await resp.json()
                except Exception:
                    return None

            async def _get_state():
                try:
                    async with session.get(f"{node_url}/state") as resp:
                        return await resp.json()
                except Exception:
                    return None

            async def _get_merkle():
                try:
                    async with session.get(f"{node_url}/state/merkle") as resp:
                        return await resp.json()
                except Exception:
                    return None

            gossip_data, state_data, merkle_data = await asyncio.gather(
                _get_gossip(), _get_state(), _get_merkle()
            )

            if gossip_data:
                stats = gossip_data.get("stats", {})
                node_info["gossip_sent"] = stats.get("sent", 0)
                node_info["gossip_received"] = stats.get("received", 0)
                node_info["gossip_merged"] = stats.get("merged", 0)
                node_info["peers_discovered"] = stats.get("peers_discovered", 0)
                node_info["sent_bytes"] = stats.get("sent_bytes", 0)
                node_info["received_bytes"] = stats.get("received_bytes", 0)
                node_info["avg_merge_ms"] = stats.get("avg_merge_ms", 0)
            else:
                node_info["gossip_error"] = True

            if state_data:
                size = len(json.dumps(state_data))
                node_info["state_size_bytes"] = size
                summary = state_data.get("state_summary", {})
                ev_count = summary.get("total_events", 0)
                if ev_count == 0:
                    ev_ids = state_data.get("event_ids", [])
                    ev_count = len(ev_ids) if isinstance(ev_ids, list) else 0
                node_info["event_count"] = ev_count
            else:
                node_info["state_size_bytes"] = 0
                node_info["event_count"] = 0

            if merkle_data:
                node_info["merkle_root"] = merkle_data.get("merkle_root", "")[:12]
                node_info["version"] = merkle_data.get("version", 0)

            return node_info

        # Fetch ALL node stats concurrently
        results = await asyncio.gather(
            *[_fetch_node_stats(node) for node in nodes],
            return_exceptions=True
        )
        for node_info in results:
            if node_info is None or isinstance(node_info, Exception):
                continue
            per_node_stats.append(node_info)
            total_gossip_sent += node_info.get("gossip_sent", 0)
            total_gossip_received += node_info.get("gossip_received", 0)
            total_state_size += node_info.get("state_size_bytes", 0)

    # Compute data loss metric per gateway
    gateway_data_loss = []

    # Group nodes by gateway
    node_by_gw = {}
    for n in per_node_stats:
        gw = n.get("assigned_gateway", "unknown")
        if gw not in node_by_gw:
            node_by_gw[gw] = []
        node_by_gw[gw].append(n)

    for gw, gw_nodes in node_by_gw.items():
        event_counts = [n.get("event_count", 0) for n in gw_nodes if "event_count" in n]
        if len(event_counts) > 1:
            max_events = max(event_counts)
            min_events = min(event_counts)
            if max_events > 0:
                gateway_data_loss.append(round((1 - min_events / max_events) * 100, 2))

    data_loss_percent = max(gateway_data_loss) if gateway_data_loss and not overall_divergent else (None if overall_divergent else 0.0)

    return {
        "convergence_time_seconds": max_convergence_time if max_convergence_time > 0 else None,
        "is_divergent": overall_divergent,
        "divergence_duration_seconds": max_divergence_duration,
        "data_loss_percent": data_loss_percent,
        "total_gossip_messages_sent": total_gossip_sent,
        "total_gossip_messages_received": total_gossip_received,
        "total_state_size_bytes": total_state_size,
        "node_count": len(per_node_stats),
        "polls_completed": total_polls_completed,
        "per_node": per_node_stats,
    }


@app.get("/metrics/timeseries")
async def metrics_timeseries(metric: str = "state_size_bytes", limit: int = 100):
    """Get time-series chart data from the gateway."""
    return await _proxy_gateway_request(
        "GET",
        "/gateway/timeseries",
        params={"metric": metric, "limit": limit},
    )


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
    "progress": 0,
    "total_steps": 0,
    "current_step": "",
    "results": [],
    "error": None,
    "csv_data": None,
}


class EvalScenarioConfig(BaseModel):
    node_counts: list = Field(default=[3, 5])
    events_per_node: int = Field(default=5)
    partition_wait_seconds: float = Field(default=8.0)
    convergence_wait_seconds: float = Field(default=15.0)
    poll_interval_seconds: float = Field(default=2.0)


async def _eval_send_events(session, nodes, count, category="sensor"):
    """Send `count` events distributed across `nodes`. Returns tracking IDs."""
    tracking_ids = []
    event_types = [
        "water_level", "temperature", "shelter_occupancy",
        "road_status", "wind_speed", "air_quality",
    ]
    for i in range(count * len(nodes)):
        target = nodes[i % len(nodes)]
        node_url = target.get("internal_url") or target.get("url")
        if not node_url:
            continue
        etype = event_types[i % len(event_types)]
        tid = str(uuid.uuid4())
        payload = {
            "type": etype,
            "value": round(random.uniform(1.0, 100.0), 2),
            "location": f"sensor_{i}",
            "category": category,
            "metadata": {"tracking_id": tid, "source": "evaluation"},
        }
        try:
            async with session.post(f"{node_url}/event", json=payload) as resp:
                await resp.json()
            tracking_ids.append(tid)
        except Exception:
            pass
    return tracking_ids


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


async def _eval_collect_metrics(session, nodes, gateways):
    """Collect all 5 report metrics from current state."""
    # Gateway runtime metrics (aggregate across all gateways)
    last_convergence = None
    for gw in gateways:
        gw_metrics = {}
        for base_url in _candidate_urls(gw.get("url"), gw.get("external_url")):
            try:
                async with session.get(f"{base_url}/gateway/runtime-metrics") as resp:
                    if resp.status >= 400:
                        continue
                    gw_data = await resp.json()
                    gw_metrics = gw_data.get("runtime_metrics", {})
                    break
            except Exception:
                continue

        convergence_value = gw_metrics.get("last_convergence_seconds")
        if convergence_value is not None:
            if last_convergence is None or convergence_value > last_convergence:
                last_convergence = convergence_value

    # Per-node stats
    total_gossip = 0
    total_state_size = 0
    event_counts = []
    merkle_roots = []

    for node in nodes:
        node_url = node.get("internal_url") or node.get("url")
        if not node_url:
            continue
        try:
            async with session.get(f"{node_url}/gossip/stats") as resp:
                gdata = await resp.json()
                total_gossip += gdata.get("stats", {}).get("sent", 0)
        except Exception:
            pass
        try:
            async with session.get(f"{node_url}/state") as resp:
                sdata = await resp.json()
                size = len(json.dumps(sdata))
                total_state_size += size
                # Extract event count from state_summary or event_ids
                summary = sdata.get("state_summary", {})
                ev_count = summary.get("total_events", 0)
                if ev_count == 0:
                    ev_ids = sdata.get("event_ids", [])
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
    data_loss = round((1 - min_ev / max_ev) * 100, 2) if max_ev > 0 else 0.0
    unique_roots = len(set(r for r in merkle_roots if r))
    is_converged = unique_roots <= 1

    return {
        "convergence_time_s": last_convergence,
        "data_loss_pct": data_loss,
        "gossip_messages_sent": total_gossip,
        "total_state_size_bytes": total_state_size,
        "avg_state_size_bytes": round(total_state_size / len(nodes), 0) if nodes else 0,
        "is_converged": is_converged,
        "unique_merkle_roots": unique_roots,
        "node_count": len(nodes),
        "max_events": max_ev,
        "min_events": min_ev,
    }


async def _run_evaluation(cfg: EvalScenarioConfig):
    """Run full evaluation suite: 3 scenarios × N node counts."""
    global _eval_state
    _eval_state["running"] = True
    _eval_state["results"] = []
    _eval_state["error"] = None
    _eval_state["csv_data"] = None

    scenarios = ["baseline_convergence", "partition_heal", "scaling_events"]
    _eval_state["total_steps"] = len(cfg.node_counts) * len(scenarios)
    step = 0

    try:
        timeout = aiohttp.ClientTimeout(total=30)

        for node_count in cfg.node_counts:
            # --- Phase: Setup nodes ---
            _eval_state["current_step"] = f"Creating {node_count} nodes..."
            _eval_state["progress"] = step

            # Clean up existing nodes first
            _require_docker()
            existing = await asyncio.to_thread(docker_mgr.list_nodes)
            for n in existing:
                try:
                    await asyncio.to_thread(docker_mgr.remove_node, n["node_id"])
                except Exception:
                    pass
            await asyncio.sleep(2)

            # Create fresh nodes
            created_nodes = []
            for _ in range(node_count):
                try:
                    result = await asyncio.to_thread(docker_mgr.create_node)
                    created_nodes.append(result)
                except Exception as e:
                    log.warning("eval_node_create_failed", error=str(e))
            await _sync_gateways_with_nodes(force=True)
            await asyncio.sleep(3)  # Let gossip settle

            nodes = await asyncio.to_thread(docker_mgr.list_nodes)
            gateways = await _discover_gateways()

            # ─── Scenario 1: Baseline Convergence ───
            _eval_state["current_step"] = f"[{node_count}N] Scenario 1: Baseline convergence"
            _eval_state["progress"] = step

            async with aiohttp.ClientSession(timeout=timeout) as session:
                tracking_ids = await _eval_send_events(session, nodes, cfg.events_per_node)

                # Wait for gossip convergence
                for _ in range(int(cfg.convergence_wait_seconds / cfg.poll_interval_seconds)):
                    await _eval_poll_all_gateways(session, gateways)
                    await asyncio.sleep(cfg.poll_interval_seconds)

                metrics = await _eval_collect_metrics(session, nodes, gateways)
                _eval_state["results"].append({
                    "scenario": "baseline_convergence",
                    "node_count": node_count,
                    "events_sent": len(tracking_ids),
                    **metrics,
                })
            step += 1

            # ─── Scenario 2: Partition → Heal → Convergence ───
            _eval_state["current_step"] = f"[{node_count}N] Scenario 2: Partition & heal"
            _eval_state["progress"] = step

            async with aiohttp.ClientSession(timeout=timeout) as session:
                # Send some events first
                pre_ids = await _eval_send_events(session, nodes, cfg.events_per_node)
                await asyncio.sleep(2)

                # Trigger a poll so gateway has baseline
                await _eval_poll_all_gateways(session, gateways)

                # Create partition (split-brain)
                _eval_state["current_step"] = f"[{node_count}N] Scenario 2: Partitioned — sending events..."
                try:
                    await asyncio.to_thread(docker_mgr.create_split_brain)
                except Exception:
                    pass
                await asyncio.sleep(1)

                # Send events during partition
                partition_ids = await _eval_send_events(session, nodes, 3)
                await asyncio.sleep(cfg.partition_wait_seconds)

                # Heal partition
                _eval_state["current_step"] = f"[{node_count}N] Scenario 2: Healing — waiting for convergence..."
                try:
                    await asyncio.to_thread(docker_mgr.heal_all)
                except Exception:
                    pass
                await asyncio.sleep(2)

                # Wait for convergence
                for _ in range(int(cfg.convergence_wait_seconds / cfg.poll_interval_seconds)):
                    await _eval_poll_all_gateways(session, gateways)
                    await asyncio.sleep(cfg.poll_interval_seconds)

                metrics = await _eval_collect_metrics(session, nodes, gateways)
                _eval_state["results"].append({
                    "scenario": "partition_heal",
                    "node_count": node_count,
                    "events_sent": len(pre_ids) + len(partition_ids),
                    **metrics,
                })
            step += 1

            # ─── Scenario 3: High Event Load ───
            _eval_state["current_step"] = f"[{node_count}N] Scenario 3: Scaling events"
            _eval_state["progress"] = step

            async with aiohttp.ClientSession(timeout=timeout) as session:
                heavy_ids = await _eval_send_events(
                    session, nodes, cfg.events_per_node * 3, category="resource",
                )
                for _ in range(int(cfg.convergence_wait_seconds / cfg.poll_interval_seconds)):
                    await _eval_poll_all_gateways(session, gateways)
                    await asyncio.sleep(cfg.poll_interval_seconds)

                metrics = await _eval_collect_metrics(session, nodes, gateways)
                _eval_state["results"].append({
                    "scenario": "scaling_events",
                    "node_count": node_count,
                    "events_sent": len(heavy_ids),
                    **metrics,
                })
            step += 1

        # Build CSV
        _eval_state["current_step"] = "Generating CSV..."
        _eval_state["progress"] = step

        csv_header = [
            "scenario", "node_count", "events_sent",
            "convergence_time_s", "data_loss_pct",
            "gossip_messages_sent", "total_state_size_bytes",
            "avg_state_size_bytes", "is_converged",
            "unique_merkle_roots", "max_events", "min_events",
        ]
        csv_lines = [",".join(csv_header)]
        for row in _eval_state["results"]:
            csv_lines.append(",".join(
                str(row.get(col, "")) for col in csv_header
            ))
        _eval_state["csv_data"] = "\n".join(csv_lines) + "\n"
        _eval_state["current_step"] = "Done!"
        _eval_state["progress"] = _eval_state["total_steps"]

    except Exception as e:
        _eval_state["error"] = str(e)
        _eval_state["current_step"] = f"Error: {e}"
        log.error("evaluation_failed", error=str(e))
    finally:
        _eval_state["running"] = False


@app.post("/evaluation/run")
async def run_evaluation(cfg: EvalScenarioConfig = None):
    """Start a structured evaluation run. Returns immediately; poll /evaluation/status."""
    if _eval_state["running"]:
        raise _api_error(409, "EVAL_RUNNING", "An evaluation is already in progress")

    if cfg is None:
        cfg = EvalScenarioConfig()

    asyncio.create_task(_run_evaluation(cfg))
    return {
        "status": "started",
        "total_steps": len(cfg.node_counts) * 3,
        "node_counts": cfg.node_counts,
        "events_per_node": cfg.events_per_node,
    }


@app.get("/evaluation/status")
async def evaluation_status():
    """Poll evaluation progress."""
    return {
        "running": _eval_state["running"],
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
        raise _api_error(404, "NO_DATA", "No evaluation data available. Run an evaluation first.")
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
        await asyncio.sleep(3)
        if not _ws_clients:
            continue
        try:
            # Gather lightweight data for push
            payload = {}

            if docker_mgr:
                try:
                    nodes_data = await list_nodes()
                    payload["nodes"] = nodes_data
                except Exception:
                    pass

                try:
                    gateways_data = await list_gateways()
                    payload["gateways"] = gateways_data
                except Exception:
                    pass

                try:
                    metrics = await live_metrics()
                    payload["metrics"] = metrics
                except Exception:
                    pass

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
