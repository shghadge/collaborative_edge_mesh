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
import uuid
from typing import Optional

import aiohttp
import structlog
import uvicorn
from fastapi import FastAPI, HTTPException, Request
from fastapi.responses import JSONResponse
from fastapi.staticfiles import StaticFiles
from pydantic import BaseModel, Field
from typing import Any, Dict

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

log = structlog.get_logger()
app = FastAPI(title="Edge Mesh Simulator")


try:
    docker_mgr = DockerManager()
except Exception as error:
    log.warning("docker_unavailable", error=str(error))
    docker_mgr = None


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


# --- Docker / Gateway helpers ---


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


# --- Node Management ---


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

    # Get gateway runtime metrics
    gateway_data = await _proxy_gateway_request("GET", "/gateway/runtime-metrics")
    runtime = gateway_data.get("runtime_metrics", {})

    # Get per-node gossip stats
    nodes = await asyncio.to_thread(docker_mgr.list_nodes)
    per_node_stats = []
    total_gossip_sent = 0
    total_gossip_received = 0
    total_state_size = 0

    timeout = aiohttp.ClientTimeout(total=3)
    async with aiohttp.ClientSession(timeout=timeout) as session:
        for node in nodes:
            node_url = node.get("internal_url") or node.get("url")
            if not node_url:
                continue

            node_info = {
                "node_id": node.get("node_id"),
                "name": node.get("name"),
                "isolated": node.get("isolated", False),
            }

            try:
                async with session.get(f"{node_url}/gossip/stats") as resp:
                    gossip_data = await resp.json()
                    stats = gossip_data.get("stats", {})
                    node_info["gossip_sent"] = stats.get("sent", 0)
                    node_info["gossip_received"] = stats.get("received", 0)
                    node_info["gossip_merged"] = stats.get("merged", 0)
                    node_info["peers_discovered"] = stats.get("peers_discovered", 0)
                    node_info["sent_bytes"] = stats.get("sent_bytes", 0)
                    node_info["received_bytes"] = stats.get("received_bytes", 0)
                    node_info["avg_merge_ms"] = stats.get("avg_merge_ms", 0)
                    total_gossip_sent += stats.get("sent", 0)
                    total_gossip_received += stats.get("received", 0)
            except Exception:
                node_info["gossip_error"] = True

            try:
                async with session.get(f"{node_url}/state") as resp:
                    state_data = await resp.json()
                    size = len(json.dumps(state_data))
                    node_info["state_size_bytes"] = size
                    summary = state_data.get("state_summary", {})
                    ev_count = summary.get("total_events", 0)
                    if ev_count == 0:
                        ev_ids = state_data.get("event_ids", [])
                        ev_count = len(ev_ids) if isinstance(ev_ids, list) else 0
                    node_info["event_count"] = ev_count
                    total_state_size += size
            except Exception:
                pass

            try:
                async with session.get(f"{node_url}/state/merkle") as resp:
                    merkle_data = await resp.json()
                    node_info["merkle_root"] = merkle_data.get("merkle_root", "")[:12]
                    node_info["version"] = merkle_data.get("version", 0)
            except Exception:
                pass

            per_node_stats.append(node_info)

    # Compute data loss metric
    # Total events submitted = sum of event_counts across nodes (each node counts its own events)
    # Since CRDTs merge, after convergence all nodes should have the same total
    event_counts = [n.get("event_count", 0) for n in per_node_stats if "event_count" in n]
    max_events = max(event_counts) if event_counts else 0
    min_events = min(event_counts) if event_counts else 0
    data_loss_percent = 0.0
    if max_events > 0 and len(event_counts) > 1:
        data_loss_percent = round((1 - min_events / max_events) * 100, 2) if not runtime.get("is_divergent") else None

    return {
        "convergence_time_seconds": runtime.get("last_convergence_seconds"),
        "is_divergent": gateway_data.get("is_divergent", False),
        "divergence_duration_seconds": runtime.get("divergence_duration_seconds", 0),
        "data_loss_percent": data_loss_percent,
        "total_gossip_messages_sent": total_gossip_sent,
        "total_gossip_messages_received": total_gossip_received,
        "total_state_size_bytes": total_state_size,
        "node_count": len(per_node_stats),
        "polls_completed": runtime.get("polls_completed", 0),
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


async def _eval_collect_metrics(session, nodes, gateway_url):
    """Collect all 5 report metrics from current state."""
    # Gateway runtime metrics
    gw_metrics = {}
    try:
        async with session.get(f"{gateway_url}/gateway/runtime-metrics") as resp:
            gw_data = await resp.json()
            gw_metrics = gw_data.get("runtime_metrics", {})
    except Exception:
        pass

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
        "convergence_time_s": gw_metrics.get("last_convergence_seconds"),
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
            await _sync_gateways_with_nodes()
            await asyncio.sleep(3)  # Let gossip settle

            nodes = await asyncio.to_thread(docker_mgr.list_nodes)
            gateways = await _discover_gateways()
            gw_url = gateways[0]["url"]

            # ─── Scenario 1: Baseline Convergence ───
            _eval_state["current_step"] = f"[{node_count}N] Scenario 1: Baseline convergence"
            _eval_state["progress"] = step

            async with aiohttp.ClientSession(timeout=timeout) as session:
                tracking_ids = await _eval_send_events(session, nodes, cfg.events_per_node)

                # Wait for gossip convergence
                for _ in range(int(cfg.convergence_wait_seconds / cfg.poll_interval_seconds)):
                    try:
                        async with session.post(f"{gw_url}/gateway/poll") as resp:
                            await resp.json()
                    except Exception:
                        pass
                    await asyncio.sleep(cfg.poll_interval_seconds)

                metrics = await _eval_collect_metrics(session, nodes, gw_url)
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
                try:
                    async with session.post(f"{gw_url}/gateway/poll") as resp:
                        await resp.json()
                except Exception:
                    pass

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
                    try:
                        async with session.post(f"{gw_url}/gateway/poll") as resp:
                            await resp.json()
                    except Exception:
                        pass
                    await asyncio.sleep(cfg.poll_interval_seconds)

                metrics = await _eval_collect_metrics(session, nodes, gw_url)
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
                    try:
                        async with session.post(f"{gw_url}/gateway/poll") as resp:
                            await resp.json()
                    except Exception:
                        pass
                    await asyncio.sleep(cfg.poll_interval_seconds)

                metrics = await _eval_collect_metrics(session, nodes, gw_url)
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
        await asyncio.gather(server.serve(), wait_shutdown())
    except asyncio.CancelledError:
        pass
    finally:
        log.info("simulator_stopped")


if __name__ == "__main__":
    asyncio.run(main())
