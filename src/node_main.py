"""
Edge node entry point. Starts the HTTP intake, UDP gossip, and mDNS discovery services.
"""

from datetime import datetime

import json
import asyncio
import logging
import signal
import time
import structlog
import aiohttp
import uvicorn

from src.config import config
from src.crdt import NodeState
from src.hash_chain import HashChainLog
from src.services import IntakeService, GossipService, PeerDiscovery
from fastapi import Request
from fastapi.responses import JSONResponse

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


async def main():
    state = NodeState(
        config.node_id,
        config.event_id_retention_limit,
        delta_log_limit=config.delta_log_limit,
    )
    chain = HashChainLog(config.node_id, f"{config.data_dir}/logs")
    intake = IntakeService(config, state, chain)

    # mDNS peer discovery
    discovery = PeerDiscovery(
        config.node_id,
        config.gossip_port,
        assigned_gateway=config.assigned_gateway,
        role="edge-node",
    )
    gossip = GossipService(config, state, peer_discovery=discovery)

    @intake.app.post("/config/gateway")
    async def config_gateway(request: Request):
        data = await request.json()
        gw = data.get("assigned_gateway", "")
        config.assigned_gateway = gw
        await discovery.update_gateway(gw)
        return {"status": "ok", "assigned_gateway": gw}

    @intake.app.get("/gossip/stats")
    async def gossip_stats():
        return {
            "node_id": config.node_id,
            "stats": gossip.get_stats(),
        }

    @intake.app.get("/state/deltas")
    async def state_deltas(since: int = 0):
        return state.get_deltas_since(since)

    @intake.app.post("/evaluation/central-submit")
    async def central_submit(request: Request):
        payload = await request.json()
        central_url = str(payload.get("central_url") or "").rstrip("/")
        event = payload.get("event")
        if not central_url or not isinstance(event, dict):
            return JSONResponse(
                status_code=422,
                content={"status": "rejected", "error": "invalid central submission"},
            )

        event_body = json.dumps(event, separators=(",", ":")).encode()
        timeout = aiohttp.ClientTimeout(total=5)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.post(
                    f"{central_url}/gateway/evaluation/central/event",
                    data=event_body,
                    headers={"Content-Type": "application/json"},
                ) as response:
                    response_body = await response.read()
                    try:
                        central_result = json.loads(response_body)
                    except Exception:
                        central_result = {"status": "invalid_response"}
                    return JSONResponse(
                        status_code=response.status,
                        content={
                            "status": central_result.get("status", "rejected"),
                            "event_id": central_result.get("event_id"),
                            "central_request_bytes": len(event_body),
                            "central_response_bytes": len(response_body),
                        },
                    )
        except Exception as error:
            return JSONResponse(
                status_code=503,
                content={"status": "unavailable", "error": str(error)},
            )

    @intake.app.get("/evaluation/central-health")
    async def central_health(central_url: str):
        timeout = aiohttp.ClientTimeout(total=2)
        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(
                    f"{central_url.rstrip('/')}/gateway/evaluation/central/health"
                ) as response:
                    return JSONResponse(
                        status_code=response.status,
                        content={"status": "ok" if response.status < 400 else "error"},
                    )
        except Exception as error:
            return JSONResponse(
                status_code=503,
                content={"status": "unavailable", "error": str(error)},
            )

    @intake.app.post("/evaluation/reset")
    async def evaluation_reset():
        state.reset()
        chain.reset()
        gossip.reset_runtime()
        intake.start_time = datetime.utcnow()
        return {
            "status": "reset",
            "node_id": config.node_id,
            "merkle_root": state.merkle_root(),
            "delta_seq": state.delta_seq,
        }

    @intake.app.get("/peers")
    async def discovered_peers():
        return {
            "node_id": config.node_id,
            "discovery": "mdns",
            "peers": discovery.get_peers(),
            "peer_count": len(discovery.get_peers()),
            "stats": discovery.stats,
        }

    @intake.app.get("/relay/observations")
    async def relay_observations(limit: int = 200):
        own = {
            "node_id": config.node_id,
            "merkle_root": state.merkle_root(),
            "state_version": state.version,
            "last_seen_at": time.time(),
            "reachable_via": "direct",
            "hops": 0,
        }
        observed = gossip.get_peer_observations(limit=max(limit - 1, 0))
        return {
            "source_node": config.node_id,
            "observations": [own, *observed],
            "observation_count": len(observed) + 1,
        }

    shutdown = asyncio.Event()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown.set)

    server = uvicorn.Server(
        uvicorn.Config(
            intake.app, host="0.0.0.0", port=config.http_port, log_level="warning"
        )
    )

    log.info(
        "node_starting",
        node_id=config.node_id,
        http=config.http_port,
        gossip=config.gossip_port,
        discovery="mdns",
    )

    async def wait_shutdown():
        await shutdown.wait()
        raise asyncio.CancelledError()

    gossip_enabled = config.sync_mode != "centralized"
    try:
        if gossip_enabled:
            await discovery.start()
            await asyncio.gather(server.serve(), gossip.start(), wait_shutdown())
        else:
            await asyncio.gather(server.serve(), wait_shutdown())
    except asyncio.CancelledError:
        pass
    finally:
        if gossip_enabled:
            await gossip.stop()
            await discovery.stop()
        log.info("node_stopped", node_id=config.node_id)


if __name__ == "__main__":
    asyncio.run(main())
