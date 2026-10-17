"""
Edge node entry point. Starts the HTTP intake, UDP gossip, and mDNS discovery services.
"""

from datetime import datetime

import asyncio
import logging
import signal
import time
import structlog
import uvicorn

from src.config import config
from src.crdt import NodeState
from src.hash_chain import HashChainLog
from src.services import IntakeService, GossipService, PeerDiscovery
from fastapi import Request

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
        sync_mode=config.sync_mode,
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

    @intake.app.post("/state/deltas")
    async def state_deltas(request: Request):
        data = await request.json()
        return state.get_deltas_for(
            data.get("vv") or {}, mode=data.get("mode", "true_delta")
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

    @intake.app.post("/evaluation/reset-metrics")
    async def evaluation_reset_metrics():
        gossip.reset_runtime()
        return {
            "status": "metrics_reset",
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

    try:
        await discovery.start()
        await asyncio.gather(server.serve(), gossip.start(), wait_shutdown())
    except asyncio.CancelledError:
        pass
    finally:
        await gossip.stop()
        await discovery.stop()
        log.info("node_stopped", node_id=config.node_id)


if __name__ == "__main__":
    asyncio.run(main())
