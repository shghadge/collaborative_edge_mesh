"""
Edge node entry point. Starts the HTTP intake, UDP gossip, and mDNS discovery services.
"""

import asyncio
import logging
import signal
import structlog
import uvicorn

from src.config import config
from src.crdt import NodeState
from src.hash_chain import HashChainLog
from src.services import IntakeService, GossipService, PeerDiscovery

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
    state = NodeState(config.node_id)
    chain = HashChainLog(config.node_id, f"{config.data_dir}/logs")
    intake = IntakeService(config, state, chain)

    # mDNS peer discovery
    discovery = PeerDiscovery(config.node_id, config.gossip_port)
    gossip = GossipService(config, state, peer_discovery=discovery)

    @intake.app.get("/gossip/stats")
    async def gossip_stats():
        return {
            "node_id": config.node_id,
            "stats": gossip.get_stats(),
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
