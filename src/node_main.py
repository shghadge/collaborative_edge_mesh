"""
Edge node entry point. Starts the HTTP intake and UDP gossip services.
"""
import asyncio
import logging
import signal
import structlog
import uvicorn

from src.config import config
from src.crdt import NodeState
from src.hash_chain import HashChainLog
from src.services import IntakeService, GossipService

logging.basicConfig(format="%(message)s", level=getattr(logging, config.log_level, logging.INFO))

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
    gossip = GossipService(config, state)

    shutdown = asyncio.Event()

    loop = asyncio.get_event_loop()
    for sig in (signal.SIGTERM, signal.SIGINT):
        loop.add_signal_handler(sig, shutdown.set)

    server = uvicorn.Server(uvicorn.Config(
        intake.app, host="0.0.0.0", port=config.http_port, log_level="warning"
    ))

    log.info("node_starting", node_id=config.node_id,
             http=config.http_port, gossip=config.gossip_port, peers=config.peers)

    async def wait_shutdown():
        await shutdown.wait()
        raise asyncio.CancelledError()

    try:
        await asyncio.gather(server.serve(), gossip.start(), wait_shutdown())
    except asyncio.CancelledError:
        pass
    finally:
        await gossip.stop()
        log.info("node_stopped", node_id=config.node_id)


if __name__ == "__main__":
    asyncio.run(main())
