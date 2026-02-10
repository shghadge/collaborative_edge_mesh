import asyncio
import json
import socket
import structlog

from ..config import Config
from ..crdt import NodeState

log = structlog.get_logger()

MAX_PACKET = 60000


class GossipService:
    """
    UDP gossip protocol. Every few seconds, broadcast our state to all peers.
    Listen for incoming state and merge it.
    """

    def __init__(self, config: Config, state: NodeState):
        self.config = config
        self.state = state
        self.running = False
        self.sock = None
        self.stats = {"sent": 0, "received": 0, "merged": 0, "errors": 0}

    async def start(self):
        self.running = True
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("0.0.0.0", self.config.gossip_port))
        self.sock.setblocking(False)

        log.info("gossip_started", port=self.config.gossip_port, peers=self.config.peers)

        await asyncio.gather(
            self._broadcast_loop(),
            self._receive_loop(),
        )

    async def stop(self):
        self.running = False
        if self.sock:
            self.sock.close()

    async def _broadcast_loop(self):
        """Send our state to all peers periodically."""
        while self.running:
            await asyncio.sleep(self.config.gossip_interval)
            try:
                msg = json.dumps({
                    "type": "state_update",
                    "sender": self.config.node_id,
                    "state": self.state.to_dict(),
                }).encode()

                if len(msg) > MAX_PACKET:
                    # if state is too big, just send merkle root
                    msg = json.dumps({
                        "type": "merkle_only",
                        "sender": self.config.node_id,
                        "merkle_root": self.state.merkle_root(),
                    }).encode()

                loop = asyncio.get_event_loop()
                for peer in self.config.peers:
                    host, port = self._parse_peer(peer)
                    try:
                        await loop.run_in_executor(None, self.sock.sendto, msg, (host, int(port)))
                        self.stats["sent"] += 1
                    except Exception as e:
                        self.stats["errors"] += 1
                        log.debug("gossip_send_failed", peer=peer, error=str(e))

            except Exception as e:
                log.error("broadcast_error", error=str(e))

    async def _receive_loop(self):
        """Listen for incoming gossip messages and merge them."""
        loop = asyncio.get_event_loop()
        while self.running:
            try:
                data, addr = await loop.run_in_executor(None, self._recv)
                if data is None:
                    continue
                self.stats["received"] += 1
                message = json.loads(data.decode())
                self._handle(message)
            except Exception as e:
                if self.running:
                    log.debug("receive_error", error=str(e))

    def _recv(self):
        """Blocking recv with timeout so we can check self.running."""
        self.sock.settimeout(1.0)
        try:
            return self.sock.recvfrom(MAX_PACKET)
        except socket.timeout:
            return None, None

    def _handle(self, message):
        sender = message.get("sender", "unknown")

        if sender == self.config.node_id:
            return  # ignore our own messages

        if message["type"] == "state_update":
            incoming = NodeState.from_dict(message["state"])
            old_root = self.state.merkle_root()
            self.state.merge(incoming)
            new_root = self.state.merkle_root()

            if old_root != new_root:
                self.stats["merged"] += 1
                log.info("gossip_merged", from_node=sender,
                         old_root=old_root[:12], new_root=new_root[:12])

        elif message["type"] == "merkle_only":
            remote_root = message.get("merkle_root")
            if remote_root != self.state.merkle_root():
                log.info("merkle_mismatch", from_node=sender,
                         ours=self.state.merkle_root()[:12], theirs=remote_root[:12])

    def _parse_peer(self, peer):
        parts = peer.rsplit(":", 1)
        return parts[0], int(parts[1]) if len(parts) > 1 else 9000

    def get_stats(self):
        return dict(self.stats)
