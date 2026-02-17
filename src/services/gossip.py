import asyncio
import json
import socket
import structlog
import time

from ..config import Config
from ..crdt import NodeState

log = structlog.get_logger()

MAX_PACKET = 60000


class GossipService:
    """
    UDP gossip protocol. Every few seconds, broadcast our state to all peers.
    Listen for incoming state and merge it.

    Messages include semantic context:
      - state_sync:  full state with summary of what's inside
      - merkle_only: compact fingerprint with event count for quick comparison
    """

    def __init__(self, config: Config, state: NodeState):
        self.config = config
        self.state = state
        self.running = False
        self.sock = None
        self.stats = {
            "sent": 0,
            "received": 0,
            "merged": 0,
            "errors": 0,
            "sent_bytes": 0,
            "received_bytes": 0,
            "broadcast_cycles": 0,
            "state_sync_sent": 0,
            "merkle_only_sent": 0,
            "merkle_mismatches": 0,
            "merge_time_ms_total": 0.0,
            "last_merge_ms": 0.0,
            "last_message_type": None,
            "last_message_at": None,
            "last_successful_merge_at": None,
        }

    async def start(self):
        self.running = True
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("0.0.0.0", self.config.gossip_port))
        self.sock.setblocking(False)

        log.info(
            "gossip_started", port=self.config.gossip_port, peers=self.config.peers
        )

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
                self.stats["broadcast_cycles"] += 1
                state_summary = self.state.summary()
                message_type = "state_sync"
                msg = json.dumps(
                    {
                        "type": "state_sync",
                        "reason": "periodic_sync",
                        "sender": self.config.node_id,
                        "state": self.state.to_dict(),
                        "state_summary": state_summary,
                    }
                ).encode()

                if len(msg) > MAX_PACKET:
                    # if state is too big, send a compact digest
                    message_type = "merkle_only"
                    msg = json.dumps(
                        {
                            "type": "merkle_only",
                            "reason": "state_too_large_for_udp",
                            "sender": self.config.node_id,
                            "merkle_root": self.state.merkle_root(),
                            "event_count": self.state.get_event_count(),
                            "state_summary": state_summary,
                        }
                    ).encode()

                loop = asyncio.get_event_loop()
                for peer in self.config.peers:
                    host, port = self._parse_peer(peer)
                    try:
                        await loop.run_in_executor(
                            None, self.sock.sendto, msg, (host, int(port))
                        )
                        self.stats["sent"] += 1
                        self.stats["sent_bytes"] += len(msg)
                        if message_type == "state_sync":
                            self.stats["state_sync_sent"] += 1
                        else:
                            self.stats["merkle_only_sent"] += 1
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
                self.stats["received_bytes"] += len(data)
                message = json.loads(data.decode())
                self.stats["last_message_type"] = message.get("type")
                self.stats["last_message_at"] = time.time()
                self._handle(message)
            except Exception as e:
                if self.running:
                    self.stats["errors"] += 1
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

        if message["type"] == "state_sync":
            incoming = NodeState.from_dict(message["state"])
            old_root = self.state.merkle_root()
            started = time.time()
            self.state.merge(incoming)
            new_root = self.state.merkle_root()
            elapsed_ms = (time.time() - started) * 1000
            self.stats["last_merge_ms"] = round(elapsed_ms, 3)
            self.stats["merge_time_ms_total"] += elapsed_ms

            if old_root != new_root:
                self.stats["merged"] += 1
                self.stats["last_successful_merge_at"] = time.time()
                log.info(
                    "gossip_merged",
                    from_node=sender,
                    reason=message.get("reason", "unknown"),
                    old_root=old_root[:12],
                    new_root=new_root[:12],
                )

        elif message["type"] == "merkle_only":
            remote_root = message.get("merkle_root")
            if remote_root != self.state.merkle_root():
                self.stats["merkle_mismatches"] += 1
                log.info(
                    "merkle_mismatch",
                    from_node=sender,
                    reason=message.get("reason", "unknown"),
                    ours=self.state.merkle_root()[:12],
                    theirs=remote_root[:12],
                    their_event_count=message.get("event_count"),
                )

    def _parse_peer(self, peer):
        parts = peer.rsplit(":", 1)
        return parts[0], int(parts[1]) if len(parts) > 1 else 9000

    def get_stats(self):
        stats = dict(self.stats)
        merged = stats.get("merged", 0)
        total_merge_ms = stats.get("merge_time_ms_total", 0.0)
        stats["avg_merge_ms"] = round(total_merge_ms / merged, 3) if merged else 0.0
        return stats
