import asyncio
import json
import socket
import structlog
import time
from typing import TYPE_CHECKING

import aiohttp

from ..config import Config
from ..crdt import NodeState

log = structlog.get_logger()

if TYPE_CHECKING:
    from .peer_discovery import PeerDiscovery

MAX_PACKET = 60000
MAX_GOSSIP_TARGETS = 3
MAX_RELAY_OBSERVATIONS = 24
REPAIR_PULL_COOLDOWN_SECONDS = 8.0


class GossipService:
    """
    UDP gossip protocol. Periodic messages are summary-first and compact.
    When a digest mismatch is detected, pull full state over HTTP on demand.
    """

    def __init__(
        self,
        config: Config,
        state: NodeState,
        peer_discovery: "PeerDiscovery | None" = None,
    ):
        self.config = config
        self.state = state
        self.peer_discovery = peer_discovery
        self.running = False
        self.sock = None
        self.peer_observations = {}
        self._last_repair_pull_at = {}
        self.stats = {
            "sent": 0,
            "received": 0,
            "merged": 0,
            "errors": 0,
            "sent_bytes": 0,
            "received_bytes": 0,
            "broadcast_cycles": 0,
            "summary_sent": 0,
            "summary_received": 0,
            "state_sync_sent": 0,
            "merkle_only_sent": 0,
            "state_sync_received": 0,
            "merkle_only_received": 0,
            "repair_pulls_started": 0,
            "repair_pulls_success": 0,
            "repair_pulls_failed": 0,
            "merkle_mismatches": 0,
            "merge_time_ms_total": 0.0,
            "last_merge_ms": 0.0,
            "last_message_type": None,
            "last_message_at": None,
            "last_successful_merge_at": None,
            "peers_discovered": 0,
            "repair_bytes_received": 0,
        }

    async def start(self):
        self.running = True
        self.sock = socket.socket(socket.AF_INET, socket.SOCK_DGRAM)
        self.sock.setsockopt(socket.SOL_SOCKET, socket.SO_REUSEADDR, 1)
        self.sock.bind(("0.0.0.0", self.config.gossip_port))
        self.sock.setblocking(False)

        log.info(
            "gossip_started",
            port=self.config.gossip_port,
            discovery="mdns" if self.peer_discovery else "none",
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
        """Send compact summaries to a random subset of peers periodically."""
        import random

        while self.running:
            await asyncio.sleep(self.config.gossip_interval)
            try:
                self.stats["broadcast_cycles"] += 1
                msg = self._build_summary_message()

                loop = asyncio.get_event_loop()
                peers = self.peer_discovery.get_peers() if self.peer_discovery else []
                self.stats["peers_discovered"] = len(peers)

                # Fan-out to a random subset of peers (standard gossip protocol)
                if len(peers) > MAX_GOSSIP_TARGETS:
                    targets = random.sample(peers, MAX_GOSSIP_TARGETS)
                else:
                    targets = peers

                for peer in targets:
                    host, port = self._parse_peer(peer)
                    try:
                        await loop.run_in_executor(
                            None, self.sock.sendto, msg, (host, int(port))
                        )
                        self.stats["sent"] += 1
                        self.stats["sent_bytes"] += len(msg)
                        self.stats["summary_sent"] += 1
                    except Exception as e:
                        self.stats["errors"] += 1
                        log.debug("gossip_send_failed", peer=peer, error=str(e))

            except Exception as e:
                log.error("broadcast_error", error=str(e))

    async def _receive_loop(self):
        """Listen for incoming gossip messages and trigger repair when needed."""
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
                await self._handle(message, addr)
            except Exception as e:
                if self.running:
                    self.stats["errors"] += 1
                    log.debug("receive_error", error=str(e))

    def _build_summary_message(self) -> bytes:
        observed_peers = self.get_peer_observations(limit=MAX_RELAY_OBSERVATIONS)
        payload = {
            "type": "state_summary",
            "reason": "periodic_summary_sync",
            "sender": self.config.node_id,
            "source_http_port": self.config.http_port,
            "merkle_root": self.state.merkle_root(),
            "state_version": self.state.version,
            "event_count": self.state.get_event_count(),
            "state_summary": self.state.summary(),
            "peer_observations": observed_peers,
        }
        message = json.dumps(payload).encode()

        if len(message) > MAX_PACKET:
            payload["peer_observations"] = []
            message = json.dumps(payload).encode()
        return message

    def _record_peer_observation(
        self,
        sender: str,
        merkle_root: str,
        state_version: int | None,
        hop_count: int = 1,
    ):
        if not sender or sender == self.config.node_id:
            return
        self.peer_observations[sender] = {
            "node_id": sender,
            "merkle_root": merkle_root or "",
            "state_version": state_version,
            "last_seen_at": time.time(),
            "reachable_via": "relayed" if hop_count > 1 else "direct",
            "hops": hop_count,
        }

    def _recv(self):
        """Blocking recv with timeout so we can check self.running."""
        self.sock.settimeout(1.0)
        try:
            return self.sock.recvfrom(MAX_PACKET)
        except socket.timeout:
            return None, None

    async def _handle(self, message, addr):
        sender = message.get("sender", "unknown")

        if sender == self.config.node_id:
            return  # ignore our own messages

        mtype = message.get("type")

        if mtype == "state_summary":
            self.stats["summary_received"] += 1
            remote_root = message.get("merkle_root")
            remote_version = message.get("state_version")
            self._record_peer_observation(
                sender=sender,
                merkle_root=remote_root,
                state_version=remote_version,
                hop_count=1,
            )

            for observed in message.get("peer_observations", []):
                if not isinstance(observed, dict):
                    continue
                observed_node = observed.get("node_id")
                if not observed_node or observed_node == self.config.node_id:
                    continue
                self.peer_observations[observed_node] = {
                    "node_id": observed_node,
                    "merkle_root": observed.get("merkle_root", ""),
                    "state_version": observed.get("state_version"),
                    "last_seen_at": observed.get("last_seen_at", time.time()),
                    "reachable_via": "relayed",
                    "hops": int(observed.get("hops", 1) or 1) + 1,
                }

            if remote_root != self.state.merkle_root():
                self.stats["merkle_mismatches"] += 1
                await self._maybe_pull_state_from_sender(addr, message)

            return

        if mtype == "state_sync":
            self.stats["state_sync_received"] += 1
            incoming = NodeState.from_dict(message["state"])
            self._record_peer_observation(
                sender=sender,
                merkle_root=incoming.merkle_root(),
                state_version=getattr(incoming, "version", None),
                hop_count=1,
            )
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

        elif mtype == "merkle_only":
            self.stats["merkle_only_received"] += 1
            remote_root = message.get("merkle_root")
            self._record_peer_observation(
                sender=sender,
                merkle_root=remote_root,
                state_version=message.get("state_version"),
                hop_count=1,
            )
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
                await self._maybe_pull_state_from_sender(addr, message)

    async def _maybe_pull_state_from_sender(self, addr, message):
        host = addr[0] if addr else None
        if not host:
            self.stats["repair_pulls_failed"] += 1
            return

        now = time.time()
        last_pull = self._last_repair_pull_at.get(host, 0.0)
        if (now - last_pull) < REPAIR_PULL_COOLDOWN_SECONDS:
            return

        self._last_repair_pull_at[host] = now
        self.stats["repair_pulls_started"] += 1

        http_port = message.get("source_http_port")
        try:
            http_port = int(http_port) if http_port is not None else 8000
        except Exception:
            http_port = 8000

        url = f"http://{host}:{http_port}/state"
        timeout = aiohttp.ClientTimeout(total=4)

        try:
            async with aiohttp.ClientSession(timeout=timeout) as session:
                async with session.get(url) as response:
                    text = await response.text()
                    self.stats["repair_bytes_received"] += len(text.encode())
                    data = json.loads(text)
            incoming = NodeState.from_dict(data)
            old_root = self.state.merkle_root()
            started = time.time()
            self.state.merge(incoming)
            elapsed_ms = (time.time() - started) * 1000
            self.stats["last_merge_ms"] = round(elapsed_ms, 3)
            self.stats["merge_time_ms_total"] += elapsed_ms
            if self.state.merkle_root() != old_root:
                self.stats["merged"] += 1
                self.stats["last_successful_merge_at"] = time.time()
            self.stats["repair_pulls_success"] += 1
            self._record_peer_observation(
                sender=message.get("sender", host),
                merkle_root=incoming.merkle_root(),
                state_version=getattr(incoming, "version", None),
                hop_count=1,
            )
        except Exception as error:
            self.stats["repair_pulls_failed"] += 1
            log.debug("repair_pull_failed", peer=host, error=str(error))

    def _parse_peer(self, peer):
        parts = peer.rsplit(":", 1)
        return parts[0], int(parts[1]) if len(parts) > 1 else 9000

    def get_stats(self):
        stats = dict(self.stats)
        merged = stats.get("merged", 0)
        total_merge_ms = stats.get("merge_time_ms_total", 0.0)
        stats["avg_merge_ms"] = round(total_merge_ms / merged, 3) if merged else 0.0
        return stats

    def get_peer_observations(self, limit: int = 200):
        observations = sorted(
            self.peer_observations.values(),
            key=lambda item: item.get("last_seen_at", 0),
            reverse=True,
        )
        return observations[: max(limit, 0)]
