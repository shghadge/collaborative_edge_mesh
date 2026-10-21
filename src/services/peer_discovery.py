"""
mDNS-based peer discovery using Zeroconf.

Each edge node registers itself as a service on the local network
and discovers other edge nodes automatically. This replaces the
static PEER_NODES environment variable.

Service type: _edge-mesh._udp.local.
"""

import asyncio
import hashlib
import socket
import structlog

from zeroconf import IPVersion, ServiceStateChange
from zeroconf.asyncio import AsyncServiceBrowser, AsyncServiceInfo, AsyncZeroconf

log = structlog.get_logger()

SERVICE_TYPE = "_edge-mesh._udp.local."


class PeerDiscovery:
    """
    Discovers edge-mesh peers via mDNS (Zeroconf).

    On start, registers this node as a service and browses for others.
    get_peers() returns the current set of discovered peers as ["ip:port", ...].
    """

    def __init__(
        self,
        node_id: str,
        gossip_port: int,
        assigned_gateway: str = "",
        role: str = "edge-node",
    ):
        self.node_id = node_id
        self.gossip_port = gossip_port
        self.assigned_gateway = assigned_gateway
        self.role = role
        self._peers: dict[str, str] = {}  # {service_name: "ip:port"}
        self._zeroconf: AsyncZeroconf | None = None
        self._browser: AsyncServiceBrowser | None = None
        self._service_info: AsyncServiceInfo | None = None
        self.stats = {
            "peers_discovered": 0,
            "peers_removed": 0,
            "total_discovered": 0,
        }
        self._update_service_type()

    def _update_service_type(self):
        """Set the mDNS service type, scoped by a hash of the assigned gateway."""
        if not self.assigned_gateway:
            self.service_type = "_em._udp.local."
            return

        # Use a stable hash to avoid collisions from truncated Docker container names.
        gateway_scope = hashlib.sha1(self.assigned_gateway.encode("utf-8")).hexdigest()[
            :10
        ]
        self.service_type = f"_em-{gateway_scope}._udp.local."

    async def start(self):
        """Register this node and start browsing for peers."""
        self._zeroconf = AsyncZeroconf(ip_version=IPVersion.V4Only)

        # Determine our IP address on the network
        local_ip = self._get_local_ip()

        service_name = f"{self.node_id}.{self.service_type}"
        self._service_info = AsyncServiceInfo(
            self.service_type,
            service_name,
            addresses=[socket.inet_aton(local_ip)],
            port=self.gossip_port,
            properties={
                b"node_id": self.node_id.encode(),
                b"role": self.role.encode(),
            },
        )

        await self._zeroconf.async_register_service(self._service_info)
        log.info(
            "mdns_registered",
            node_id=self.node_id,
            service=service_name,
            ip=local_ip,
            port=self.gossip_port,
        )

        # Browse for other edge-mesh services
        self._browser = AsyncServiceBrowser(
            self._zeroconf.zeroconf,
            self.service_type,
            handlers=[self._on_service_state_change],
        )

        log.info("mdns_browsing_started", service_type=self.service_type)

    async def stop(self):
        """Unregister and close."""
        if self._browser:
            await self._browser.async_cancel()
            self._browser = None

        if self._service_info and self._zeroconf:
            await self._zeroconf.async_unregister_service(self._service_info)

        if self._zeroconf:
            await self._zeroconf.async_close()
            self._zeroconf = None

        log.info("mdns_stopped", node_id=self.node_id)

    async def update_gateway(self, assigned_gateway: str):
        """Update gateway and restart discovery under the new semantic network."""
        if self.assigned_gateway == assigned_gateway:
            return
        self.assigned_gateway = assigned_gateway
        self._update_service_type()
        log.info(
            "mdns_updating_gateway",
            new_gateway=assigned_gateway,
            service_type=self.service_type,
        )

        await self.stop()
        self._peers.clear()
        self.stats["peers_discovered"] = 0
        await self.start()

    def get_peers(self) -> list[str]:
        """Return currently discovered peers as ["ip:port", ...], excluding self."""
        return list(self._peers.values())

    def _on_service_state_change(
        self,
        zeroconf,
        service_type: str,
        name: str,
        state_change: ServiceStateChange,
    ):
        """Callback for service browser — schedules async resolution."""
        if state_change in (ServiceStateChange.Added, ServiceStateChange.Updated):
            asyncio.ensure_future(self._resolve_and_add(zeroconf, service_type, name))
        elif state_change == ServiceStateChange.Removed:
            self._remove_peer(name)

    async def _resolve_and_add(self, zeroconf, service_type: str, name: str):
        """Resolve service info and add to peer set."""
        info = AsyncServiceInfo(service_type, name)
        await info.async_request(zeroconf, 3000)

        if not info.addresses:
            log.debug("mdns_resolve_failed", service=name)
            return

        # Extract node_id from properties to filter self
        props = info.properties or {}
        peer_node_id = props.get(b"node_id", b"").decode()
        peer_role = props.get(b"role", b"").decode()

        if peer_node_id == self.node_id:
            return  # skip self

        # Only include peers from the same role/class (edge nodes discover edge nodes).
        if peer_role and peer_role != self.role:
            return

        ip = socket.inet_ntoa(info.addresses[0])
        port = info.port
        peer_addr = f"{ip}:{port}"

        was_new = name not in self._peers
        self._peers[name] = peer_addr

        if was_new:
            self.stats["peers_discovered"] = len(self._peers)
            self.stats["total_discovered"] += 1
            log.info(
                "peer_discovered",
                peer_node_id=peer_node_id,
                peer_addr=peer_addr,
                total_peers=len(self._peers),
            )

    def _remove_peer(self, name: str):
        """Remove a peer from the discovered set."""
        if name in self._peers:
            removed_addr = self._peers.pop(name)
            self.stats["peers_discovered"] = len(self._peers)
            self.stats["peers_removed"] += 1
            log.info(
                "peer_removed",
                service=name,
                peer_addr=removed_addr,
                total_peers=len(self._peers),
            )

    def _get_local_ip(self) -> str:
        """Get the local IP address used for outbound connections."""
        try:
            # Connect to a non-routable address to discover our interface IP
            with socket.socket(socket.AF_INET, socket.SOCK_DGRAM) as s:
                s.connect(("10.255.255.255", 1))
                return s.getsockname()[0]
        except Exception:
            return "127.0.0.1"
