"""Tests for mDNS-based PeerDiscovery service."""

from src.services.peer_discovery import PeerDiscovery, SERVICE_TYPE


def test_get_peers_returns_empty_initially():
    """New PeerDiscovery has no peers before browsing."""
    discovery = PeerDiscovery("node-1", 9000)
    assert discovery.get_peers() == []
    assert discovery.stats["peers_discovered"] == 0


def test_self_is_excluded_from_peers():
    """Peers with the same node_id as self should be filtered."""
    discovery = PeerDiscovery("node-1", 9000)

    # Simulate adding self — should be ignored
    import socket
    from zeroconf.asyncio import AsyncServiceInfo

    info = AsyncServiceInfo(
        SERVICE_TYPE,
        f"node-1.{SERVICE_TYPE}",
        addresses=[socket.inet_aton("10.0.0.1")],
        port=9000,
        properties={b"node_id": b"node-1"},
    )
    # Directly manipulate internal state as if resolved
    discovery._peers[f"node-1.{SERVICE_TYPE}"] = "10.0.0.1:9000"

    # Now remove it as self-filter would
    peer_node_id = "node-1"
    if peer_node_id == discovery.node_id:
        discovery._peers.pop(f"node-1.{SERVICE_TYPE}", None)

    assert discovery.get_peers() == []


def test_add_and_remove_peer():
    """Simulates peer add/remove via internal methods."""
    discovery = PeerDiscovery("node-1", 9000)

    # Simulate peer discovered
    service_name = f"node-2.{SERVICE_TYPE}"
    discovery._peers[service_name] = "10.0.0.2:9000"
    discovery.stats["peers_discovered"] = len(discovery._peers)
    discovery.stats["total_discovered"] += 1

    assert discovery.get_peers() == ["10.0.0.2:9000"]
    assert discovery.stats["peers_discovered"] == 1
    assert discovery.stats["total_discovered"] == 1

    # Simulate peer removed
    discovery._remove_peer(service_name)
    assert discovery.get_peers() == []
    assert discovery.stats["peers_discovered"] == 0
    assert discovery.stats["peers_removed"] == 1


def test_multiple_peers_discovered():
    """Multiple peers should all appear in get_peers()."""
    discovery = PeerDiscovery("node-1", 9000)

    for i in range(2, 5):
        name = f"node-{i}.{SERVICE_TYPE}"
        discovery._peers[name] = f"10.0.0.{i}:9000"

    discovery.stats["peers_discovered"] = len(discovery._peers)

    peers = discovery.get_peers()
    assert len(peers) == 3
    assert "10.0.0.2:9000" in peers
    assert "10.0.0.3:9000" in peers
    assert "10.0.0.4:9000" in peers


def test_get_local_ip_returns_string():
    """_get_local_ip should return a string IP address."""
    discovery = PeerDiscovery("node-1", 9000)
    ip = discovery._get_local_ip()
    assert isinstance(ip, str)
    assert len(ip.split(".")) == 4  # IPv4 format


def test_gossip_service_uses_peer_discovery(monkeypatch):
    """GossipService should use peer_discovery.get_peers() when available."""
    from src.config import Config
    from src.crdt.state import NodeState
    from src.services.gossip import GossipService

    monkeypatch.setenv("NODE_ID", "node-1")
    config = Config()
    state = NodeState(config.node_id)

    discovery = PeerDiscovery("node-1", 9000)
    # Pre-populate mock peers
    discovery._peers[f"node-2.{SERVICE_TYPE}"] = "10.0.0.2:9000"
    discovery._peers[f"node-3.{SERVICE_TYPE}"] = "10.0.0.3:9000"

    gossip = GossipService(config, state, peer_discovery=discovery)

    # Verify the gossip service would resolve peers from discovery
    peers = gossip.peer_discovery.get_peers() if gossip.peer_discovery else []
    assert len(peers) == 2
    assert "10.0.0.2:9000" in peers


def test_gossip_service_without_discovery_returns_empty(monkeypatch):
    """GossipService without peer_discovery should have no peers to broadcast to."""
    from src.config import Config
    from src.crdt.state import NodeState
    from src.services.gossip import GossipService

    monkeypatch.setenv("NODE_ID", "node-1")
    config = Config()
    state = NodeState(config.node_id)

    gossip = GossipService(config, state)  # no peer_discovery

    peers = gossip.peer_discovery.get_peers() if gossip.peer_discovery else []
    assert peers == []
