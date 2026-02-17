from src.config import Config
from src.crdt.state import NodeState
from src.services.gossip import GossipService


def _build_gossip_service(monkeypatch, node_id="node-1"):
    monkeypatch.setenv("NODE_ID", node_id)
    monkeypatch.setenv("PEER_NODES", "")
    config = Config()
    state = NodeState(config.node_id)
    return GossipService(config, state)


def test_state_sync_message_merges_remote_state(monkeypatch):
    service = _build_gossip_service(monkeypatch, node_id="node-1")

    remote = NodeState("node-2")
    remote.record_event(
        "evt-1",
        "water_level",
        {"value": 3.2, "location": "bridge_north"},
        category="sensor",
    )

    message = {
        "type": "state_sync",
        "sender": "node-2",
        "reason": "unit_test",
        "state": remote.to_dict(),
    }

    service._handle(message)

    assert service.stats["merged"] == 1
    assert service.stats["last_merge_ms"] >= 0
    assert service.stats["merge_time_ms_total"] >= service.stats["last_merge_ms"]
    assert service.stats["last_successful_merge_at"] is not None
    assert "sensor:bridge_north:water_level" in service.state.registers

    full_stats = service.get_stats()
    assert full_stats["avg_merge_ms"] >= 0


def test_own_state_sync_message_is_ignored(monkeypatch):
    service = _build_gossip_service(monkeypatch, node_id="node-1")

    remote = NodeState("node-1")
    remote.record_event(
        "evt-1",
        "water_level",
        {"value": 3.2, "location": "bridge_north"},
        category="sensor",
    )
    message = {
        "type": "state_sync",
        "sender": "node-1",
        "reason": "self_message",
        "state": remote.to_dict(),
    }

    service._handle(message)

    assert service.stats["merged"] == 0
    assert service.state.get_event_count() == 0


def test_merkle_only_message_does_not_mutate_state(monkeypatch):
    service = _build_gossip_service(monkeypatch, node_id="node-1")
    before_root = service.state.merkle_root()

    message = {
        "type": "merkle_only",
        "sender": "node-2",
        "reason": "state_too_large_for_udp",
        "merkle_root": "different_remote_root",
        "event_count": 10,
    }

    service._handle(message)

    assert service.stats["merged"] == 0
    assert service.stats["merkle_mismatches"] == 1
    assert service.state.merkle_root() == before_root
