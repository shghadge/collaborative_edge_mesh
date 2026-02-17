import asyncio

from src.config import Config
from src.crdt.state import NodeState
from src.services import gateway as gateway_module
from src.services.gateway import GatewayService


class FakeStore:
    def __init__(self):
        self.divergence_logs = []
        self.snapshots = []
        self.metrics = []

    def log_divergence(self, is_divergent, merkle_roots):
        self.divergence_logs.append((is_divergent, merkle_roots))

    def save_snapshot(self, merkle_root, node_count, source_nodes, state_dict):
        self.snapshots.append(
            {
                "merkle_root": merkle_root,
                "node_count": node_count,
                "source_nodes": source_nodes,
                "state": state_dict,
            }
        )

    def save_metric(self, name, value, metadata=None):
        self.metrics.append({"name": name, "value": value, "metadata": metadata or {}})


class FakeResponse:
    def __init__(self, payload):
        self._payload = payload

    async def json(self):
        return self._payload


class FakeResponseContext:
    def __init__(self, payload=None, error=None):
        self.payload = payload
        self.error = error

    async def __aenter__(self):
        if self.error:
            raise self.error
        return FakeResponse(self.payload)

    async def __aexit__(self, exc_type, exc, tb):
        return False


class FakeClientSession:
    def __init__(self, responses, timeout=None):
        self.responses = responses
        self.timeout = timeout
        self.get_calls = {}

    async def __aenter__(self):
        return self

    async def __aexit__(self, exc_type, exc, tb):
        return False

    def get(self, url):
        self.get_calls[url] = self.get_calls.get(url, 0) + 1
        response = self.responses[url]
        if isinstance(response, list):
            if not response:
                raise RuntimeError(f"no more fake responses for {url}")
            response = response.pop(0)
        if isinstance(response, Exception):
            return FakeResponseContext(error=response)
        return FakeResponseContext(payload=response)


def _state_with_event(node_id, event_id, event_type, value, location, category):
    state = NodeState(node_id)
    state.record_event(
        event_id,
        event_type,
        {"value": value, "location": location},
        category=category,
    )
    return state


def test_register_and_unregister_node(monkeypatch):
    monkeypatch.setenv("NODE_ID", "gateway-1")
    monkeypatch.setenv("EDGE_NODES", "")
    service = GatewayService(Config(), FakeStore())

    service.register_node("node-x", "http://node-x:8001")
    assert "node-x" in service.edge_nodes
    assert service.edge_nodes["node-x"]["url"] == "http://node-x:8001"

    service.unregister_node("node-x")
    assert "node-x" not in service.edge_nodes


def test_poll_once_detects_divergence_and_saves_snapshot(monkeypatch):
    monkeypatch.setenv("NODE_ID", "gateway-1")
    monkeypatch.setenv("EDGE_NODES", "node-a:8001,node-b:8002")

    state_a = _state_with_event("node-a", "evt-a", "water_level", 3.2, "bridge_north", "sensor")
    state_b = _state_with_event(
        "node-b",
        "evt-b",
        "shelter_occupancy",
        8,
        "shelter_east",
        "resource",
    )

    responses = {
        "http://node-a:8001/state/merkle": {"merkle_root": state_a.merkle_root()},
        "http://node-b:8002/state/merkle": {"merkle_root": state_b.merkle_root()},
        "http://node-a:8001/state": state_a.to_dict(),
        "http://node-b:8002/state": state_b.to_dict(),
    }

    monkeypatch.setattr(
        gateway_module.aiohttp,
        "ClientSession",
        lambda timeout=None: FakeClientSession(responses, timeout=timeout),
    )

    store = FakeStore()
    service = GatewayService(Config(), store)
    asyncio.run(service.poll_once())

    assert service.is_divergent is True
    assert service.poll_count == 1
    assert service.merged_state is not None
    assert "sensor:bridge_north:water_level" in service.merged_state.registers
    assert "resource:shelter_east:shelter_occupancy" in service.merged_state.pn_counters
    assert store.divergence_logs[-1][0] is True
    assert len(store.snapshots) == 1
    assert store.snapshots[0]["node_count"] == 2
    assert any(m["name"] == "is_divergent" and m["value"] == 1 for m in store.metrics)


def test_poll_once_handles_unreachable_node(monkeypatch):
    monkeypatch.setenv("NODE_ID", "gateway-1")
    monkeypatch.setenv("EDGE_NODES", "node-a:8001,node-b:8002")

    state_a = _state_with_event("node-a", "evt-a", "water_level", 3.2, "bridge_north", "sensor")

    responses = {
        "http://node-a:8001/state/merkle": {"merkle_root": state_a.merkle_root()},
        "http://node-b:8002/state/merkle": RuntimeError("node unavailable"),
        "http://node-a:8001/state": state_a.to_dict(),
    }

    monkeypatch.setattr(
        gateway_module.aiohttp,
        "ClientSession",
        lambda timeout=None: FakeClientSession(responses, timeout=timeout),
    )

    store = FakeStore()
    service = GatewayService(Config(), store)
    asyncio.run(service.poll_once())

    assert service.is_divergent is False
    assert len(store.snapshots) == 1
    assert store.snapshots[0]["node_count"] == 1
    assert store.divergence_logs[-1][1]["node-b"] == "unreachable"


def test_poll_once_retries_http_then_succeeds(monkeypatch):
    monkeypatch.setenv("NODE_ID", "gateway-1")
    monkeypatch.setenv("EDGE_NODES", "node-a:8001")
    monkeypatch.setenv("GATEWAY_HTTP_RETRIES", "2")
    monkeypatch.setenv("GATEWAY_HTTP_RETRY_BACKOFF_MS", "0")

    state_a = _state_with_event("node-a", "evt-a", "water_level", 3.2, "bridge_north", "sensor")

    responses = {
        "http://node-a:8001/state/merkle": [RuntimeError("temporary"), {"merkle_root": state_a.merkle_root()}],
        "http://node-a:8001/state": state_a.to_dict(),
    }

    fake_session = FakeClientSession(responses)
    monkeypatch.setattr(
        gateway_module.aiohttp,
        "ClientSession",
        lambda timeout=None: fake_session,
    )

    store = FakeStore()
    service = GatewayService(Config(), store)
    asyncio.run(service.poll_once())

    assert service.runtime_metrics["http_retries"] == 1
    assert service.runtime_metrics["total_http_success"] >= 2
    assert service.runtime_metrics["polls_completed"] == 1
    assert fake_session.get_calls["http://node-a:8001/state/merkle"] == 2


def test_poll_once_skips_stale_state(monkeypatch):
    monkeypatch.setenv("NODE_ID", "gateway-1")
    monkeypatch.setenv("EDGE_NODES", "node-a:8001")

    stale_state = _state_with_event("node-a", "evt-a", "water_level", 2.8, "bridge_north", "sensor")
    stale_state.version = 1

    responses = {
        "http://node-a:8001/state/merkle": {"merkle_root": stale_state.merkle_root()},
        "http://node-a:8001/state": stale_state.to_dict(),
    }

    monkeypatch.setattr(
        gateway_module.aiohttp,
        "ClientSession",
        lambda timeout=None: FakeClientSession(responses, timeout=timeout),
    )

    store = FakeStore()
    service = GatewayService(Config(), store)
    service.edge_nodes["node-a"]["last_version"] = 5
    asyncio.run(service.poll_once())

    assert service.runtime_metrics["stale_state_skips"] == 1
    assert service.runtime_metrics["state_merges_successful"] == 0
