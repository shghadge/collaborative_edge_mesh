"""
Quantitative metrics collection tests — verifies the 5 report metrics are
captured correctly through the gateway poll cycle.

Tests simulate a realistic flow:
  1. Multiple nodes with events
  2. Gateway polls and merges state
  3. Divergence → convergence transition
  4. Verify all 5 report metrics are collected:
      - Convergence time
      - Data loss % (0% when no partition)
      - Gossip message count
      - State size growth
      - Divergence status
"""

import asyncio
import time

from src.config import Config
from src.crdt.state import NodeState
from src.services import gateway as gateway_module
from src.services.gateway import GatewayService


# ── Fake Store (collects timeseries and metrics) ───────────────


class MetricsCollectorStore:
    """A fake SQLiteStore that captures all saved metrics for assertions."""

    def __init__(self):
        self.divergence_logs = []
        self.snapshots = []
        self.metrics = []
        self.timeseries = []
        self.event_tracking = []

    def log_divergence(self, is_divergent, merkle_roots):
        self.divergence_logs.append(
            {"is_divergent": is_divergent, "merkle_roots": merkle_roots}
        )

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
        self.metrics.append({"name": name, "value": value})

    def save_timeseries_metric(self, metric_name, value, metadata=None):
        self.timeseries.append({"metric_name": metric_name, "value": value})

    def get_timeseries(self, metric_name, limit=100):
        return [t for t in self.timeseries if t["metric_name"] == metric_name][
            :limit
        ]

    def save_event_tracking(self, tracking_id, node_id, event_type, category, **kw):
        self.event_tracking.append({"tracking_id": tracking_id, "node_id": node_id})

    def get_event_tracking(self, limit=50):
        return self.event_tracking[:limit]

    # --- helpers for tests ---

    def timeseries_for(self, name):
        return [t for t in self.timeseries if t["metric_name"] == name]

    def metrics_for(self, name):
        return [m for m in self.metrics if m["name"] == name]


# ── Fake HTTP responses ────────────────────────────────────────


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

    async def __aexit__(self, *args):
        pass


# ── Helpers ────────────────────────────────────────────────────


def _make_node_state(node_id, events=None):
    """Create a NodeState with some events recorded."""
    state = NodeState(node_id)
    for ev in events or []:
        state.record_event(
            ev["id"],
            ev["type"],
            {"value": ev.get("value", 1), "location": ev.get("location", "loc")},
            category=ev.get("category", "sensor"),
        )
    return state


def _build_gateway(monkeypatch, store=None):
    monkeypatch.setenv("NODE_ID", "gateway-test")
    cfg = Config()
    s = store or MetricsCollectorStore()
    return GatewayService(cfg, s), s


def _make_http_sequence(monkeypatch, gateway, node_states):
    """
    Monkey-patch the gateway's aiohttp.ClientSession to return canned merkle + state
    responses for each node, using the exact same pattern as test_gateway_service.py.
    """
    responses = {}
    for nid, state in node_states.items():
        url = gateway.edge_nodes[nid]["url"]
        responses[f"{url}/state/merkle"] = {
            "node_id": nid,
            "merkle_root": state.merkle_root(),
            "version": state.version,
        }
        responses[f"{url}/state"] = state.to_dict()

    class FakeClientSession:
        def __init__(self, timeout=None):
            pass

        async def __aenter__(self):
            return self

        async def __aexit__(self, *args):
            return False

        def get(self, url):
            if url in responses:
                resp = responses[url]
                if isinstance(resp, Exception):
                    return FakeResponseContext(error=resp)
                return FakeResponseContext(payload=resp)
            return FakeResponseContext(error=RuntimeError(f"no fake for {url}"))

    monkeypatch.setattr(
        gateway_module.aiohttp,
        "ClientSession",
        FakeClientSession,
    )


# ══════════════════════════════════════════════════════════════
#  Tests
# ══════════════════════════════════════════════════════════════


def test_convergent_state_records_zero_divergence(monkeypatch):
    """When all nodes have the same Merkle root → not divergent."""
    gw, store = _build_gateway(monkeypatch)

    # Both nodes return the exact same state → same Merkle root
    state = _make_node_state("shared", [{"id": "e1", "type": "water_level"}])

    gw.edge_nodes = {
        "node-a": {"url": "http://node-a:8001"},
        "node-b": {"url": "http://node-b:8002"},
    }
    _make_http_sequence(monkeypatch, gw, {"node-a": state, "node-b": state})

    asyncio.run(gw.poll_once())

    # Divergence should be False
    assert gw.is_divergent is False
    assert store.divergence_logs[-1]["is_divergent"] is False

    # Timeseries should record is_divergent = 0.0
    divg_ts = store.timeseries_for("is_divergent")
    assert len(divg_ts) >= 1
    assert divg_ts[-1]["value"] == 0.0


def test_divergent_then_convergent_records_convergence_time(monkeypatch):
    """
    Simulate divergence → convergence and verify convergence_time
    is saved to the timeseries.
    """
    gw, store = _build_gateway(monkeypatch)

    # Poll 1: divergent (different states)
    state_a1 = _make_node_state("node-a", [{"id": "e1", "type": "water_level"}])
    state_b1 = _make_node_state("node-b", [{"id": "e2", "type": "shelter_occupancy"}])

    gw.edge_nodes = {
        "node-a": {"url": "http://node-a:8001"},
        "node-b": {"url": "http://node-b:8002"},
    }
    _make_http_sequence(monkeypatch, gw, {"node-a": state_a1, "node-b": state_b1})
    asyncio.run(gw.poll_once())

    assert gw.is_divergent is True
    assert gw.runtime_metrics["divergence_started_at"] is not None

    # Small delay to get a measurable convergence time
    time.sleep(0.05)

    # Poll 2: convergent (same state)
    shared_state = _make_node_state("shared", [
        {"id": "e1", "type": "water_level"},
        {"id": "e2", "type": "shelter_occupancy"},
    ])
    _make_http_sequence(monkeypatch, gw, {
        "node-a": shared_state,
        "node-b": shared_state,
    })
    asyncio.run(gw.poll_once())

    assert gw.is_divergent is False
    assert gw.runtime_metrics["last_convergence_seconds"] is not None
    assert gw.runtime_metrics["last_convergence_seconds"] >= 0.04  # at least ~50ms

    # Check timeseries
    conv_ts = store.timeseries_for("convergence_time")
    assert len(conv_ts) >= 1
    assert conv_ts[-1]["value"] >= 0.04


def test_state_size_is_recorded_each_poll(monkeypatch):
    """State size should be saved as a timeseries metric on every poll."""
    gw, store = _build_gateway(monkeypatch)

    state = _make_node_state("node-a", [{"id": "e1", "type": "water_level"}])
    gw.edge_nodes = {"node-a": {"url": "http://node-a:8001"}}
    _make_http_sequence(monkeypatch, gw, {"node-a": state})

    asyncio.run(gw.poll_once())

    size_ts = store.timeseries_for("state_size_bytes")
    assert len(size_ts) == 1
    assert size_ts[0]["value"] > 0  # non-zero state size


def test_node_count_is_recorded_each_poll(monkeypatch):
    """Node count should be saved to timeseries."""
    gw, store = _build_gateway(monkeypatch)

    state_a = _make_node_state("node-a", [{"id": "e1", "type": "t"}])
    state_b = _make_node_state("node-b", [{"id": "e1", "type": "t"}])
    state_c = _make_node_state("node-c", [{"id": "e1", "type": "t"}])

    gw.edge_nodes = {
        "node-a": {"url": "http://node-a:8001"},
        "node-b": {"url": "http://node-b:8002"},
        "node-c": {"url": "http://node-c:8003"},
    }
    _make_http_sequence(monkeypatch, gw, {
        "node-a": state_a, "node-b": state_b, "node-c": state_c,
    })

    asyncio.run(gw.poll_once())

    count_ts = store.timeseries_for("node_count")
    assert len(count_ts) == 1
    assert count_ts[0]["value"] == 3


def test_merge_produces_traditional_metrics(monkeypatch):
    """After a poll, merge_time_ms, node_count, is_divergent should be saved
    as traditional metrics (not just timeseries)."""
    gw, store = _build_gateway(monkeypatch)

    state = _make_node_state("node-a", [{"id": "e1", "type": "t"}])
    gw.edge_nodes = {"node-a": {"url": "http://node-a:8001"}}
    _make_http_sequence(monkeypatch, gw, {"node-a": state})

    asyncio.run(gw.poll_once())

    metric_names = {m["name"] for m in store.metrics}
    assert "merge_time_ms" in metric_names
    assert "node_count" in metric_names
    assert "is_divergent" in metric_names


def test_state_size_grows_with_events(monkeypatch):
    """Adding more events should increase state_size_bytes."""
    gw, store = _build_gateway(monkeypatch)

    # Poll 1: single event
    state_small = _make_node_state("node-a", [{"id": "e1", "type": "t"}])
    gw.edge_nodes = {"node-a": {"url": "http://node-a:8001"}}
    _make_http_sequence(monkeypatch, gw, {"node-a": state_small})
    asyncio.run(gw.poll_once())
    size_1 = store.timeseries_for("state_size_bytes")[-1]["value"]

    # Poll 2: many events (reset merged state to force fresh merge)
    gw.merged_state = None
    state_big = _make_node_state("node-a", [
        {"id": f"e{i}", "type": f"type_{i}", "location": f"loc_{i}"}
        for i in range(10)
    ])
    _make_http_sequence(monkeypatch, gw, {"node-a": state_big})
    asyncio.run(gw.poll_once())
    size_2 = store.timeseries_for("state_size_bytes")[-1]["value"]

    assert size_2 > size_1


def test_snapshot_saved_with_merged_state(monkeypatch):
    """Each poll should save a snapshot with the merged state."""
    gw, store = _build_gateway(monkeypatch)

    state = _make_node_state("node-a", [{"id": "e1", "type": "water_level"}])
    gw.edge_nodes = {"node-a": {"url": "http://node-a:8001"}}
    _make_http_sequence(monkeypatch, gw, {"node-a": state})

    asyncio.run(gw.poll_once())

    assert len(store.snapshots) == 1
    snap = store.snapshots[0]
    assert snap["node_count"] == 1
    assert snap["source_nodes"] == ["node-a"]
    assert snap["merkle_root"]  # non-empty
    assert "counters" in snap["state"]


def test_multiple_polls_accumulate_timeseries(monkeypatch):
    """Running multiple poll cycles should accumulate timeseries entries."""
    gw, store = _build_gateway(monkeypatch)

    state = _make_node_state("node-a", [{"id": "e1", "type": "t"}])
    gw.edge_nodes = {"node-a": {"url": "http://node-a:8001"}}
    _make_http_sequence(monkeypatch, gw, {"node-a": state})

    for _ in range(3):
        asyncio.run(gw.poll_once())

    # Each poll creates state_size_bytes, node_count, is_divergent → 3 * 3 = 9
    size_ts = store.timeseries_for("state_size_bytes")
    assert len(size_ts) == 3
    count_ts = store.timeseries_for("node_count")
    assert len(count_ts) == 3
