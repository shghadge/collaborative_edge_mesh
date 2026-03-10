"""
API negative/validation tests — error cases for intake, gateway, and simulator endpoints.
"""

from fastapi.testclient import TestClient

from src.config import Config
from src.crdt.state import NodeState
from src.hash_chain import HashChainLog
from src.services.intake import IntakeService


# ── Helpers ────────────────────────────────────────────────────


def _build_intake(tmp_path, monkeypatch, node_id="node-test"):
    monkeypatch.setenv("NODE_ID", node_id)
    config = Config()
    state = NodeState(config.node_id)
    chain = HashChainLog(config.node_id, data_dir=str(tmp_path / "logs"))
    service = IntakeService(config, state, chain)
    return TestClient(service.app), state


# ══════════════════════════════════════════════════════════════
#  Intake Endpoint Validation
# ══════════════════════════════════════════════════════════════


class TestIntakeEventValidation:
    def test_missing_type_field(self, tmp_path, monkeypatch):
        """POST /event without 'type' should return 422."""
        client, _ = _build_intake(tmp_path, monkeypatch)
        response = client.post("/event", json={"value": 42})
        assert response.status_code == 422

    def test_missing_value_field(self, tmp_path, monkeypatch):
        """POST /event without 'value' should return 422."""
        client, _ = _build_intake(tmp_path, monkeypatch)
        response = client.post("/event", json={"type": "water_level"})
        assert response.status_code == 422

    def test_empty_body(self, tmp_path, monkeypatch):
        """POST /event with empty body should return 422."""
        client, _ = _build_intake(tmp_path, monkeypatch)
        response = client.post("/event", json={})
        assert response.status_code == 422

    def test_invalid_json(self, tmp_path, monkeypatch):
        """POST /event with invalid JSON should return 422."""
        client, _ = _build_intake(tmp_path, monkeypatch)
        response = client.post(
            "/event", content="not-json", headers={"content-type": "application/json"}
        )
        assert response.status_code == 422

    def test_invalid_category(self, tmp_path, monkeypatch):
        """POST /event with an unsupported category should return 422."""
        client, _ = _build_intake(tmp_path, monkeypatch)
        response = client.post(
            "/event",
            json={
                "type": "water_level",
                "value": 5.0,
                "category": "nonexistent_category",
            },
        )
        assert response.status_code == 422


# ══════════════════════════════════════════════════════════════
#  Intake Tracking Metadata in Responses
# ══════════════════════════════════════════════════════════════


class TestIntakeTrackingMetadata:
    def test_event_response_contains_tracking_id(self, tmp_path, monkeypatch):
        client, _ = _build_intake(tmp_path, monkeypatch)
        response = client.post(
            "/event",
            json={"type": "water_level", "value": 3.0, "category": "sensor"},
        )
        body = response.json()
        assert "tracking_id" in body
        assert len(body["tracking_id"]) == 36  # UUID format

    def test_event_response_contains_recorded_at(self, tmp_path, monkeypatch):
        client, _ = _build_intake(tmp_path, monkeypatch)
        response = client.post(
            "/event",
            json={"type": "water_level", "value": 3.0, "category": "sensor"},
        )
        body = response.json()
        assert "recorded_at" in body
        assert "T" in body["recorded_at"]  # ISO format

    def test_event_response_contains_state_size_bytes(self, tmp_path, monkeypatch):
        client, _ = _build_intake(tmp_path, monkeypatch)
        response = client.post(
            "/event",
            json={"type": "water_level", "value": 3.0, "category": "sensor"},
        )
        body = response.json()
        assert "state_size_bytes" in body
        assert isinstance(body["state_size_bytes"], int)
        assert body["state_size_bytes"] > 0

    def test_event_response_contains_merkle_root_after(self, tmp_path, monkeypatch):
        client, _ = _build_intake(tmp_path, monkeypatch)
        response = client.post(
            "/event",
            json={"type": "water_level", "value": 3.0, "category": "sensor"},
        )
        body = response.json()
        assert "merkle_root_after" in body
        assert len(body["merkle_root_after"]) == 64  # SHA256 hex

    def test_state_size_grows_after_multiple_events(self, tmp_path, monkeypatch):
        """State size should increase as more events are recorded."""
        client, _ = _build_intake(tmp_path, monkeypatch)

        r1 = client.post(
            "/event",
            json={"type": "water_level", "value": 3.0, "category": "sensor"},
        )
        size_1 = r1.json()["state_size_bytes"]

        r2 = client.post(
            "/event",
            json={
                "type": "shelter_occupancy",
                "value": 12,
                "category": "resource",
                "operation": "increment",
            },
        )
        size_2 = r2.json()["state_size_bytes"]

        assert size_2 > size_1

    def test_merkle_root_changes_after_event(self, tmp_path, monkeypatch):
        """Merkle root should change after recording a new event."""
        client, _ = _build_intake(tmp_path, monkeypatch)

        # Get initial merkle root
        r1 = client.get("/state/merkle")
        root_before = r1.json()["merkle_root"]

        # Send event
        r2 = client.post(
            "/event",
            json={"type": "water_level", "value": 3.0, "category": "sensor"},
        )
        root_after = r2.json()["merkle_root_after"]

        assert root_after != root_before

    def test_tracking_ids_are_unique(self, tmp_path, monkeypatch):
        """Each event should get a unique tracking ID."""
        client, _ = _build_intake(tmp_path, monkeypatch)

        ids = set()
        for i in range(5):
            r = client.post(
                "/event",
                json={"type": f"type_{i}", "value": i, "category": "sensor"},
            )
            ids.add(r.json()["tracking_id"])

        assert len(ids) == 5  # All unique


# ══════════════════════════════════════════════════════════════
#  Intake Read-Only Endpoints
# ══════════════════════════════════════════════════════════════


class TestIntakeReadEndpoints:
    def test_state_returns_empty_on_startup(self, tmp_path, monkeypatch):
        client, _ = _build_intake(tmp_path, monkeypatch)
        response = client.get("/state")
        assert response.status_code == 200
        body = response.json()
        assert "counters" in body

    def test_merkle_returns_node_id(self, tmp_path, monkeypatch):
        client, _ = _build_intake(tmp_path, monkeypatch)
        response = client.get("/state/merkle")
        assert response.status_code == 200
        assert response.json()["node_id"] == "node-test"

    def test_status_returns_node_metadata(self, tmp_path, monkeypatch):
        client, _ = _build_intake(tmp_path, monkeypatch)
        response = client.get("/status")
        assert response.status_code == 200
        body = response.json()
        assert body["node_id"] == "node-test"
        assert body["version"] >= 0
        assert body["uptime_seconds"] >= 0

    def test_log_returns_empty_on_startup(self, tmp_path, monkeypatch):
        client, _ = _build_intake(tmp_path, monkeypatch)
        response = client.get("/log")
        assert response.status_code == 200
        body = response.json()
        assert body["entries"] == []
        assert body["total"] == 0

    def test_health_endpoint(self, tmp_path, monkeypatch):
        client, _ = _build_intake(tmp_path, monkeypatch)
        response = client.get("/health")
        assert response.status_code == 200
        assert response.json()["status"] == "ok"

    def test_metrics_summary_endpoint(self, tmp_path, monkeypatch):
        client, _ = _build_intake(tmp_path, monkeypatch)
        # Send an event first so there's some state
        client.post(
            "/event",
            json={"type": "water_level", "value": 3.0, "category": "sensor"},
        )
        response = client.get("/metrics/summary")
        assert response.status_code == 200
        body = response.json()
        assert body["node_id"] == "node-test"
        assert body["event_count"] >= 1
        assert body["state_size_bytes"] > 0
        assert len(body["merkle_root"]) == 64
        assert body["uptime_seconds"] >= 0


# ══════════════════════════════════════════════════════════════
#  Merge Endpoint Validation
# ══════════════════════════════════════════════════════════════


class TestMergeValidation:
    def test_merge_with_invalid_state(self, tmp_path, monkeypatch):
        """Merging garbage data should return 400."""
        client, _ = _build_intake(tmp_path, monkeypatch)
        response = client.post("/merge", json={"garbage": True})
        assert response.status_code == 400

    def test_merge_with_empty_body(self, tmp_path, monkeypatch):
        """Merging empty dict should return 400."""
        client, _ = _build_intake(tmp_path, monkeypatch)
        response = client.post("/merge", json={})
        assert response.status_code == 400

    def test_merge_with_valid_state(self, tmp_path, monkeypatch):
        """Merging a valid NodeState dict should succeed."""
        client, _ = _build_intake(tmp_path, monkeypatch)
        remote = NodeState("remote-node")
        remote.record_event("e1", "water_level", {"value": 5.0, "location": "loc"}, category="sensor")
        response = client.post("/merge", json=remote.to_dict())
        assert response.status_code == 200
        body = response.json()
        assert body["status"] == "merged"
        assert "merkle_root" in body

    def test_merge_updates_state(self, tmp_path, monkeypatch):
        """After merging remote state, the local state should include remote data."""
        client, state = _build_intake(tmp_path, monkeypatch)

        # Record a local event
        client.post(
            "/event",
            json={"type": "local_event", "value": 1, "category": "sensor"},
        )
        root_before = state.merkle_root()

        # Merge remote state
        remote = NodeState("remote-node")
        remote.record_event("e-remote", "remote_event", {"value": 99, "location": "far"}, category="sensor")
        client.post("/merge", json=remote.to_dict())

        root_after = state.merkle_root()
        assert root_after != root_before  # State changed

    def test_nonexistent_endpoint_returns_404(self, tmp_path, monkeypatch):
        client, _ = _build_intake(tmp_path, monkeypatch)
        response = client.get("/nonexistent")
        assert response.status_code == 404
