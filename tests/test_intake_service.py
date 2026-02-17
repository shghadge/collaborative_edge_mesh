from fastapi.testclient import TestClient

from src.config import Config
from src.crdt.state import NodeState
from src.hash_chain import HashChainLog
from src.services.intake import IntakeService


def _build_service(tmp_path, monkeypatch):
    monkeypatch.setenv("NODE_ID", "node-test")
    monkeypatch.setenv("PEER_NODES", "")
    config = Config()
    state = NodeState(config.node_id)
    chain = HashChainLog(config.node_id, data_dir=str(tmp_path / "logs"))
    return IntakeService(config, state, chain)


def test_event_sensor_returns_stored_keys(tmp_path, monkeypatch):
    service = _build_service(tmp_path, monkeypatch)
    client = TestClient(service.app)

    payload = {
        "type": "water_level",
        "value": 3.2,
        "location": "bridge_north",
        "category": "sensor",
        "metadata": {"unit": "meters", "severity": "warning"},
    }

    response = client.post("/event", json=payload)
    assert response.status_code == 200

    body = response.json()
    assert body["status"] == "accepted"
    assert body["category"] == "sensor"
    assert body["stored_in"]["counter_key"] == "event_count:water_level"
    assert body["stored_in"]["register_key"] == "sensor:bridge_north:water_level"


def test_event_resource_decrement_updates_pn_counter(tmp_path, monkeypatch):
    service = _build_service(tmp_path, monkeypatch)
    client = TestClient(service.app)

    increment_payload = {
        "type": "shelter_occupancy",
        "value": 12,
        "location": "shelter_east",
        "category": "resource",
        "operation": "increment",
        "metadata": {},
    }
    decrement_payload = {
        "type": "shelter_occupancy",
        "value": 3,
        "location": "shelter_east",
        "category": "resource",
        "operation": "decrement",
        "metadata": {},
    }

    assert client.post("/event", json=increment_payload).status_code == 200
    response = client.post("/event", json=decrement_payload)
    assert response.status_code == 200
    assert response.json()["stored_in"]["operation"] == "decrement"

    state_response = client.get("/state")
    assert state_response.status_code == 200
    pn_value = state_response.json()["pn_counters"]["resource:shelter_east:shelter_occupancy"]["total_value"]
    assert pn_value == 9


def test_event_without_category_uses_general_default(tmp_path, monkeypatch):
    service = _build_service(tmp_path, monkeypatch)
    client = TestClient(service.app)

    payload = {
        "type": "temperature",
        "value": 27.4,
        "location": "bridge_north",
        "metadata": {},
    }

    response = client.post("/event", json=payload)
    assert response.status_code == 200

    body = response.json()
    assert body["category"] == "general"
    assert body["stored_in"]["counter_key"] == "event_count:temperature"
    assert body["stored_in"]["register_key"] == "general:bridge_north:temperature"
