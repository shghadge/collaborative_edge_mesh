from fastapi.testclient import TestClient

from src.config import Config
from src.crdt.state import NodeState
from src.hash_chain import HashChainLog
from src.services.intake import IntakeService


def _build_node(node_id, tmp_path, monkeypatch):
    monkeypatch.setenv("NODE_ID", node_id)
    monkeypatch.setenv("PEER_NODES", "")
    config = Config()
    state = NodeState(config.node_id)
    chain = HashChainLog(config.node_id, data_dir=str(tmp_path / f"{node_id}_logs"))
    service = IntakeService(config, state, chain)
    return TestClient(service.app)


def test_two_nodes_merge_and_converge(tmp_path, monkeypatch):
    node1 = _build_node("node-1", tmp_path, monkeypatch)
    node2 = _build_node("node-2", tmp_path, monkeypatch)

    r1 = node1.post(
        "/event",
        json={
            "type": "water_level",
            "value": 3.2,
            "location": "bridge_north",
            "category": "sensor",
            "metadata": {"unit": "meters"},
        },
    )
    assert r1.status_code == 200

    r2 = node2.post(
        "/event",
        json={
            "type": "shelter_occupancy",
            "value": 10,
            "location": "shelter_east",
            "category": "resource",
            "operation": "increment",
            "metadata": {},
        },
    )
    assert r2.status_code == 200

    node2_state = node2.get("/state").json()
    merge_into_node1 = node1.post("/merge", json=node2_state)
    assert merge_into_node1.status_code == 200

    node1_state = node1.get("/state").json()
    assert "sensor:bridge_north:water_level" in node1_state["registers"]
    assert "resource:shelter_east:shelter_occupancy" in node1_state["pn_counters"]

    merge_into_node2 = node2.post("/merge", json=node1_state)
    assert merge_into_node2.status_code == 200

    node1_root = node1.get("/state/merkle").json()["merkle_root"]
    node2_root = node2.get("/state/merkle").json()["merkle_root"]
    assert node1_root == node2_root
