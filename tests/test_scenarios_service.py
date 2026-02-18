import asyncio

from src.services.scenarios import (
    run_bootstrap_events_convergence,
    run_split_brain_then_heal,
)


class FakeDockerManager:
    def __init__(self, split_status="split_brain"):
        self.split_status = split_status
        self.split_called = 0
        self.heal_called = 0

    def create_split_brain(self):
        self.split_called += 1
        if self.split_status == "split_brain":
            return {
                "status": "split_brain",
                "group_a": ["edge-node-1"],
                "group_b": ["edge-node-2"],
            }
        return {"status": "failed", "message": "Need at least 2 nodes"}

    def heal_all(self):
        self.heal_called += 1
        return {"status": "healed", "nodes": ["edge-node-1", "edge-node-2"]}

    def create_node(self):
        return {
            "action": "create_node",
            "status": "created",
            "node_id": "node-99",
            "url": "http://localhost:8099",
        }

    def list_nodes(self):
        return [
            {
                "name": "edge-node-1",
                "node_id": "node-1",
                "url": "http://localhost:8001",
            },
            {
                "name": "edge-node-2",
                "node_id": "node-2",
                "url": "http://localhost:8002",
            },
        ]


class FakeGatewayService:
    def __init__(self):
        self.poll_count = 0
        self.is_divergent = False

    async def poll_once(self):
        self.poll_count += 1
        if self.poll_count == 1:
            self.is_divergent = True
        else:
            self.is_divergent = False


def test_split_brain_heal_scenario_success():
    docker_manager = FakeDockerManager(split_status="split_brain")
    gateway = FakeGatewayService()

    result = asyncio.run(
        run_split_brain_then_heal(
            docker_manager=docker_manager,
            gateway_service=gateway,
            isolate_seconds=0,
            verify_polls=2,
        )
    )

    assert result["status"] == "completed"
    assert result["divergent_after_split"] is True
    assert result["converged"] is True
    assert len(result["verification_states"]) == 2
    assert docker_manager.split_called == 1
    assert docker_manager.heal_called == 1


def test_split_brain_heal_scenario_split_failure():
    docker_manager = FakeDockerManager(split_status="failed")
    gateway = FakeGatewayService()

    result = asyncio.run(
        run_split_brain_then_heal(
            docker_manager=docker_manager,
            gateway_service=gateway,
            isolate_seconds=0,
            verify_polls=2,
        )
    )

    assert result["status"] == "failed"
    assert result["split_result"]["status"] == "failed"
    assert docker_manager.heal_called == 0
    assert gateway.poll_count == 0


def test_bootstrap_events_convergence_success():
    docker_manager = FakeDockerManager(split_status="split_brain")
    gateway = FakeGatewayService()

    async def fake_sender(node_url, payload):
        return {"ok": True, "status_code": 200, "body": {"status": "accepted"}}

    result = asyncio.run(
        run_bootstrap_events_convergence(
            docker_manager=docker_manager,
            gateway_service=gateway,
            create_nodes=1,
            events_per_node=2,
            verify_polls=2,
            event_sender=fake_sender,
        )
    )

    assert result["action"] == "bootstrap_events_convergence"
    assert result["created_count"] == 1
    assert result["target_node_count"] == 2
    assert result["successful_events"] == 4
    assert result["failed_events"] == 0
    assert len(result["verification_states"]) == 2


def test_bootstrap_events_convergence_handles_send_failures():
    docker_manager = FakeDockerManager(split_status="split_brain")
    gateway = FakeGatewayService()

    async def failing_sender(node_url, payload):
        raise RuntimeError("send failed")

    result = asyncio.run(
        run_bootstrap_events_convergence(
            docker_manager=docker_manager,
            gateway_service=gateway,
            create_nodes=0,
            events_per_node=1,
            verify_polls=1,
            event_sender=failing_sender,
        )
    )

    assert result["target_node_count"] == 2
    assert result["successful_events"] == 0
    assert result["failed_events"] == 2
