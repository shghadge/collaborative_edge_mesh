from types import SimpleNamespace

from src.services import docker_manager as docker_manager_module
from src.services.docker_manager import DockerManager


class FakeContainer:
    def __init__(self, name, short_id, ip, status="running"):
        self.name = name
        self.short_id = short_id
        self.status = status
        self.attrs = {
            "NetworkSettings": {
                "Networks": {
                    "collaborative_edge_mesh_mesh": {
                        "IPAddress": ip,
                    }
                }
            }
        }
        self.commands = []

    def exec_run(self, command):
        self.commands.append(command)

    def stop(self, timeout=5):
        self.status = "exited"

    def remove(self):
        return None

    def reload(self):
        return None


class FakeContainersManager:
    def __init__(self, containers):
        self._containers = containers
        self._counter = 30

    def list(self):
        return [c for c in self._containers.values() if c.status == "running"]

    def get(self, name):
        if name not in self._containers:
            raise docker_manager_module.docker.errors.NotFound("not found")
        return self._containers[name]

    def run(self, image, detach, name, environment, ports, network, cap_add):
        ip = f"172.28.0.{self._counter}"
        self._counter += 1
        container = FakeContainer(
            name=name, short_id=name[-4:], ip=ip, status="running"
        )
        self._containers[name] = container
        return container


class FakeImagesManager:
    def list(self):
        return [SimpleNamespace(tags=["collaborative_edge_mesh-edge-node-1:latest"])]


class FakeNetwork:
    def __init__(self, name):
        self.name = name


class FakeNetworksManager:
    def __init__(self):
        self._network = FakeNetwork("collaborative_edge_mesh_mesh")

    def get(self, name):
        return self._network

    def list(self):
        return [self._network]


class FakeDockerClient:
    def __init__(self, containers):
        self.containers = FakeContainersManager(containers)
        self.images = FakeImagesManager()
        self.networks = FakeNetworksManager()


class FakeGateway:
    def __init__(self):
        self.registered = []
        self.unregistered = []

    def register_node(self, node_id, url):
        self.registered.append((node_id, url))

    def unregister_node(self, node_id):
        self.unregistered.append(node_id)


def _build_manager(monkeypatch):
    containers = {
        "edge-node-1": FakeContainer("edge-node-1", "aaaa", "172.28.0.11"),
        "edge-node-2": FakeContainer("edge-node-2", "bbbb", "172.28.0.12"),
    }
    fake_client = FakeDockerClient(containers)
    monkeypatch.setattr(docker_manager_module.docker, "from_env", lambda: fake_client)
    gateway = FakeGateway()
    manager = DockerManager(gateway)
    return manager, gateway


def test_list_nodes_includes_dashboard_fields(monkeypatch):
    manager, _gateway = _build_manager(monkeypatch)
    nodes = manager.list_nodes()

    node1 = next(n for n in nodes if n["name"] == "edge-node-1")
    assert node1["node_id"] == "node-1"
    assert node1["host_port"] == 8001
    assert node1["url"] == "http://localhost:8001"
    assert node1["isolated"] is False


def test_isolate_and_heal_are_idempotent(monkeypatch):
    manager, _gateway = _build_manager(monkeypatch)

    first_isolate = manager.isolate_node("node-1")
    second_isolate = manager.isolate_node("node-1")
    first_heal = manager.heal_node("node-1")
    second_heal = manager.heal_node("node-1")

    assert first_isolate["status"] == "isolated"
    assert second_isolate["status"] == "already_isolated"
    assert first_heal["status"] == "healed"
    assert second_heal["status"] == "already_healthy"


def test_create_node_returns_action_metadata(monkeypatch):
    manager, gateway = _build_manager(monkeypatch)
    result = manager.create_node("node-4")

    assert result["action"] == "create_node"
    assert result["status"] == "created"
    assert result["target"] == "edge-node-4"
    assert result["node_id"] == "node-4"
    assert result["url"].startswith("http://localhost:")
    assert "action_id" in result
    assert gateway.registered
