import asyncio
from datetime import datetime
import structlog
import docker
import uuid

log = structlog.get_logger()

NETWORK_NAME = "collaborative_edge_mesh_mesh"
IMAGE_NAME = "collaborative_edge_mesh-edge-node-1"  # built by docker compose
SUBNET = "172.28.0"


class DockerManager:
    """Manages edge node containers via the Docker API."""

    def __init__(self, gateway_service=None):
        self.client = docker.from_env()
        self.gateway = gateway_service
        self.managed_nodes = {}  # node_id -> container info
        self._next_ip_suffix = 20  # start dynamic nodes at .20
        self._next_host_port = 8010  # fallback for non-numeric node IDs
        self.isolated_nodes = set()  # track container names of isolated nodes

    def _next_ip(self):
        ip = f"{SUBNET}.{self._next_ip_suffix}"
        self._next_ip_suffix += 1
        return ip

    def _list_all_containers(self):
        try:
            return self.client.containers.list(all=True)
        except TypeError:
            # test doubles may not support keyword args
            return self.client.containers.list()

    def _action_response(self, action, target, status, message, **extra):
        payload = {
            "action_id": str(uuid.uuid4()),
            "timestamp": datetime.utcnow().isoformat(),
            "action": action,
            "target": target,
            "status": status,
            "message": message,
        }
        payload.update(extra)
        return payload

    def _node_number_from_name(self, name):
        try:
            return int(name.replace("edge-node-", ""))
        except ValueError:
            return None

    def _host_port_for_node_name(self, name):
        number = self._node_number_from_name(name)
        if number is None:
            return None
        return 8000 + number

    def _node_id_from_container_name(self, name):
        number = self._node_number_from_name(name)
        if number is None:
            return name
        return f"node-{number}"

    def _internal_url_for_container_name(self, name):
        return f"http://{name}:8000"

    def _existing_node_numbers(self):
        numbers = set()

        # include stopped containers too, so we avoid name collisions
        for container in self._list_all_containers():
            number = self._node_number_from_name(container.name)
            if number is not None:
                numbers.add(number)

        for container_name in self.managed_nodes:
            number = self._node_number_from_name(container_name)
            if number is not None:
                numbers.add(number)

        return numbers

    def _existing_published_ports(self):
        ports = set()
        for container in self._list_all_containers():
            host_config = container.attrs.get("HostConfig") or {}
            bindings = host_config.get("PortBindings") or {}
            if not isinstance(bindings, dict):
                continue
            for entries in bindings.values():
                if not entries:
                    continue
                for binding in entries:
                    host_port = binding.get("HostPort")
                    if host_port and str(host_port).isdigit():
                        ports.add(int(host_port))
        return ports

    def _select_host_port(self, node_id):
        preferred = None
        node_num = node_id.replace("node-", "")
        try:
            preferred = 8000 + int(node_num)
        except ValueError:
            preferred = None

        used_ports = self._existing_published_ports()
        if preferred is not None and preferred not in used_ports:
            return preferred

        candidate = self._next_host_port
        while candidate in used_ports:
            candidate += 1
        self._next_host_port = candidate + 1
        return candidate

    def _next_available_node_id(self):
        used = self._existing_node_numbers()
        candidate = 1
        while candidate in used:
            candidate += 1
        return f"node-{candidate}"

    def _get_network(self):
        """Find the mesh network."""
        try:
            return self.client.networks.get(NETWORK_NAME)
        except docker.errors.NotFound:
            # try without project prefix
            for net in self.client.networks.list():
                if "mesh" in net.name:
                    return net
            raise RuntimeError("mesh network not found, is docker compose up?")

    def _get_image_name(self):
        """Find the built image."""
        for img in self.client.images.list():
            for tag in img.tags:
                if "edge-mesh" in tag or "collaborative_edge_mesh" in tag:
                    return tag
        return IMAGE_NAME

    def list_nodes(self):
        """List all running edge node containers (both static and dynamic)."""
        nodes = []
        for container in self.client.containers.list():
            if "edge-node" in container.name:
                managed_info = self.managed_nodes.get(container.name, {})
                node_id = self._node_id_from_container_name(container.name)
                host_port = managed_info.get("host_port")
                if host_port is None:
                    host_port = self._host_port_for_node_name(container.name)

                nodes.append(
                    {
                        "name": container.name,
                        "node_id": node_id,
                        "status": container.status,
                        "id": container.short_id,
                        "managed": container.name in self.managed_nodes,
                        "isolated": container.name in self.isolated_nodes,
                        "host_port": host_port,
                        "url": f"http://localhost:{host_port}" if host_port else None,
                        "internal_url": self._internal_url_for_container_name(
                            container.name
                        ),
                    }
                )
        return nodes

    def list_gateways(self):
        """List all running gateway containers with internal service URLs."""
        gateways = []
        for container in self.client.containers.list():
            if "gateway" not in container.name or "simulator" in container.name:
                continue

            gateways.append(
                {
                    "name": container.name,
                    "status": container.status,
                    "url": f"http://{container.name}:8000",
                }
            )

        gateways.sort(key=lambda item: item["name"])
        return gateways

    def create_node(self, node_id=None):
        """Spin up a new edge node container and register it with the gateway."""
        if node_id is None:
            node_id = self._next_available_node_id()

        container_name = f"edge-node-{node_id.replace('node-', '')}"

        # if container name exists, resolve before creating to avoid Docker 409 conflict
        try:
            existing_container = self.client.containers.get(container_name)
            if existing_container.status == "running":
                host_port = self._host_port_for_node_name(container_name)
                return self._action_response(
                    action="create_node",
                    target=container_name,
                    status="already_exists",
                    message=f"{container_name} already exists",
                    node_id=node_id,
                    container=container_name,
                    host_port=host_port,
                    url=f"http://localhost:{host_port}" if host_port else None,
                )

            # stale stopped container with same name: remove and continue
            existing_container.remove(force=True)
            self.managed_nodes.pop(container_name, None)
            self.isolated_nodes.discard(container_name)
        except docker.errors.NotFound:
            pass

        ip = self._next_ip()
        http_port = 8000

        host_port = self._select_host_port(node_id)

        # figure out existing peers
        peers = []
        for container in self.client.containers.list():
            if "edge-node" in container.name and container.status == "running":
                networks = container.attrs.get("NetworkSettings", {}).get(
                    "Networks", {}
                )
                for net_name, net_info in networks.items():
                    if "mesh" in net_name:
                        peer_ip = net_info.get("IPAddress")
                        if peer_ip:
                            peers.append(f"{peer_ip}:9000")

        peer_str = ",".join(peers)
        image = self._get_image_name()
        network = self._get_network()

        container = self.client.containers.run(
            image,
            detach=True,
            name=container_name,
            environment={
                "NODE_ID": node_id,
                "HTTP_PORT": str(http_port),
                "GOSSIP_PORT": "9000",
                "GOSSIP_INTERVAL": "5",
                "PEER_NODES": peer_str,
            },
            ports={f"{http_port}/tcp": host_port},
            network=network.name,
            cap_add=["NET_ADMIN"],
        )

        # get assigned IP
        container.reload()
        networks = container.attrs["NetworkSettings"]["Networks"]
        actual_ip = ip
        for net_name, net_info in networks.items():
            if "mesh" in net_name:
                actual_ip = net_info["IPAddress"]

        self.managed_nodes[container_name] = {
            "node_id": node_id,
            "container_id": container.short_id,
            "ip": actual_ip,
            "http_port": http_port,
            "host_port": host_port,
        }

        # register with gateway for polling
        if self.gateway is not None:
            self.gateway.register_node(container_name, f"http://{actual_ip}:{http_port}")

        log.info(
            "node_created",
            node_id=node_id,
            container=container_name,
            ip=actual_ip,
            host_port=host_port,
        )

        return self._action_response(
            action="create_node",
            target=container_name,
            status="created",
            message=f"Created {container_name}",
            node_id=node_id,
            container=container_name,
            ip=actual_ip,
            host_port=host_port,
            url=f"http://localhost:{host_port}",
            internal_url=self._internal_url_for_container_name(container_name),
            peers=peer_str,
        )

    def remove_node(self, node_id):
        """Stop and remove a node container."""
        container_name = f"edge-node-{node_id.replace('node-', '')}"

        try:
            container = self.client.containers.get(container_name)
            container.stop(timeout=5)
            container.remove()
        except docker.errors.NotFound:
            return self._action_response(
                action="remove_node",
                target=container_name,
                status="not_found",
                message=f"{container_name} not found",
            )

        self.managed_nodes.pop(container_name, None)
        self.isolated_nodes.discard(container_name)
        if self.gateway is not None:
            self.gateway.unregister_node(container_name)
        log.info("node_removed", node_id=node_id, container=container_name)

        return self._action_response(
            action="remove_node",
            target=container_name,
            status="removed",
            message=f"Removed {container_name}",
            container=container_name,
        )

    def isolate_node(self, node_id):
        """Drop all UDP traffic to/from a node (partition it from gossip)."""
        container_name = f"edge-node-{node_id.replace('node-', '')}"

        if container_name in self.isolated_nodes:
            return self._action_response(
                action="isolate_node",
                target=container_name,
                status="already_isolated",
                message=f"{container_name} is already isolated",
            )

        try:
            container = self.client.containers.get(container_name)
            container.exec_run("iptables -A INPUT -p udp -j DROP")
            container.exec_run("iptables -A OUTPUT -p udp -j DROP")
            self.isolated_nodes.add(container_name)
            log.info("node_isolated", node_id=node_id)
            return self._action_response(
                action="isolate_node",
                target=container_name,
                status="isolated",
                message=f"Isolated {container_name}",
                node=container_name,
            )
        except docker.errors.NotFound:
            return self._action_response(
                action="isolate_node",
                target=container_name,
                status="not_found",
                message=f"{container_name} not found",
            )

    def heal_node(self, node_id):
        """Flush iptables rules, restoring connectivity."""
        container_name = f"edge-node-{node_id.replace('node-', '')}"

        if container_name not in self.isolated_nodes:
            return self._action_response(
                action="heal_node",
                target=container_name,
                status="already_healthy",
                message=f"{container_name} is not isolated",
            )

        try:
            container = self.client.containers.get(container_name)
            container.exec_run("iptables -F INPUT")
            container.exec_run("iptables -F OUTPUT")
            self.isolated_nodes.discard(container_name)
            log.info("node_healed", node_id=node_id)
            return self._action_response(
                action="heal_node",
                target=container_name,
                status="healed",
                message=f"Healed {container_name}",
                node=container_name,
            )
        except docker.errors.NotFound:
            return self._action_response(
                action="heal_node",
                target=container_name,
                status="not_found",
                message=f"{container_name} not found",
            )

    def heal_all(self):
        """Heal all running edge nodes."""
        results = []
        for container in self.client.containers.list():
            if "edge-node" in container.name:
                container.exec_run("iptables -F INPUT")
                container.exec_run("iptables -F OUTPUT")
                results.append(container.name)

        self.isolated_nodes.clear()
        log.info("all_healed", nodes=results)
        return self._action_response(
            action="heal_all",
            target="mesh",
            status="healed",
            message="Healed all edge nodes",
            nodes=results,
        )

    def create_split_brain(self):
        """Partition the mesh into two groups."""
        nodes = [c for c in self.client.containers.list() if "edge-node" in c.name]
        if len(nodes) < 2:
            return self._action_response(
                action="split_brain",
                target="mesh",
                status="failed",
                message="Need at least 2 nodes",
            )

        mid = len(nodes) // 2
        group_a = nodes[:mid]
        group_b = nodes[mid:]

        # get IPs for each group
        def get_ip(container):
            networks = container.attrs["NetworkSettings"]["Networks"]
            for net_name, net_info in networks.items():
                if "mesh" in net_name:
                    return net_info["IPAddress"]
            return None

        ips_a = [get_ip(c) for c in group_a]
        ips_b = [get_ip(c) for c in group_b]

        # group A blocks traffic from group B and vice versa
        for c in group_a:
            for ip in ips_b:
                if ip:
                    c.exec_run(f"iptables -A INPUT -s {ip} -j DROP")
                    c.exec_run(f"iptables -A OUTPUT -d {ip} -j DROP")

        for c in group_b:
            for ip in ips_a:
                if ip:
                    c.exec_run(f"iptables -A INPUT -s {ip} -j DROP")
                    c.exec_run(f"iptables -A OUTPUT -d {ip} -j DROP")

        # Mark all as 'isolated' (partially) so UI shows red lines
        # This is simplification but effective for demo visualization
        for c in nodes:
            self.isolated_nodes.add(c.name)

        log.info(
            "split_brain_created",
            group_a=[c.name for c in group_a],
            group_b=[c.name for c in group_b],
        )

        return self._action_response(
            action="split_brain",
            target="mesh",
            status="split_brain",
            message="Created split-brain partition",
            group_a=[c.name for c in group_a],
            group_b=[c.name for c in group_b],
        )
