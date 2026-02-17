import asyncio
import structlog
import docker

log = structlog.get_logger()

NETWORK_NAME = "collaborative_edge_mesh_mesh"
IMAGE_NAME = "collaborative_edge_mesh-edge-node-1"  # built by docker compose
SUBNET = "172.28.0"


class DockerManager:
    """Manages edge node containers via the Docker API."""

    def __init__(self, gateway_service):
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
                nodes.append(
                    {
                        "name": container.name,
                        "status": container.status,
                        "id": container.short_id,
                        "managed": container.name in self.managed_nodes,
                        "isolated": container.name in self.isolated_nodes,
                    }
                )
        return nodes

    def create_node(self, node_id=None):
        """Spin up a new edge node container and register it with the gateway."""
        if node_id is None:
            existing = len(self.list_nodes())
            node_id = f"node-{existing + 1}"

        container_name = f"edge-node-{node_id.replace('node-', '')}"
        ip = self._next_ip()
        http_port = 8000

        # assign a host port: node-4 -> 8004, node-5 -> 8005, etc.
        node_num = node_id.replace("node-", "")
        try:
            host_port = 8000 + int(node_num)
        except ValueError:
            host_port = self._next_host_port
            self._next_host_port += 1

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
        self.gateway.register_node(container_name, f"http://{actual_ip}:{http_port}")

        log.info(
            "node_created",
            node_id=node_id,
            container=container_name,
            ip=actual_ip,
            host_port=host_port,
        )

        return {
            "node_id": node_id,
            "container": container_name,
            "ip": actual_ip,
            "host_port": host_port,
            "url": f"http://localhost:{host_port}",
            "peers": peer_str,
        }

    def remove_node(self, node_id):
        """Stop and remove a node container."""
        container_name = f"edge-node-{node_id.replace('node-', '')}"

        try:
            container = self.client.containers.get(container_name)
            container.stop(timeout=5)
            container.remove()
        except docker.errors.NotFound:
            return {"error": f"{container_name} not found"}

        self.managed_nodes.pop(container_name, None)
        self.gateway.unregister_node(container_name)
        log.info("node_removed", node_id=node_id, container=container_name)

        return {"status": "removed", "container": container_name}

    def isolate_node(self, node_id):
        """Drop all UDP traffic to/from a node (partition it from gossip)."""
        container_name = f"edge-node-{node_id.replace('node-', '')}"
        try:
            container = self.client.containers.get(container_name)
            container.exec_run("iptables -A INPUT -p udp -j DROP")
            container.exec_run("iptables -A OUTPUT -p udp -j DROP")
            self.isolated_nodes.add(container_name)
            log.info("node_isolated", node_id=node_id)
            return {"status": "isolated", "node": container_name}
        except docker.errors.NotFound:
            return {"error": f"{container_name} not found"}

    def heal_node(self, node_id):
        """Flush iptables rules, restoring connectivity."""
        container_name = f"edge-node-{node_id.replace('node-', '')}"
        try:
            container = self.client.containers.get(container_name)
            container.exec_run("iptables -F INPUT")
            container.exec_run("iptables -F OUTPUT")
            self.isolated_nodes.discard(container_name)
            log.info("node_healed", node_id=node_id)
            return {"status": "healed", "node": container_name}
        except docker.errors.NotFound:
            return {"error": f"{container_name} not found"}

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
        return {"status": "healed", "nodes": results}

    def create_split_brain(self):
        """Partition the mesh into two groups."""
        nodes = [c for c in self.client.containers.list() if "edge-node" in c.name]
        if len(nodes) < 2:
            return {"error": "need at least 2 nodes"}

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

        return {
            "status": "split_brain",
            "group_a": [c.name for c in group_a],
            "group_b": [c.name for c in group_b],
        }
