from concurrent.futures import ThreadPoolExecutor
from datetime import datetime
import ipaddress
import math
import re
import shlex
import socket
import threading
import uuid

import docker
import structlog

log = structlog.get_logger()

NETWORK_NAME = "collaborative_edge_mesh_mesh"
IMAGE_NAME = "collaborative_edge_mesh-simulator:latest"  # built by docker compose
SUBNET = "172.28.0"

NETWORK_PROFILE_SETTINGS = {
    "clean_lan": {
        "delay_ms": 1.0,
        "jitter_ms": 0.2,
        "loss_percent": 0.0,
        "bandwidth_mbps": 1000.0,
    },
    "constrained_wifi": {
        "delay_ms": 50.0,
        "jitter_ms": 15.0,
        "loss_percent": 2.0,
        "bandwidth_mbps": 10.0,
    },
    "degraded": {
        "delay_ms": 300.0,
        "jitter_ms": 100.0,
        "loss_percent": 10.0,
        "bandwidth_mbps": 2.0,
    },
    "severe": {
        "delay_ms": 600.0,
        "jitter_ms": 200.0,
        "loss_percent": 20.0,
        "bandwidth_mbps": 0.5,
    },
}


BYPASS_CLASS_RATE = "10gbit"


def build_network_profile_commands(profile: str, bypass_ip: str) -> list[str]:
    """Build tc commands that impair mesh egress while exempting the evaluator.

    Unmatched traffic lands in the impaired default class 1:10; the u32 filter
    steers packets addressed to ``bypass_ip`` into the unimpaired class 1:1.
    """
    settings = NETWORK_PROFILE_SETTINGS[profile]
    bandwidth = settings["bandwidth_mbps"]
    delay = settings["delay_ms"]
    jitter = settings["jitter_ms"]
    loss = settings["loss_percent"]
    return [
        "tc qdisc del dev eth0 root 2>/dev/null || true",
        "tc qdisc add dev eth0 root handle 1: htb default 10",
        (
            "tc class add dev eth0 parent 1: classid 1:1 htb "
            f"rate {BYPASS_CLASS_RATE} ceil {BYPASS_CLASS_RATE}"
        ),
        (
            "tc class add dev eth0 parent 1: classid 1:10 htb "
            f"rate {bandwidth:g}mbit ceil {bandwidth:g}mbit burst 32k"
        ),
        (
            "tc qdisc add dev eth0 parent 1:10 handle 10: netem "
            f"delay {delay:g}ms {jitter:g}ms loss {loss:g}%"
        ),
        (
            "tc filter add dev eth0 parent 1: protocol ip prio 1 u32 "
            f"match ip dst {shlex.quote(bypass_ip + '/32')} flowid 1:1"
        ),
    ]


def _time_to_ms(value: str, unit: str) -> float:
    return (
        float(value)
        * {
            "ns": 0.000001,
            "us": 0.001,
            "ms": 1.0,
            "s": 1000.0,
        }[unit.lower()]
    )


def _rate_to_mbps(value: str, unit: str) -> float:
    return (
        float(value)
        * {
            "bit": 0.000001,
            "kbit": 0.001,
            "mbit": 1.0,
            "gbit": 1000.0,
        }[unit.lower()]
    )


def _htb_class_line(class_text: str, class_id: str) -> str | None:
    match = re.search(
        rf"^class htb {re.escape(class_id)}(?![0-9a-f]).*$", class_text, re.MULTILINE
    )
    return match.group(0) if match else None


def _has_bypass_filter(filter_text: str, bypass_ip: str) -> bool:
    try:
        hex_ip = ipaddress.IPv4Address(bypass_ip).packed.hex()
    except ValueError:
        return False
    match_pattern = re.compile(rf"\bmatch\s+{hex_ip}/ffffffff\s+at\s+16\b")
    flowid_pattern = re.compile(r"\bflowid\s+1:1(?![0-9a-f])")
    entries = re.split(r"^(?=filter\s)", filter_text, flags=re.MULTILINE)
    return any(
        match_pattern.search(entry) and flowid_pattern.search(entry)
        for entry in entries
    )


def verify_network_profile_state(
    settings: dict[str, float],
    qdisc_output: str,
    class_output: str,
    filter_output: str,
    bypass_ip: str,
) -> bool:
    qdisc_text = qdisc_output.lower()
    class_text = class_output.lower()
    filter_text = filter_output.lower()
    if "netem" not in qdisc_text:
        return False

    impaired_class = _htb_class_line(class_text, "1:10")
    if impaired_class is None or _htb_class_line(class_text, "1:1") is None:
        return False
    if not _has_bypass_filter(filter_text, bypass_ip):
        return False

    delay_match = re.search(
        r"\bdelay\s+(\d+(?:\.\d+)?)\s*(ns|us|ms|s)"
        r"(?:\s+(\d+(?:\.\d+)?)\s*(ns|us|ms|s))?",
        qdisc_text,
    )
    rate_match = re.search(
        r"\brate\s+(\d+(?:\.\d+)?)\s*(bit|kbit|mbit|gbit)", impaired_class
    )
    ceil_match = re.search(
        r"\bceil\s+(\d+(?:\.\d+)?)\s*(bit|kbit|mbit|gbit)", impaired_class
    )
    if delay_match is None or rate_match is None or ceil_match is None:
        return False

    delay_ms = _time_to_ms(delay_match.group(1), delay_match.group(2))
    jitter_ms = (
        _time_to_ms(delay_match.group(3), delay_match.group(4))
        if delay_match.group(3) is not None
        else 0.0
    )
    loss_match = re.search(r"\bloss\s+(\d+(?:\.\d+)?)%", qdisc_text)
    loss_percent = float(loss_match.group(1)) if loss_match else 0.0
    rate_mbps = _rate_to_mbps(rate_match.group(1), rate_match.group(2))
    ceil_mbps = _rate_to_mbps(ceil_match.group(1), ceil_match.group(2))

    return all(
        (
            math.isclose(delay_ms, settings["delay_ms"], abs_tol=0.001),
            math.isclose(jitter_ms, settings["jitter_ms"], abs_tol=0.001),
            math.isclose(loss_percent, settings["loss_percent"], abs_tol=0.001),
            math.isclose(rate_mbps, settings["bandwidth_mbps"], abs_tol=0.001),
            math.isclose(ceil_mbps, settings["bandwidth_mbps"], abs_tol=0.001),
        )
    )


def _decode(output) -> str:
    return output.decode(errors="replace") if isinstance(output, bytes) else str(output)


class DockerManager:
    """Manages edge node containers via the Docker API."""

    def __init__(self, gateway_service=None):
        self.client = docker.from_env()
        self.gateway = gateway_service
        self.managed_nodes = {}  # node_id -> container info
        self._next_ip_suffix = 20  # start dynamic nodes at .20
        self._next_host_port = 8010  # fallback for non-numeric node IDs
        self.isolated_nodes = set()  # track container names of isolated nodes
        # TTL cache for list operations
        self._nodes_cache = None
        self._nodes_cache_time = 0.0
        self._gateways_cache = None
        self._gateways_cache_time = 0.0
        self._CACHE_TTL = 2.0  # seconds

        # Concurrency safety
        self._creation_lock = threading.Lock()
        self._reserved_nodes = set()
        self._reserved_ports = set()

    def _next_ip(self):
        ip = f"{SUBNET}.{self._next_ip_suffix}"
        self._next_ip_suffix += 1
        return ip

    def _list_all_containers(self):
        try:
            return self.client.containers.list(all=True)
        except TypeError:
            # Some lightweight Docker client fakes may not support keyword args.
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

        numbers.update(self._reserved_nodes)
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

        ports.update(self._reserved_ports)
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
        """Return the exact image name built by this project's docker-compose."""
        return IMAGE_NAME

    def invalidate_cache(self):
        """Invalidate cached node/gateway lists (call after mutations)."""
        self._nodes_cache = None
        self._gateways_cache = None

    def list_nodes(self):
        """List all running edge node containers (both static and dynamic)."""
        import time as _time

        now = _time.time()
        if (
            self._nodes_cache is not None
            and (now - self._nodes_cache_time) < self._CACHE_TTL
        ):
            return self._nodes_cache

        nodes = []
        for container in self.client.containers.list():
            if "edge-node" in container.name:
                managed_info = self.managed_nodes.get(container.name, {})
                node_id = self._node_id_from_container_name(container.name)
                host_port = managed_info.get("host_port")
                if host_port is None:
                    host_port = self._host_port_for_node_name(container.name)

                # Use IP address directly to avoid Docker DNS issues at scale
                internal_url = self._internal_url_for_container_name(container.name)
                container_ip = managed_info.get("ip")
                if not container_ip:
                    try:
                        container.reload()
                        networks = container.attrs.get("NetworkSettings", {}).get(
                            "Networks", {}
                        )
                        for net_name, net_info in networks.items():
                            if "mesh" in net_name and net_info.get("IPAddress"):
                                container_ip = net_info["IPAddress"]
                                break
                    except Exception:
                        pass
                if container_ip:
                    internal_url = f"http://{container_ip}:8000"

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
                        "internal_url": internal_url,
                    }
                )
        self._nodes_cache = nodes
        self._nodes_cache_time = now
        return nodes

    def list_gateways(self):
        """List all running gateway containers with internal service URLs."""
        import time as _time

        now = _time.time()
        if (
            self._gateways_cache is not None
            and (now - self._gateways_cache_time) < self._CACHE_TTL
        ):
            return self._gateways_cache

        gateways = []
        for container in self.client.containers.list():
            if "gateway" not in container.name or "simulator" in container.name:
                continue

            host_port = None
            host_config = container.attrs.get("HostConfig") or {}
            bindings = host_config.get("PortBindings") or {}

            # Find the port binding for 8000/tcp
            port_8000 = bindings.get("8000/tcp")
            if port_8000:
                host_port = int(port_8000[0].get("HostPort", 0))

            # Use IP address directly to avoid Docker DNS issues at scale
            internal_url = f"http://{container.name}:8000"
            try:
                container.reload()
                networks = container.attrs.get("NetworkSettings", {}).get(
                    "Networks", {}
                )
                for net_name, net_info in networks.items():
                    if "mesh" in net_name and net_info.get("IPAddress"):
                        internal_url = f"http://{net_info['IPAddress']}:8000"
                        break
            except Exception:
                pass

            gateways.append(
                {
                    "name": container.name,
                    "status": container.status,
                    "id": container.short_id,
                    "url": internal_url,
                    "host_port": host_port,
                    "external_url": f"http://localhost:{host_port}"
                    if host_port
                    else None,
                }
            )

        gateways.sort(key=lambda item: item["name"])
        self._gateways_cache = gateways
        self._gateways_cache_time = now
        return gateways

    def _existing_gateway_numbers(self):
        numbers = set()
        for container in self._list_all_containers():
            if "gateway" in container.name and "simulator" not in container.name:
                num_str = container.name.replace("gateway-", "").replace("gateway", "")
                if num_str == "":
                    numbers.add(1)
                elif num_str.isdigit():
                    numbers.add(int(num_str))
        return numbers

    def _next_available_gateway_id(self):
        used = self._existing_gateway_numbers()
        candidate = 1
        while candidate in used:
            candidate += 1
        return f"gateway-{candidate}"

    def _select_gateway_host_port(self, gateway_id):
        gw_num = gateway_id.replace("gateway-", "").replace("gateway", "")
        if gw_num == "":
            gw_num = "1"
        try:
            preferred = (
                7000 + int(gw_num) - 1
            )  # e.g. gateway -> 7000, gateway-2 -> 7001
        except ValueError:
            preferred = None

        used_ports = self._existing_published_ports()
        if preferred is not None and preferred not in used_ports:
            return preferred

        candidate = 7000
        while candidate in used_ports:
            candidate += 1
        return candidate

    def create_gateway(self, gateway_id=None):
        """Spin up a new gateway container."""
        if gateway_id is None:
            gateway_id = self._next_available_gateway_id()

        container_name = gateway_id

        try:
            existing_container = self.client.containers.get(container_name)
            if existing_container.status == "running":
                host_port = self._select_gateway_host_port(gateway_id)
                return self._action_response(
                    action="create_gateway",
                    target=container_name,
                    status="already_exists",
                    message=f"{container_name} already exists",
                    gateway_id=gateway_id,
                    host_port=host_port,
                    url=f"http://localhost:{host_port}" if host_port else None,
                )
            existing_container.remove(force=True)
        except docker.errors.NotFound:
            pass

        http_port = 8000
        host_port = self._select_gateway_host_port(gateway_id)
        image = self._get_image_name()
        network = self._get_network()

        self.client.containers.run(
            image,
            detach=True,
            name=container_name,
            command="python -m src.gateway_main",
            environment={
                "NODE_ID": gateway_id,
                "HTTP_PORT": str(http_port),
                "GATEWAY_POLL_INTERVAL": "10",
                "DATA_DIR": "/data",
            },
            ports={f"{http_port}/tcp": host_port},
            network=network.name,
        )

        log.info("gateway_created", gateway_id=gateway_id, host_port=host_port)

        result = self._action_response(
            action="create_gateway",
            target=container_name,
            status="created",
            message=f"Created {container_name}",
            gateway_id=gateway_id,
            host_port=host_port,
            url=f"http://localhost:{host_port}",
            internal_url=f"http://{container_name}:{http_port}",
        )
        self.invalidate_cache()
        return result

    def remove_gateway(self, gateway_id):
        """Stop and remove a gateway container."""
        self.invalidate_cache()
        try:
            container = self.client.containers.get(gateway_id)
            container.stop(timeout=5)
            container.remove()
            container.stop(timeout=5)
            container.remove()
        except docker.errors.NotFound:
            return self._action_response(
                action="remove_gateway",
                target=gateway_id,
                status="not_found",
                message=f"{gateway_id} not found",
            )

        log.info("gateway_removed", gateway_id=gateway_id)
        return self._action_response(
            action="remove_gateway",
            target=gateway_id,
            status="removed",
            message=f"Removed {gateway_id}",
        )

    def create_node(
        self,
        node_id=None,
        assigned_gateway="",
        sync_mode="full_state",
        event_id_retention_limit=100,
        delta_log_limit=1000,
        cpu_limit=None,
        memory_limit=None,
    ):
        """Spin up a new edge node container and register it with the gateway."""
        with self._creation_lock:
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

            # reservation tracking
            node_num_str = node_id.replace("node-", "")
            if node_num_str.isdigit():
                self._reserved_nodes.add(int(node_num_str))
            if host_port:
                self._reserved_ports.add(host_port)

            image = self._get_image_name()
            network = self._get_network()

        run_options = {}
        if cpu_limit is not None:
            run_options["nano_cpus"] = int(float(cpu_limit) * 1_000_000_000)
        if memory_limit:
            run_options["mem_limit"] = memory_limit

        # Execute docker.run OUTSIDE the lock to allow concurrent image fetches & daemon processing
        container = self.client.containers.run(
            image,
            detach=True,
            name=container_name,
            environment={
                "NODE_ID": node_id,
                "HTTP_PORT": str(http_port),
                "GOSSIP_PORT": "9000",
                "GOSSIP_INTERVAL": "5",
                "ASSIGNED_GATEWAY": assigned_gateway,
                "SYNC_MODE": sync_mode,
                "EVENT_ID_RETENTION_LIMIT": str(event_id_retention_limit),
                "DELTA_LOG_LIMIT": str(delta_log_limit),
            },
            ports={f"{http_port}/tcp": host_port},
            network=network.name,
            cap_add=["NET_ADMIN"],
            **run_options,
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
        self.invalidate_cache()

        # register with gateway for polling
        if self.gateway is not None:
            self.gateway.register_node(
                container_name, f"http://{actual_ip}:{http_port}"
            )

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
        )

    def remove_node(self, node_id):
        """Stop and remove a node container."""
        container_name = f"edge-node-{node_id.replace('node-', '')}"
        node_number = self._node_number_from_name(container_name)
        managed_info = self.managed_nodes.get(container_name, {})
        host_port = managed_info.get("host_port") or self._host_port_for_node_name(
            container_name
        )
        self.invalidate_cache()

        try:
            container = self.client.containers.get(container_name)
            container.stop(timeout=5)
            container.remove()
        except docker.errors.NotFound:
            if node_number is not None:
                self._reserved_nodes.discard(node_number)
            if host_port is not None:
                self._reserved_ports.discard(host_port)
            return self._action_response(
                action="remove_node",
                target=container_name,
                status="not_found",
                message=f"{container_name} not found",
            )

        self.managed_nodes.pop(container_name, None)
        self.isolated_nodes.discard(container_name)
        if node_number is not None:
            self._reserved_nodes.discard(node_number)
        if host_port is not None:
            self._reserved_ports.discard(host_port)
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
        """Isolate gossip and gateway connectivity while keeping simulator control access."""
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

            # 1) Cut gossip traffic entirely (UDP mesh traffic).
            container.exec_run("iptables -A INPUT -p udp -j DROP")
            container.exec_run("iptables -A OUTPUT -p udp -j DROP")

            # 2) Cut TCP only between this node and gateway container IPs.
            for gateway in self.list_gateways():
                gateway_url = gateway.get("url") or ""
                gateway_ip = ""
                if gateway_url.startswith("http://"):
                    gateway_ip = gateway_url.replace("http://", "").split(":")[0]

                if gateway_ip:
                    container.exec_run(
                        f"iptables -A INPUT -p tcp -s {gateway_ip} -j DROP"
                    )
                    container.exec_run(
                        f"iptables -A OUTPUT -p tcp -d {gateway_ip} -j DROP"
                    )

            self.isolated_nodes.add(container_name)
            self.invalidate_cache()
            log.info("node_isolated", node_id=node_id)
            return self._action_response(
                action="isolate_node",
                target=container_name,
                status="isolated",
                message=f"Isolated {container_name} (UDP blocked + TCP to/from gateway blocked)",
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
            self.invalidate_cache()
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

    def _edge_containers(self):
        return [c for c in self.client.containers.list() if "edge-node" in c.name]

    def _run_node_scripts(self, jobs):
        """Run one ``sh -c`` script per container concurrently.

        Returns ``(container, exit_code, output)`` tuples in job order.
        """
        if not jobs:
            return []

        def run(job):
            container, script = job
            exit_code, output = container.exec_run(["sh", "-c", script])
            return container, exit_code, _decode(output)

        with ThreadPoolExecutor(max_workers=len(jobs)) as executor:
            return list(executor.map(run, jobs))

    def _evaluator_mesh_ip(self):
        """Return the mesh-network IP of the container this process runs in."""
        hostname = socket.gethostname()
        try:
            container = self.client.containers.get(hostname)
        except docker.errors.DockerException as error:
            log.warning(
                "evaluator_container_lookup_failed", hostname=hostname, error=str(error)
            )
            return None
        networks = (container.attrs.get("NetworkSettings") or {}).get("Networks") or {}
        for net_name, net_info in networks.items():
            if "mesh" in net_name and net_info.get("IPAddress"):
                return net_info["IPAddress"]
        log.warning(
            "evaluator_mesh_ip_missing", hostname=hostname, networks=sorted(networks)
        )
        return None

    def heal_all(self):
        """Heal all running edge nodes."""
        script = "iptables -F INPUT && iptables -F OUTPUT"
        outcomes = self._run_node_scripts(
            [(container, script) for container in self._edge_containers()]
        )
        results = []
        for container, exit_code, output in outcomes:
            if exit_code != 0:
                log.warning(
                    "heal_all_node_failed",
                    node=container.name,
                    exit_code=exit_code,
                    output=output,
                )
            results.append(container.name)

        self.isolated_nodes.clear()
        self.invalidate_cache()
        log.info("all_healed", nodes=results)
        return self._action_response(
            action="heal_all",
            target="mesh",
            status="healed",
            message="Healed all edge nodes",
            nodes=results,
        )

    def create_split_brain(self, majority_ratio=0.5):
        """Partition the mesh into two groups using a configurable A/B split."""
        nodes = sorted(self._edge_containers(), key=lambda c: c.name)
        if len(nodes) < 2:
            return self._action_response(
                action="split_brain",
                target="mesh",
                status="failed",
                message="Need at least 2 nodes",
            )

        ratio = max(0.0, min(float(majority_ratio), 1.0))
        split_index = round(len(nodes) * ratio)
        split_index = max(1, min(split_index, len(nodes) - 1))
        group_a = nodes[:split_index]
        group_b = nodes[split_index:]

        # get IPs for each group
        def get_ip(container):
            networks = container.attrs["NetworkSettings"]["Networks"]
            for net_name, net_info in networks.items():
                if "mesh" in net_name:
                    return net_info["IPAddress"]
            return None

        ips_a = [get_ip(c) for c in group_a]
        ips_b = [get_ip(c) for c in group_b]

        applied = True
        jobs = []
        for container, opposite_ips in (
            *((container, ips_b) for container in group_a),
            *((container, ips_a) for container in group_b),
        ):
            if not all(opposite_ips):
                applied = False
            rules = [
                f"{direction} {address_flag} {shlex.quote(ip)} -j DROP"
                for ip in opposite_ips
                if ip
                for direction, address_flag in (("INPUT", "-s"), ("OUTPUT", "-d"))
            ]
            if not rules:
                continue
            commands = [f"iptables -A {rule}" for rule in rules] + [
                f"iptables -C {rule}" for rule in rules
            ]
            jobs.append((container, " && ".join(commands)))

        results = []
        for (container, exit_code, output), (_, script) in zip(
            self._run_node_scripts(jobs), jobs
        ):
            if exit_code != 0:
                applied = False
                log.warning(
                    "split_brain_node_failed",
                    node=container.name,
                    exit_code=exit_code,
                    output=output,
                )
            results.append(
                {
                    "node": container.name,
                    "command": script,
                    "exit_code": exit_code,
                    "output": output,
                }
            )

        if applied:
            for container in nodes:
                self.isolated_nodes.add(container.name)
            log.info(
                "split_brain_created",
                group_a=[container.name for container in group_a],
                group_b=[container.name for container in group_b],
            )

        return self._action_response(
            action="split_brain",
            target="mesh",
            status="split_brain" if applied else "failed",
            message=(
                "Created and verified split-brain partition"
                if applied
                else "Failed to apply every split-brain rule"
            ),
            partition_ratio=ratio,
            group_a=[container.name for container in group_a],
            group_b=[container.name for container in group_b],
            results=results,
        )

    def apply_netem_profile(self, profile: str):
        """Apply and verify a supported traffic-control profile on every edge node.

        Only mesh traffic is impaired: node egress to the evaluator container
        (the one this process runs in) bypasses the htb/netem shaping.
        """
        if profile not in NETWORK_PROFILE_SETTINGS:
            return self._action_response(
                action="apply_netem_profile",
                target="mesh",
                status="failed",
                message=f"Unsupported network profile: {profile}",
                profile=profile,
            )

        settings = NETWORK_PROFILE_SETTINGS[profile]
        containers = self._edge_containers()
        if not containers:
            return self._action_response(
                action="apply_netem_profile",
                target="mesh",
                status="failed",
                message=f"No edge nodes to apply {profile} to",
                profile=profile,
                settings=dict(settings),
                results=[],
            )

        bypass_ip = self._evaluator_mesh_ip()
        if bypass_ip is None:
            return self._action_response(
                action="apply_netem_profile",
                target="mesh",
                status="failed",
                message=(
                    f"Cannot apply {profile}: evaluator mesh IP for the tc bypass "
                    f"could not be resolved from container {socket.gethostname()}"
                ),
                profile=profile,
                settings=dict(settings),
                results=[],
            )

        setup_command = " && ".join(build_network_profile_commands(profile, bypass_ip))
        show_commands = (
            "tc qdisc show dev eth0",
            "tc class show dev eth0",
            "tc filter show dev eth0",
        )
        run_marker = uuid.uuid4().hex
        markers = [
            f"__cem_tc_{run_marker}_{index}__" for index in range(len(show_commands))
        ]
        script = " && ".join(
            [
                setup_command,
                *(
                    f"echo {marker} && {command}"
                    for marker, command in zip(markers, show_commands)
                ),
            ]
        )

        results = []
        applied = True
        for container, exit_code, output in self._run_node_scripts(
            [(container, script) for container in containers]
        ):
            sections = []
            rest = output
            for marker in markers:
                head, found, tail = rest.partition(f"{marker}\n")
                if not found:
                    break
                sections.append(head)
                rest = tail
            sections.append(rest)

            setup_ok = len(sections) > 1
            shows = sections[1:]
            settings_verified = (
                exit_code == 0
                and len(shows) == len(show_commands)
                and verify_network_profile_state(settings, *shows, bypass_ip)
            )
            if not settings_verified:
                applied = False
                log.warning(
                    "netem_profile_node_failed",
                    node=container.name,
                    profile=profile,
                    bypass_ip=bypass_ip,
                    exit_code=exit_code,
                    setup_ok=setup_ok,
                    output=output,
                )
            results.append(
                {
                    "node": container.name,
                    "commands": [
                        {
                            "command": setup_command,
                            "exit_code": 0 if setup_ok else exit_code,
                            "output": sections[0],
                        }
                    ],
                    "verification": [
                        {
                            "command": command,
                            "exit_code": 0 if index < len(shows) - 1 else exit_code,
                            "output": show_output,
                            "verified": settings_verified,
                        }
                        for index, (command, show_output) in enumerate(
                            zip(show_commands, shows)
                        )
                    ],
                }
            )

        return self._action_response(
            action="apply_netem_profile",
            target="mesh",
            status="applied" if applied else "failed",
            message=(
                f"Applied and verified {profile}"
                if applied
                else f"Failed to apply {profile} to every edge node"
            ),
            profile=profile,
            settings=dict(settings),
            bypass_ip=bypass_ip,
            results=results,
        )

    def clear_netem(self):
        """Remove tc/netem qdisc from all dynamic edge nodes."""
        outcomes = self._run_node_scripts(
            [
                (container, "tc qdisc del dev eth0 root")
                for container in self._edge_containers()
            ]
        )
        results = [
            {"node": container.name, "exit_code": exit_code, "output": output}
            for container, exit_code, output in outcomes
        ]
        return self._action_response(
            action="clear_netem",
            target="mesh",
            status="cleared",
            message="Cleared netem qdisc",
            results=results,
        )

    def update_node_resources(self, assignments: list[dict]) -> dict:
        """Change CPU/memory limits of running edge nodes in place, in parallel.

        docker-py's ``Container.update`` cannot set NanoCpus, so the Engine
        ``/containers/{id}/update`` endpoint is called directly. MemorySwap is
        set to twice Memory, matching Docker's default for ``mem_limit``-only
        containers created by ``create_node``.
        """
        api = self.client.api

        def update(assignment):
            name = assignment["container"]
            body = {}
            if assignment.get("cpus") is not None:
                body["NanoCpus"] = int(round(float(assignment["cpus"]) * 1_000_000_000))
            if assignment.get("memory_mib") is not None:
                memory_bytes = int(round(float(assignment["memory_mib"]) * 1024 * 1024))
                body["Memory"] = memory_bytes
                body["MemorySwap"] = 2 * memory_bytes
            if not body:
                return {"node": name, "ok": True, "error": None}
            try:
                response = api.post(api._url("/containers/{0}/update", name), json=body)
                api._raise_for_status(response)
            except Exception as error:
                log.warning(
                    "node_resource_update_failed",
                    node=name,
                    body=body,
                    error=str(error),
                )
                return {"node": name, "ok": False, "error": str(error)}
            return {"node": name, "ok": True, "error": None}

        if assignments:
            with ThreadPoolExecutor(max_workers=len(assignments)) as executor:
                results = list(executor.map(update, assignments))
        else:
            results = []
        self.invalidate_cache()

        failed = [result["node"] for result in results if not result["ok"]]
        if not failed:
            log.info("node_resources_updated", nodes=[r["node"] for r in results])
        return self._action_response(
            action="update_node_resources",
            target="mesh",
            status="failed" if failed else "updated",
            message=(
                f"Failed to update resource limits on {', '.join(failed)}"
                if failed
                else f"Updated resource limits on {len(results)} edge nodes"
            ),
            results=results,
        )

    def inspect_node_resource_limits(
        self, node_ids: list[str]
    ) -> dict[str, dict[str, float | int | bool | str | None]]:
        """Return configured limits and runtime state for the requested edge nodes."""
        inspected = {}
        for node_id in node_ids:
            container_name = f"edge-node-{node_id.replace('node-', '')}"
            try:
                container = self.client.containers.get(container_name)
                container.reload()
                host_config = container.attrs.get("HostConfig") or {}
                state = container.attrs.get("State") or {}
                nano_cpus = int(host_config.get("NanoCpus") or 0)
                inspected[container_name] = {
                    "node_id": node_id,
                    "cpus": nano_cpus / 1_000_000_000 if nano_cpus else None,
                    "memory_bytes": int(host_config.get("Memory") or 0),
                    "running": bool(state.get("Running")),
                    "oom_killed": bool(state.get("OOMKilled")),
                    "status": str(state.get("Status") or container.status),
                }
            except docker.errors.NotFound:
                inspected[container_name] = {
                    "node_id": node_id,
                    "cpus": None,
                    "memory_bytes": 0,
                    "running": False,
                    "oom_killed": False,
                    "status": "missing",
                }
        return inspected

    def resource_snapshot(self) -> list[dict[str, float | int | str]]:
        """Return one resource-counter sample for each running edge node."""
        samples = []
        for container in self.client.containers.list():
            if "edge-node" not in container.name:
                continue
            try:
                try:
                    stats = container.stats(stream=False, one_shot=True)
                except TypeError:
                    stats = container.stats(stream=False)

                cpu_stats = stats.get("cpu_stats") or {}
                cpu_usage = cpu_stats.get("cpu_usage") or {}
                online_cpus = cpu_stats.get("online_cpus") or len(
                    cpu_usage.get("percpu_usage") or []
                )

                throttling_data = cpu_stats.get("throttling_data") or {}
                memory_stats = stats.get("memory_stats") or {}
                memory_detail = memory_stats.get("stats") or {}
                cache = memory_detail.get(
                    "inactive_file", memory_detail.get("cache", 0)
                )
                memory_bytes = max(memory_stats.get("usage", 0) - cache, 0)

                network_rx_bytes = 0
                network_tx_bytes = 0
                for network in (stats.get("networks") or {}).values():
                    network_rx_bytes += network.get("rx_bytes", 0)
                    network_tx_bytes += network.get("tx_bytes", 0)

                block_read_bytes = 0
                block_write_bytes = 0
                entries = (
                    stats.get("blkio_stats", {}).get("io_service_bytes_recursive") or []
                )
                for entry in entries:
                    operation = str(entry.get("op", "")).lower()
                    if operation == "read":
                        block_read_bytes += entry.get("value", 0)
                    elif operation == "write":
                        block_write_bytes += entry.get("value", 0)

                samples.append(
                    {
                        "node": container.name,
                        "cpu_usage_ns": int(cpu_usage.get("total_usage", 0)),
                        "system_cpu_usage_ns": int(
                            cpu_stats.get("system_cpu_usage", 0)
                        ),
                        "online_cpus": max(int(online_cpus), 1),
                        "cpu_periods": int(throttling_data.get("periods", 0)),
                        "cpu_throttled_periods": int(
                            throttling_data.get("throttled_periods", 0)
                        ),
                        "cpu_throttled_time_ns": int(
                            throttling_data.get("throttled_time", 0)
                        ),
                        "memory_bytes": memory_bytes,
                        "memory_limit_bytes": memory_stats.get("limit", 0),
                        "network_rx_bytes": network_rx_bytes,
                        "network_tx_bytes": network_tx_bytes,
                        "block_read_bytes": block_read_bytes,
                        "block_write_bytes": block_write_bytes,
                    }
                )
            except Exception as error:
                log.warning(
                    "node_resource_snapshot_failed",
                    node=container.name,
                    error=str(error),
                )
        return samples
