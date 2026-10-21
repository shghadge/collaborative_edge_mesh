"""Simulator control plane: gateway discovery, node-to-gateway assignment sync, and gateway request proxying."""

import asyncio
import time
from typing import Optional

import aiohttp


class SimulatorControlService:
    """Control-plane helper service for simulator orchestration paths."""

    def __init__(self, docker_manager, api_error, logger):
        self.docker_manager = docker_manager
        self._api_error = api_error
        self.log = logger
        self.user_gateway_assignments: dict[str, str] = {}
        self._last_sync_time: float = 0.0
        self._sync_debounce_seconds = 5.0
        self._sync_lock: Optional[asyncio.Lock] = None
        self._last_sync_fingerprint: str = ""

    def normalize_node_id(self, node_id: str) -> str:
        """Map a container-style `edge-node-N` name to the `node-N` node ID; other values pass through."""
        if node_id.startswith("edge-node-"):
            suffix = node_id.replace("edge-node-", "", 1)
            return f"node-{suffix}"
        return node_id

    def candidate_urls(self, *urls: Optional[str]) -> list[str]:
        """Return the given URLs in order, dropping empty values and duplicates."""
        ordered: list[str] = []
        for url in urls:
            if not url:
                continue
            if url not in ordered:
                ordered.append(url)
        return ordered

    def require_docker(self):
        """Raise a 503 API error if Docker is not available."""
        if self.docker_manager is None:
            raise self._api_error(503, "DOCKER_UNAVAILABLE", "Docker not available")

    async def discover_gateways(self):
        """List running gateway containers via Docker, raising a 503 API error if there are none."""
        self.require_docker()
        gateways = await asyncio.to_thread(self.docker_manager.list_gateways)
        if not gateways:
            raise self._api_error(
                503, "NO_GATEWAY", "No running gateway containers discovered"
            )
        return gateways

    def build_node_gateway_map(
        self,
        valid_nodes: list[dict],
        gateways: list[dict],
        *,
        persist_auto_assignments: bool,
    ) -> dict[str, str]:
        """Map node IDs to gateway names, keeping valid user assignments and round-robining the rest.

        With `persist_auto_assignments`, assignments for nodes no longer present are dropped
        and the computed map is saved into `user_gateway_assignments`.
        """
        if not gateways:
            return {}

        gateway_names = [gateway["name"] for gateway in gateways]
        node_to_gateway: dict[str, str] = {}
        unassigned_nodes: list[dict] = []

        for node in valid_nodes:
            node_id = node.get("node_id")
            if not node_id:
                continue

            assigned_gateway = self.user_gateway_assignments.get(node_id)
            if assigned_gateway and assigned_gateway in gateway_names:
                node_to_gateway[node_id] = assigned_gateway
            else:
                unassigned_nodes.append(node)

        for i, node in enumerate(unassigned_nodes):
            node_id = node.get("node_id")
            if not node_id:
                continue
            gateway_name = gateways[i % len(gateways)]["name"]
            node_to_gateway[node_id] = gateway_name

        if persist_auto_assignments:
            valid_node_ids = {
                node.get("node_id") for node in valid_nodes if node.get("node_id")
            }
            for node_id in list(self.user_gateway_assignments.keys()):
                if node_id not in valid_node_ids:
                    self.user_gateway_assignments.pop(node_id, None)
            self.user_gateway_assignments.update(node_to_gateway)

        return node_to_gateway

    async def sync_gateways_with_nodes(self, force: bool = False):
        """Sync gateway-node assignments. Debounced to avoid hammering."""
        self.require_docker()

        if self._sync_lock is None:
            self._sync_lock = asyncio.Lock()

        now = time.time()
        if not force and (now - self._last_sync_time) < self._sync_debounce_seconds:
            return

        async with self._sync_lock:
            if (
                not force
                and (time.time() - self._last_sync_time) < self._sync_debounce_seconds
            ):
                return

            nodes = await asyncio.to_thread(self.docker_manager.list_nodes)
            valid_nodes = [
                node
                for node in nodes
                if node.get("node_id") and (node.get("internal_url") or node.get("url"))
            ]
            valid_nodes.sort(key=lambda item: item["name"])

            try:
                gateways = await self.discover_gateways()
            except Exception:
                gateways = []

            if not gateways:
                return

            fingerprint_parts = sorted(
                f"{node.get('node_id')}:{self.user_gateway_assignments.get(node.get('node_id'), '')}"
                for node in valid_nodes
            )
            fingerprint = (
                "|".join(fingerprint_parts)
                + "||"
                + "|".join(gateway["name"] for gateway in gateways)
            )
            if not force and fingerprint == self._last_sync_fingerprint:
                self._last_sync_time = time.time()
                return

            assignments = {gateway["name"]: [] for gateway in gateways}
            node_to_gateway = self.build_node_gateway_map(
                valid_nodes,
                gateways,
                persist_auto_assignments=True,
            )

            for node in valid_nodes:
                gateway_name = node_to_gateway.get(node.get("node_id"))
                if not gateway_name:
                    continue
                assignments[gateway_name].append(
                    {
                        "node_id": node.get("node_id"),
                        "url": node.get("internal_url") or node.get("url"),
                        "internal_url": node.get("internal_url"),
                        "external_url": node.get("url"),
                    }
                )

            timeout = aiohttp.ClientTimeout(total=6)
            connector = aiohttp.TCPConnector(limit=50)
            async with aiohttp.ClientSession(
                timeout=timeout, connector=connector
            ) as session:

                async def _sync_gateway(gateway):
                    """POST the gateway's node list to /gateway/nodes/sync, trying each of its URLs."""
                    payload = {"nodes": assignments[gateway["name"]]}
                    for base_url in self.candidate_urls(
                        gateway.get("url"), gateway.get("external_url")
                    ):
                        url = f"{base_url}/gateway/nodes/sync"
                        try:
                            async with session.post(url, json=payload) as response:
                                if response.status < 400:
                                    return
                        except Exception:
                            pass
                    self.log.debug("gateway_sync_unreachable", gateway=gateway["name"])

                await asyncio.gather(
                    *[_sync_gateway(gateway) for gateway in gateways],
                    return_exceptions=True,
                )

                async def _notify_node(node_info, gateway_name):
                    """Tell a node its assigned gateway, retrying for about 15 seconds."""
                    node_urls = self.candidate_urls(
                        node_info.get("internal_url"),
                        node_info.get("external_url"),
                        node_info.get("url"),
                    )
                    for _ in range(15):
                        for base_url in node_urls:
                            try:
                                async with session.post(
                                    f"{base_url}/config/gateway",
                                    json={"assigned_gateway": gateway_name},
                                ) as response:
                                    if response.status < 400:
                                        return
                            except Exception:
                                pass
                        await asyncio.sleep(1)
                    self.log.warning(
                        "node_config_sync_failed", node=node_info.get("node_id")
                    )

                notify_tasks = []
                for gateway_name, nodes_in_gateway in assignments.items():
                    for node_info in nodes_in_gateway:
                        notify_tasks.append(_notify_node(node_info, gateway_name))
                await asyncio.gather(*notify_tasks, return_exceptions=True)

            self._last_sync_time = time.time()
            self._last_sync_fingerprint = fingerprint

    async def proxy_gateway_request(
        self, method: str, path: str, params: Optional[dict] = None
    ):
        """Forward a request to the first discovered gateway and return its JSON response.

        Tries the gateway's internal URL, container hostname, and external URL in order.
        Raises an API error with the first HTTP failure status, or 503 if none responded.
        """
        gateways = await self.discover_gateways()
        primary = gateways[0]

        timeout_seconds = (
            30 if (method.upper() == "POST" and path == "/gateway/poll") else 8
        )
        timeout = aiohttp.ClientTimeout(total=timeout_seconds)
        async with aiohttp.ClientSession(timeout=timeout) as session:
            errors: list[dict] = []
            candidates = self.candidate_urls(
                primary.get("url"),
                f"http://{primary.get('name')}:8000" if primary.get("name") else None,
                primary.get("external_url"),
            )

            for base_url in candidates:
                try:
                    async with session.request(
                        method,
                        f"{base_url}{path}",
                        params=params,
                    ) as response:
                        if response.status >= 400:
                            body = await response.text()
                            errors.append(
                                {
                                    "url": base_url,
                                    "status": response.status,
                                    "body": body,
                                }
                            )
                            continue
                        return await response.json()
                except Exception as error:
                    error_msg = str(error) or error.__class__.__name__
                    errors.append({"url": base_url, "error": error_msg})

            # If every candidate failed, preserve status context when possible.
            http_failures = [e for e in errors if "status" in e]
            if http_failures:
                first = http_failures[0]
                raise self._api_error(
                    first["status"],
                    "GATEWAY_REQUEST_FAILED",
                    f"Gateway request failed for {path}",
                    {"gateway": primary.get("name"), "attempts": errors},
                )

            raise self._api_error(
                503,
                "GATEWAY_UNREACHABLE",
                "Unable to reach discovered gateway",
                {"gateway": primary.get("name"), "attempts": errors},
            )
