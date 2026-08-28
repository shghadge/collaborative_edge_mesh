import asyncio
import json
import time

import aiohttp
import structlog


log = structlog.get_logger()


class SimulatorObservabilityService:
    def __init__(
        self,
        config,
        discover_gateways,
        list_nodes,
        list_gateways,
        proxy_gateway_request,
    ):
        self.config = config
        self._discover_gateways = discover_gateways
        self._list_nodes = list_nodes
        self._list_gateways = list_gateways
        self._proxy_gateway_request = proxy_gateway_request
        self._live_metrics_cache = None
        self._live_metrics_cache_time = 0.0
        self._timeseries_cache = {}

    def _gateway_candidate_urls(self, gateway: dict) -> list[str]:
        ordered = []
        for url in (
            gateway.get("url"),
            f"http://{gateway.get('name')}:8000" if gateway.get("name") else None,
            gateway.get("external_url"),
        ):
            if not url:
                continue
            if url not in ordered:
                ordered.append(url)
        return ordered

    async def get_live_metrics(self, force_refresh: bool = False) -> dict:
        now = time.monotonic()
        cache_ttl = self.config.observability_cache_ttl_seconds

        if (
            not force_refresh
            and self._live_metrics_cache is not None
            and cache_ttl > 0
            and (now - self._live_metrics_cache_time) < cache_ttl
        ):
            return self._live_metrics_cache

        metrics = await self._build_live_metrics()

        # If gateway probes temporarily fail, keep the last good snapshot instead
        # of flipping UI to synthetic 0%/unknown values.
        if (
            metrics.get("_gateway_probe_ok") is False
            and self._live_metrics_cache is not None
        ):
            fallback = dict(self._live_metrics_cache)
            fallback["_stale_gateway_probe"] = True
            return fallback

        metrics.pop("_gateway_probe_ok", None)
        metrics.pop("_stale_gateway_probe", None)
        self._live_metrics_cache = metrics
        self._live_metrics_cache_time = now
        return metrics

    async def build_ws_payload(self) -> dict:
        payload = {}

        try:
            payload["nodes"] = await self._list_nodes()
        except Exception as error:
            log.debug("observability_nodes_unavailable", error=str(error))

        try:
            payload["gateways"] = await self._list_gateways()
        except Exception as error:
            log.debug("observability_gateways_unavailable", error=str(error))

        try:
            payload["metrics"] = await self.get_live_metrics()
        except Exception as error:
            log.debug("observability_metrics_unavailable", error=str(error))

        return payload

    async def get_timeseries(self, metric: str, limit: int) -> list:
        if self.config.observability_mode == "off":
            return []

        cache_key = f"{metric}:{limit}"
        now = time.monotonic()
        cache_ttl = self.config.observability_timeseries_cache_ttl_seconds
        cached = self._timeseries_cache.get(cache_key)
        if cached is not None and cache_ttl > 0 and (now - cached["time"]) < cache_ttl:
            return cached["value"]

        data = await self._proxy_gateway_request(
            "GET",
            "/gateway/timeseries",
            params={"metric": metric, "limit": limit},
        )
        if not isinstance(data, list):
            data = []

        self._timeseries_cache[cache_key] = {"time": now, "value": data}
        return data

    async def _build_live_metrics(self) -> dict:
        gateway_summary = await self._fetch_gateway_summary()
        nodes = await self._list_nodes()
        valid_nodes = [
            node
            for node in nodes
            if node.get("node_id") and (node.get("internal_url") or node.get("url"))
        ]

        metrics = {
            "runtime_mode": self.config.runtime_mode,
            "observability_mode": self.config.observability_mode,
            "convergence_time_seconds": gateway_summary["max_convergence_time"] or None,
            "is_divergent": gateway_summary["overall_divergent"],
            "known_status": gateway_summary["known_status"],
            "coverage_percent": gateway_summary["coverage_percent"],
            "divergence_duration_seconds": gateway_summary["max_divergence_duration"],
            "data_loss_percent": None if gateway_summary["overall_divergent"] else 0.0,
            "total_gossip_messages_sent": 0,
            "total_gossip_messages_received": 0,
            "total_state_size_bytes": 0,
            "node_count": len(valid_nodes),
            "polls_completed": gateway_summary["total_polls_completed"],
            "per_node": [],
            "_gateway_probe_ok": gateway_summary.get("gateway_probe_ok", True),
        }

        if not self.config.deep_observability_enabled:
            return metrics

        per_node_stats = await self._fetch_per_node_stats(valid_nodes)
        gateway_data_loss = self._compute_gateway_data_loss(per_node_stats)
        data_loss_percent = (
            max(gateway_data_loss)
            if gateway_data_loss and not gateway_summary["overall_divergent"]
            else (None if gateway_summary["overall_divergent"] else 0.0)
        )

        metrics.update(
            {
                "data_loss_percent": data_loss_percent,
                "total_gossip_messages_sent": sum(
                    node.get("gossip_sent", 0) for node in per_node_stats
                ),
                "total_gossip_messages_received": sum(
                    node.get("gossip_received", 0) for node in per_node_stats
                ),
                "total_state_size_bytes": sum(
                    node.get("state_size_bytes", 0) for node in per_node_stats
                ),
                "per_node": per_node_stats,
            }
        )
        return metrics

    async def _fetch_gateway_summary(self) -> dict:
        try:
            gateways = await self._discover_gateways()
        except Exception:
            gateways = []

        overall_divergent = False
        max_divergence_duration = 0.0
        max_convergence_time = 0.0
        total_polls_completed = 0
        status_values = []
        max_coverage = 0.0
        successful_gateway_probes = 0

        if not gateways:
            return {
                "overall_divergent": overall_divergent,
                "max_divergence_duration": max_divergence_duration,
                "max_convergence_time": max_convergence_time,
                "total_polls_completed": total_polls_completed,
                "known_status": "unknown_partial",
                "coverage_percent": 0.0,
                "gateway_probe_ok": False,
            }

        timeout = aiohttp.ClientTimeout(total=3)
        connector = aiohttp.TCPConnector(limit=50)
        async with aiohttp.ClientSession(
            timeout=timeout, connector=connector
        ) as session:

            async def _fetch_gw_metrics(gateway: dict):
                for base_url in self._gateway_candidate_urls(gateway):
                    try:
                        async with session.get(
                            f"{base_url}/gateway/runtime-metrics"
                        ) as response:
                            if response.status >= 400:
                                continue
                            return await response.json()
                    except Exception:
                        continue
                return None

            results = await asyncio.gather(
                *[_fetch_gw_metrics(gateway) for gateway in gateways],
                return_exceptions=True,
            )

        for gateway_data in results:
            if gateway_data is None or isinstance(gateway_data, Exception):
                continue
            successful_gateway_probes += 1
            runtime_metrics = gateway_data.get("runtime_metrics", {})
            gateway_status = gateway_data.get("known_status")
            if gateway_status:
                status_values.append(gateway_status)
            gateway_coverage = gateway_data.get("coverage", 0.0)
            if isinstance(gateway_coverage, (int, float)):
                max_coverage = max(max_coverage, gateway_coverage)
            if gateway_data.get("is_divergent"):
                overall_divergent = True
            max_divergence_duration = max(
                max_divergence_duration,
                runtime_metrics.get("divergence_duration_seconds", 0.0),
            )
            max_convergence_time = max(
                max_convergence_time,
                runtime_metrics.get("last_convergence_seconds", 0.0) or 0.0,
            )
            total_polls_completed += runtime_metrics.get("polls_completed", 0)

        known_status = "known_converged"
        if any(status == "known_diverged" for status in status_values):
            known_status = "known_diverged"
        elif any(status == "unknown_partial" for status in status_values):
            known_status = "unknown_partial"
        elif not status_values:
            known_status = "unknown_partial"

        return {
            "overall_divergent": overall_divergent or known_status == "known_diverged",
            "max_divergence_duration": max_divergence_duration,
            "max_convergence_time": max_convergence_time,
            "total_polls_completed": total_polls_completed,
            "known_status": known_status,
            "coverage_percent": round(max_coverage * 100, 1),
            "gateway_probe_ok": successful_gateway_probes > 0,
        }

    async def _fetch_per_node_stats(self, nodes: list[dict]) -> list[dict]:
        if not nodes:
            return []

        timeout = aiohttp.ClientTimeout(total=3)
        connector = aiohttp.TCPConnector(limit=50)
        async with aiohttp.ClientSession(
            timeout=timeout, connector=connector
        ) as session:
            results = await asyncio.gather(
                *[self._fetch_single_node_stats(session, node) for node in nodes],
                return_exceptions=True,
            )

        per_node_stats = []
        for node_info in results:
            if node_info is None or isinstance(node_info, Exception):
                continue
            per_node_stats.append(node_info)
        return per_node_stats

    async def _fetch_single_node_stats(
        self, session: aiohttp.ClientSession, node: dict
    ) -> dict | None:
        node_url = node.get("internal_url") or node.get("url")
        if not node_url:
            return None

        node_info = {
            "node_id": node.get("node_id"),
            "name": node.get("name"),
            "isolated": node.get("isolated", False),
            "assigned_gateway": node.get("assigned_gateway", "unknown"),
        }

        async def _get_json(path: str):
            try:
                async with session.get(f"{node_url}{path}") as response:
                    return await response.json()
            except Exception:
                return None

        gossip_data, state_data, merkle_data = await asyncio.gather(
            _get_json("/gossip/stats"),
            _get_json("/state"),
            _get_json("/state/merkle"),
        )

        if gossip_data:
            stats = gossip_data.get("stats", {})
            node_info["gossip_sent"] = stats.get("sent", 0)
            node_info["gossip_received"] = stats.get("received", 0)
            node_info["gossip_merged"] = stats.get("merged", 0)
            node_info["peers_discovered"] = stats.get("peers_discovered", 0)
            node_info["sent_bytes"] = stats.get("sent_bytes", 0)
            node_info["received_bytes"] = stats.get("received_bytes", 0)
            node_info["avg_merge_ms"] = stats.get("avg_merge_ms", 0)
        else:
            node_info["gossip_error"] = True

        if state_data:
            state_size = len(json.dumps(state_data))
            node_info["state_size_bytes"] = state_size
            summary = state_data.get("state_summary", {})
            event_count = summary.get("total_events", 0)
            if event_count == 0:
                event_ids = state_data.get("event_ids", [])
                event_count = len(event_ids) if isinstance(event_ids, list) else 0
            node_info["event_count"] = event_count
        else:
            node_info["state_size_bytes"] = 0
            node_info["event_count"] = 0

        if merkle_data:
            node_info["merkle_root"] = merkle_data.get("merkle_root", "")[:12]
            node_info["version"] = merkle_data.get("version", 0)

        return node_info

    def _compute_gateway_data_loss(self, per_node_stats: list[dict]) -> list[float]:
        node_by_gateway = {}
        for node in per_node_stats:
            gateway_name = node.get("assigned_gateway", "unknown")
            node_by_gateway.setdefault(gateway_name, []).append(node)

        gateway_data_loss = []
        for gateway_nodes in node_by_gateway.values():
            event_counts = [
                node.get("event_count", 0)
                for node in gateway_nodes
                if "event_count" in node
            ]
            if len(event_counts) <= 1:
                continue
            max_events = max(event_counts)
            min_events = min(event_counts)
            if max_events > 0:
                gateway_data_loss.append(round((1 - min_events / max_events) * 100, 2))

        return gateway_data_loss
