import os


class Config:
    """Reads node configuration from environment variables."""

    def __init__(self):
        self.node_id = os.getenv("NODE_ID", "node-1")
        self.http_port = int(os.getenv("HTTP_PORT", "8000"))
        self.gossip_port = int(os.getenv("GOSSIP_PORT", "9000"))
        self.gossip_interval = float(os.getenv("GOSSIP_INTERVAL", "5"))
        self.sync_mode = os.getenv("SYNC_MODE", "full_state").strip().lower()
        if self.sync_mode not in {"full_state", "true_delta"}:
            self.sync_mode = "full_state"
        retention_limit = int(os.getenv("EVENT_ID_RETENTION_LIMIT", "100"))
        self.event_id_retention_limit = retention_limit if retention_limit > 0 else None
        self.gateway_poll_interval = float(os.getenv("GATEWAY_POLL_INTERVAL", "10"))
        self.data_dir = os.getenv("DATA_DIR", "/data")
        self.log_level = os.getenv("LOG_LEVEL", "INFO")
        self.assigned_gateway = os.getenv("ASSIGNED_GATEWAY", "")
        self.runtime_mode = self._parse_runtime_mode(
            os.getenv("RUNTIME_MODE", "simulation")
        )
        self.observability_mode = (
            os.getenv(
                "OBSERVABILITY_MODE",
                "full" if self.runtime_mode in {"simulation", "debug"} else "light",
            )
            .strip()
            .lower()
        )
        if self.observability_mode not in {"off", "light", "full"}:
            self.observability_mode = "full"
        self.observability_cache_ttl_seconds = max(
            float(os.getenv("OBSERVABILITY_CACHE_TTL_SECONDS", "5")), 0.0
        )
        self.observability_timeseries_cache_ttl_seconds = max(
            float(
                os.getenv(
                    "OBSERVABILITY_TIMESERIES_CACHE_TTL_SECONDS",
                    "2" if self.observability_mode == "full" else "20",
                )
            ),
            0.0,
        )
        self.observability_push_interval_seconds = max(
            float(os.getenv("OBSERVABILITY_PUSH_INTERVAL_SECONDS", "3")), 1.0
        )

        self.gateway_http_retries = max(int(os.getenv("GATEWAY_HTTP_RETRIES", "2")), 1)
        self.gateway_http_retry_backoff_ms = max(
            int(os.getenv("GATEWAY_HTTP_RETRY_BACKOFF_MS", "150")), 0
        )
        self.gateway_node_failure_backoff = max(
            float(os.getenv("GATEWAY_NODE_FAILURE_BACKOFF", "2")), 0.0
        )
        self.gateway_visibility_freshness_seconds = max(
            float(os.getenv("GATEWAY_VISIBILITY_FRESHNESS_SECONDS", "10")), 1.0
        )
        self.gateway_coverage_confidence_threshold = min(
            max(float(os.getenv("GATEWAY_COVERAGE_CONFIDENCE_THRESHOLD", "0.6")), 0.0),
            1.0,
        )
        self.gateway_snapshot_retention_limit = max(
            int(os.getenv("GATEWAY_SNAPSHOT_RETENTION_LIMIT", "50")), 1
        )

        # comma-separated list like "edge-node-1:8000,edge-node-2:8000"
        raw_edge_nodes = os.getenv("EDGE_NODES", "")
        self.edge_nodes = [p.strip() for p in raw_edge_nodes.split(",") if p.strip()]
        self.deep_observability_enabled = self.observability_mode == "full"

    def _parse_runtime_mode(self, value: str) -> str:
        runtime_mode = value.strip().lower()
        if runtime_mode not in {"normal", "simulation", "debug"}:
            return "simulation"
        return runtime_mode


config = Config()
