"""Environment-driven configuration shared by node and gateway processes."""

import os


class Config:
    """Reads node configuration from environment variables."""

    def __init__(self):
        self.node_id = os.getenv("NODE_ID", "node-1")
        self.http_port = int(os.getenv("HTTP_PORT", "8000"))
        self.gossip_port = int(os.getenv("GOSSIP_PORT", "9000"))
        self.gossip_interval = float(os.getenv("GOSSIP_INTERVAL", "5"))
        self.sync_mode = os.getenv("SYNC_MODE", "full_state").strip().lower()
        if self.sync_mode not in {
            "full_state",
            "optimized_full_state",
            "naive_key_delta",
            "true_delta",
        }:
            self.sync_mode = "full_state"
        retention_limit = int(os.getenv("EVENT_ID_RETENTION_LIMIT", "100"))
        delta_log_limit = int(os.getenv("DELTA_LOG_LIMIT", "1000"))
        self.delta_log_limit = delta_log_limit if delta_log_limit > 0 else None
        self.event_id_retention_limit = retention_limit if retention_limit > 0 else None
        self.gateway_poll_interval = float(os.getenv("GATEWAY_POLL_INTERVAL", "10"))
        self.data_dir = os.getenv("DATA_DIR", "/data")
        self.log_level = os.getenv("LOG_LEVEL", "INFO")
        self.assigned_gateway = os.getenv("ASSIGNED_GATEWAY", "")
        self.enable_heal_state_push = self._bool_env("ENABLE_HEAL_STATE_PUSH", False)

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

    @staticmethod
    def _bool_env(name: str, default: bool = False) -> bool:
        """Parse a boolean environment variable, returning ``default`` if unset."""
        value = os.getenv(name)
        if value is None:
            return default
        return value.strip().lower() in {"1", "true", "yes", "on"}


config = Config()
