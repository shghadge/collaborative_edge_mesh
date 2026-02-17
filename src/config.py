import os


class Config:
    """Reads node configuration from environment variables."""

    def __init__(self):
        self.node_id = os.getenv("NODE_ID", "node-1")
        self.http_port = int(os.getenv("HTTP_PORT", "8000"))
        self.gossip_port = int(os.getenv("GOSSIP_PORT", "9000"))
        self.gossip_interval = float(os.getenv("GOSSIP_INTERVAL", "5"))
        self.gateway_poll_interval = float(os.getenv("GATEWAY_POLL_INTERVAL", "10"))
        self.data_dir = os.getenv("DATA_DIR", "/data")
        self.log_level = os.getenv("LOG_LEVEL", "INFO")

        self.gateway_http_retries = max(int(os.getenv("GATEWAY_HTTP_RETRIES", "2")), 1)
        self.gateway_http_retry_backoff_ms = max(
            int(os.getenv("GATEWAY_HTTP_RETRY_BACKOFF_MS", "150")), 0
        )
        self.gateway_node_failure_backoff = max(
            float(os.getenv("GATEWAY_NODE_FAILURE_BACKOFF", "2")), 0.0
        )

        # comma-separated list like "edge-node-2:9000,edge-node-3:9000"
        raw = os.getenv("PEER_NODES", "")
        self.peers = [p.strip() for p in raw.split(",") if p.strip()]

        # comma-separated list like "edge-node-1:8000,edge-node-2:8000"
        raw_edge_nodes = os.getenv("EDGE_NODES", "")
        self.edge_nodes = [p.strip() for p in raw_edge_nodes.split(",") if p.strip()]


config = Config()
