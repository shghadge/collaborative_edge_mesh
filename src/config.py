import os


class Config:
    """Reads node configuration from environment variables."""

    def __init__(self):
        self.node_id = os.getenv("NODE_ID", "node-1")
        self.http_port = int(os.getenv("HTTP_PORT", "8000"))
        self.gossip_port = int(os.getenv("GOSSIP_PORT", "9000"))
        self.gossip_interval = float(os.getenv("GOSSIP_INTERVAL", "5"))
        self.data_dir = os.getenv("DATA_DIR", "/data")
        self.log_level = os.getenv("LOG_LEVEL", "INFO")

        # comma-separated list like "edge-node-2:9000,edge-node-3:9000"
        raw = os.getenv("PEER_NODES", "")
        self.peers = [p.strip() for p in raw.split(",") if p.strip()]


config = Config()
