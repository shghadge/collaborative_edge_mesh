import json
import sqlite3
import os
from datetime import datetime


class SQLiteStore:
    """Simple SQLite storage for merged state snapshots and metrics."""

    def __init__(self, db_path="/data/gateway.db"):
        os.makedirs(os.path.dirname(db_path) or ".", exist_ok=True)
        self.conn = sqlite3.connect(db_path, check_same_thread=False)
        self.conn.row_factory = sqlite3.Row
        self._init_tables()

    def _init_tables(self):
        self.conn.executescript("""
            CREATE TABLE IF NOT EXISTS snapshots (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                merkle_root TEXT NOT NULL,
                node_count INTEGER NOT NULL,
                source_nodes TEXT NOT NULL,
                state_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS divergence_log (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                is_divergent INTEGER NOT NULL,
                merkle_roots_json TEXT NOT NULL
            );
            CREATE TABLE IF NOT EXISTS metrics (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                name TEXT NOT NULL,
                value REAL NOT NULL,
                metadata_json TEXT
            );
            CREATE TABLE IF NOT EXISTS event_tracking (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                tracking_id TEXT NOT NULL,
                node_id TEXT NOT NULL,
                event_type TEXT NOT NULL,
                category TEXT NOT NULL,
                state_size_bytes INTEGER,
                merkle_root_after TEXT,
                metadata_json TEXT
            );
            CREATE TABLE IF NOT EXISTS metric_timeseries (
                id INTEGER PRIMARY KEY AUTOINCREMENT,
                timestamp TEXT NOT NULL,
                metric_name TEXT NOT NULL,
                value REAL NOT NULL,
                metadata_json TEXT
            );
        """)

    def save_snapshot(self, merkle_root, node_count, source_nodes, state_dict):
        self.conn.execute(
            "INSERT INTO snapshots (timestamp, merkle_root, node_count, source_nodes, state_json) VALUES (?, ?, ?, ?, ?)",
            (
                datetime.utcnow().isoformat(),
                merkle_root,
                node_count,
                json.dumps(source_nodes),
                json.dumps(state_dict),
            ),
        )
        self.conn.commit()

    def get_latest_snapshot(self):
        row = self.conn.execute(
            "SELECT * FROM snapshots ORDER BY id DESC LIMIT 1"
        ).fetchone()
        if not row:
            return None
        return {
            "id": row["id"],
            "timestamp": row["timestamp"],
            "merkle_root": row["merkle_root"],
            "node_count": row["node_count"],
            "source_nodes": json.loads(row["source_nodes"]),
            "state": json.loads(row["state_json"]),
        }

    def get_snapshot_history(self, limit=20):
        rows = self.conn.execute(
            "SELECT id, timestamp, merkle_root, node_count, source_nodes FROM snapshots ORDER BY id DESC LIMIT ?",
            (limit,),
        ).fetchall()
        return [dict(r) for r in rows]

    def log_divergence(self, is_divergent, merkle_roots):
        self.conn.execute(
            "INSERT INTO divergence_log (timestamp, is_divergent, merkle_roots_json) VALUES (?, ?, ?)",
            (
                datetime.utcnow().isoformat(),
                int(is_divergent),
                json.dumps(merkle_roots),
            ),
        )
        self.conn.commit()

    def get_divergence_log(self, limit=50):
        rows = self.conn.execute(
            "SELECT * FROM divergence_log ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        return [
            {
                "id": r["id"],
                "timestamp": r["timestamp"],
                "is_divergent": bool(r["is_divergent"]),
                "merkle_roots": json.loads(r["merkle_roots_json"]),
            }
            for r in rows
        ]

    def save_metric(self, name, value, metadata=None):
        self.conn.execute(
            "INSERT INTO metrics (timestamp, name, value, metadata_json) VALUES (?, ?, ?, ?)",
            (datetime.utcnow().isoformat(), name, value, json.dumps(metadata or {})),
        )
        self.conn.commit()

    def get_metrics(self, name=None, limit=100):
        if name:
            rows = self.conn.execute(
                "SELECT * FROM metrics WHERE name = ? ORDER BY id DESC LIMIT ?",
                (name, limit),
            ).fetchall()
        else:
            rows = self.conn.execute(
                "SELECT * FROM metrics ORDER BY id DESC LIMIT ?", (limit,)
            ).fetchall()
        return [
            {
                "timestamp": r["timestamp"],
                "name": r["name"],
                "value": r["value"],
                "metadata": json.loads(r["metadata_json"] or "{}"),
            }
            for r in rows
        ]

    # --- Event Tracking ---

    def save_event_tracking(
        self,
        tracking_id,
        node_id,
        event_type,
        category,
        state_size_bytes=None,
        merkle_root_after=None,
        metadata=None,
    ):
        self.conn.execute(
            "INSERT INTO event_tracking (timestamp, tracking_id, node_id, event_type, category, state_size_bytes, merkle_root_after, metadata_json) VALUES (?, ?, ?, ?, ?, ?, ?, ?)",
            (
                datetime.utcnow().isoformat(),
                tracking_id,
                node_id,
                event_type,
                category,
                state_size_bytes,
                merkle_root_after,
                json.dumps(metadata or {}),
            ),
        )
        self.conn.commit()

    def get_event_tracking(self, limit=50):
        rows = self.conn.execute(
            "SELECT * FROM event_tracking ORDER BY id DESC LIMIT ?", (limit,)
        ).fetchall()
        return [
            {
                "id": r["id"],
                "timestamp": r["timestamp"],
                "tracking_id": r["tracking_id"],
                "node_id": r["node_id"],
                "event_type": r["event_type"],
                "category": r["category"],
                "state_size_bytes": r["state_size_bytes"],
                "merkle_root_after": r["merkle_root_after"],
                "metadata": json.loads(r["metadata_json"] or "{}"),
            }
            for r in rows
        ]

    # --- Time-Series Metrics ---

    def save_timeseries_metric(self, metric_name, value, metadata=None):
        self.conn.execute(
            "INSERT INTO metric_timeseries (timestamp, metric_name, value, metadata_json) VALUES (?, ?, ?, ?)",
            (
                datetime.utcnow().isoformat(),
                metric_name,
                value,
                json.dumps(metadata or {}),
            ),
        )
        self.conn.commit()

    def get_timeseries(self, metric_name, limit=100):
        rows = self.conn.execute(
            "SELECT * FROM metric_timeseries WHERE metric_name = ? ORDER BY id DESC LIMIT ?",
            (metric_name, limit),
        ).fetchall()
        return [
            {
                "timestamp": r["timestamp"],
                "metric_name": r["metric_name"],
                "value": r["value"],
                "metadata": json.loads(r["metadata_json"] or "{}"),
            }
            for r in rows
        ]
