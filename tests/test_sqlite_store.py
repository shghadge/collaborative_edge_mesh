"""Tests for SQLiteStore — snapshots, divergence, metrics, event tracking, time-series."""

import os
import tempfile

import pytest

from src.storage.sqlite_store import SQLiteStore


@pytest.fixture()
def store(tmp_path):
    """Create an in-memory-like SQLiteStore backed by a temp file."""
    db_path = os.path.join(str(tmp_path), "test.db")
    return SQLiteStore(db_path=db_path)


# ── Snapshot Tests ──────────────────────────────────────────────


class TestSnapshots:
    def test_save_and_get_latest_snapshot(self, store):
        store.save_snapshot("root-aaa", 2, ["node-a", "node-b"], {"counter": 1})
        latest = store.get_latest_snapshot()
        assert latest is not None
        assert latest["merkle_root"] == "root-aaa"
        assert latest["node_count"] == 2
        assert latest["source_nodes"] == ["node-a", "node-b"]
        assert latest["state"] == {"counter": 1}

    def test_latest_snapshot_returns_most_recent(self, store):
        store.save_snapshot("root-1", 1, ["a"], {"v": 1})
        store.save_snapshot("root-2", 2, ["a", "b"], {"v": 2})
        store.save_snapshot("root-3", 3, ["a", "b", "c"], {"v": 3})
        latest = store.get_latest_snapshot()
        assert latest["merkle_root"] == "root-3"
        assert latest["node_count"] == 3

    def test_get_latest_snapshot_empty_db(self, store):
        assert store.get_latest_snapshot() is None

    def test_snapshot_history_respects_limit(self, store):
        for i in range(5):
            store.save_snapshot(f"root-{i}", i, [f"node-{i}"], {"i": i})
        history = store.get_snapshot_history(limit=3)
        assert len(history) == 3
        # Most recent first
        assert history[0]["merkle_root"] == "root-4"

    def test_snapshot_history_empty_db(self, store):
        assert store.get_snapshot_history() == []


# ── Divergence Log Tests ────────────────────────────────────────


class TestDivergenceLog:
    def test_log_and_retrieve_divergence(self, store):
        store.log_divergence(True, {"a": "root-1", "b": "root-2"})
        logs = store.get_divergence_log()
        assert len(logs) == 1
        assert logs[0]["is_divergent"] is True
        assert logs[0]["merkle_roots"] == {"a": "root-1", "b": "root-2"}

    def test_log_convergent_state(self, store):
        store.log_divergence(False, {"a": "same", "b": "same"})
        logs = store.get_divergence_log()
        assert logs[0]["is_divergent"] is False

    def test_divergence_log_ordering(self, store):
        store.log_divergence(False, {"a": "x"})
        store.log_divergence(True, {"a": "x", "b": "y"})
        store.log_divergence(False, {"a": "z", "b": "z"})
        logs = store.get_divergence_log()
        assert len(logs) == 3
        # Most recent first (DESC)
        assert logs[0]["is_divergent"] is False
        assert logs[1]["is_divergent"] is True
        assert logs[2]["is_divergent"] is False

    def test_divergence_log_limit(self, store):
        for i in range(10):
            store.log_divergence(i % 2 == 0, {"node": f"root-{i}"})
        logs = store.get_divergence_log(limit=3)
        assert len(logs) == 3

    def test_divergence_log_empty_db(self, store):
        assert store.get_divergence_log() == []


# ── Metrics Tests ───────────────────────────────────────────────


class TestMetrics:
    def test_save_and_get_metric(self, store):
        store.save_metric("cpu_usage", 85.5)
        metrics = store.get_metrics()
        assert len(metrics) == 1
        assert metrics[0]["name"] == "cpu_usage"
        assert metrics[0]["value"] == 85.5

    def test_save_metric_with_metadata(self, store):
        store.save_metric("merge_time_ms", 42.0, metadata={"node": "gw-1"})
        m = store.get_metrics()[0]
        assert m["metadata"] == {"node": "gw-1"}

    def test_get_metrics_by_name(self, store):
        store.save_metric("cpu", 10.0)
        store.save_metric("memory", 2048.0)
        store.save_metric("cpu", 20.0)

        cpu_metrics = store.get_metrics(name="cpu")
        assert len(cpu_metrics) == 2
        assert all(m["name"] == "cpu" for m in cpu_metrics)

        mem_metrics = store.get_metrics(name="memory")
        assert len(mem_metrics) == 1

    def test_get_metrics_limit(self, store):
        for i in range(20):
            store.save_metric("counter", float(i))
        metrics = store.get_metrics(limit=5)
        assert len(metrics) == 5

    def test_metrics_empty_db(self, store):
        assert store.get_metrics() == []
        assert store.get_metrics(name="nonexistent") == []


# ── Event Tracking Tests ───────────────────────────────────────


class TestEventTracking:
    def test_save_and_get_event_tracking(self, store):
        store.save_event_tracking(
            tracking_id="tid-001",
            node_id="node-a",
            event_type="water_level",
            category="sensor",
            state_size_bytes=1024,
            merkle_root_after="abc123",
        )
        events = store.get_event_tracking()
        assert len(events) == 1
        ev = events[0]
        assert ev["tracking_id"] == "tid-001"
        assert ev["node_id"] == "node-a"
        assert ev["event_type"] == "water_level"
        assert ev["category"] == "sensor"
        assert ev["state_size_bytes"] == 1024
        assert ev["merkle_root_after"] == "abc123"

    def test_event_tracking_with_metadata(self, store):
        store.save_event_tracking(
            tracking_id="tid-002",
            node_id="node-b",
            event_type="shelter_occupancy",
            category="resource",
            metadata={"source": "dashboard", "scenario": "manual"},
        )
        events = store.get_event_tracking()
        assert events[0]["metadata"] == {"source": "dashboard", "scenario": "manual"}

    def test_event_tracking_optional_fields(self, store):
        """state_size_bytes and merkle_root_after can be None."""
        store.save_event_tracking(
            tracking_id="tid-003",
            node_id="node-c",
            event_type="road_status",
            category="infrastructure",
        )
        ev = store.get_event_tracking()[0]
        assert ev["state_size_bytes"] is None
        assert ev["merkle_root_after"] is None

    def test_event_tracking_ordering_and_limit(self, store):
        for i in range(10):
            store.save_event_tracking(
                tracking_id=f"tid-{i:03}",
                node_id="node-a",
                event_type="test",
                category="general",
            )
        events = store.get_event_tracking(limit=3)
        assert len(events) == 3
        # Most recent first
        assert events[0]["tracking_id"] == "tid-009"

    def test_event_tracking_empty_db(self, store):
        assert store.get_event_tracking() == []


# ── Time-Series Metrics Tests ──────────────────────────────────


class TestTimeseries:
    def test_save_and_get_timeseries(self, store):
        store.save_timeseries_metric("convergence_time", 2.5)
        ts = store.get_timeseries("convergence_time")
        assert len(ts) == 1
        assert ts[0]["metric_name"] == "convergence_time"
        assert ts[0]["value"] == 2.5

    def test_timeseries_filters_by_metric_name(self, store):
        store.save_timeseries_metric("state_size_bytes", 4096.0)
        store.save_timeseries_metric("convergence_time", 1.2)
        store.save_timeseries_metric("state_size_bytes", 5120.0)

        size_ts = store.get_timeseries("state_size_bytes")
        assert len(size_ts) == 2
        assert all(t["metric_name"] == "state_size_bytes" for t in size_ts)

        conv_ts = store.get_timeseries("convergence_time")
        assert len(conv_ts) == 1

    def test_timeseries_with_metadata(self, store):
        store.save_timeseries_metric(
            "node_count", 3.0, metadata={"poll_id": "p-001"}
        )
        ts = store.get_timeseries("node_count")
        assert ts[0]["metadata"] == {"poll_id": "p-001"}

    def test_timeseries_limit(self, store):
        for i in range(20):
            store.save_timeseries_metric("counter", float(i))
        ts = store.get_timeseries("counter", limit=5)
        assert len(ts) == 5

    def test_timeseries_nonexistent_metric(self, store):
        assert store.get_timeseries("does_not_exist") == []

    def test_timeseries_ordering_desc(self, store):
        store.save_timeseries_metric("val", 10.0)
        store.save_timeseries_metric("val", 20.0)
        store.save_timeseries_metric("val", 30.0)
        ts = store.get_timeseries("val")
        # Most recent first (ORDER BY id DESC)
        assert ts[0]["value"] == 30.0
        assert ts[2]["value"] == 10.0


# ── Persistence / Restart Tests ────────────────────────────────


class TestPersistence:
    def test_data_survives_reconnect(self, tmp_path):
        """Data written by one SQLiteStore instance is readable by another."""
        db_path = os.path.join(str(tmp_path), "persist.db")

        store1 = SQLiteStore(db_path=db_path)
        store1.save_snapshot("root-persist", 2, ["a", "b"], {"data": True})
        store1.save_metric("test_metric", 99.0)
        store1.save_timeseries_metric("ts_metric", 42.0)
        store1.save_event_tracking("tid-p", "node-a", "type", "sensor")
        store1.log_divergence(False, {"a": "same"})
        del store1  # close first instance

        store2 = SQLiteStore(db_path=db_path)
        assert store2.get_latest_snapshot()["merkle_root"] == "root-persist"
        assert len(store2.get_metrics()) == 1
        assert len(store2.get_timeseries("ts_metric")) == 1
        assert len(store2.get_event_tracking()) == 1
        assert len(store2.get_divergence_log()) == 1

    def test_tables_created_on_new_db(self, tmp_path):
        """A fresh database should have all tables ready (no errors on CRUD)."""
        db_path = os.path.join(str(tmp_path), "fresh.db")
        store = SQLiteStore(db_path=db_path)

        # All operations should work on a fresh DB
        assert store.get_latest_snapshot() is None
        assert store.get_divergence_log() == []
        assert store.get_metrics() == []
        assert store.get_event_tracking() == []
        assert store.get_timeseries("any") == []
