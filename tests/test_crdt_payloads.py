"""
Tests for CRDT payloads — verifying that each CRDT type works correctly,
serializes with meaningful descriptions, and that NodeState routes events
to the right CRDT by category.
"""

import json
import pytest
from datetime import datetime, timedelta

from src.crdt.gcounter import GCounter
from src.crdt.lww_register import LWWRegister
from src.crdt.pncounter import PNCounter
from src.crdt.orset import ORSet
from src.crdt.state import NodeState


# ── G-Counter ───────────────────────────────────────────────────────


class TestGCounter:
    def test_increment_and_value(self):
        c = GCounter("node-1", description="Water level readings count")
        c.increment(3)
        c.increment(2)
        assert c.value == 5

    def test_merge_takes_max(self):
        c1 = GCounter("node-1", description="readings")
        c2 = GCounter("node-2", description="readings")
        c1.increment(5)
        c2.increment(3)
        c1.merge(c2)
        assert c1.value == 8  # 5 from node-1, 3 from node-2

    def test_to_dict_includes_description_and_total(self):
        c = GCounter("node-1", description="Number of water_level readings")
        c.increment(7)
        d = c.to_dict()
        assert d["description"] == "Number of water_level readings"
        assert d["total_value"] == 7
        assert d["type"] == "gcounter"

    def test_round_trip(self):
        c = GCounter("node-1", description="test counter")
        c.increment(4)
        restored = GCounter.from_dict(c.to_dict())
        assert restored.value == 4
        assert restored.description == "test counter"

    def test_backward_compat_no_description(self):
        """Old serialized data without description field should still load."""
        data = {"node_id": "n1", "counts": {"n1": 5}}
        c = GCounter.from_dict(data)
        assert c.value == 5
        assert c.description == ""


# ── LWW-Register ────────────────────────────────────────────────────


class TestLWWRegister:
    def test_set_and_get(self):
        r = LWWRegister("node-1", description="Latest water level at bridge_north")
        r.set({"value": 3.2, "unit": "meters"})
        assert r.value["value"] == 3.2

    def test_last_writer_wins(self):
        r = LWWRegister("node-1")
        t1 = datetime(2026, 1, 1)
        t2 = datetime(2026, 1, 2)
        r.set({"value": 3.2}, timestamp=t1)
        r.set({"value": 4.1}, timestamp=t2)
        assert r.value["value"] == 4.1

    def test_merge_later_wins(self):
        r1 = LWWRegister("node-1")
        r2 = LWWRegister("node-2")
        r1.set({"value": 3.2}, timestamp=datetime(2026, 1, 1))
        r2.set({"value": 4.1}, timestamp=datetime(2026, 1, 2))
        r1.merge(r2)
        assert r1.value["value"] == 4.1

    def test_to_dict_includes_description(self):
        r = LWWRegister("node-1", description="Latest water level at bridge_north")
        r.set({"value": 3.2})
        d = r.to_dict()
        assert d["description"] == "Latest water level at bridge_north"
        assert d["type"] == "lww_register"

    def test_round_trip(self):
        r = LWWRegister("node-1", description="test register")
        r.set({"value": 42})
        restored = LWWRegister.from_dict(r.to_dict())
        assert restored.value["value"] == 42
        assert restored.description == "test register"


# ── PN-Counter ──────────────────────────────────────────────────────


class TestPNCounter:
    def test_increment_only(self):
        c = PNCounter("node-1", description="Shelter occupancy")
        c.increment(12)
        assert c.value == 12

    def test_increment_and_decrement(self):
        c = PNCounter("node-1", description="Shelter occupancy")
        c.increment(12)
        c.decrement(3)
        assert c.value == 9  # 12 arrived, 3 departed

    def test_merge(self):
        c1 = PNCounter("node-1", description="occupancy")
        c2 = PNCounter("node-2", description="occupancy")
        c1.increment(10)
        c2.increment(5)
        c2.decrement(2)
        c1.merge(c2)
        assert c1.value == 13  # node-1: +10, node-2: +5 -2

    def test_to_dict_payload(self):
        c = PNCounter("node-1", description="Net occupancy at shelter_east")
        c.increment(20)
        c.decrement(5)
        d = c.to_dict()
        assert d["type"] == "pn_counter"
        assert d["description"] == "Net occupancy at shelter_east"
        assert d["total_value"] == 15
        assert "positive" in d
        assert "negative" in d

    def test_round_trip(self):
        c = PNCounter("node-1", description="test pn")
        c.increment(10)
        c.decrement(3)
        restored = PNCounter.from_dict(c.to_dict())
        assert restored.value == 7
        assert restored.description == "test pn"


# ── OR-Set ──────────────────────────────────────────────────────────


class TestORSet:
    def test_add_and_lookup(self):
        s = ORSet("node-1", description="Active road hazards")
        s.add("highway_101")
        assert s.lookup("highway_101")
        assert "highway_101" in s.value

    def test_add_and_remove(self):
        s = ORSet("node-1", description="Active road hazards")
        s.add("highway_101")
        s.remove("highway_101")
        assert not s.lookup("highway_101")

    def test_concurrent_add_wins(self):
        """Add on one node + remove on another → add wins after merge."""
        s1 = ORSet("node-1", description="hazards")
        s2 = ORSet("node-2", description="hazards")

        # both start with the element
        s1.add("highway_101")
        s2.merge(s1)  # s2 now sees highway_101

        # concurrent: s1 adds again, s2 removes
        s1.add("highway_101")  # new unique tag
        s2.remove("highway_101")  # removes only observed tags

        # merge: s1's new tag survives s2's remove
        s2.merge(s1)
        assert s2.lookup("highway_101")

    def test_merge_union(self):
        s1 = ORSet("node-1")
        s2 = ORSet("node-2")
        s1.add("highway_101")
        s2.add("bridge_north")
        s1.merge(s2)
        assert s1.value == {"highway_101", "bridge_north"}

    def test_to_dict_payload(self):
        s = ORSet("node-1", description="Active road_status hazards")
        s.add("highway_101")
        s.add("bridge_north")
        d = s.to_dict()
        assert d["type"] == "or_set"
        assert d["description"] == "Active road_status hazards"
        assert set(d["active_elements"]) == {"highway_101", "bridge_north"}

    def test_round_trip(self):
        s = ORSet("node-1", description="test set")
        s.add("a")
        s.add("b")
        restored = ORSet.from_dict(s.to_dict())
        assert restored.value == {"a", "b"}
        assert restored.description == "test set"


# ── NodeState — category routing ────────────────────────────────────


class TestNodeStateSensorRouting:
    def test_sensor_creates_counter_and_register(self):
        state = NodeState("node-1")
        result = state.record_event(
            "evt-1",
            "water_level",
            {
                "value": 3.2,
                "location": "bridge_north",
                "unit": "meters",
                "severity": "warning",
            },
            category="sensor",
        )
        assert result["category"] == "sensor"
        assert result["counter_key"] == "event_count:water_level"
        assert result["register_key"] == "sensor:bridge_north:water_level"

        # counter was created
        assert state.counters["event_count:water_level"].value == 1
        # register was created with rich value
        reg_val = state.registers["sensor:bridge_north:water_level"].value
        assert reg_val["value"] == 3.2
        assert reg_val["unit"] == "meters"
        assert reg_val["severity"] == "warning"
        assert reg_val["category"] == "sensor"

    def test_different_types_same_location_no_clobber(self):
        """Two different event types at the same location should NOT overwrite each other."""
        state = NodeState("node-1")
        state.record_event(
            "evt-1",
            "water_level",
            {"value": 3.2, "location": "bridge_north"},
            category="sensor",
        )
        state.record_event(
            "evt-2",
            "temperature",
            {"value": 28.5, "location": "bridge_north"},
            category="sensor",
        )
        # separate register keys
        assert "sensor:bridge_north:water_level" in state.registers
        assert "sensor:bridge_north:temperature" in state.registers
        assert state.registers["sensor:bridge_north:water_level"].value["value"] == 3.2
        assert state.registers["sensor:bridge_north:temperature"].value["value"] == 28.5


class TestNodeStateResourceRouting:
    def test_resource_creates_pn_counter(self):
        state = NodeState("node-1")
        result = state.record_event(
            "evt-1",
            "shelter_occupancy",
            {"value": 12, "location": "shelter_east"},
            category="resource",
            operation="increment",
        )
        assert result["category"] == "resource"
        assert result["pn_counter_key"] == "resource:shelter_east:shelter_occupancy"
        assert state.pn_counters["resource:shelter_east:shelter_occupancy"].value == 12

    def test_resource_decrement(self):
        state = NodeState("node-1")
        state.record_event(
            "evt-1",
            "shelter_occupancy",
            {"value": 12, "location": "shelter_east"},
            category="resource",
        )
        state.record_event(
            "evt-2",
            "shelter_occupancy",
            {"value": 3, "location": "shelter_east"},
            category="resource",
            operation="decrement",
        )
        assert state.pn_counters["resource:shelter_east:shelter_occupancy"].value == 9


class TestNodeStateInfraRouting:
    def test_infra_creates_or_set(self):
        state = NodeState("node-1")
        result = state.record_event(
            "evt-1",
            "road_status",
            {"value": "blocked", "location": "highway_101", "cause": "flooding"},
            category="infrastructure",
        )
        assert result["category"] == "infrastructure"
        assert result["set_key"] == "hazards:road_status"
        assert "highway_101" in state.sets["hazards:road_status"].value

    def test_infra_remove_clears_hazard(self):
        state = NodeState("node-1")
        state.record_event(
            "evt-1",
            "road_status",
            {"value": "blocked", "location": "highway_101"},
            category="infrastructure",
        )
        state.record_event(
            "evt-2",
            "road_status",
            {"value": "clear", "location": "highway_101"},
            category="infrastructure",
            operation="remove",
        )
        assert "highway_101" not in state.sets["hazards:road_status"].value


class TestNodeStateGeneral:
    def test_general_backward_compat(self):
        """Events without category default to general behavior."""
        state = NodeState("node-1")
        result = state.record_event(
            "evt-1", "water_level", {"value": 3.2, "location": "bridge_north"}
        )
        assert result["category"] == "general"
        assert "event_count:water_level" in state.counters
        assert "general:bridge_north:water_level" in state.registers


# ── NodeState — merge, merkle, serialization ────────────────────────


class TestNodeStateMerge:
    def test_merge_preserves_all_crdt_types(self):
        s1 = NodeState("node-1")
        s2 = NodeState("node-2")

        s1.record_event(
            "e1",
            "water_level",
            {"value": 3.2, "location": "bridge_north"},
            category="sensor",
        )
        s2.record_event(
            "e2",
            "shelter_occupancy",
            {"value": 10, "location": "shelter_east"},
            category="resource",
        )
        s2.record_event(
            "e3",
            "road_status",
            {"value": "blocked", "location": "highway_101"},
            category="infrastructure",
        )

        s1.merge(s2)

        # s1 now has all types
        assert "event_count:water_level" in s1.counters
        assert "event_count:shelter_occupancy" in s1.counters
        assert "sensor:bridge_north:water_level" in s1.registers
        assert "resource:shelter_east:shelter_occupancy" in s1.pn_counters
        assert "hazards:road_status" in s1.sets

    def test_merkle_changes_on_new_data(self):
        state = NodeState("node-1")
        root1 = state.merkle_root()
        state.record_event(
            "e1",
            "water_level",
            {"value": 3.2, "location": "bridge_north"},
            category="sensor",
        )
        root2 = state.merkle_root()
        assert root1 != root2

    def test_merged_states_converge(self):
        s1 = NodeState("node-1")
        s2 = NodeState("node-2")

        s1.record_event(
            "e1",
            "water_level",
            {"value": 3.2, "location": "bridge_north"},
            category="sensor",
        )
        s2.record_event(
            "e1",
            "water_level",
            {"value": 3.2, "location": "bridge_north"},
            category="sensor",
        )

        s1.merge(s2)
        s2.merge(s1)

        assert s1.merkle_root() == s2.merkle_root()


class TestNodeStateSerialization:
    def test_round_trip(self):
        state = NodeState("node-1")
        state.record_event(
            "e1",
            "water_level",
            {"value": 3.2, "location": "bridge_north", "unit": "meters"},
            category="sensor",
        )
        state.record_event(
            "e2",
            "shelter_occupancy",
            {"value": 10, "location": "shelter_east"},
            category="resource",
        )
        state.record_event(
            "e3",
            "road_status",
            {"value": "blocked", "location": "highway_101"},
            category="infrastructure",
        )

        d = state.to_dict()
        restored = NodeState.from_dict(d)

        assert restored.merkle_root() == state.merkle_root()
        assert restored.version == state.version
        assert len(restored.event_ids) == 3
        assert "sensor:bridge_north:water_level" in restored.registers
        assert "resource:shelter_east:shelter_occupancy" in restored.pn_counters
        assert "hazards:road_status" in restored.sets

    def test_to_dict_includes_summary(self):
        state = NodeState("node-1")
        state.record_event(
            "e1",
            "water_level",
            {"value": 3.2, "location": "bridge_north"},
            category="sensor",
        )
        d = state.to_dict()
        assert "state_summary" in d
        assert d["state_summary"]["sensor_readings"] == 1
        assert d["state_summary"]["total_events"] == 1


# ── Summary ─────────────────────────────────────────────────────────


class TestNodeStateSummary:
    def test_summary_counts_categories(self):
        state = NodeState("node-1")
        state.record_event(
            "e1",
            "water_level",
            {"value": 3.2, "location": "bridge_north"},
            category="sensor",
        )
        state.record_event(
            "e2",
            "temperature",
            {"value": 28.5, "location": "bridge_north"},
            category="sensor",
        )
        state.record_event(
            "e3",
            "shelter_occupancy",
            {"value": 10, "location": "shelter_east"},
            category="resource",
        )
        state.record_event(
            "e4",
            "road_status",
            {"value": "blocked", "location": "highway_101"},
            category="infrastructure",
        )

        summary = state.summary()
        assert summary["sensor_readings"] == 2
        assert summary["resource_trackers"] == 1
        assert summary["active_hazard_sets"] == 1
        assert summary["crdt_counts"]["g_counters"] == 4  # 4 event_count:* counters
        assert summary["crdt_counts"]["pn_counters"] == 1
        assert summary["crdt_counts"]["or_sets"] == 1
