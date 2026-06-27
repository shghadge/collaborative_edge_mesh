import unittest

from src.crdt.gcounter import GCounter
from src.crdt.pncounter import PNCounter
from src.crdt.orset import ORSet
from src.crdt.state import NodeState


class DeltaMutatorTests(unittest.TestCase):
    def test_gcounter_last_delta_only_local_slot(self):
        counter = GCounter("edge-1")
        counter.counts["edge-2"] = 17
        counter.increment(3)

        delta = counter.last_delta()

        self.assertEqual(delta["counts"], {"edge-1": 3})

    def test_pncounter_last_delta_only_local_slots(self):
        counter = PNCounter("edge-1")
        counter._p.counts["edge-2"] = 11
        counter._n.counts["edge-2"] = 5
        counter.increment(4)
        counter.decrement(2)

        delta = counter.last_delta()

        self.assertEqual(delta["positive"]["counts"], {"edge-1": 4})
        self.assertEqual(delta["negative"]["counts"], {"edge-1": 2})

    def test_node_state_sensor_delta_excludes_merged_peer_counter_slots(self):
        local = NodeState("edge-1")
        peer = NodeState("edge-2")
        peer.record_event(
            "peer-event",
            "water_level",
            {"location": "bridge", "value": 9},
            category="sensor",
        )
        local.merge(peer)

        local.record_event(
            "local-event",
            "water_level",
            {"location": "bridge", "value": 10},
            category="sensor",
        )

        delta_state = local.delta_log[-1]["state"]
        self.assertEqual(
            delta_state["counters"]["event_count:water_level"]["counts"],
            {"edge-1": 1},
        )

    def test_node_state_delta_log_uses_compact_serialization(self):
        source = NodeState("edge-1")
        source.record_event(
            "event-1",
            "water_level",
            {
                "tracking_id": "event-1",
                "location": "bridge",
                "value": 10,
                "evaluation_submitted_at": 123.0,
            },
            category="sensor",
        )

        delta_state = source.delta_log[-1]["state"]
        replica = NodeState("edge-2")
        applied = replica.apply_delta_payload(source.get_deltas_since(0))

        self.assertEqual(applied, 1)
        self.assertEqual(replica.get_event_count("water_level"), 1)
        self.assertIn("event-1", replica.first_seen_tracking)
        for full_state_field in (
            "version",
            "updated_at",
            "event_id_retention_limit",
            "event_id_count",
            "delta_seq",
            "delta_log_limit",
            "merkle_root",
            "state_summary",
        ):
            self.assertNotIn(full_state_field, delta_state)

    def test_orset_remove_delta_tombstones_observed_add(self):
        source = ORSet("edge-1")
        replica = ORSet("edge-2")

        source.add("highway_101")
        replica.merge(ORSet.from_dict(source.to_dict()))
        replica.remove("highway_101")

        source.merge(ORSet.from_dict(replica.last_delta()))

        self.assertNotIn("highway_101", source.value)
        self.assertIn("highway_101", source._removed)

    def test_node_state_infrastructure_remove_delta_clears_peer_hazard(self):
        source = NodeState("edge-1")
        replica = NodeState("edge-2")

        source.record_event(
            "add-event",
            "road_status",
            {"location": "highway_101", "value": "blocked"},
            category="infrastructure",
        )
        replica.apply_delta_payload(
            {"has_gap": False, "deltas": [source.delta_log[-1]]}
        )

        source.record_event(
            "remove-event",
            "road_status",
            {"location": "highway_101", "value": "clear"},
            category="infrastructure",
            operation="remove",
        )
        replica.apply_delta_payload(
            {"has_gap": False, "deltas": [source.delta_log[-1]]}
        )

        self.assertNotIn("highway_101", replica.sets["hazards:road_status"].value)

    def test_unbounded_event_id_retention_survives_serialization(self):
        state = NodeState("edge-1", event_id_retention_limit=None)
        for index in range(3):
            state.record_event(
                f"event-{index}",
                "water_level",
                {"location": f"bridge-{index}", "value": index},
                category="sensor",
            )

        restored = NodeState.from_dict(state.to_dict())

        self.assertIsNone(restored.event_id_retention_limit)
        self.assertEqual(restored.event_ids, ["event-0", "event-1", "event-2"])

    def test_node_state_reset_preserves_configuration_and_clears_runtime_state(self):
        state = NodeState("edge-1", event_id_retention_limit=None, delta_log_limit=25)
        state.record_event(
            "event-1",
            "temperature",
            {"value": 12, "location": "sensor-a"},
            category="sensor",
        )

        state.reset()

        self.assertEqual(state.node_id, "edge-1")
        self.assertEqual(state.delta_log_limit, 25)
        self.assertIsNone(state.event_id_retention_limit)
        self.assertEqual(state.version, 0)
        self.assertEqual(state.delta_seq, 0)
        self.assertEqual(state.delta_log, [])
        self.assertEqual(state.event_ids, [])
        self.assertEqual(state.get_event_count(), 0)


if __name__ == "__main__":
    unittest.main()
