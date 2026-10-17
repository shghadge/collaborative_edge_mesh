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

    def test_naive_key_delta_contains_full_changed_key(self):
        peer = NodeState("edge-2")
        peer.record_event(
            "peer-event",
            "water_level",
            {"location": "bridge", "value": 9},
            category="sensor",
        )
        true_delta_source = NodeState("edge-1", sync_mode="true_delta")
        key_delta_source = NodeState("edge-1", sync_mode="naive_key_delta")
        for source in (true_delta_source, key_delta_source):
            source.merge(peer)
            source.record_event(
                "local-event",
                "water_level",
                {"location": "bridge", "value": 10},
                category="sensor",
            )

        # Both sources already hold edge-2's mutation through the full-state merge.
        requested_vv = {"edge-2": 1}
        true_delta = true_delta_source.get_deltas_for(requested_vv, mode="true_delta")
        key_delta = key_delta_source.get_deltas_for(
            requested_vv, mode="naive_key_delta"
        )
        counter_key = "event_count:water_level"

        self.assertEqual(
            true_delta["state"]["counters"][counter_key]["counts"],
            {"edge-1": 1},
        )
        self.assertEqual(
            key_delta["state"]["counters"][counter_key]["counts"],
            {"edge-1": 1, "edge-2": 1},
        )

        replica = NodeState("edge-3", sync_mode="naive_key_delta")
        self.assertEqual(replica.apply_delta_payload(key_delta, requested_vv), 1)
        self.assertEqual(replica.merkle_root(), key_delta_source.merkle_root())

    def test_full_state_modes_do_not_build_unused_delta_logs(self):
        for sync_mode in ("full_state", "optimized_full_state"):
            with self.subTest(sync_mode=sync_mode):
                state = NodeState("edge-1", sync_mode=sync_mode)
                state.record_event(
                    "event-1",
                    "water_level",
                    {"location": "bridge", "value": 10},
                    category="sensor",
                )

                self.assertEqual(state.delta_seq, 0)
                self.assertEqual(state.delta_log, [])

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
        applied = replica.apply_delta_payload(source.get_deltas_for({}), {})

        self.assertEqual(applied, 1)
        self.assertEqual(replica.get_event_count("water_level"), 1)
        self.assertIn("event-1", replica.first_seen_tracking)
        for full_state_field in (
            "version",
            "updated_at",
            "event_id_retention_limit",
            "event_id_count",
            "delta_vv",
            "delta_log_limit",
            "merkle_root",
            "state_summary",
        ):
            self.assertNotIn(full_state_field, delta_state)

    def test_applied_delta_is_relayed_until_all_replicas_receive_it(self):
        source = NodeState("edge-1")
        relay = NodeState("edge-2")
        sink = NodeState("edge-3")
        source.record_event(
            "event-1",
            "water_level",
            {
                "tracking_id": "event-1",
                "location": "bridge",
                "value": 10,
            },
            category="sensor",
        )

        self.assertEqual(relay.apply_delta_payload(source.get_deltas_for({}), {}), 1)
        self.assertEqual(relay.delta_vv, {"edge-1": 1})
        self.assertEqual(relay.apply_delta_payload(sink.get_deltas_for({}), {}), 0)
        self.assertEqual(sink.apply_delta_payload(relay.get_deltas_for({}), {}), 1)
        self.assertEqual(sink.merkle_root(), source.merkle_root())

        source_log_length = len(source.delta_log)
        echo = sink.get_deltas_for(source.delta_vv)
        self.assertIsNone(echo["state"])
        self.assertEqual(source.apply_delta_payload(echo, source.delta_vv), 0)
        self.assertEqual(len(source.delta_log), source_log_length)

    def test_relay_does_not_resend_mutations_received_from_another_peer(self):
        source = NodeState("edge-1")
        relay = NodeState("edge-2")
        sink = NodeState("edge-3")
        source.record_event(
            "event-1",
            "water_level",
            {"location": "bridge", "value": 10},
            category="sensor",
        )
        relay.apply_delta_payload(source.get_deltas_for({}), {})
        sink.apply_delta_payload(source.get_deltas_for({}), {})

        from_relay = relay.get_deltas_for(sink.delta_vv)

        self.assertIsNone(from_relay["state"])
        self.assertEqual(sink.apply_delta_payload(from_relay, sink.delta_vv), 0)

    def test_converged_replica_pulls_only_the_new_mutation(self):
        nodes = [NodeState(f"edge-{index}") for index in range(3)]
        for index in range(30):
            nodes[index % 3].record_event(
                f"event-{index}",
                "water_level",
                {"location": f"bridge-{index}", "value": index},
                category="sensor",
            )
        for target in nodes:
            for source in nodes:
                if source is not target:
                    requested_vv = dict(target.delta_vv)
                    target.apply_delta_payload(
                        source.get_deltas_for(requested_vv), requested_vv
                    )
        self.assertEqual(len({node.merkle_root() for node in nodes}), 1)

        nodes[0].record_event(
            "event-new",
            "water_level",
            {"location": "bridge-new", "value": 99},
            category="sensor",
        )
        requested_vv = dict(nodes[1].delta_vv)
        nodes[1].apply_delta_payload(
            nodes[0].get_deltas_for(requested_vv), requested_vv
        )
        requested_vv = dict(nodes[2].delta_vv)
        payload = nodes[1].get_deltas_for(requested_vv)

        self.assertEqual(
            list(payload["state"]["registers"]), ["sensor:bridge-new:water_level"]
        )
        self.assertEqual(nodes[2].apply_delta_payload(payload, requested_vv), 1)
        self.assertEqual(nodes[2].merkle_root(), nodes[0].merkle_root())

    def test_delta_group_is_applied_once_and_relayed(self):
        source = NodeState("edge-1")
        replica = NodeState("edge-2")
        sink = NodeState("edge-3")
        source.record_event(
            "sensor-event",
            "water_level",
            {"location": "bridge", "value": 10},
            category="sensor",
        )
        source.record_event(
            "resource-event",
            "shelter_occupancy",
            {"location": "shelter-a", "value": 4},
            category="resource",
        )
        source.record_event(
            "hazard-event",
            "road_status",
            {"location": "highway_101", "value": "blocked"},
            category="infrastructure",
        )
        payload = source.get_deltas_for({})

        self.assertEqual(replica.apply_delta_payload(payload, {}), 3)
        self.assertEqual(replica.version, 1)
        self.assertEqual(replica.delta_vv, {"edge-1": 3})
        self.assertEqual(sink.apply_delta_payload(replica.get_deltas_for({}), {}), 3)
        self.assertEqual(sink.merkle_root(), source.merkle_root())

        log_length = len(replica.delta_log)
        self.assertEqual(replica.apply_delta_payload(payload, {}), 0)
        self.assertEqual(replica.version, 1)
        self.assertEqual(len(replica.delta_log), log_length)

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
        replica.apply_delta_payload(source.get_deltas_for(replica.delta_vv), {})

        source.record_event(
            "remove-event",
            "road_status",
            {"location": "highway_101", "value": "clear"},
            category="infrastructure",
            operation="remove",
        )
        requested_vv = dict(replica.delta_vv)
        replica.apply_delta_payload(source.get_deltas_for(requested_vv), requested_vv)

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
