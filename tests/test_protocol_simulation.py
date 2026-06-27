import json
import unittest

from src.crdt import GCounter, LWWRegister, NodeState, ORSet, PNCounter


def _record_sample_events(nodes: list[NodeState], events_per_node: int = 3) -> None:
    for node_index, node in enumerate(nodes):
        for event_index in range(events_per_node):
            event_id = f"{node.node_id}-sensor-{event_index}"
            node.record_event(
                event_id,
                "water_level",
                {
                    "location": f"bridge-{event_index % 2}",
                    "value": node_index * 100 + event_index,
                    "unit": "cm",
                },
                category="sensor",
            )
        node.record_event(
            f"{node.node_id}-resource",
            "shelter_occupancy",
            {"location": f"shelter-{node_index % 2}", "value": node_index + 1},
            category="resource",
        )
        node.record_event(
            f"{node.node_id}-hazard-add",
            "road_status",
            {"location": f"road-{node_index}", "value": "blocked"},
            category="infrastructure",
        )


def _merge_full_mesh(nodes: list[NodeState]) -> None:
    snapshots = [NodeState.from_dict(node.to_dict()) for node in nodes]
    for target in nodes:
        for snapshot in snapshots:
            if snapshot.node_id != target.node_id:
                target.merge(snapshot)


def _replay_all_deltas(nodes: list[NodeState]) -> None:
    for source in nodes:
        payload = source.get_deltas_since(0)
        for target in nodes:
            if target is not source:
                target.apply_delta_payload(payload)


def _roots(nodes: list[NodeState]) -> set[str]:
    return {node.merkle_root() for node in nodes}


def _naive_key_level_delta_state(
    state: NodeState, stored_in: dict, event_id: str
) -> dict:
    delta = NodeState(state.node_id, state.event_id_retention_limit)
    counter_key = stored_in.get("counter_key")
    if counter_key:
        delta.counters[counter_key] = GCounter.from_dict(
            state.counters[counter_key].to_dict()
        )
    register_key = stored_in.get("register_key")
    if register_key:
        delta.registers[register_key] = LWWRegister.from_dict(
            state.registers[register_key].to_dict()
        )
    pn_counter_key = stored_in.get("pn_counter_key")
    if pn_counter_key:
        delta.pn_counters[pn_counter_key] = PNCounter.from_dict(
            state.pn_counters[pn_counter_key].to_dict()
        )
    set_key = stored_in.get("set_key")
    if set_key:
        delta.sets[set_key] = ORSet.from_dict(state.sets[set_key].to_dict())
    delta.event_ids = [event_id]
    return delta.to_dict()


class ProtocolSimulationTests(unittest.TestCase):
    def test_full_state_mesh_converges_in_process(self):
        nodes = [NodeState(f"edge-{index}") for index in range(5)]
        _record_sample_events(nodes)

        _merge_full_mesh(nodes)

        self.assertEqual(len(_roots(nodes)), 1)
        self.assertEqual({node.get_event_count() for node in nodes}, {25})

    def test_true_delta_mesh_converges_in_process(self):
        nodes = [NodeState(f"edge-{index}") for index in range(5)]
        _record_sample_events(nodes)

        _replay_all_deltas(nodes)

        self.assertEqual(len(_roots(nodes)), 1)
        self.assertEqual({node.get_event_count() for node in nodes}, {25})

    def test_delta_pull_can_leave_valid_root_mismatch(self):
        remote = NodeState("edge-1")
        local = NodeState("edge-2")
        remote.record_event(
            "remote-event",
            "water_level",
            {"location": "bridge", "value": 3},
            category="sensor",
        )
        local.record_event(
            "local-event",
            "water_level",
            {"location": "bridge", "value": 4},
            category="sensor",
        )
        remote_advertised_root = remote.merkle_root()

        local.apply_delta_payload(remote.get_deltas_since(0))

        self.assertNotEqual(local.merkle_root(), remote_advertised_root)
        self.assertEqual(local.get_event_count("water_level"), 2)

    def test_true_delta_payload_is_smaller_than_key_level_snapshot(self):
        source = NodeState("edge-1")
        peer = NodeState("edge-2")
        peer.record_event(
            "peer-event",
            "water_level",
            {"location": "bridge", "value": 3},
            category="sensor",
        )
        source.merge(peer)

        stored_in = source.record_event(
            "source-event",
            "water_level",
            {"location": "bridge", "value": 4},
            category="sensor",
        )

        true_delta = source.delta_log[-1]["state"]
        naive_delta = _naive_key_level_delta_state(source, stored_in, "source-event")

        self.assertLess(
            len(json.dumps(true_delta, sort_keys=True)),
            len(json.dumps(naive_delta, sort_keys=True)),
        )
        self.assertEqual(
            true_delta["counters"]["event_count:water_level"]["counts"],
            {"edge-1": 1},
        )
        self.assertEqual(
            naive_delta["counters"]["event_count:water_level"]["counts"],
            {"edge-1": 1, "edge-2": 1},
        )

    def test_delta_log_gap_signals_full_state_fallback_needed(self):
        source = NodeState("edge-1", delta_log_limit=2)
        for index in range(4):
            source.record_event(
                f"event-{index}",
                "water_level",
                {"location": "bridge", "value": index},
                category="sensor",
            )

        payload = source.get_deltas_since(0)

        self.assertTrue(payload["has_gap"])
        self.assertEqual(payload["deltas"], [])
        self.assertEqual(source.apply_delta_payload(payload), 0)

    def test_orset_remove_converges_through_delta_replay(self):
        source = NodeState("edge-1")
        replica = NodeState("edge-2")

        source.record_event(
            "hazard-add",
            "road_status",
            {"location": "road-9", "value": "blocked"},
            category="infrastructure",
        )
        replica.apply_delta_payload(source.get_deltas_since(0))
        last_seen = source.delta_seq

        source.record_event(
            "hazard-remove",
            "road_status",
            {"location": "road-9", "value": "clear"},
            category="infrastructure",
            operation="remove",
        )
        replica.apply_delta_payload(source.get_deltas_since(last_seen))

        self.assertEqual(replica.merkle_root(), source.merkle_root())
        self.assertNotIn("road-9", replica.sets["hazards:road_status"].value)


if __name__ == "__main__":
    unittest.main()
