import copy
import unittest
from datetime import datetime, timedelta

from hypothesis import given, settings
from hypothesis import strategies as st

from src.crdt.gcounter import GCounter
from src.crdt.lww_register import LWWRegister
from src.crdt.orset import ORSet
from src.crdt.pncounter import PNCounter
from src.crdt.state import NodeState

NODE_IDS = ("edge-a", "edge-b", "edge-c")


def _clone(crdt):
    return crdt.__class__.from_dict(crdt.to_dict())


def _merge_copy(left, right):
    merged = _clone(left)
    merged.merge(_clone(right))
    return merged


def _state_clone(state: NodeState) -> NodeState:
    return NodeState.from_dict(state.to_dict(include_deltas=True))


def _state_merge_copy(left: NodeState, right: NodeState) -> NodeState:
    merged = _state_clone(left)
    merged.merge(_state_clone(right))
    return merged


def _gcounter_from_counts(node_id: str, counts: dict[str, int]) -> GCounter:
    counter = GCounter(node_id)
    counter.counts = dict(counts)
    return counter


def _pncounter_from_counts(
    node_id: str,
    positive: dict[str, int],
    negative: dict[str, int],
) -> PNCounter:
    counter = PNCounter(node_id)
    counter._p.counts = dict(positive)
    counter._n.counts = dict(negative)
    return counter


def _lww_register(node_id: str, timestamp_offset: int) -> LWWRegister:
    register = LWWRegister(node_id)
    register.set(
        f"{node_id}:{timestamp_offset}",
        datetime(2026, 1, 1) + timedelta(seconds=timestamp_offset),
    )
    return register


def _orset_from_maps(
    node_id: str,
    elements: dict[str, set[str]],
    removed: dict[str, set[str]],
) -> ORSet:
    crdt = ORSet(node_id)
    crdt._elements = copy.deepcopy(elements)
    crdt._removed = copy.deepcopy(removed)
    return crdt


def _gcounter_lattice(counter: GCounter) -> dict[str, int]:
    return dict(sorted(counter.counts.items()))


def _pncounter_lattice(counter: PNCounter) -> tuple[dict[str, int], dict[str, int]]:
    return (
        dict(sorted(counter._p.counts.items())),
        dict(sorted(counter._n.counts.items())),
    )


def _lww_lattice(register: LWWRegister) -> tuple[object, str, str, str]:
    return (
        register.value,
        register._timestamp.isoformat(),
        register._writer_id,
        register._event_id,
    )


def _orset_lattice(crdt: ORSet) -> tuple[dict[str, list[str]], dict[str, list[str]]]:
    return (
        {key: sorted(value) for key, value in sorted(crdt._elements.items()) if value},
        {key: sorted(value) for key, value in sorted(crdt._removed.items()) if value},
    )


count_maps = st.dictionaries(
    st.sampled_from(NODE_IDS),
    st.integers(min_value=0, max_value=50),
    min_size=1,
    max_size=3,
)

set_maps = st.dictionaries(
    st.sampled_from(("road-1", "road-2", "bridge-1")),
    st.sets(
        st.sampled_from(
            tuple(f"{node}:tag-{index}" for node in NODE_IDS for index in range(4))
        ),
        max_size=4,
    ),
    max_size=3,
)

node_events = st.lists(
    st.tuples(
        st.sampled_from(NODE_IDS),
        st.sampled_from(("sensor", "resource", "infrastructure", "general")),
        st.sampled_from(("water_level", "shelter_occupancy", "road_status")),
        st.integers(min_value=1, max_value=20),
        st.sampled_from(("loc-a", "loc-b", "loc-c")),
        st.sampled_from((None, "decrement", "remove")),
    ),
    min_size=1,
    max_size=20,
)


class CRDTPropertyTests(unittest.TestCase):
    @given(count_maps, count_maps, count_maps)
    def test_gcounter_merge_laws(self, a_counts, b_counts, c_counts):
        a = _gcounter_from_counts("edge-a", a_counts)
        b = _gcounter_from_counts("edge-b", b_counts)
        c = _gcounter_from_counts("edge-c", c_counts)

        self.assertEqual(
            _gcounter_lattice(_merge_copy(a, b)),
            _gcounter_lattice(_merge_copy(b, a)),
        )
        self.assertEqual(
            _gcounter_lattice(_merge_copy(_merge_copy(a, b), c)),
            _gcounter_lattice(_merge_copy(a, _merge_copy(b, c))),
        )
        self.assertEqual(_gcounter_lattice(_merge_copy(a, a)), _gcounter_lattice(a))

    @given(count_maps, count_maps, count_maps, count_maps, count_maps, count_maps)
    def test_pncounter_merge_laws(self, ap, an, bp, bn, cp, cn):
        a = _pncounter_from_counts("edge-a", ap, an)
        b = _pncounter_from_counts("edge-b", bp, bn)
        c = _pncounter_from_counts("edge-c", cp, cn)

        self.assertEqual(
            _pncounter_lattice(_merge_copy(a, b)),
            _pncounter_lattice(_merge_copy(b, a)),
        )
        self.assertEqual(
            _pncounter_lattice(_merge_copy(_merge_copy(a, b), c)),
            _pncounter_lattice(_merge_copy(a, _merge_copy(b, c))),
        )
        self.assertEqual(_pncounter_lattice(_merge_copy(a, a)), _pncounter_lattice(a))

    @given(
        st.tuples(st.sampled_from(NODE_IDS), st.integers(0, 10)),
        st.tuples(st.sampled_from(NODE_IDS), st.integers(0, 10)),
        st.tuples(st.sampled_from(NODE_IDS), st.integers(0, 10)),
    )
    def test_lww_register_merge_laws(self, a_data, b_data, c_data):
        a = _lww_register(*a_data)
        b = _lww_register(*b_data)
        c = _lww_register(*c_data)

        self.assertEqual(
            _lww_lattice(_merge_copy(a, b)),
            _lww_lattice(_merge_copy(b, a)),
        )
        self.assertEqual(
            _lww_lattice(_merge_copy(_merge_copy(a, b), c)),
            _lww_lattice(_merge_copy(a, _merge_copy(b, c))),
        )
        self.assertEqual(_lww_lattice(_merge_copy(a, a)), _lww_lattice(a))

    def test_lww_register_breaks_same_writer_timestamp_ties_with_event_id(self):
        timestamp = datetime(2026, 1, 1)
        first = LWWRegister("edge-a")
        first.set("first", timestamp=timestamp, event_id="event-a")
        second = LWWRegister("edge-a")
        second.set("second", timestamp=timestamp, event_id="event-b")

        left = _merge_copy(first, second)
        right = _merge_copy(second, first)

        self.assertEqual(_lww_lattice(left), _lww_lattice(right))
        self.assertEqual(left.value, "second")
        self.assertEqual(
            _lww_lattice(LWWRegister.from_dict(left.to_dict())),
            _lww_lattice(left),
        )

    @given(set_maps, set_maps, set_maps, set_maps, set_maps, set_maps)
    def test_orset_merge_laws(self, ae, ar, be, br, ce, cr):
        a = _orset_from_maps("edge-a", ae, ar)
        b = _orset_from_maps("edge-b", be, br)
        c = _orset_from_maps("edge-c", ce, cr)

        self.assertEqual(
            _orset_lattice(_merge_copy(a, b)),
            _orset_lattice(_merge_copy(b, a)),
        )
        self.assertEqual(
            _orset_lattice(_merge_copy(_merge_copy(a, b), c)),
            _orset_lattice(_merge_copy(a, _merge_copy(b, c))),
        )
        self.assertEqual(_orset_lattice(_merge_copy(a, a)), _orset_lattice(a))

    @settings(max_examples=50)
    @given(node_events)
    def test_node_state_eventual_convergence(self, events):
        replicas = {node_id: NodeState(node_id) for node_id in NODE_IDS}
        for index, (
            node_id,
            category,
            event_type,
            value,
            location,
            operation,
        ) in enumerate(events):
            if category == "sensor":
                op = None
            elif category == "resource":
                op = "decrement" if operation == "decrement" else None
            elif category == "infrastructure":
                op = "remove" if operation == "remove" else None
            else:
                op = None
            replicas[node_id].record_event(
                f"event-{index}",
                event_type,
                {"value": value, "location": location},
                category=category,
                operation=op,
            )

        left_fold = _state_clone(replicas["edge-a"])
        left_fold.merge(replicas["edge-b"])
        left_fold.merge(replicas["edge-c"])

        right_fold = _state_clone(replicas["edge-c"])
        right_fold.merge(replicas["edge-b"])
        right_fold.merge(replicas["edge-a"])

        self.assertEqual(left_fold.merkle_root(), right_fold.merkle_root())
        self.assertEqual(
            _state_merge_copy(left_fold, left_fold).merkle_root(),
            left_fold.merkle_root(),
        )


if __name__ == "__main__":
    unittest.main()
