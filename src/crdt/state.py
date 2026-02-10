import hashlib
import json
from datetime import datetime

from .gcounter import GCounter
from .lww_register import LWWRegister


class NodeState:
    """
    Composite state for a single edge node. Wraps multiple CRDTs:
    - event counters (G-Counter per event type)
    - registers (LWW-Register per key, e.g. location readings)
    Also tracks event IDs and computes a Merkle root for quick comparison.
    """

    def __init__(self, node_id):
        self.node_id = node_id
        self.version = 0
        self.updated_at = datetime.utcnow()
        self.counters = {}   # event_type -> GCounter
        self.registers = {}  # key -> LWWRegister
        self.event_ids = []

    def record_event(self, event_id, event_type, data):
        """Record an incoming event into the CRDT state."""
        # increment counter for this event type
        if event_type not in self.counters:
            self.counters[event_type] = GCounter(self.node_id)
        self.counters[event_type].increment(1)

        # store location-based values in a register
        if "location" in data and "value" in data:
            key = f"location:{data['location']}"
            if key not in self.registers:
                self.registers[key] = LWWRegister(self.node_id)
            self.registers[key].set({
                "value": data["value"],
                "event_id": event_id,
                "type": event_type,
            })

        # track event id
        if event_id not in self.event_ids:
            self.event_ids.append(event_id)

        self.version += 1
        self.updated_at = datetime.utcnow()

    def get_event_count(self, event_type=None):
        if event_type:
            c = self.counters.get(event_type)
            return c.value if c else 0
        return sum(c.value for c in self.counters.values())

    def merge(self, other):
        """Merge another node's state. All CRDTs merge independently."""
        for etype, counter in other.counters.items():
            if etype not in self.counters:
                self.counters[etype] = GCounter(self.node_id)
                self.counters[etype].counts = {}  # don't pre-seed with our node_id
            self.counters[etype].merge(counter)

        for key, reg in other.registers.items():
            if key not in self.registers:
                self.registers[key] = LWWRegister(self.node_id)
            self.registers[key].merge(reg)

        for eid in other.event_ids:
            if eid not in self.event_ids:
                self.event_ids.append(eid)

        self.version += 1
        self.updated_at = datetime.utcnow()

    def merkle_root(self):
        """Compute a hash fingerprint of the current state for quick comparison.
        Only hashes convergent data (counts, values) -- not node-specific fields."""
        leaves = []

        for etype in sorted(self.counters):
            # only hash the counts dict, not the node_id
            raw = f"c:{etype}:{json.dumps(self.counters[etype].counts, sort_keys=True)}"
            leaves.append(hashlib.sha256(raw.encode()).hexdigest())

        for key in sorted(self.registers):
            reg = self.registers[key]
            # only hash value, timestamp, and writer -- not node_id
            raw = f"r:{key}:{json.dumps({'v': reg._value, 't': reg._timestamp.isoformat(), 'w': reg._writer_id}, sort_keys=True)}"
            leaves.append(hashlib.sha256(raw.encode()).hexdigest())

        if not leaves:
            return hashlib.sha256(b"empty").hexdigest()

        # simple pairwise hashing until one root remains
        while len(leaves) > 1:
            next_level = []
            for i in range(0, len(leaves), 2):
                left = leaves[i]
                right = leaves[i + 1] if i + 1 < len(leaves) else left
                combined = hashlib.sha256((left + right).encode()).hexdigest()
                next_level.append(combined)
            leaves = next_level

        return leaves[0]

    def to_dict(self):
        return {
            "node_id": self.node_id,
            "version": self.version,
            "updated_at": self.updated_at.isoformat(),
            "counters": {k: v.to_dict() for k, v in self.counters.items()},
            "registers": {k: v.to_dict() for k, v in self.registers.items()},
            "event_ids": list(self.event_ids),
            "merkle_root": self.merkle_root(),
        }

    @classmethod
    def from_dict(cls, data):
        s = cls(data["node_id"])
        s.version = data["version"]
        s.updated_at = datetime.fromisoformat(data["updated_at"])
        s.counters = {k: GCounter.from_dict(v) for k, v in data.get("counters", {}).items()}
        s.registers = {k: LWWRegister.from_dict(v) for k, v in data.get("registers", {}).items()}
        s.event_ids = list(data.get("event_ids", []))
        return s
