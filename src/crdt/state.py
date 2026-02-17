"""
Composite CRDT state for a single edge node.

Wraps four CRDT types, each serving a specific disaster-response role:
  - G-Counter:    count how many events of each type have been recorded
  - LWW-Register: latest sensor reading at a given location/type
  - PN-Counter:   net resource counts that can go up and down (shelter occupancy)
  - OR-Set:       set of active hazards (blocked roads, outages) — add wins

Events are routed to the correct CRDT based on their category.
"""

import hashlib
import json
from datetime import datetime

from .gcounter import GCounter
from .lww_register import LWWRegister
from .pncounter import PNCounter
from .orset import ORSet


class NodeState:
    """
    Composite state for a single edge node.

    Domain-aware event routing:
      SENSOR events        → G-Counter (event count) + LWW-Register (latest reading)
      RESOURCE events      → PN-Counter (net occupancy, arrivals minus departures)
      INFRASTRUCTURE events → OR-Set (active hazards set, add wins over remove)
      GENERAL events       → G-Counter + LWW-Register (backward compatible)
    """

    def __init__(self, node_id):
        self.node_id = node_id
        self.version = 0
        self.updated_at = datetime.utcnow()
        self.counters = {}       # key -> GCounter
        self.registers = {}      # key -> LWWRegister
        self.pn_counters = {}    # key -> PNCounter
        self.sets = {}           # key -> ORSet
        self.event_ids = []

    def record_event(self, event_id, event_type, data, category="general", operation=None):
        """Record an incoming event, routing it to the appropriate CRDT.

        Returns a dict describing where the data was stored.
        """
        stored_in = {}

        if category == "sensor":
            stored_in = self._record_sensor(event_id, event_type, data)
        elif category == "resource":
            stored_in = self._record_resource(event_id, event_type, data, operation)
        elif category == "infrastructure":
            stored_in = self._record_infrastructure(event_id, event_type, data, operation)
        else:
            stored_in = self._record_general(event_id, event_type, data)

        # track event id
        if event_id not in self.event_ids:
            self.event_ids.append(event_id)

        self.version += 1
        self.updated_at = datetime.utcnow()
        return stored_in

    def _record_sensor(self, event_id, event_type, data):
        """Sensor data: count the event, store latest reading in register.

        Register key: sensor:<location>:<event_type>
        This prevents cross-type clobbering at the same location.
        """
        # count this event type
        counter_key = f"event_count:{event_type}"
        if counter_key not in self.counters:
            self.counters[counter_key] = GCounter(
                self.node_id,
                description=f"Number of {event_type} readings recorded"
            )
        self.counters[counter_key].increment(1)

        # store latest reading
        location = data.get("location", "unknown")
        register_key = f"sensor:{location}:{event_type}"
        if register_key not in self.registers:
            self.registers[register_key] = LWWRegister(
                self.node_id,
                description=f"Latest {event_type} reading at {location}"
            )
        self.registers[register_key].set({
            "value": data.get("value"),
            "unit": data.get("unit", ""),
            "severity": data.get("severity", ""),
            "event_id": event_id,
            "event_type": event_type,
            "category": "sensor",
        })

        return {
            "counter_key": counter_key,
            "register_key": register_key,
            "category": "sensor",
        }

    def _record_resource(self, event_id, event_type, data, operation=None):
        """Resource tracking: use PN-Counter for net value.

        Counter key: resource:<location>:<event_type>
        """
        location = data.get("location", "unknown")
        counter_key = f"resource:{location}:{event_type}"
        if counter_key not in self.pn_counters:
            self.pn_counters[counter_key] = PNCounter(
                self.node_id,
                description=f"Net {event_type} at {location}"
            )

        value = data.get("value", 0)
        if isinstance(value, (int, float)):
            if operation == "decrement":
                self.pn_counters[counter_key].decrement(int(value))
            else:
                self.pn_counters[counter_key].increment(int(value))

        # also count the event
        event_counter_key = f"event_count:{event_type}"
        if event_counter_key not in self.counters:
            self.counters[event_counter_key] = GCounter(
                self.node_id,
                description=f"Number of {event_type} reports recorded"
            )
        self.counters[event_counter_key].increment(1)

        return {
            "pn_counter_key": counter_key,
            "counter_key": event_counter_key,
            "operation": operation or "increment",
            "category": "resource",
        }

    def _record_infrastructure(self, event_id, event_type, data, operation=None):
        """Infrastructure hazards: use OR-Set to track active hazards.

        Set key: hazards:<event_type>
        Element: the location or value representing the hazard
        """
        set_key = f"hazards:{event_type}"
        if set_key not in self.sets:
            self.sets[set_key] = ORSet(
                self.node_id,
                description=f"Active {event_type} hazards"
            )

        location = data.get("location", "unknown")
        hazard_element = location

        if operation == "remove":
            self.sets[set_key].remove(hazard_element)
        else:
            self.sets[set_key].add(hazard_element)

        # store details in a register for context
        register_key = f"infra:{location}:{event_type}"
        if register_key not in self.registers:
            self.registers[register_key] = LWWRegister(
                self.node_id,
                description=f"Latest {event_type} status at {location}"
            )
        self.registers[register_key].set({
            "value": data.get("value"),
            "cause": data.get("cause", ""),
            "estimated_restore": data.get("estimated_restore", ""),
            "event_id": event_id,
            "event_type": event_type,
            "category": "infrastructure",
        })

        # count the event
        event_counter_key = f"event_count:{event_type}"
        if event_counter_key not in self.counters:
            self.counters[event_counter_key] = GCounter(
                self.node_id,
                description=f"Number of {event_type} reports recorded"
            )
        self.counters[event_counter_key].increment(1)

        return {
            "set_key": set_key,
            "register_key": register_key,
            "counter_key": event_counter_key,
            "operation": operation or "add",
            "category": "infrastructure",
        }

    def _record_general(self, event_id, event_type, data):
        """General/uncategorized events. Backward-compatible behavior."""
        # count event type
        counter_key = f"event_count:{event_type}"
        if counter_key not in self.counters:
            self.counters[counter_key] = GCounter(
                self.node_id,
                description=f"Number of {event_type} events recorded"
            )
        self.counters[counter_key].increment(1)

        # store in register if location and value present
        location = data.get("location")
        register_key = None
        if location and "value" in data:
            register_key = f"general:{location}:{event_type}"
            if register_key not in self.registers:
                self.registers[register_key] = LWWRegister(
                    self.node_id,
                    description=f"Latest {event_type} at {location}"
                )
            self.registers[register_key].set({
                "value": data["value"],
                "event_id": event_id,
                "event_type": event_type,
                "category": "general",
            })

        result = {"counter_key": counter_key, "category": "general"}
        if register_key:
            result["register_key"] = register_key
        return result

    def get_event_count(self, event_type=None):
        if event_type:
            key = f"event_count:{event_type}"
            c = self.counters.get(key)
            return c.value if c else 0
        return sum(c.value for c in self.counters.values())

    def summary(self):
        """Human-readable summary of current state."""
        sensor_keys = [k for k in self.registers if k.startswith("sensor:")]
        resource_keys = list(self.pn_counters.keys())
        infra_keys = list(self.sets.keys())

        return {
            "sensor_readings": len(sensor_keys),
            "resource_trackers": len(resource_keys),
            "active_hazard_sets": len(infra_keys),
            "total_events": self.get_event_count(),
            "crdt_counts": {
                "g_counters": len(self.counters),
                "lww_registers": len(self.registers),
                "pn_counters": len(self.pn_counters),
                "or_sets": len(self.sets),
            },
        }

    def merge(self, other):
        """Merge another node's state. All CRDTs merge independently.
        Only bumps version if state actually changed."""
        old_root = self.merkle_root()

        # merge G-Counters
        for key, counter in other.counters.items():
            if key not in self.counters:
                self.counters[key] = GCounter(self.node_id, description=counter.description)
                self.counters[key].counts = {}
            self.counters[key].merge(counter)

        # merge LWW-Registers
        for key, reg in other.registers.items():
            if key not in self.registers:
                self.registers[key] = LWWRegister(self.node_id, description=reg.description)
            self.registers[key].merge(reg)

        # merge PN-Counters
        for key, pnc in other.pn_counters.items():
            if key not in self.pn_counters:
                self.pn_counters[key] = PNCounter(self.node_id, description=pnc.description)
                self.pn_counters[key]._p.counts = {}
                self.pn_counters[key]._n.counts = {}
            self.pn_counters[key].merge(pnc)

        # merge OR-Sets
        for key, orset in other.sets.items():
            if key not in self.sets:
                self.sets[key] = ORSet(self.node_id, description=orset.description)
            self.sets[key].merge(orset)

        # merge event_ids
        for eid in other.event_ids:
            if eid not in self.event_ids:
                self.event_ids.append(eid)

        if self.merkle_root() != old_root:
            self.version += 1
            self.updated_at = datetime.utcnow()

    def merkle_root(self):
        """Compute a hash fingerprint of the current state for quick comparison.
        Includes all four CRDT types."""
        leaves = []

        # G-Counters
        for key in sorted(self.counters):
            raw = f"c:{key}:{json.dumps(self.counters[key].counts, sort_keys=True)}"
            leaves.append(hashlib.sha256(raw.encode()).hexdigest())

        # LWW-Registers
        for key in sorted(self.registers):
            reg = self.registers[key]
            raw = f"r:{key}:{json.dumps({'v': reg._value, 't': reg._timestamp.isoformat(), 'w': reg._writer_id}, sort_keys=True)}"
            leaves.append(hashlib.sha256(raw.encode()).hexdigest())

        # PN-Counters
        for key in sorted(self.pn_counters):
            pnc = self.pn_counters[key]
            raw = f"pn:{key}:{json.dumps({'p': pnc._p.counts, 'n': pnc._n.counts}, sort_keys=True)}"
            leaves.append(hashlib.sha256(raw.encode()).hexdigest())

        # OR-Sets
        for key in sorted(self.sets):
            orset = self.sets[key]
            raw = f"s:{key}:{json.dumps({e: sorted(t) for e, t in orset._elements.items()}, sort_keys=True)}"
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
            "pn_counters": {k: v.to_dict() for k, v in self.pn_counters.items()},
            "sets": {k: v.to_dict() for k, v in self.sets.items()},
            "event_ids": list(self.event_ids),
            "merkle_root": self.merkle_root(),
            "state_summary": self.summary(),
        }

    @classmethod
    def from_dict(cls, data):
        s = cls(data["node_id"])
        s.version = data["version"]
        s.updated_at = datetime.fromisoformat(data["updated_at"])
        s.counters = {
            k: GCounter.from_dict(v) for k, v in data.get("counters", {}).items()
        }
        s.registers = {
            k: LWWRegister.from_dict(v) for k, v in data.get("registers", {}).items()
        }
        s.pn_counters = {
            k: PNCounter.from_dict(v) for k, v in data.get("pn_counters", {}).items()
        }
        s.sets = {
            k: ORSet.from_dict(v) for k, v in data.get("sets", {}).items()
        }
        s.event_ids = list(data.get("event_ids", []))
        return s
