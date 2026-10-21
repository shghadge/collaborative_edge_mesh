"""
OR-Set (Observed-Remove Set) CRDT.

Used for sets where elements can be added and removed concurrently.
Add wins over concurrent remove — the safe default for disaster response
(better to over-report a hazard than to silently drop one).

Each add operation generates a globally unique tag. Remove only removes
tags that the removing node has *observed*. Concurrent adds from other
nodes survive a remove because their tags haven't been observed yet.
"""

import uuid


class ORSet:
    """
    Observed-Remove Set.

    Disaster use-case: tracking active hazards.
      - add("highway_101")    → highway_101 is blocked
      - remove("highway_101") → highway_101 is cleared
      - concurrent add + remove → add wins (road stays reported as blocked)

    Internally: elements maps element -> add tags, and removed maps
    element -> observed add tags removed by this replica. Merge is a
    monotonic union of both maps; the visible value is adds minus removals.
    """

    def __init__(self, node_id, description=""):
        self.node_id = node_id
        self.description = description
        self._elements = {}
        self._removed = {}
        self._last_delta = None

    @property
    def value(self):
        """Return the set of elements currently in the set."""
        active = set()
        for elem, tags in self._elements.items():
            if tags.difference(self._removed.get(elem, set())):
                active.add(elem)
        return active

    def add(self, element):
        """Add an element. Generates a unique tag for this add operation."""
        tag = f"{self.node_id}:{uuid.uuid4().hex[:12]}"
        self._elements.setdefault(element, set()).add(tag)
        self._last_delta = {
            "elements": {element: {tag}},
            "removed": {},
        }

    def remove(self, element):
        """Remove an element by tombstoning all *observed* tags.

        If another node concurrently adds the same element, their tag
        won't be in our observed set, so the element survives (add-wins).
        """
        observed = set(self._elements.get(element, set()))
        if observed:
            self._removed.setdefault(element, set()).update(observed)
        self._last_delta = {
            "elements": {},
            "removed": {element: observed} if observed else {},
        }

    def lookup(self, element):
        """Check if an element is in the set."""
        return element in self.value

    def merge(self, other):
        """Merge another OR-Set. Union of add tags and remove tombstones."""
        for elem, tags in other._elements.items():
            self._elements.setdefault(elem, set()).update(tags)
        for elem, tags in other._removed.items():
            self._removed.setdefault(elem, set()).update(tags)

    def to_dict(self):
        """Serialize the set, including add tags and removal tombstones."""
        return {
            "type": "or_set",
            "node_id": self.node_id,
            "description": self.description,
            "elements": {
                elem: sorted(tags) for elem, tags in self._elements.items() if tags
            },
            "removed_elements": {
                elem: sorted(tags) for elem, tags in self._removed.items() if tags
            },
            "active_elements": sorted(self.value),
        }

    def last_delta(self):
        """Minimal lattice join fragment for the most recent OR-Set mutation."""
        if self._last_delta is None:
            return self.to_dict()
        elements = {
            elem: sorted(tags)
            for elem, tags in self._last_delta["elements"].items()
            if tags
        }
        removed = {
            elem: sorted(tags)
            for elem, tags in self._last_delta["removed"].items()
            if tags
        }
        fragment = ORSet(self.node_id, self.description)
        fragment._elements = {elem: set(tags) for elem, tags in elements.items()}
        fragment._removed = {elem: set(tags) for elem, tags in removed.items()}
        return {
            "type": "or_set",
            "node_id": self.node_id,
            "description": self.description,
            "elements": elements,
            "removed_elements": removed,
            "active_elements": sorted(fragment.value),
        }

    @classmethod
    def from_dict(cls, data):
        """Build an OR-Set from a dict produced by ``to_dict``."""
        s = cls(data["node_id"], description=data.get("description", ""))
        s._elements = {
            elem: set(tags) for elem, tags in data.get("elements", {}).items()
        }
        s._removed = {
            elem: set(tags) for elem, tags in data.get("removed_elements", {}).items()
        }
        return s
