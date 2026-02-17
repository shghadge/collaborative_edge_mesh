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

    Internally: elements maps element -> set of unique tags.
    """

    def __init__(self, node_id, description=""):
        self.node_id = node_id
        self.description = description
        # element -> { tag1, tag2, ... }
        # an element is "in the set" if it has at least one tag
        self._elements = {}

    @property
    def value(self):
        """Return the set of elements currently in the set."""
        return {elem for elem, tags in self._elements.items() if tags}

    def add(self, element):
        """Add an element. Generates a unique tag for this add operation."""
        tag = f"{self.node_id}:{uuid.uuid4().hex[:12]}"
        if element not in self._elements:
            self._elements[element] = set()
        self._elements[element].add(tag)

    def remove(self, element):
        """Remove an element by clearing all *observed* tags.

        If another node concurrently adds the same element, their tag
        won't be in our observed set, so the element survives (add-wins).
        """
        if element in self._elements:
            self._elements[element] = set()

    def lookup(self, element):
        """Check if an element is in the set."""
        return bool(self._elements.get(element))

    def merge(self, other):
        """Merge another OR-Set. Union of all tags per element."""
        for elem, tags in other._elements.items():
            if elem not in self._elements:
                self._elements[elem] = set()
            self._elements[elem] = self._elements[elem].union(tags)

    def to_dict(self):
        return {
            "type": "or_set",
            "node_id": self.node_id,
            "description": self.description,
            "elements": {
                elem: sorted(tags)
                for elem, tags in self._elements.items()
                if tags  # only include elements that are actually present
            },
            "active_elements": sorted(self.value),
        }

    @classmethod
    def from_dict(cls, data):
        s = cls(data["node_id"], description=data.get("description", ""))
        s._elements = {
            elem: set(tags) for elem, tags in data.get("elements", {}).items()
        }
        return s
