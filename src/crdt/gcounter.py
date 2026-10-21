"""Grow-only counter (G-Counter) CRDT."""


class GCounter:
    """
    Grow-only counter CRDT. Each node tracks its own count.
    Total = sum of all nodes. Merge = max per node.

    Disaster use-case: counting how many events of a given type
    have been received (e.g., "14 water_level readings recorded").
    """

    def __init__(self, node_id, description=""):
        self.node_id = node_id
        self.description = description
        self.counts = {node_id: 0}

    @property
    def value(self):
        """Return the sum of all per-node counts."""
        return sum(self.counts.values())

    def increment(self, amount=1):
        """Add ``amount`` to this node's own slot."""
        self.counts[self.node_id] = self.counts.get(self.node_id, 0) + amount

    def merge(self, other):
        """Join another G-Counter into this one by taking the max per node."""
        for node_id, count in other.counts.items():
            self.counts[node_id] = max(self.counts.get(node_id, 0), count)

    def to_dict(self):
        """Serialize the counter to a JSON-compatible dict."""
        return {
            "type": "gcounter",
            "node_id": self.node_id,
            "description": self.description,
            "counts": dict(self.counts),
            "total_value": self.value,
        }

    def last_delta(self):
        """Minimal lattice join fragment: only this node's current count.

        Per Almeida et al., a delta mutator for a G-Counter increment
        produces only the updated slot for the mutating node. Merging
        this single-slot dict into a peer's counter via max-per-node
        advances the peer without transferring all other node slots.
        """
        return {
            "type": "gcounter",
            "node_id": self.node_id,
            "description": self.description,
            "counts": {self.node_id: self.counts.get(self.node_id, 0)},
            "total_value": self.counts.get(self.node_id, 0),
        }

    @classmethod
    def from_dict(cls, data):
        """Build a G-Counter from a dict produced by ``to_dict``."""
        c = cls(data["node_id"], description=data.get("description", ""))
        c.counts = dict(data["counts"])
        return c
