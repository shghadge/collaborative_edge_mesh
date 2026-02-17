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
        return sum(self.counts.values())

    def increment(self, amount=1):
        self.counts[self.node_id] = self.counts.get(self.node_id, 0) + amount

    def merge(self, other):
        for node_id, count in other.counts.items():
            self.counts[node_id] = max(self.counts.get(node_id, 0), count)

    def to_dict(self):
        return {
            "type": "gcounter",
            "node_id": self.node_id,
            "description": self.description,
            "counts": dict(self.counts),
            "total_value": self.value,
        }

    @classmethod
    def from_dict(cls, data):
        c = cls(data["node_id"], description=data.get("description", ""))
        c.counts = dict(data["counts"])
        return c
