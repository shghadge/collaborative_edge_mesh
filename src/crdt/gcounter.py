class GCounter:
    """
    Grow-only counter. Each node tracks its own count.
    Total = sum of all nodes. Merge = max per node.
    """

    def __init__(self, node_id):
        self.node_id = node_id
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
            "counts": dict(self.counts),
        }

    @classmethod
    def from_dict(cls, data):
        c = cls(data["node_id"])
        c.counts = dict(data["counts"])
        return c
