"""
PN-Counter (Positive-Negative Counter) CRDT.

Used for values that can both increase and decrease, such as shelter
occupancy (arrivals minus departures). Internally composed of two
G-Counters: one for increments (P) and one for decrements (N).
Value = sum(P) - sum(N). Merge takes max per node on both sides.
"""

from .gcounter import GCounter


class PNCounter:
    """
    Positive-Negative Counter.

    Disaster use-case: shelter occupancy tracking.
      - 12 people arrive  → increment(12)
      - 3 people depart   → decrement(3)
      - value → 9 currently present

    Merge is conflict-free: each side takes max per node.
    """

    def __init__(self, node_id, description=""):
        self.node_id = node_id
        self.description = description
        self._p = GCounter(node_id)  # positive increments
        self._n = GCounter(node_id)  # negative decrements

    @property
    def value(self):
        """Net value = total increments - total decrements."""
        return self._p.value - self._n.value

    def increment(self, amount=1):
        """Record a positive change (e.g., people arriving)."""
        self._p.increment(amount)

    def decrement(self, amount=1):
        """Record a negative change (e.g., people departing)."""
        self._n.increment(amount)

    def merge(self, other):
        """Merge another PN-Counter. Both P and N counters merge independently."""
        self._p.merge(other._p)
        self._n.merge(other._n)

    def to_dict(self):
        return {
            "type": "pn_counter",
            "node_id": self.node_id,
            "description": self.description,
            "positive": self._p.to_dict(),
            "negative": self._n.to_dict(),
            "total_value": self.value,
        }

    @classmethod
    def from_dict(cls, data):
        c = cls(data["node_id"], description=data.get("description", ""))
        c._p = GCounter.from_dict(data["positive"])
        c._n = GCounter.from_dict(data["negative"])
        return c
