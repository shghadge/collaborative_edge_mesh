"""Last-Writer-Wins register CRDT."""

from datetime import datetime


class LWWRegister:
    """
    Last-Writer-Wins register CRDT. Stores a single value.
    On merge, the value with the later timestamp wins.

    Disaster use-case: latest sensor reading at a location
    (e.g., "water level at bridge_north is 4.1 meters").
    """

    def __init__(self, node_id, description=""):
        self.node_id = node_id
        self.description = description
        self._value = None
        self._timestamp = datetime.min
        self._writer_id = node_id
        self._event_id = ""

    @property
    def value(self):
        """Return the currently stored value."""
        return self._value

    def set(self, value, timestamp=None, event_id=None):
        """Write ``value`` if it orders after the current write.

        Writes are ordered by (timestamp, writer node id, event id). The
        timestamp defaults to now (UTC); the event id defaults to
        ``value["event_id"]`` when ``value`` is a dict.
        """
        ts = timestamp or datetime.utcnow()
        if event_id is None and isinstance(value, dict):
            event_id = value.get("event_id")
        event_id = str(event_id or "")
        candidate_order = (ts, self.node_id, event_id)
        current_order = (self._timestamp, self._writer_id, self._event_id)
        if candidate_order > current_order:
            self._value = value
            self._timestamp = ts
            self._writer_id = self.node_id
            self._event_id = event_id

    def merge(self, other):
        """Adopt the other register's value if its write orders later."""
        local_order = (self._timestamp, self._writer_id, self._event_id)
        remote_order = (other._timestamp, other._writer_id, other._event_id)
        if remote_order > local_order:
            self._value = other._value
            self._timestamp = other._timestamp
            self._writer_id = other._writer_id
            self._event_id = other._event_id

    def to_dict(self):
        """Serialize the register to a JSON-compatible dict."""
        return {
            "type": "lww_register",
            "node_id": self.node_id,
            "description": self.description,
            "value": self._value,
            "timestamp": self._timestamp.isoformat(),
            "writer_id": self._writer_id,
            "event_id": self._event_id,
        }

    def last_delta(self):
        """Minimal delta for an LWW-Register is the full register state.

        A register holds exactly one value, so the delta and the full
        state are identical. Included for interface consistency.
        """
        return self.to_dict()

    @classmethod
    def from_dict(cls, data):
        """Build a register from a dict produced by ``to_dict``."""
        r = cls(data["node_id"], description=data.get("description", ""))
        r._value = data["value"]
        r._timestamp = datetime.fromisoformat(data["timestamp"])
        r._writer_id = data["writer_id"]
        r._event_id = str(data.get("event_id") or "")
        return r
