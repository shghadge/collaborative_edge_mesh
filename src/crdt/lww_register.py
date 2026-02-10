from datetime import datetime


class LWWRegister:
    """
    Last-Writer-Wins register. Stores a single value.
    On merge, the value with the later timestamp wins.
    """

    def __init__(self, node_id):
        self.node_id = node_id
        self._value = None
        self._timestamp = datetime.min
        self._writer_id = node_id

    @property
    def value(self):
        return self._value

    def set(self, value, timestamp=None):
        ts = timestamp or datetime.utcnow()
        # only update if this write is newer (or same time but higher node id)
        if ts > self._timestamp or (ts == self._timestamp and self.node_id >= self._writer_id):
            self._value = value
            self._timestamp = ts
            self._writer_id = self.node_id

    def merge(self, other):
        if other._timestamp > self._timestamp:
            self._value = other._value
            self._timestamp = other._timestamp
            self._writer_id = other._writer_id
        elif other._timestamp == self._timestamp and other._writer_id > self._writer_id:
            self._value = other._value
            self._timestamp = other._timestamp
            self._writer_id = other._writer_id

    def to_dict(self):
        return {
            "type": "lww_register",
            "node_id": self.node_id,
            "value": self._value,
            "timestamp": self._timestamp.isoformat(),
            "writer_id": self._writer_id,
        }

    @classmethod
    def from_dict(cls, data):
        r = cls(data["node_id"])
        r._value = data["value"]
        r._timestamp = datetime.fromisoformat(data["timestamp"])
        r._writer_id = data["writer_id"]
        return r
