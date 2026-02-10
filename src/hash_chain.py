import hashlib
import json
import os
from datetime import datetime


class HashChainLog:
    """
    Append-only log where each entry includes the hash of the previous entry.
    This makes the log tamper-evident -- changing any entry breaks the chain.
    """

    def __init__(self, node_id, data_dir="/data/logs"):
        self.node_id = node_id
        self.entries = []
        self.data_dir = data_dir
        os.makedirs(data_dir, exist_ok=True)

    def append(self, event_id, event_type, event_data):
        prev_hash = self.entries[-1]["hash"] if self.entries else "genesis"

        entry = {
            "sequence": len(self.entries),
            "timestamp": datetime.utcnow().isoformat(),
            "event_id": event_id,
            "event_type": event_type,
            "data_hash": hashlib.sha256(
                json.dumps(event_data, sort_keys=True).encode()
            ).hexdigest(),
            "prev_hash": prev_hash,
        }
        # hash the entry itself (including prev_hash) to form the chain
        entry["hash"] = hashlib.sha256(
            json.dumps(entry, sort_keys=True).encode()
        ).hexdigest()

        self.entries.append(entry)
        return entry

    def verify(self):
        """Walk the chain and check every hash link."""
        for i, entry in enumerate(self.entries):
            expected_prev = self.entries[i - 1]["hash"] if i > 0 else "genesis"
            if entry["prev_hash"] != expected_prev:
                return False

            check = dict(entry)
            stored_hash = check.pop("hash")
            if (
                hashlib.sha256(json.dumps(check, sort_keys=True).encode()).hexdigest()
                != stored_hash
            ):
                return False

        return True

    def get_entries(self, since=0):
        return self.entries[since:]

    def latest_hash(self):
        return self.entries[-1]["hash"] if self.entries else "genesis"
