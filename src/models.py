import uuid
from datetime import datetime
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field


class Event(BaseModel):
    """An event submitted to an edge node."""

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    type: str
    value: Any
    location: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = Field(default_factory=dict)


class NodeStatus(BaseModel):
    """Status info returned by /status endpoint."""

    node_id: str
    version: int
    merkle_root: str
    peer_count: int
    event_count: int
    uptime_seconds: float
