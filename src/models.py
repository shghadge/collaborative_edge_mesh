import uuid
from datetime import datetime
from enum import Enum
from typing import Any, Dict, Optional
from pydantic import BaseModel, Field


class EventCategory(str, Enum):
    """Category of disaster event — determines which CRDT stores the data."""

    SENSOR = "sensor"  # water level, temperature, wind → LWW-Register
    RESOURCE = "resource"  # shelter occupancy, supplies → PN-Counter
    INFRASTRUCTURE = "infrastructure"  # blocked roads, outages → OR-Set
    GENERAL = "general"  # uncategorized events → G-Counter + LWW-Register


class Event(BaseModel):
    """An event submitted to an edge node.

    The `category` field determines how the event is stored in CRDTs:
      - SENSOR:         latest reading stored in LWW-Register, event counted in G-Counter
      - RESOURCE:       net value tracked in PN-Counter (use operation=decrement for departures)
      - INFRASTRUCTURE: element added/removed from OR-Set (use operation=remove to clear hazard)
      - GENERAL:        default — same behavior as before (G-Counter + LWW-Register)
    """

    id: str = Field(default_factory=lambda: str(uuid.uuid4()))
    type: str
    value: Any
    location: Optional[str] = None
    timestamp: datetime = Field(default_factory=datetime.utcnow)
    metadata: Dict[str, Any] = Field(default_factory=dict)
    category: EventCategory = EventCategory.GENERAL
    operation: Optional[str] = None  # "increment" | "decrement" | "add" | "remove"


class NodeStatus(BaseModel):
    """Status info returned by /status endpoint."""

    node_id: str
    version: int
    merkle_root: str
    peer_count: int
    event_count: int
    uptime_seconds: float
