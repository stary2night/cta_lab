"""Lightweight event primitives for event-driven research backtests."""

from __future__ import annotations

from dataclasses import dataclass, field
from enum import Enum
from typing import Any, Mapping

import pandas as pd


class EventType(str, Enum):
    """Core event categories used by the lightweight event loop."""

    MARKET = "market"
    SIGNAL = "signal"
    ORDER = "order"
    FILL = "fill"
    PORTFOLIO = "portfolio"
    TIMER = "timer"
    START = "start"
    END = "end"


@dataclass(frozen=True)
class Event:
    """Generic timestamped event.

    The first-stage event layer intentionally keeps one small primitive instead
    of a deep event hierarchy. Domain-specific payloads can be carried in
    ``payload`` and formalized later only when repeated usage justifies it.
    """

    timestamp: pd.Timestamp
    type: EventType
    payload: Mapping[str, Any] = field(default_factory=dict)
    source: str | None = None

    @classmethod
    def at(
        cls,
        timestamp: pd.Timestamp | str,
        type: EventType | str,
        payload: Mapping[str, Any] | None = None,
        source: str | None = None,
    ) -> "Event":
        """Build an event with normalized timestamp and type."""

        return cls(
            timestamp=pd.Timestamp(timestamp),
            type=type if isinstance(type, EventType) else EventType(type),
            payload={} if payload is None else payload,
            source=source,
        )
