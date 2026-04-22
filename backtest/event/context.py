"""Simulation context passed into event-driven strategies."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Any, Protocol

import pandas as pd

from .market import MarketSnapshot
from .order import Order
from .state import PortfolioState, PositionState, StrategyState


class OrderSink(Protocol):
    """Minimal broker-like protocol accepted by ``SimulationContext``."""

    def submit_order(self, order: Order) -> None:
        """Submit an order to the execution component."""


@dataclass
class SimulationContext:
    """Runtime dependencies exposed to event-driven strategies."""

    portfolio: PortfolioState
    strategy_state: StrategyState
    snapshot: MarketSnapshot | None = None
    broker: OrderSink | None = None
    data_portal: Any | None = None
    metrics: dict[str, Any] = field(default_factory=dict)

    @property
    def now(self) -> pd.Timestamp | None:
        """Current simulation timestamp."""

        if self.snapshot is not None:
            return self.snapshot.timestamp
        return self.portfolio.timestamp

    def price(self, symbol: str, field: str = "settle") -> float:
        """Read the current price for a symbol."""

        if self.snapshot is None:
            raise RuntimeError("SimulationContext.snapshot is not set.")
        return self.snapshot.price(symbol, field=field)

    def position(self, symbol: str) -> PositionState:
        """Return portfolio position state for a symbol."""

        return self.portfolio.get_position(symbol)

    def submit_order(self, order: Order) -> None:
        """Submit an order through the configured broker/order sink."""

        if self.broker is None:
            raise RuntimeError("SimulationContext.broker is not set.")
        self.broker.submit_order(order)
