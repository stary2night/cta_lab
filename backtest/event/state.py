"""Portfolio and strategy state containers for event-driven backtests."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Mapping, MutableMapping

import pandas as pd


@dataclass
class PositionState:
    """State of one instrument in the simulated portfolio."""

    symbol: str
    quantity: float = 0.0
    market_price: float = 0.0
    market_value: float = 0.0
    cost_basis: float = 0.0
    realized_pnl: float = 0.0
    unrealized_pnl: float = 0.0
    metadata: MutableMapping[str, object] = field(default_factory=dict)

    @property
    def is_flat(self) -> bool:
        """Whether this position has no open quantity."""

        return abs(self.quantity) < 1e-12

    @property
    def weight(self) -> float:
        """Alias retained for compatibility when ``market_value`` is normalized."""

        return self.market_value

    def mark_to_market(self, price: float) -> None:
        """Update price-sensitive fields without changing quantity."""

        self.market_price = float(price)
        self.market_value = self.quantity * self.market_price
        self.unrealized_pnl = (self.market_price - self.cost_basis) * self.quantity


@dataclass
class PortfolioState:
    """Mutable portfolio state tracked by an event-driven engine."""

    timestamp: pd.Timestamp | None = None
    cash: float = 1.0
    nav: float = 1.0
    positions: MutableMapping[str, PositionState] = field(default_factory=dict)
    metadata: MutableMapping[str, object] = field(default_factory=dict)

    @property
    def gross_exposure(self) -> float:
        """Gross exposure as sum of absolute market values divided by NAV."""

        if self.nav == 0:
            return 0.0
        return sum(abs(pos.market_value) for pos in self.positions.values()) / self.nav

    @property
    def net_exposure(self) -> float:
        """Net exposure as sum of market values divided by NAV."""

        if self.nav == 0:
            return 0.0
        return sum(pos.market_value for pos in self.positions.values()) / self.nav

    def get_position(self, symbol: str) -> PositionState:
        """Return an existing position or create an empty one."""

        if symbol not in self.positions:
            self.positions[symbol] = PositionState(symbol=symbol)
        return self.positions[symbol]

    def weights(self) -> pd.Series:
        """Return current symbol weights."""

        if self.nav == 0 or not self.positions:
            return pd.Series(dtype=float)
        values = {
            symbol: pos.market_value / self.nav
            for symbol, pos in self.positions.items()
            if not pos.is_flat
        }
        return pd.Series(values, dtype=float)

    def mark_to_market(self, prices: Mapping[str, float], timestamp: pd.Timestamp | None = None) -> None:
        """Mark known positions and recompute NAV."""

        if timestamp is not None:
            self.timestamp = timestamp
        for symbol, price in prices.items():
            if symbol in self.positions:
                self.positions[symbol].mark_to_market(float(price))
        self.nav = self.cash + sum(pos.market_value for pos in self.positions.values())


@dataclass
class StrategyState:
    """Small mutable storage owned by a strategy instance."""

    name: str
    timestamp: pd.Timestamp | None = None
    data: MutableMapping[str, object] = field(default_factory=dict)

    def get(self, key: str, default: object | None = None) -> object | None:
        """Read strategy-local state."""

        return self.data.get(key, default)

    def set(self, key: str, value: object) -> None:
        """Write strategy-local state."""

        self.data[key] = value
