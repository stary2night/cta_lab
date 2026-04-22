"""Market snapshot objects consumed by event-driven strategies."""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import TYPE_CHECKING, Mapping

import pandas as pd

if TYPE_CHECKING:
    from data.model import Bar


@dataclass(frozen=True)
class MarketSnapshot:
    """Cross-sectional market view for one simulation timestamp."""

    timestamp: pd.Timestamp
    bars: Mapping[str, "Bar"] = field(default_factory=dict)
    prices: pd.Series | None = None
    returns: pd.Series | None = None
    metadata: Mapping[str, object] = field(default_factory=dict)

    def symbols(self) -> list[str]:
        """Return symbols available in this snapshot."""

        if self.prices is not None:
            return [str(symbol) for symbol in self.prices.index]
        return list(self.bars.keys())

    def price(self, symbol: str, field: str = "settle") -> float:
        """Return a symbol price from ``prices`` or the stored ``Bar``."""

        if self.prices is not None and symbol in self.prices.index:
            return float(self.prices.loc[symbol])
        if symbol not in self.bars:
            raise KeyError(f"Symbol {symbol!r} is not available in snapshot.")
        bar = self.bars[symbol]
        if not hasattr(bar, field):
            raise AttributeError(f"Bar has no price field {field!r}.")
        return float(getattr(bar, field))

    def return_of(self, symbol: str, default: float = 0.0) -> float:
        """Return a symbol's current-period return if available."""

        if self.returns is None or symbol not in self.returns.index:
            return default
        value = self.returns.loc[symbol]
        return default if pd.isna(value) else float(value)
