"""Market data portal for lightweight event-driven backtests."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterator

import pandas as pd

from .market import MarketSnapshot


@dataclass(frozen=True)
class MarketDataPortal:
    """Iterate DataFrame market data as timestamped snapshots.

    The first version intentionally targets the dominant research input in
    ``cta_lab``: date x symbol price/return matrices. Richer ``BarSeries`` based
    portals can be added later without changing strategy callbacks.
    """

    prices: pd.DataFrame
    returns: pd.DataFrame | None = None

    def __post_init__(self) -> None:
        if not isinstance(self.prices.index, pd.DatetimeIndex):
            raise TypeError("prices index must be pd.DatetimeIndex.")
        if self.returns is not None and not isinstance(self.returns.index, pd.DatetimeIndex):
            raise TypeError("returns index must be pd.DatetimeIndex.")

    @classmethod
    def from_prices(cls, prices: pd.DataFrame) -> "MarketDataPortal":
        """Build a portal from a price matrix and derive simple returns."""

        return cls(prices=prices.sort_index(), returns=prices.sort_index().pct_change())

    @classmethod
    def from_returns(cls, returns: pd.DataFrame, base_price: float = 1.0) -> "MarketDataPortal":
        """Build synthetic prices from returns for callback-style tests."""

        returns = returns.sort_index()
        prices = base_price * (1.0 + returns.fillna(0.0)).cumprod()
        return cls(prices=prices, returns=returns)

    @property
    def symbols(self) -> list[str]:
        """Return the symbols covered by this data portal."""

        return [str(symbol) for symbol in self.prices.columns]

    @property
    def dates(self) -> pd.DatetimeIndex:
        """Return available simulation dates."""

        return pd.DatetimeIndex(self.prices.index)

    def snapshot_at(self, timestamp: pd.Timestamp | str) -> MarketSnapshot:
        """Return one market snapshot."""

        ts = pd.Timestamp(timestamp)
        price_row = self.prices.loc[ts].astype(float)
        returns_row = None
        if self.returns is not None and ts in self.returns.index:
            returns_row = self.returns.loc[ts].astype(float)
        return MarketSnapshot(timestamp=ts, prices=price_row, returns=returns_row)

    def __iter__(self) -> Iterator[MarketSnapshot]:
        for timestamp in self.dates:
            yield self.snapshot_at(timestamp)
