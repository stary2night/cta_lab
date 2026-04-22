"""Unified cost models shared by vectorized and event-driven backtests."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import pandas as pd


class CostModel:
    """Base class for lightweight research cost models.

    Costs are expressed as cash amounts in event-driven simulations and as
    return fractions in vectorized simulations where NAV is normalized to 1.
    """

    def trade_cost(self, traded_notional: float, *, timestamp: pd.Timestamp | None = None) -> float:
        """Cost charged on one executed trade notional."""

        return 0.0

    def turnover_cost(self, turnover: float, *, timestamp: pd.Timestamp | None = None) -> float:
        """Cost charged on portfolio turnover expressed as fraction of NAV."""

        return 0.0

    def daily_cost(self, nav: float, *, timestamp: pd.Timestamp | None = None) -> float:
        """Daily accrual cost in cash amount."""

        return 0.0

    def daily_return_cost(self, *, timestamp: pd.Timestamp | None = None) -> float:
        """Daily accrual cost as return fraction for vectorized backtests."""

        return 0.0


@dataclass(frozen=True)
class ZeroCostModel(CostModel):
    """No transaction or daily accrual cost."""


@dataclass(frozen=True)
class ProportionalCostModel(CostModel):
    """Charge a fixed rate on traded notional or weight turnover."""

    rate: float = 0.0

    def trade_cost(self, traded_notional: float, *, timestamp: pd.Timestamp | None = None) -> float:
        return float(self.rate * abs(traded_notional))

    def turnover_cost(self, turnover: float, *, timestamp: pd.Timestamp | None = None) -> float:
        return float(self.rate * abs(turnover))


@dataclass(frozen=True)
class DailyAccrualCostModel(CostModel):
    """Daily accrual cost such as management or tracking fee."""

    annual_rate: float
    trading_days: int = 252

    def daily_cost(self, nav: float, *, timestamp: pd.Timestamp | None = None) -> float:
        return float(nav * self.daily_return_cost(timestamp=timestamp))

    def daily_return_cost(self, *, timestamp: pd.Timestamp | None = None) -> float:
        return float(self.annual_rate / self.trading_days)


class CompositeCostModel(CostModel):
    """Combine multiple cost models."""

    def __init__(self, models: Iterable[CostModel]) -> None:
        self.models = list(models)

    def trade_cost(self, traded_notional: float, *, timestamp: pd.Timestamp | None = None) -> float:
        return float(sum(model.trade_cost(traded_notional, timestamp=timestamp) for model in self.models))

    def turnover_cost(self, turnover: float, *, timestamp: pd.Timestamp | None = None) -> float:
        return float(sum(model.turnover_cost(turnover, timestamp=timestamp) for model in self.models))

    def daily_cost(self, nav: float, *, timestamp: pd.Timestamp | None = None) -> float:
        return float(sum(model.daily_cost(nav, timestamp=timestamp) for model in self.models))

    def daily_return_cost(self, *, timestamp: pd.Timestamp | None = None) -> float:
        return float(sum(model.daily_return_cost(timestamp=timestamp) for model in self.models))
