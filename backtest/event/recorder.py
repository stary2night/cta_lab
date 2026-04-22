"""Recording helpers for event-driven backtest results."""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from .market import MarketSnapshot
from .order import Fill
from .state import PortfolioState
from ..result import BacktestResult


@dataclass
class EventRecorder:
    """Collect portfolio snapshots and convert them to ``BacktestResult``."""

    nav_records: list[tuple[pd.Timestamp, float]] = field(default_factory=list)
    holdings_records: list[tuple[pd.Timestamp, pd.Series]] = field(default_factory=list)
    fee_records: list[dict[str, object]] = field(default_factory=list)
    turnover_records: list[tuple[pd.Timestamp, float]] = field(default_factory=list)
    fill_records: list[dict[str, object]] = field(default_factory=list)
    _last_weights: pd.Series = field(default_factory=lambda: pd.Series(dtype=float))

    def record(
        self,
        snapshot: MarketSnapshot,
        portfolio: PortfolioState,
        fills: list[Fill],
        daily_cost: float = 0.0,
    ) -> None:
        """Record one end-of-bar portfolio state."""

        timestamp = snapshot.timestamp
        weights = portfolio.weights()
        turnover = self._turnover(fills, portfolio)
        commission = sum(fill.commission for fill in fills)
        slippage = sum(fill.slippage for fill in fills)

        self.nav_records.append((timestamp, float(portfolio.nav)))
        self.holdings_records.append((timestamp, weights))
        self.turnover_records.append((timestamp, turnover))
        self.fee_records.append(
            {
                "date": timestamp,
                "commission": commission,
                "slippage": slippage,
                "daily_cost": daily_cost,
                "total_cost": commission + daily_cost + slippage,
            }
        )

        for fill in fills:
            self.fill_records.append(
                {
                    "date": fill.timestamp,
                    "order_id": fill.order_id,
                    "symbol": fill.symbol,
                    "side": fill.side.value,
                    "quantity": fill.quantity,
                    "price": fill.price,
                    "commission": fill.commission,
                    "slippage": fill.slippage,
                    "mid_price": fill.metadata.get("mid_price"),
                }
            )

        self._last_weights = weights

    def to_result(self) -> BacktestResult:
        """Build a standard ``BacktestResult`` from recorded states."""

        if not self.nav_records:
            empty = pd.Series(dtype=float, name="nav")
            return BacktestResult(nav=empty, returns=empty.rename("returns"))

        nav = pd.Series(
            [value for _, value in self.nav_records],
            index=pd.DatetimeIndex([date for date, _ in self.nav_records]),
            name="nav",
        )
        returns = nav.pct_change().fillna(0.0).rename("returns")

        holdings_log = self._holdings_frame()
        turnover = pd.Series(
            [value for _, value in self.turnover_records],
            index=pd.DatetimeIndex([date for date, _ in self.turnover_records]),
            name="turnover",
        )
        fee_log = pd.DataFrame(self.fee_records).set_index("date")

        return BacktestResult(
            nav=nav,
            returns=returns,
            positions_df=holdings_log,
            turnover_series=turnover,
            holdings_log=holdings_log,
            fee_log=fee_log,
        )

    def fills_frame(self) -> pd.DataFrame:
        """Return recorded fills as a DataFrame."""

        if not self.fill_records:
            return pd.DataFrame()
        return pd.DataFrame(self.fill_records).set_index("date")

    def _turnover(self, fills: list[Fill], portfolio: PortfolioState) -> float:
        if portfolio.nav == 0:
            return 0.0
        traded_notional = sum(abs(fill.notional) for fill in fills)
        return float(traded_notional / portfolio.nav)

    def _holdings_frame(self) -> pd.DataFrame:
        if not self.holdings_records:
            return pd.DataFrame()
        rows = []
        dates = []
        for timestamp, weights in self.holdings_records:
            rows.append(weights)
            dates.append(timestamp)
        return pd.DataFrame(rows, index=pd.DatetimeIndex(dates)).fillna(0.0)
