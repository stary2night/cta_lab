"""Example event-driven strategies."""

from __future__ import annotations

from backtest.event import Order
from strategies.base import EventDrivenStrategy


class SimpleRelativeMomentumEventStrategy(EventDrivenStrategy):
    """Sparse relative-momentum example for the event-driven path."""

    name = "simple_relative_momentum"

    def __init__(self, lookback: int = 20, rebalance_every: int = 20) -> None:
        self.lookback = lookback
        self.rebalance_every = rebalance_every

    def on_bar(self, context):
        now = context.now
        prices = context.data_portal.prices
        loc = prices.index.get_loc(now)

        if loc < self.lookback:
            return []
        if loc % self.rebalance_every != 0:
            return []

        window = prices.iloc[loc - self.lookback: loc + 1]
        trailing_ret = window.iloc[-1] / window.iloc[0] - 1.0
        winner = trailing_ret.idxmax()

        context.strategy_state.set("last_winner", winner)
        context.strategy_state.set("last_signal_date", now)

        return [
            Order.target_weight("TREND", 1.0 if winner == "TREND" else 0.0, now),
            Order.target_weight("DEFENSIVE", 1.0 if winner == "DEFENSIVE" else 0.0, now),
        ]
