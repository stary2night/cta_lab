"""Adapters that bridge matrix-style research outputs into event callbacks."""

from __future__ import annotations

from dataclasses import dataclass
from typing import Iterable

import pandas as pd

from .context import SimulationContext
from .order import Order


@dataclass
class TargetWeightStrategyAdapter:
    """Expose a target-weight matrix as an event-driven strategy.

    This adapter is the compatibility bridge between the existing vectorized
    research path and the callback-style event engine. Each rebalance timestamp
    emits one ``Order.target_weight`` per symbol in the configured matrix.
    """

    weights: pd.DataFrame
    name: str = "target_weight_adapter"
    rebalance_dates: Iterable[pd.Timestamp | str] | None = None
    execution_lag: int = 0
    skip_unchanged: bool = True
    tolerance: float = 1e-12

    def __post_init__(self) -> None:
        if not isinstance(self.weights.index, pd.DatetimeIndex):
            raise TypeError("weights index must be pd.DatetimeIndex.")
        if self.execution_lag < 0:
            raise ValueError("execution_lag must be non-negative.")
        self.weights = self.weights.sort_index()
        self._effective_weights = self.weights.shift(self.execution_lag)
        self._rebalance_dates = None
        if self.rebalance_dates is not None:
            self._rebalance_dates = self._shift_rebalance_dates(self.rebalance_dates)

    def on_start(self, context: SimulationContext) -> None:
        """No-op hook for protocol compatibility."""

    def on_bar(self, context: SimulationContext) -> list[Order]:
        """Emit target-weight orders for the current timestamp."""

        if context.now is None:
            return []
        timestamp = pd.Timestamp(context.now)
        if timestamp not in self._effective_weights.index:
            return []
        if self._rebalance_dates is not None and timestamp not in self._rebalance_dates:
            return []

        target = self._effective_weights.loc[timestamp].fillna(0.0)
        current = context.portfolio.weights()
        orders: list[Order] = []
        for symbol, weight in target.items():
            symbol = str(symbol)
            target_weight = float(weight)
            current_weight = float(current.get(symbol, 0.0))
            if self.skip_unchanged and abs(target_weight - current_weight) <= self.tolerance:
                continue
            orders.append(Order.target_weight(symbol, target_weight, timestamp))
        return orders

    def on_event(self, event, context: SimulationContext) -> list[Order]:
        """No-op hook for protocol compatibility."""

        return []

    def on_order(self, order: Order, context: SimulationContext) -> None:
        """No-op hook for protocol compatibility."""

    def on_fill(self, fill, context: SimulationContext) -> None:
        """No-op hook for protocol compatibility."""

    def on_finish(self, context: SimulationContext) -> None:
        """No-op hook for protocol compatibility."""

    def _shift_rebalance_dates(
        self,
        rebalance_dates: Iterable[pd.Timestamp | str],
    ) -> set[pd.Timestamp]:
        """Shift signal rebalance dates to execution dates by index position."""

        shifted: set[pd.Timestamp] = set()
        index = pd.DatetimeIndex(self.weights.index)
        for date in rebalance_dates:
            timestamp = pd.Timestamp(date)
            if timestamp not in index:
                raise ValueError(f"rebalance date {timestamp!s} is not in weights index.")
            pos = index.get_loc(timestamp)
            if not isinstance(pos, int):
                raise ValueError(f"rebalance date {timestamp!s} is not unique.")
            exec_pos = pos + self.execution_lag
            if exec_pos < len(index):
                shifted.add(pd.Timestamp(index[exec_pos]))
        return shifted
