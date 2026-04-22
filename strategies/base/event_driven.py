"""Protocol for lightweight event-driven research strategies."""

from __future__ import annotations

from abc import ABC

import pandas as pd

from backtest.event import Event, Fill, Order, SimulationContext
from backtest.result import BacktestResult


class EventDrivenStrategy(ABC):
    """Base hook interface for event-driven strategies.

    Subclasses usually implement ``on_bar`` and optionally react to order/fill
    events. Hooks are intentionally no-op by default so research strategies can
    stay small and override only the callbacks they need.
    """

    name: str

    def run_event_backtest(
        self,
        price_df: pd.DataFrame | None = None,
        *,
        data_portal=None,
        engine=None,
        initial_cash: float = 1.0,
        commission_rate: float = 0.0,
        cost_model=None,
        slippage_model=None,
    ) -> BacktestResult:
        """Run this strategy on the lightweight event-driven engine.

        Parameters
        ----------
        price_df:
            Price matrix used to build a ``MarketDataPortal`` when no portal is
            supplied.
        data_portal:
            Optional pre-built event market data portal.
        engine:
            Optional pre-configured ``EventDrivenBacktestEngine``.
        initial_cash:
            Initial NAV/cash when the engine is constructed here.
        commission_rate:
            Immediate-fill broker commission rate when the engine is
            constructed here.
        cost_model:
            Optional unified cost model. When supplied, it supersedes the
            legacy ``commission_rate`` shortcut.
        slippage_model:
            Optional event execution price adjustment model.
        """

        if engine is None:
            from backtest.event import EventDrivenBacktestEngine, MarketDataPortal

            if data_portal is None:
                if price_df is None:
                    raise ValueError("price_df or data_portal is required.")
                data_portal = MarketDataPortal.from_prices(price_df)
            engine = EventDrivenBacktestEngine(
                data_portal=data_portal,
                initial_cash=initial_cash,
                commission_rate=commission_rate,
                cost_model=cost_model,
                slippage_model=slippage_model,
            )
        return engine.run(self)

    def on_start(self, context: SimulationContext) -> None:
        """Called once before the event loop starts."""

    def on_bar(self, context: SimulationContext) -> list[Order]:
        """Called for each market snapshot; return orders to submit."""

        return []

    def on_event(self, event: Event, context: SimulationContext) -> list[Order]:
        """Called for non-bar events when the engine chooses to expose them."""

        return []

    def on_order(self, order: Order, context: SimulationContext) -> None:
        """Called after an order state changes."""

    def on_fill(self, fill: Fill, context: SimulationContext) -> None:
        """Called after a fill is applied to portfolio state."""

    def on_finish(self, context: SimulationContext) -> None:
        """Called once after the event loop finishes."""
