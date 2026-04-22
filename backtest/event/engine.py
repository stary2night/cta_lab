"""Lightweight event-driven backtest engine."""

from __future__ import annotations

from typing import Protocol

from ..costs import CostModel
from ..slippage import SlippageModel
from .broker import SimulatedBroker
from .context import SimulationContext
from .data_portal import MarketDataPortal
from .events import Event, EventType
from .order import Fill, Order
from .recorder import EventRecorder
from .state import PortfolioState, StrategyState
from ..result import BacktestResult


class EventStrategyProtocol(Protocol):
    """Callback surface consumed by ``EventDrivenBacktestEngine``."""

    name: str

    def on_start(self, context: SimulationContext) -> None: ...

    def on_bar(self, context: SimulationContext) -> list[Order]: ...

    def on_event(self, event: Event, context: SimulationContext) -> list[Order]: ...

    def on_order(self, order: Order, context: SimulationContext) -> None: ...

    def on_fill(self, fill: Fill, context: SimulationContext) -> None: ...

    def on_finish(self, context: SimulationContext) -> None: ...


class EventDrivenBacktestEngine:
    """Run callback-style research strategies over market snapshots."""

    def __init__(
        self,
        data_portal: MarketDataPortal,
        initial_cash: float = 1.0,
        commission_rate: float = 0.0,
        cost_model: CostModel | None = None,
        slippage_model: SlippageModel | None = None,
        recorder: EventRecorder | None = None,
    ) -> None:
        self.data_portal = data_portal
        self.initial_cash = initial_cash
        self.commission_rate = commission_rate
        self.cost_model = cost_model
        self.slippage_model = slippage_model
        self.recorder = recorder if recorder is not None else EventRecorder()

    def run(self, strategy: EventStrategyProtocol) -> BacktestResult:
        """Execute a full event-driven backtest."""

        portfolio = PortfolioState(cash=self.initial_cash, nav=self.initial_cash)
        broker = SimulatedBroker(
            portfolio=portfolio,
            commission_rate=self.commission_rate,
            cost_model=self.cost_model,
            slippage_model=self.slippage_model,
        )
        context = SimulationContext(
            portfolio=portfolio,
            strategy_state=StrategyState(name=strategy.name),
            broker=broker,
            data_portal=self.data_portal,
        )

        strategy.on_start(context)

        for snapshot in self.data_portal:
            context.snapshot = snapshot
            context.strategy_state.timestamp = snapshot.timestamp
            prices = snapshot.prices.to_dict() if snapshot.prices is not None else {}
            portfolio.mark_to_market(prices, timestamp=snapshot.timestamp)

            event = Event.at(snapshot.timestamp, EventType.MARKET, {"snapshot": snapshot})
            for order in strategy.on_event(event, context):
                broker.submit_order(order)

            for order in strategy.on_bar(context):
                broker.submit_order(order)

            orders = list(broker.submitted_orders)
            fills = broker.execute_pending(snapshot)

            # Notify after execution so hooks see the updated portfolio state.
            for order in orders:
                strategy.on_order(order, context)
            for fill in fills:
                strategy.on_fill(fill, context)

            portfolio.mark_to_market(prices, timestamp=snapshot.timestamp)
            daily_cost = broker.accrue_daily_cost(snapshot.timestamp)
            portfolio.mark_to_market(prices, timestamp=snapshot.timestamp)
            self.recorder.record(snapshot, portfolio, fills, daily_cost=daily_cost)

        strategy.on_finish(context)
        return self.recorder.to_result()
