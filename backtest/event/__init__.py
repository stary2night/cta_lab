"""Event-driven backtest primitives.

This package contains the lightweight domain objects shared by the future
event-driven engine. It intentionally does not replace the vectorized backtest
path; both paradigms can coexist behind different strategy protocols.
"""

from .adapters import TargetWeightStrategyAdapter
from .context import OrderSink, SimulationContext
from .broker import SimulatedBroker
from .data_portal import MarketDataPortal
from .engine import EventDrivenBacktestEngine, EventStrategyProtocol
from .events import Event, EventType
from .market import MarketSnapshot
from .order import Fill, Order, OrderSide, OrderStatus, OrderType, Transaction
from .recorder import EventRecorder
from .state import PortfolioState, PositionState, StrategyState

__all__ = [
    "Event",
    "EventDrivenBacktestEngine",
    "EventRecorder",
    "EventStrategyProtocol",
    "EventType",
    "Fill",
    "MarketDataPortal",
    "MarketSnapshot",
    "Order",
    "OrderSide",
    "OrderSink",
    "OrderStatus",
    "OrderType",
    "PortfolioState",
    "PositionState",
    "SimulatedBroker",
    "SimulationContext",
    "StrategyState",
    "TargetWeightStrategyAdapter",
    "Transaction",
]
