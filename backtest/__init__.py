"""Backtest 回测层。"""

from .engine import BacktestEngine
from .vectorized import VectorizedBacktest
from .walk_forward import WalkForwardEngine, WalkForwardResult, WalkForwardFold
from .position import PositionTracker, SimpleTracker, FXTracker
from .result import BacktestResult
from .costs import CostModel, ZeroCostModel, ProportionalCostModel, DailyAccrualCostModel, CompositeCostModel
from .reporting import turnover_cost_frame, turnover_cost_summary, turnover_from_weights
from .fees import FeeModel, ZeroFee, TradingFee, TrackingFee
from .slippage import SlippageModel, NoSlippage, FixedBpsSlippage
from .execution import apply_lag, VRS
from .event import (
    Event,
    EventDrivenBacktestEngine,
    EventRecorder,
    EventStrategyProtocol,
    EventType,
    Fill,
    MarketDataPortal,
    MarketSnapshot,
    Order,
    OrderSide,
    OrderSink,
    OrderStatus,
    OrderType,
    PortfolioState,
    PositionState,
    SimulatedBroker,
    SimulationContext,
    StrategyState,
    TargetWeightStrategyAdapter,
    Transaction,
)

__all__ = [
    # engine
    "BacktestEngine",
    "VectorizedBacktest",
    # walk-forward
    "WalkForwardEngine",
    "WalkForwardResult",
    "WalkForwardFold",
    # position
    "PositionTracker",
    "SimpleTracker",
    "FXTracker",
    # result
    "BacktestResult",
    # costs
    "CostModel",
    "ZeroCostModel",
    "ProportionalCostModel",
    "DailyAccrualCostModel",
    "CompositeCostModel",
    "turnover_from_weights",
    "turnover_cost_frame",
    "turnover_cost_summary",
    # fees
    "FeeModel",
    "ZeroFee",
    "TradingFee",
    "TrackingFee",
    # slippage
    "SlippageModel",
    "NoSlippage",
    "FixedBpsSlippage",
    # execution
    "apply_lag",
    "VRS",
    # event-driven primitives
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
