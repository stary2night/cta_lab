"""Immediate-fill simulated broker for research event backtests."""

from __future__ import annotations

from dataclasses import dataclass, field

import pandas as pd

from ..costs import CostModel, ProportionalCostModel, ZeroCostModel
from ..slippage import NoSlippage, SlippageModel
from .market import MarketSnapshot
from .order import Fill, Order, OrderSide, OrderStatus, OrderType, Transaction
from .state import PortfolioState


@dataclass
class SimulatedBroker:
    """A small broker that fills orders immediately at snapshot prices."""

    portfolio: PortfolioState
    commission_rate: float = 0.0
    cost_model: CostModel | None = None
    slippage_model: SlippageModel | None = None
    submitted_orders: list[Order] = field(default_factory=list)
    fills: list[Fill] = field(default_factory=list)
    transactions: list[Transaction] = field(default_factory=list)

    def __post_init__(self) -> None:
        if self.cost_model is None:
            self.cost_model = (
                ProportionalCostModel(self.commission_rate)
                if self.commission_rate > 0
                else ZeroCostModel()
            )
        if self.slippage_model is None:
            self.slippage_model = NoSlippage()

    def submit_order(self, order: Order) -> None:
        """Queue an order for execution on the current snapshot."""

        order.status = OrderStatus.SUBMITTED
        self.submitted_orders.append(order)

    def execute_pending(self, snapshot: MarketSnapshot) -> list[Fill]:
        """Execute all submitted orders and clear the queue."""

        pending = list(self.submitted_orders)
        self.submitted_orders.clear()
        fills: list[Fill] = []
        for order in pending:
            fill = self.execute_order(order, snapshot)
            if fill is not None:
                fills.append(fill)
        return fills

    def execute_order(self, order: Order, snapshot: MarketSnapshot) -> Fill | None:
        """Execute one order and apply it to portfolio state."""

        mid_price = snapshot.price(order.symbol)
        if mid_price == 0:
            order.status = OrderStatus.REJECTED
            return None

        signed_notional = self._signed_notional(order, mid_price)
        if abs(signed_notional) < 1e-12:
            order.status = OrderStatus.FILLED
            return None

        side = OrderSide.BUY if signed_notional > 0 else OrderSide.SELL
        assert self.slippage_model is not None
        price = self.slippage_model.fill_price(
            mid_price,
            side,
            timestamp=snapshot.timestamp,
            symbol=order.symbol,
        )
        if price <= 0:
            order.status = OrderStatus.REJECTED
            return None

        quantity = abs(signed_notional) / price
        assert self.cost_model is not None
        commission = self.cost_model.trade_cost(signed_notional, timestamp=snapshot.timestamp)
        slippage = abs(price - mid_price) * quantity
        fill = Fill(
            order_id=order.order_id,
            symbol=order.symbol,
            side=side,
            timestamp=snapshot.timestamp,
            quantity=quantity,
            price=price,
            commission=commission,
            slippage=slippage,
            metadata={"order_type": order.order_type.value, "mid_price": mid_price},
        )
        self._apply_fill(fill)
        order.status = OrderStatus.FILLED
        self.fills.append(fill)
        return fill

    def accrue_daily_cost(self, timestamp: pd.Timestamp) -> float:
        """Apply daily accrual costs such as tracking or management fees."""

        assert self.cost_model is not None
        cost = self.cost_model.daily_cost(self.portfolio.nav, timestamp=timestamp)
        if cost <= 0:
            return 0.0
        self.portfolio.cash -= cost
        self.portfolio.nav -= cost
        self.portfolio.timestamp = pd.Timestamp(timestamp)
        return float(cost)

    def _signed_notional(self, order: Order, price: float) -> float:
        """Convert order intent into signed traded notional."""

        if order.order_type == OrderType.TARGET_WEIGHT:
            if order.target_weight is None:
                raise ValueError("target_weight order requires target_weight.")
            position = self.portfolio.get_position(order.symbol)
            target_value = float(order.target_weight) * self.portfolio.nav
            return target_value - position.market_value

        if order.quantity is None:
            raise ValueError("market/limit order requires quantity.")
        sign = 1.0 if order.side == OrderSide.BUY else -1.0
        return sign * float(order.quantity) * price

    def _apply_fill(self, fill: Fill) -> None:
        """Apply a fill to portfolio cash and position state."""

        position = self.portfolio.get_position(fill.symbol)
        signed_quantity = fill.quantity if fill.side == OrderSide.BUY else -fill.quantity
        signed_notional = fill.notional

        old_quantity = position.quantity
        new_quantity = old_quantity + signed_quantity

        if new_quantity == 0:
            position.cost_basis = 0.0
        elif old_quantity == 0 or (old_quantity > 0) == (signed_quantity > 0):
            old_cost = position.cost_basis * abs(old_quantity)
            add_cost = fill.price * abs(signed_quantity)
            position.cost_basis = (old_cost + add_cost) / abs(new_quantity)
        else:
            position.cost_basis = position.cost_basis if abs(new_quantity) > 1e-12 else 0.0

        position.quantity = new_quantity
        position.mark_to_market(fill.price)

        cash_delta = -signed_notional - fill.commission
        self.portfolio.cash += cash_delta
        self.portfolio.nav = self.portfolio.cash + sum(
            pos.market_value for pos in self.portfolio.positions.values()
        )
        self.portfolio.timestamp = pd.Timestamp(fill.timestamp)

        transaction = Transaction(
            fill=fill,
            cash_delta=cash_delta,
            position_delta=signed_quantity,
        )
        self.transactions.append(transaction)
