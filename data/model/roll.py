"""换仓规则抽象接口与标准机械规则实现，及合约切换时间表。"""

from __future__ import annotations

from abc import ABC, abstractmethod
from dataclasses import dataclass

import pandas as pd

from .contract import Contract
from .bar import BarSeries


class RollRule(ABC):
    """换仓规则抽象接口，data/model 层和 strategies/roll_research 共用。"""

    @abstractmethod
    def select_contract(
        self,
        date: pd.Timestamp,
        candidates: list[Contract],
        bar_data: dict[str, BarSeries],
    ) -> Contract:
        """给定日期和候选合约列表，返回应持有的合约。"""
        ...

    def reset(self) -> None:
        """重置内部状态，在新一轮序列构建前由 ContinuousSeries.build() 调用。
        无状态规则无需重写此方法。"""


class OIMaxRoll(RollRule):
    """持有当日持仓量（open_interest）最大的合约。"""

    def select_contract(
        self,
        date: pd.Timestamp,
        candidates: list[Contract],
        bar_data: dict[str, BarSeries],
    ) -> Contract:
        """选择当日持仓量最大的合约。"""
        best: Contract | None = None
        best_oi: float = -1.0

        for contract in candidates:
            bs = bar_data.get(contract.code)
            if bs is None or date not in bs.data.index:
                continue
            oi = float(bs.data.loc[date, "open_interest"])
            if oi > best_oi:
                best_oi = oi
                best = contract

        if best is None:
            raise ValueError(
                f"OIMaxRoll: no valid contract found on {date} among {[c.code for c in candidates]}"
            )
        return best


class VolumeMaxRoll(RollRule):
    """持有当日成交量最大的合约。"""

    def select_contract(
        self,
        date: pd.Timestamp,
        candidates: list[Contract],
        bar_data: dict[str, BarSeries],
    ) -> Contract:
        """选择当日成交量最大的合约。"""
        best: Contract | None = None
        best_vol: float = -1.0

        for contract in candidates:
            bs = bar_data.get(contract.code)
            if bs is None or date not in bs.data.index:
                continue
            vol = float(bs.data.loc[date, "volume"])
            if vol > best_vol:
                best_vol = vol
                best = contract

        if best is None:
            raise ValueError(
                f"VolumeMaxRoll: no valid contract found on {date} among {[c.code for c in candidates]}"
            )
        return best


class CalendarRoll(RollRule):
    """固定日历换月：到期前 N 个交易日强制切换到下一合约。"""

    def __init__(self, days_before_expiry: int = 5) -> None:
        self.days_before_expiry = days_before_expiry

    def select_contract(
        self,
        date: pd.Timestamp,
        candidates: list[Contract],
        bar_data: dict[str, BarSeries],
    ) -> Contract:
        """到期前 days_before_expiry 个自然日内切换到下一合约，否则持有到期最近合约。"""
        ref = date.date()

        # 按 last_trade_date 升序排列，优先持有最近到期但未到换月阈值的合约
        sorted_contracts = sorted(candidates, key=lambda c: c.last_trade_date)

        for contract in sorted_contracts:
            days_left = (contract.last_trade_date - ref).days
            if days_left >= self.days_before_expiry and contract.is_active(ref):
                return contract

        # 所有合约均在切换窗口内，返回最远到期合约
        active = [c for c in sorted_contracts if c.is_active(ref)]
        if active:
            return active[-1]

        raise ValueError(
            f"CalendarRoll: no active contract found on {date} among {[c.code for c in candidates]}"
        )


@dataclass
class RollEvent:
    """记录一次合约换仓事件。"""

    date: pd.Timestamp
    from_contract: str
    to_contract: str


class ContractSchedule:
    """合约切换时间表：逐日记录持有哪个合约代码。"""

    def __init__(self, events: list[RollEvent], symbol: str) -> None:
        self.events = sorted(events, key=lambda e: e.date)
        self.symbol = symbol

    def get_active_contract(self, date: pd.Timestamp) -> str:
        """返回给定日期应持有的合约代码。"""
        active = None
        for event in self.events:
            if event.date <= date:
                active = event.to_contract
            else:
                break
        if active is None:
            raise ValueError(
                f"ContractSchedule: no contract scheduled for {date} in symbol '{self.symbol}'"
            )
        return active

    def to_series(self) -> pd.Series:
        """返回以换仓日期为 index、合约代码为 value 的 Series。"""
        if not self.events:
            return pd.Series(dtype=str)
        dates = [e.date for e in self.events]
        codes = [e.to_contract for e in self.events]
        return pd.Series(codes, index=pd.DatetimeIndex(dates), name=self.symbol)


class StabilizedRule(RollRule):
    """稳定性过滤包装器：新合约需连续 stability_days 天保持最高指标才确认切换。

    包装任意基础规则（OIMaxRoll、VolumeMaxRoll 等），在其输出之上加一层稳定性
    过滤，避免两合约持仓量交替时来回跳动。算法与 cta/module1_data.py 一致。

    用法::

        rule = StabilizedRule(OIMaxRoll(), stability_days=3)
        cs = ContinuousSeries.build(..., roll_rule=rule)
    """

    def __init__(self, base: RollRule, stability_days: int = 3) -> None:
        if stability_days < 1:
            raise ValueError("stability_days 必须 >= 1。")
        self.base = base
        self.stability_days = stability_days
        self._current: str | None = None
        self._candidate: str | None = None
        self._streak: int = 0

    def reset(self) -> None:
        """重置稳定性追踪状态，每次新序列构建前自动调用。"""
        self._current = None
        self._candidate = None
        self._streak = 0

    def select_contract(
        self,
        date: pd.Timestamp,
        candidates: list[Contract],
        bar_data: dict[str, BarSeries],
    ) -> Contract:
        """经稳定性过滤后返回已确认的主力合约。"""
        raw = self.base.select_contract(date, candidates, bar_data)
        raw_code = raw.code

        if self._current is None:
            # 首次调用，直接确认，无需等待
            self._current = raw_code
            self._candidate = raw_code
            self._streak = 0
            return raw

        if raw_code == self._current:
            # 仍是当前已确认主力，重置候选计数
            self._candidate = raw_code
            self._streak = 0
        else:
            # 出现新候选合约
            if raw_code == self._candidate:
                self._streak += 1
            else:
                # 候选本身也换了，重新计数
                self._candidate = raw_code
                self._streak = 1

            if self._streak >= self.stability_days:
                # 稳定性条件满足，正式切换
                self._current = self._candidate
                self._streak = 0

        # 返回已确认主力对应的 Contract 对象
        for c in candidates:
            if c.code == self._current:
                return c

        # fallback：确认的主力当日不在候选列表（极少见），返回 base 选择
        return raw
