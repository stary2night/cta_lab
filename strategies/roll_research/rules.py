"""研究级展期规则：基差驱动、Carry 最大化、动量展期。"""

from __future__ import annotations

import pandas as pd

from data.model.roll import RollRule
from data.model.contract import Contract
from data.model.bar import BarSeries


class BasisDrivenRoll(RollRule):
    """基差驱动展期：当近月合约相对远月基差超过阈值时触发展期。

    基差 = 近月结算价 / 远月结算价 - 1
    basis > threshold 时切换到远月。
    """

    def __init__(self, threshold: float = 0.005) -> None:
        self.threshold = threshold

    def select_contract(
        self,
        date: pd.Timestamp,
        candidates: list[Contract],
        bar_data: dict[str, BarSeries],
    ) -> Contract:
        """给定日期选择应持有的合约。"""
        valid = [
            c
            for c in candidates
            if c.code in bar_data and date in bar_data[c.code].data.index
        ]
        if not valid:
            return candidates[0]
        if len(valid) == 1:
            return valid[0]

        # 按到期日排序
        valid_sorted = sorted(valid, key=lambda c: c.expire_date)
        near, far = valid_sorted[0], valid_sorted[1]
        near_price = float(bar_data[near.code].data.loc[date, "settle"])
        far_price = float(bar_data[far.code].data.loc[date, "settle"])
        if far_price > 0:
            basis = near_price / far_price - 1
            if basis > self.threshold:
                return far  # 近月溢价过高，切换到远月
        return near


class CarryOptimizedRoll(RollRule):
    """Carry 最大化展期：持有隐含 carry 最高的合约。"""

    def select_contract(
        self,
        date: pd.Timestamp,
        candidates: list[Contract],
        bar_data: dict[str, BarSeries],
    ) -> Contract:
        """选择结算价最低的合约（期货升水结构下近月价格低 = carry 高）。"""
        valid: list[tuple[Contract, float]] = [
            (c, float(bar_data[c.code].data.loc[date, "settle"]))
            for c in candidates
            if c.code in bar_data and date in bar_data[c.code].data.index
        ]
        if not valid:
            return candidates[0]
        return min(valid, key=lambda x: x[1])[0]


class MomentumRoll(RollRule):
    """动量展期：持有近期成交量动量最强的合约。"""

    def __init__(self, window: int = 5) -> None:
        self.window = window

    def select_contract(
        self,
        date: pd.Timestamp,
        candidates: list[Contract],
        bar_data: dict[str, BarSeries],
    ) -> Contract:
        """选择近 window 日成交量均值最高的合约。"""
        best: Contract | None = None
        best_vol: float = -1.0

        for c in candidates:
            if c.code not in bar_data:
                continue
            bs = bar_data[c.code]
            idx = bs.data.index
            if date not in idx:
                continue
            pos = idx.get_loc(date)
            if pos < self.window:
                continue
            avg_vol = float(bs.data.iloc[pos - self.window : pos]["volume"].mean())
            if avg_vol > best_vol:
                best_vol = avg_vol
                best = c

        return best if best is not None else candidates[0]
