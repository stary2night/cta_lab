"""持仓追踪器。"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import Optional

import pandas as pd


class PositionTracker(ABC):
    """持仓追踪基类。"""

    @abstractmethod
    def update(
        self,
        date: pd.Timestamp,
        target_weights: Optional[pd.Series],  # None 表示非调仓日，保持当前持仓
        returns: pd.Series,                   # 当日各品种收益率
        is_rebalance: bool,
        **kwargs,
    ) -> float:
        """更新持仓，返回当日 P&L（占上期 NAV 的比例）。"""

    @abstractmethod
    def get_holdings(self) -> pd.Series:
        """返回当前持仓权重快照。"""

    @abstractmethod
    def reset(self) -> None:
        """重置持仓到初始状态。"""


class SimpleTracker(PositionTracker):
    """简单权重追踪器（CTA）。

    无显式持仓，每日 P&L = Σ(w_{t-1} × r_t)。
    调仓日将 holdings 更新为 target_weights（lag 已由引擎处理）。
    """

    def __init__(self, symbols: list[str]) -> None:
        self._symbols = symbols
        self._holdings = pd.Series(0.0, index=symbols)

    def update(
        self,
        date: pd.Timestamp,
        target_weights: Optional[pd.Series],
        returns: pd.Series,
        is_rebalance: bool,
        **kwargs,
    ) -> float:
        # 1. pnl = 持仓前一日权重 × 当日收益率之和
        common = self._holdings.index.intersection(returns.index)
        pnl = (self._holdings[common] * returns[common]).sum()

        # 2. 若为调仓日且 target_weights 不为 None，更新持仓
        if is_rebalance and target_weights is not None:
            # 重新索引以保证 symbol 对齐，缺失品种权重为 0
            self._holdings = target_weights.reindex(self._symbols, fill_value=0.0)

        return float(pnl)

    def get_holdings(self) -> pd.Series:
        return self._holdings.copy()

    def reset(self) -> None:
        self._holdings[:] = 0.0


class FXTracker(PositionTracker):
    """双轨持仓追踪器（GMAT3）。

    持仓分 CNY 和 USD 两轨，USD P&L 每日按 FX 变动重估。

    P&L 公式：
      pnl = Σ(h_cny × r) + (fx_t - fx_{t-1}) × accum_usd_pnl + fx_t × Σ(h_usd × r_usd)

    其中 accum_usd_pnl 在每次调仓后重置为 0。
    """

    def __init__(self, symbols: list[str], currency_map: dict[str, str]) -> None:
        # currency_map: {symbol: "CNY"/"USD"}
        self._symbols = symbols
        self._currency_map = currency_map
        self._cny_syms = [s for s in symbols if currency_map.get(s, "CNY") == "CNY"]
        self._usd_syms = [s for s in symbols if currency_map.get(s, "USD") == "USD"]
        self._h_cny = pd.Series(0.0, index=self._cny_syms)
        self._h_usd = pd.Series(0.0, index=self._usd_syms)
        self._accum_usd_pnl: float = 0.0
        self._prev_fx: Optional[float] = None

    def update(
        self,
        date: pd.Timestamp,
        target_weights: Optional[pd.Series],
        returns: pd.Series,
        is_rebalance: bool,
        fx: float = 1.0,
        **kwargs,
    ) -> float:
        # 1. CNY 端 P&L
        cny_common = self._h_cny.index.intersection(returns.index)
        cny_pnl = (self._h_cny[cny_common] * returns[cny_common]).sum()

        # 2. FX 重估收益
        if self._prev_fx is not None:
            fx_revalue = (fx - self._prev_fx) * self._accum_usd_pnl
        else:
            fx_revalue = 0.0

        # 3. USD 端 P&L（折算为 CNY）
        usd_common = self._h_usd.index.intersection(returns.index)
        usd_ret = (self._h_usd[usd_common] * returns[usd_common]).sum()
        usd_pnl = fx * usd_ret

        # 4. 合计
        total_pnl = float(cny_pnl) + float(fx_revalue) + float(usd_pnl)

        # 5. 累计 USD P&L（用于下一日 FX 重估）
        self._accum_usd_pnl += float(usd_ret)

        # 6. 更新 prev_fx
        self._prev_fx = fx

        # 7. 调仓日更新持仓
        if is_rebalance and target_weights is not None:
            tw = target_weights.reindex(self._symbols, fill_value=0.0)
            self._h_cny = tw.reindex(self._cny_syms, fill_value=0.0)
            self._h_usd = tw.reindex(self._usd_syms, fill_value=0.0)
            self._accum_usd_pnl = 0.0  # 重置

        return total_pnl

    def get_holdings(self) -> pd.Series:
        """合并 h_cny 和 h_usd 为完整 holdings Series。"""
        return pd.concat([self._h_cny, self._h_usd]).reindex(self._symbols, fill_value=0.0)

    def reset(self) -> None:
        self._h_cny[:] = 0.0
        self._h_usd[:] = 0.0
        self._accum_usd_pnl = 0.0
        self._prev_fx = None
