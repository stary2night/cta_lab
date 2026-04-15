"""波动率重置信号（Volatility Reset Signal，GMAT3）。"""

from __future__ import annotations

from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    from backtest.position import FXTracker


class VRS:
    """波动率重置信号（Volatility Reset Signal，GMAT3）。

    触发条件：max(vol_22, vol_65, vol_130) > threshold 且未来 lookahead 日无调仓日
    执行：h_new = h × (target_vol / vol_trigger)，扣 TradingFee
    仅对 FXTracker 有意义。
    """

    def __init__(
        self,
        threshold: float = 0.045,
        target_vol: float = 0.040,
    ) -> None:
        self.threshold = threshold
        self.target_vol = target_vol

    def check_trigger(
        self,
        date: pd.Timestamp,
        vol_22: float,
        vol_65: float,
        vol_130: float,
        adjust_dates: set[pd.Timestamp],
        lookahead: int = 2,
    ) -> tuple[bool, float]:
        """返回 (triggered, vol_max)。

        触发条件：vol_max > threshold 且未来 lookahead 个自然日内无调仓日。
        """
        vol_max = max(vol_22, vol_65, vol_130)

        if vol_max <= self.threshold:
            return False, vol_max

        # 检查未来 lookahead 日内是否有调仓日（若有则不触发）
        for offset in range(1, lookahead + 1):
            future_date = date + pd.Timedelta(days=offset)
            if future_date in adjust_dates:
                return False, vol_max

        return True, vol_max

    def apply(
        self,
        tracker: "FXTracker",
        vol_max: float,
    ) -> float:
        """缩减持仓，返回缩减比例（< 1 表示缩减）。"""
        scale = self.target_vol / vol_max
        tracker._h_cny = tracker._h_cny * scale
        tracker._h_usd = tracker._h_usd * scale
        return scale
