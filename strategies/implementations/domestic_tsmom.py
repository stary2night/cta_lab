"""国内期货时序动量策略。"""

from __future__ import annotations

from strategies.base.trend import TrendFollowingStrategy


class DomesticTSMOM(TrendFollowingStrategy):
    """国内期货时序动量策略。

    继承 TrendFollowingStrategy，仅设置专用默认参数。
    """

    DEFAULT_CONFIG: dict = {
        "lookbacks": [21, 63, 126, 252],
        "signal_weights": [0.25, 0.25, 0.25, 0.25],
        "vol_halflife": 60,
        "target_vol": 0.40,
        "universe": "domestic_futures_main",
        "rebalance_freq": "monthly",
    }

    def __init__(self, config: dict | None = None) -> None:
        merged = {**self.DEFAULT_CONFIG, **(config or {})}
        super().__init__(merged)
