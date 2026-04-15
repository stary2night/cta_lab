"""趋势策略族：多周期 TSMOM 信号合成 + 等风险定仓。"""

from __future__ import annotations

import pandas as pd

from .strategy import StrategyBase


class TrendFollowingStrategy(StrategyBase):
    """趋势策略族：多周期 TSMOM 信号合成 + 等风险定仓。"""

    def __init__(self, config: dict) -> None:
        super().__init__(config)
        self.lookbacks: list[int] = config.get("lookbacks", [21, 63, 126, 252])
        self.signal_weights: list[float] | None = config.get("signal_weights", None)
        self.vol_halflife: int = config.get("vol_halflife", 60)
        self.target_vol: float = config.get("target_vol", 0.40)

    def generate_signals(self, price_df: pd.DataFrame) -> pd.DataFrame:
        """多周期 TSMOM 信号加权合成。"""
        from signals.momentum.tsmom import TSMOM

        signals: list[pd.DataFrame] = []
        for lb in self.lookbacks:
            sig = price_df.apply(lambda col, lb=lb: TSMOM(lb).compute(col))
            signals.append(sig)

        weights = self.signal_weights or [1.0 / len(signals)] * len(signals)
        result: pd.DataFrame = sum(s * w for s, w in zip(signals, weights))  # type: ignore[assignment]
        return result

    def build_weights(
        self,
        signal_df: pd.DataFrame,
        vol_df: pd.DataFrame,
        corr_cache=None,
    ) -> pd.DataFrame:
        """等风险定仓。"""
        from portfolio.sizing.equal_risk import EqualRiskSizer

        return EqualRiskSizer(self.target_vol).compute(signal_df, vol_df)
