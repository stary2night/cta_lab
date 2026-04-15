"""截面策略族：排名打分 + 分位数多空切分 + 风险预算定仓。"""

from __future__ import annotations

import pandas as pd

from .strategy import StrategyBase


class CrossSectionalStrategy(StrategyBase):
    """截面策略族：排名打分 + 分位数多空切分 + 风险预算定仓。"""

    def __init__(self, config: dict) -> None:
        super().__init__(config)
        self.score_lookbacks: list[int] = config.get("score_lookbacks", [63, 252])
        self.top_pct: float = config.get("top_pct", 0.30)
        self.bottom_pct: float = config.get("bottom_pct", 0.30)
        self.target_vol: float = config.get("target_vol", 0.40)

    def score_assets(self, price_df: pd.DataFrame) -> pd.DataFrame:
        """截面打分：多周期 SharpeMomentum 均值排名。"""
        from signals.momentum.sharpe_mom import SharpeMomentum

        scores: list[pd.DataFrame] = []
        for lb in self.score_lookbacks:
            sc = price_df.apply(lambda col, lb=lb: SharpeMomentum(lb).compute(col))
            # 截面排名：每日按行 rank，pct=True 归一化到 [0, 1]
            ranked = sc.rank(axis=1, pct=True)
            scores.append(ranked)
        return sum(scores) / len(scores)  # type: ignore[return-value]

    def generate_signals(self, price_df: pd.DataFrame) -> pd.DataFrame:
        """截面打分 → 多空方向信号。"""
        scores = self.score_assets(price_df)
        n = price_df.shape[1]
        top_n = max(1, int(n * self.top_pct))
        bot_n = max(1, int(n * self.bottom_pct))

        signal = pd.DataFrame(0.0, index=scores.index, columns=scores.columns)
        for dt in scores.index:
            row = scores.loc[dt].dropna()
            if len(row) < 2:
                continue
            top_thresh = row.nlargest(top_n).min()
            bot_thresh = row.nsmallest(bot_n).max()
            signal.loc[dt, row[row >= top_thresh].index] = 1.0
            if bot_n > 0:
                signal.loc[dt, row[row <= bot_thresh].index] = -1.0
        return signal

    def build_weights(
        self,
        signal_df: pd.DataFrame,
        vol_df: pd.DataFrame,
        corr_cache=None,
    ) -> pd.DataFrame:
        """等风险定仓（可被子类覆盖）。"""
        from portfolio.sizing.equal_risk import EqualRiskSizer

        return EqualRiskSizer(self.target_vol).compute(signal_df, vol_df)
