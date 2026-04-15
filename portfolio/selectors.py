"""将截面 score 映射为可定仓的仓位意图矩阵。"""

from __future__ import annotations

from dataclasses import dataclass
import math

import pandas as pd


@dataclass
class TopBottomSelector:
    """按截面得分选择 top / bottom 品种，输出 {-1, 0, +1}。"""

    top_n: int = 0
    bottom_n: int = 0

    def __post_init__(self) -> None:
        if self.top_n < 0 or self.bottom_n < 0:
            raise ValueError("top_n / bottom_n 不能为负数。")
        if self.top_n == 0 and self.bottom_n == 0:
            raise ValueError("top_n / bottom_n 不能同时为 0。")

    def apply(self, score_df: pd.DataFrame) -> pd.DataFrame:
        """将截面 score 转成 long/short/flat 仓位意图。"""

        def select_row(row: pd.Series) -> pd.Series:
            valid = row.dropna()
            out = pd.Series(0.0, index=row.index, dtype=float)
            if valid.empty:
                return out

            if self.top_n + self.bottom_n > len(valid):
                raise ValueError("top_n + bottom_n 不能超过当日有效品种数。")

            if self.top_n > 0:
                out.loc[valid.nlargest(self.top_n).index] = 1.0

            if self.bottom_n > 0:
                out.loc[valid.nsmallest(self.bottom_n).index] = -1.0

            return out

        return score_df.apply(select_row, axis=1)


@dataclass
class ThresholdSelector:
    """按上下阈值将 score 转成 long/short/flat 仓位意图。"""

    long_threshold: float = 0.8
    short_threshold: float = 0.2

    def __post_init__(self) -> None:
        if math.isnan(self.long_threshold) or math.isnan(self.short_threshold):
            raise ValueError("threshold 不能为 NaN。")
        if self.short_threshold > self.long_threshold:
            raise ValueError("short_threshold 不能大于 long_threshold。")

    def apply(self, score_df: pd.DataFrame) -> pd.DataFrame:
        """将截面 score 转成 long/short/flat 仓位意图。"""
        long_mask = score_df >= self.long_threshold
        short_mask = score_df <= self.short_threshold

        out = pd.DataFrame(0.0, index=score_df.index, columns=score_df.columns)
        out = out.where(~long_mask, other=1.0)
        out = out.where(~short_mask, other=-1.0)
        return out
