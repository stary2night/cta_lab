"""IntradayMomSignal：中国期货日内时序动量信号。

基于 Jin et al. (SSRN #3493927, 2019)：
    signal_t = sign( r_first_t )

其中 r_first_t 为交易日 t 日盘开盘首 N 分钟的价格收益率。

可选：成交量/波动率条件缩放（论文图3/图5的条件分组效应的连续化版本）：
    signal_t = sign( r_first_t ) × volume_scale_t × vol_scale_t

两个 scale 均为 0 时关闭（纯二元信号）。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from typing import Optional

import numpy as np
import pandas as pd


@dataclass
class IntradayMomParams:
    """信号参数。

    Attributes
    ----------
    volume_scale : bool
        是否按首时段成交量排名做连续缩放（默认 False）。
        scale = rolling_volume_rank × 2，均值约 1，高量时放大，低量时缩小。
    vol_scale : bool
        是否按首时段波动率（|r_first|）排名做连续缩放（默认 False）。
        scale = rolling_vol_rank × 2，均值约 1。
    rank_window : int
        计算滚动分位数的回望窗口（交易日数），默认 60。
    min_periods : int
        滚动分位数启动所需的最少有效样本，默认 20。
    """

    volume_scale: bool = False
    vol_scale: bool = False
    rank_window: int = 60
    min_periods: int = 20


class IntradayMomSignal:
    """中国期货日内时序动量信号生成器。

    Parameters
    ----------
    params : IntradayMomParams | None
        信号参数，None 使用默认值（纯二元信号）。
    """

    def __init__(self, params: Optional[IntradayMomParams] = None) -> None:
        self.params = params if params is not None else IntradayMomParams()

    # ── 核心接口 ──────────────────────────────────────────────────────────────

    def compute(
        self,
        first_ret_df: pd.DataFrame,
        first_vol_df: Optional[pd.DataFrame] = None,
    ) -> pd.DataFrame:
        """计算日内动量信号矩阵。

        Parameters
        ----------
        first_ret_df : DataFrame, shape (days, symbols)
            首时段日度收益率矩阵（由 ChinaMinuteLoader 生成）。
        first_vol_df : DataFrame, shape (days, symbols) | None
            首时段成交量矩阵；volume_scale=True 时必须提供。

        Returns
        -------
        signal_df : DataFrame, shape (days, symbols)
            信号矩阵：
            - 纯二元模式：{-1.0, NaN, +1.0}
            - 条件缩放模式：连续值，均值约 ±1
        """
        p = self.params

        # 基础方向信号：sign(r_first)，0 值（r_first=0）标记为 NaN
        base = np.sign(first_ret_df).replace(0.0, np.nan)

        if not p.volume_scale and not p.vol_scale:
            return base

        # ── 成交量条件缩放 ────────────────────────────────────────────────────
        if p.volume_scale:
            if first_vol_df is None:
                raise ValueError("first_vol_df must be provided when volume_scale=True")
            vol_rank = self._rolling_rank(first_vol_df, p.rank_window, p.min_periods)
            base = base.mul(vol_rank * 2.0)

        # ── 波动率条件缩放（|r_first| 作为代理波动率）────────────────────────
        if p.vol_scale:
            abs_ret = first_ret_df.abs()
            vol_rank = self._rolling_rank(abs_ret, p.rank_window, p.min_periods)
            base = base.mul(vol_rank * 2.0)

        return base

    # ── 辅助方法 ──────────────────────────────────────────────────────────────

    @staticmethod
    def _rolling_rank(df: pd.DataFrame, window: int, min_periods: int) -> pd.DataFrame:
        """滚动分位数排名（0 到 1，前向查看，无未来信息）。

        使用 expanding rank 作为 warmup，过渡至 rolling rank。
        结果为 [0, 1] 之间的连续值，高值表示相对本期历史处于高位。
        """
        result = pd.DataFrame(index=df.index, columns=df.columns, dtype=float)
        for col in df.columns:
            s = df[col].dropna()
            if s.empty:
                continue
            # rolling rank: 当日值在过去 window 天中的分位数
            ranks = s.rolling(window, min_periods=min_periods).apply(
                lambda x: float(np.sum(x[:-1] <= x[-1])) / max(len(x) - 1, 1),
                raw=True,
            )
            result[col] = ranks.reindex(df.index)
        return result.astype(float)
