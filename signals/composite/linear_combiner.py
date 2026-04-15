from __future__ import annotations

import pandas as pd
import numpy as np

from signals.base import Signal


class LinearCombiner(Signal):
    """线性加权信号合成器（时序信号用）。

    将多个 Signal 实例对同一价格序列的输出按权重加权平均。
    NaN 位置不参与加权（skipna 式加权），权重自动归一化。
    """

    def __init__(
        self,
        signals: list[Signal],
        weights: list[float] | None = None,
    ) -> None:
        """初始化 LinearCombiner。

        Args:
            signals: Signal 实例列表，至少包含一个。
            weights: 各信号权重，默认等权。长度必须与 signals 一致，自动归一化。

        Raises:
            ValueError: signals 为空，或 weights 长度与 signals 不一致。
        """
        if not signals:
            raise ValueError("signals 列表不能为空")
        if weights is None:
            weights = [1.0] * len(signals)
        if len(weights) != len(signals):
            raise ValueError("weights 长度必须与 signals 一致")

        total = sum(weights)
        if total == 0:
            raise ValueError("weights 之和不能为 0")

        self.signals = signals
        self.weights: list[float] = [w / total for w in weights]

    def compute(self, prices: pd.Series) -> pd.Series:
        """计算各信号的加权平均，NaN 位置跳过（skipna 式加权）。"""
        # 收集各信号输出，拼成矩阵（dates × n_signals）
        signal_matrix = pd.concat(
            [sig.compute(prices) for sig in self.signals], axis=1
        )
        w = np.array(self.weights)  # shape (n_signals,)

        # skipna 加权平均：每行有效权重之和用于归一化
        values = signal_matrix.values  # shape (n_dates, n_signals)
        mask = ~np.isnan(values)  # True 表示非 NaN

        weighted_sum = np.where(mask, values * w, 0.0).sum(axis=1)
        weight_sum = np.where(mask, w, 0.0).sum(axis=1)

        with np.errstate(invalid="ignore", divide="ignore"):
            ratio = weighted_sum / weight_sum
        result = np.where(weight_sum > 0, ratio, np.nan)
        return pd.Series(result, index=prices.index)
