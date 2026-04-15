from __future__ import annotations

import pandas as pd
import numpy as np

from signals.base import Signal, CrossSectionalSignal


class RankCombiner(CrossSectionalSignal):
    """截面排名综合器（截面信号用，GMAT3 多因子选品逻辑）。

    将多个 Signal 的输出在截面上排名后按权重加权平均，输出综合排名分数。
    """

    def __init__(
        self,
        signals: list[Signal],
        weights: list[float] | None = None,
    ) -> None:
        """初始化 RankCombiner。

        Args:
            signals: Signal 实例列表，每个 signal 逐列作用于 price_matrix。
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

    def compute(self, price_matrix: pd.DataFrame) -> pd.DataFrame:
        """计算截面综合排名分数。

        Args:
            price_matrix: DataFrame(dates × symbols)，各品种价格。

        Returns:
            DataFrame(dates × symbols)，值为 [0, 1] 的综合截面排名分数。
        """
        ranked_matrices: list[pd.DataFrame] = []

        for sig in self.signals:
            # 对每列独立计算信号，组成信号矩阵
            sig_matrix = price_matrix.apply(sig.compute, axis=0)  # dates × symbols
            # 对每日截面做 pct 排名（axis=1 表示在品种间排名）
            ranked = sig_matrix.rank(pct=True, axis=1)
            ranked_matrices.append(ranked)

        # skipna 式加权平均：缺失信号不参与加权，而不是当成 0 分
        values = np.stack([mat.to_numpy(dtype=float) for mat in ranked_matrices], axis=2)
        weights = np.array(self.weights, dtype=float).reshape(1, 1, -1)
        mask = ~np.isnan(values)

        weighted_sum = np.where(mask, values * weights, 0.0).sum(axis=2)
        weight_sum = np.where(mask, weights, 0.0).sum(axis=2)

        with np.errstate(invalid="ignore", divide="ignore"):
            combined = weighted_sum / weight_sum
        combined = np.where(weight_sum > 0, combined, np.nan)

        return pd.DataFrame(combined, index=price_matrix.index, columns=price_matrix.columns)
