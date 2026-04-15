import pandas as pd
import numpy as np

from signals.base import Signal

# 代表性 MA 窗口（从短到长）
_MA_WINDOWS = [5, 20, 60, 120, 260]
# 窗口长度的排名向量（固定，用于 Spearman 相关）
_RANKS = np.arange(1, len(_MA_WINDOWS) + 1, dtype=float)
# 预计算 _RANKS 的中心化，用于 pearson 计算
_RANKS_C = _RANKS - _RANKS.mean()
_RANKS_STD = np.sqrt((_RANKS_C ** 2).sum())


class MASS260Reversal(Signal):
    """MASS260 反转信号：衡量均线结构有序程度。

    输出 [-1, 1]，正值表示趋势有序（偏多），负值表示均线结构紊乱（偏空/反转）。
    使用 5 个代表性窗口 [5, 20, 60, 120, 260] 的 MA，计算其与窗口序号的 Spearman 相关。
    上升趋势时 MA 随窗口增大单调递减，相关系数 ≈ -1，输出取负后为正值。
    """

    def __init__(self, window: int = 260) -> None:
        """初始化 MASS260Reversal 信号。

        Args:
            window: 历史考察窗口，当前实现中使用最大 MA 窗口（260）作为预热期。
        """
        self.window = window

    def compute(self, prices: pd.Series) -> pd.Series:
        """计算 MASS260 均线结构有序度信号。

        对每日截面，计算 [ma5, ma20, ma60, ma120, ma260] 与 [1,2,3,4,5] 的
        Spearman 相关系数（等价于对 MA 向量 rank 后与固定 rank 向量做 Pearson 相关），
        然后取负值使上升趋势（MA 从短到长递减）输出正值。
        """
        # 计算各窗口 MA
        ma_series = [prices.rolling(w).mean() for w in _MA_WINDOWS]
        # 拼成 DataFrame，每行是一个时间点的 5 个 MA 值
        ma_df = pd.concat(ma_series, axis=1)
        ma_df.columns = [f"ma{w}" for w in _MA_WINDOWS]

        def _spearman_with_fixed_ranks(row: np.ndarray) -> float:
            """计算 row 与固定序号 [1,2,3,4,5] 的 Spearman 相关。"""
            if np.any(np.isnan(row)):
                return np.nan
            # rank row（取平均处理并列）
            order = row.argsort().argsort().astype(float) + 1  # ranks 1..5
            # Pearson(rank(row), _RANKS) = Spearman(row, _RANKS)
            row_c = order - order.mean()
            denom = np.sqrt((row_c ** 2).sum()) * _RANKS_STD
            if denom == 0:
                return np.nan
            return float(np.dot(row_c, _RANKS_C) / denom)

        corr = ma_df.apply(_spearman_with_fixed_ranks, axis=1, raw=True)
        # 上升趋势时 MA 从短到长递减，corr ≈ -1；取负后输出正值
        return -corr
