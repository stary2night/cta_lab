"""执行延迟模拟。"""

from __future__ import annotations

import pandas as pd


def apply_lag(weight_df: pd.DataFrame, lag: int = 1) -> pd.DataFrame:
    """将权重矩阵向前移 lag 日（shift(lag)），模拟执行延迟。

    第一个 lag 行填充 0（无持仓）。
    """
    return weight_df.shift(lag).fillna(0.0)
