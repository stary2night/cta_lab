"""品种权重硬上界约束。"""

from __future__ import annotations

import numpy as np
import pandas as pd


class WeightCap:
    """品种权重硬上界约束。"""

    def __init__(self, cap: float | dict[str, float] = 0.10) -> None:
        """初始化权重上界，cap 可以是全局标量或 {symbol: cap} 字典。"""
        self.cap = cap

    def apply(self, weight_df: pd.DataFrame) -> pd.DataFrame:
        """将权重绝对值限制在 cap 以内，保留符号方向。"""
        if isinstance(self.cap, dict):
            # 逐品种应用不同上界
            result = weight_df.copy()
            for symbol, symbol_cap in self.cap.items():
                if symbol in result.columns:
                    col = result[symbol]
                    result[symbol] = np.sign(col) * np.minimum(np.abs(col), symbol_cap)
            # 未在字典中的品种不做限制（或可用全局默认，此处保持原值）
        else:
            # 全局统一上界
            result = np.sign(weight_df) * np.minimum(np.abs(weight_df), self.cap)
            result = pd.DataFrame(result, index=weight_df.index, columns=weight_df.columns)
        return result
