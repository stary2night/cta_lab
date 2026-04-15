from abc import ABC, abstractmethod
import pandas as pd
import numpy as np


class Signal(ABC):
    """时序信号基类：per-asset，每个品种独立计算。"""

    @abstractmethod
    def compute(self, prices: pd.Series) -> pd.Series:
        """计算信号序列。

        输入：价格或净值序列（DatetimeIndex）
        输出：浮点信号序列，前导 NaN 表示数据不足。

        说明：
            - 多数研究型信号输出连续值
            - 少数方向型信号（如 TSMOM）可直接输出 -1/0/+1
        """

    def to_direction(self, prices: pd.Series) -> pd.Series:
        """将连续信号离散化为 {-1, 0, +1}。"""
        return np.sign(self.compute(prices))


class CrossSectionalSignal(ABC):
    """截面信号基类：需要所有品种同时参与。"""

    @abstractmethod
    def compute(self, price_matrix: pd.DataFrame) -> pd.DataFrame:
        """计算截面信号矩阵。

        输入：DataFrame，shape=(dates, symbols)，为各品种价格
        输出：DataFrame，shape=(dates, symbols)，各品种各日信号强度
        """
