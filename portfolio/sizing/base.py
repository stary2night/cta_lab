"""权重计算基类：将信号和波动率转换为原始目标权重。"""

from __future__ import annotations

from abc import ABC, abstractmethod
from typing import TYPE_CHECKING

import pandas as pd

if TYPE_CHECKING:
    import numpy as np


class Sizer(ABC):
    """权重计算基类：将信号和波动率转换为原始目标权重。"""

    @abstractmethod
    def compute(
        self,
        signal_df: pd.DataFrame,                              # shape: (dates, symbols)，方向/仓位意图矩阵
        vol_df: pd.DataFrame,                                 # shape: (dates, symbols)，年化波动率
        corr_cache: "dict[pd.Timestamp, np.ndarray] | None" = None,  # 可选：{date: 相关性矩阵}
    ) -> pd.DataFrame:
        """返回原始目标权重 DataFrame，shape: (dates, symbols)。

        约定：
            - `signal_df` 应是已经可用于定仓的仓位意图，可为 {-1, 0, +1}
              的方向矩阵，也可为保留强度信息的浮点矩阵。
            - 纯截面排名分数（如 [0, 1] rank score）在进入 sizer 前，应先映射为
              long / short / flat 仓位意图，而不是直接传入。
            - `corr_cache` 为协方差感知型 Sizer（如 CorrCapSizer）提供滚动相关性
              矩阵。不需要相关性信息的 Sizer 可忽略此参数。
        """
