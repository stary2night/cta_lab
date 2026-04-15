"""CTA 等风险定仓：每个品种贡献等量波动率。"""

from __future__ import annotations

from typing import Literal

import numpy as np
import pandas as pd

from .base import Sizer


class EqualRiskSizer(Sizer):
    """等风险定仓：每个品种贡献等量波动率。

    w_i = signal_i × (target_vol / sigma_i) / N_active
    N_active = 当日信号非零且波动率有效的品种数
    """

    def __init__(
        self,
        target_vol: float = 0.40,
        signal_mode: Literal["direction", "raw"] = "direction",
    ) -> None:
        """初始化。

        signal_mode:
            - "direction"：仅使用方向，等价于 sign(signal)
            - "raw"：保留信号强度，调用方需自行控制 score 的量纲与范围
        """
        self.target_vol = target_vol
        if signal_mode not in {"direction", "raw"}:
            raise ValueError("signal_mode must be 'direction' or 'raw'.")
        self.signal_mode = signal_mode

    def compute(
        self,
        signal_df: pd.DataFrame,
        vol_df: pd.DataFrame,
        corr_cache=None,
    ) -> pd.DataFrame:
        """计算等风险目标权重，返回 shape: (dates, symbols) 的 DataFrame。"""
        # 1. 解释输入信号
        exposure = np.sign(signal_df) if self.signal_mode == "direction" else signal_df.astype(float)

        # 2. 计算每日有效品种 mask
        valid_mask = exposure.notna() & (exposure != 0) & vol_df.notna() & (vol_df > 0)

        # 3. N_active 每日有效品种数，0 时后续权重为 0
        n_active = valid_mask.sum(axis=1).replace(0, np.nan)  # 避免除以 0

        # 4. 计算权重：w = direction × (target_vol / vol) / N_active
        vol_safe = vol_df.where(vol_df > 0)  # 将 <=0 置 NaN，避免除以 0
        w = exposure.mul(self.target_vol / vol_safe).div(n_active, axis=0)

        # 5. 无效位置置 0（vol=0/NaN 或 exposure=0）
        w = w.where(valid_mask, other=0.0)

        return w.fillna(0.0)
