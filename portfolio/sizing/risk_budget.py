"""GMAT3 风险预算定仓。"""

from __future__ import annotations

from typing import Literal

import numpy as np
import pandas as pd

from .base import Sizer


class RiskBudgetSizer(Sizer):
    """风险预算定仓（GMAT3）。

    w_i = direction_i × (rb_unit / vol_divisor_i)

    rb_unit = base_risk / (N_mom + N_rev * rev_weight)
      base_risk：每子指数的总风险预算，默认 10%
      N_mom：当日动量选中品种数
      N_rev：反转选中品种数
      rev_weight：反转品种风险权重是动量的一半，默认 0.5

    vol_divisor 由 TVS 决定：
      TVS > 0：vol_divisor = max(vol_22, vol_65, vol_130)（更严格）
      TVS <= 0：vol_divisor = vol_22（宽松）
    """

    def __init__(
        self,
        base_risk: float = 0.10,
        rev_weight: float = 0.5,
        signal_mode: Literal["direction", "raw"] = "direction",
    ) -> None:
        """初始化风险预算定仓器。"""
        self.base_risk = base_risk
        self.rev_weight = rev_weight
        if signal_mode not in {"direction", "raw"}:
            raise ValueError("signal_mode must be 'direction' or 'raw'.")
        self.signal_mode = signal_mode

    def compute(
        self,
        signal_df: pd.DataFrame,
        vol_df: pd.DataFrame,
        corr_cache=None,
        tvs_df: pd.DataFrame | None = None,
        vol_65_df: pd.DataFrame | None = None,
        vol_130_df: pd.DataFrame | None = None,
        is_reversal_df: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        """计算风险预算目标权重，返回 shape: (dates, symbols) 的 DataFrame。"""
        # 1. 解释输入信号
        exposure = np.sign(signal_df) if self.signal_mode == "direction" else signal_df.astype(float)

        # 构建 is_reversal 掩码（默认全部为非反转）
        if is_reversal_df is None:
            is_reversal = pd.DataFrame(
                False, index=exposure.index, columns=exposure.columns
            )
        else:
            is_reversal = is_reversal_df.reindex_like(exposure).fillna(False).astype(bool)

        active = exposure.notna() & (exposure != 0)

        # 2. 统计动量与反转品种数
        n_mom = (active & ~is_reversal).sum(axis=1)
        n_rev = (active & is_reversal).sum(axis=1)

        # 3. rb_unit = base_risk / (N_mom + N_rev * rev_weight)，分母=0时 rb_unit=0
        denominator = n_mom + n_rev * self.rev_weight
        rb_unit = (self.base_risk / denominator.replace(0, np.nan)).fillna(0.0)

        # 4. vol_divisor：根据 TVS 决定使用宽松或严格波动率
        if tvs_df is not None and vol_65_df is not None and vol_130_df is not None:
            # TVS > 0 的品种用 max(vol_22, vol_65, vol_130)
            vol_max = pd.concat(
                [vol_df, vol_65_df, vol_130_df], axis=0, keys=["v22", "v65", "v130"]
            ).groupby(level=1).max()
            # 按日期对齐
            vol_max = vol_max.reindex(index=vol_df.index, columns=vol_df.columns)

            tvs_aligned = tvs_df.reindex_like(vol_df)
            vol_divisor = vol_df.copy()
            use_strict = tvs_aligned > 0
            vol_divisor = vol_divisor.where(~use_strict, other=vol_max)
        else:
            vol_divisor = vol_df.copy()

        # 5. w = direction × rb_unit / vol_divisor
        vol_safe = vol_divisor.where(vol_divisor > 0)  # 将 <=0/NaN 置 NaN 防止除零
        w = exposure.mul(rb_unit, axis=0).div(vol_safe)

        # 无效位置（vol=0/NaN 或 direction=0）置 0
        valid_mask = active & vol_divisor.notna() & (vol_divisor > 0)
        w = w.where(valid_mask, other=0.0)

        return w.fillna(0.0)
