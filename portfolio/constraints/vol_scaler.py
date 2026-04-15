"""WAF（Weight Adjustment Factor）—— 组合层面的波动缩放约束。"""

from __future__ import annotations

import numpy as np
import pandas as pd


class WAF:
    """WAF 波动缩放约束（组合整体，非品种级别）。

    若组合历史波动 vol_max = max(vol_22, vol_65, vol_130) > threshold：
        WAF = min(max_waf, target_vol / vol_max)
    否则 WAF = 1.0
    将组合整体权重乘以 WAF。
    """

    def __init__(
        self,
        threshold: float = 0.045,
        target_vol: float = 0.040,
        max_waf: float = 1.5,
    ) -> None:
        """初始化 WAF 参数：触发阈值、目标波动率和 WAF 上界。"""
        self.threshold = threshold
        self.target_vol = target_vol
        self.max_waf = max_waf

    def apply(
        self,
        weight_df: pd.DataFrame,
        portfolio_vol_22: pd.Series,
        portfolio_vol_65: pd.Series,
        portfolio_vol_130: pd.Series,
    ) -> pd.DataFrame:
        """计算每日 WAF，整体缩放权重矩阵。"""
        # vol_max：逐日取三期波动最大值
        vol_max = pd.concat(
            [portfolio_vol_22, portfolio_vol_65, portfolio_vol_130], axis=1
        ).max(axis=1)

        # WAF = where(vol_max > threshold, clip(target_vol / vol_max, None, max_waf), 1.0)
        raw_waf = (self.target_vol / vol_max.where(vol_max > 0)).clip(upper=self.max_waf)
        waf_series = pd.Series(
            np.where(vol_max > self.threshold, raw_waf, 1.0),
            index=vol_max.index,
        )

        # 对齐到权重矩阵的日期索引
        waf_aligned = waf_series.reindex(weight_df.index).fillna(1.0)

        return weight_df.multiply(waf_aligned, axis=0)
