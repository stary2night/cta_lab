"""多空不对称分析模块。"""

from __future__ import annotations

import numpy as np
import pandas as pd


def long_short_asymmetry(
    returns_df: pd.DataFrame,    # shape: (dates, symbols)
    signal_df: pd.DataFrame,     # shape: (dates, symbols)，用于区分多/空
    vol_df: pd.DataFrame,        # shape: (dates, symbols)，年化波动率
    target_vol: float = 0.40,
) -> pd.DataFrame:
    """构建纯多头、纯空头、多空双向三条 NAV 序列。

    - long_only：signal > 0 的品种等风险持仓
    - short_only：signal < 0 的品种等风险持仓（做空）
    - long_short：所有信号等风险持仓（标准 CTA）
    每条 NAV 从 1.0 开始。

    Parameters
    ----------
    returns_df:
        品种日收益率矩阵。
    signal_df:
        信号矩阵，正值表示做多，负值表示做空。
    vol_df:
        年化波动率矩阵，用于计算等风险权重。
    target_vol:
        组合目标波动率（年化），默认 0.40。

    Returns
    -------
    DataFrame，columns=[long_only, short_only, long_short]，index=dates
    """
    # 对齐索引和列
    common_dates = returns_df.index.intersection(signal_df.index).intersection(vol_df.index)
    common_syms = returns_df.columns.intersection(signal_df.columns).intersection(vol_df.columns)

    r = returns_df.loc[common_dates, common_syms]
    sig = signal_df.loc[common_dates, common_syms]
    vol = vol_df.loc[common_dates, common_syms]

    def _build_nav(weights: pd.DataFrame) -> pd.Series:
        """给定权重矩阵，计算 NAV 序列（从 1.0 开始）。
        使用 t-1 权重 × t 收益率。
        """
        w_lagged = weights.shift(1).fillna(0.0)
        port_returns = (w_lagged * r).sum(axis=1)
        nav = (1 + port_returns).cumprod()
        if len(nav) > 0:
            nav = nav / nav.iloc[0]
        return nav

    def _compute_weights(signal_mask: pd.DataFrame, direction: float) -> pd.DataFrame:
        """
        按等风险分配权重。
        - signal_mask：布尔矩阵，True 表示参与该组合
        - direction：+1（做多）或 -1（做空）
        等风险权重 = target_vol / (vol × N_active) × direction
        """
        # 避免除以零
        safe_vol = vol.replace(0, np.nan)

        # 等风险单品种权重（未归一化）：target_vol / vol
        raw_weight = target_vol / safe_vol

        # 仅保留信号激活的品种
        w = raw_weight.where(signal_mask, 0.0).fillna(0.0)

        # 计算各行活跃品种数
        n_active = signal_mask.sum(axis=1).replace(0, np.nan)

        # 归一化：每个品种权重 / 活跃品种数（等风险分配）
        w = w.div(n_active, axis=0).fillna(0.0)

        return w * direction

    # 构建三种持仓
    long_mask = sig > 0
    short_mask = sig < 0

    w_long = _compute_weights(long_mask, direction=1.0)
    w_short = _compute_weights(short_mask, direction=-1.0)
    w_ls = _compute_weights(sig != 0, direction=0.0)

    # long_short：按信号方向分配，做多品种正权重，做空品种负权重
    safe_vol = vol.replace(0, np.nan)
    raw_weight = target_vol / safe_vol

    n_active_long = long_mask.sum(axis=1).replace(0, np.nan)
    n_active_short = short_mask.sum(axis=1).replace(0, np.nan)

    w_ls_long = raw_weight.where(long_mask, 0.0).fillna(0.0).div(n_active_long, axis=0).fillna(0.0)
    w_ls_short = raw_weight.where(short_mask, 0.0).fillna(0.0).div(n_active_short, axis=0).fillna(0.0) * (-1.0)
    w_ls_combined = w_ls_long + w_ls_short

    nav_long = _build_nav(w_long)
    nav_short = _build_nav(w_short)
    nav_ls = _build_nav(w_ls_combined)

    result = pd.DataFrame({
        "long_only": nav_long,
        "short_only": nav_short,
        "long_short": nav_ls,
    }, index=common_dates)

    return result
