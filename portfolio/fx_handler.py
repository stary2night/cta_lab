"""FX 辅助计算模块。"""

from __future__ import annotations

import pandas as pd


def revalue_usd_pnl(
    accum_usd_pnl: pd.Series,
    fx_series: pd.Series,
) -> pd.Series:
    """计算 FX 变动引起的 CNY 重估收益。

    公式：(fx_t - fx_{t-1}) × accum_usd_pnl_{t-1}
    """
    fx_diff = fx_series.diff()
    pnl_lagged = accum_usd_pnl.shift(1)
    return (fx_diff * pnl_lagged).rename("fx_revalue_pnl")


def usd_to_cny(
    usd_holdings: pd.DataFrame,
    fx_series: pd.Series,
) -> pd.DataFrame:
    """USD 持仓转换为 CNY 市值权重。"""
    fx_aligned = fx_series.reindex(usd_holdings.index).ffill()
    return usd_holdings.multiply(fx_aligned, axis=0)
