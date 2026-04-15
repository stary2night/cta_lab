"""板块收益归因模块。"""

from __future__ import annotations

import numpy as np
import pandas as pd

from analysis.metrics import performance_summary


def sector_performance(
    returns_df: pd.DataFrame,             # shape: (dates, symbols)
    sector_map: dict[str, str],           # {symbol: sector_name}
    weights_df: pd.DataFrame | None = None,  # 可选，有则做加权分析
) -> pd.DataFrame:
    """按板块汇总绩效指标。

    对每个板块内的品种等权（或按 weights 加权）合成板块收益序列，
    然后调用 performance_summary 计算各板块指标。

    Parameters
    ----------
    returns_df:
        品种日收益率矩阵。
    sector_map:
        品种到板块的映射字典。
    weights_df:
        可选持仓权重矩阵。有则加权，无则等权。

    Returns
    -------
    DataFrame，index=sector，columns=绩效指标名称。
    """
    # 构建板块分组
    sectors: dict[str, list[str]] = {}
    for symbol, sector in sector_map.items():
        if symbol in returns_df.columns:
            sectors.setdefault(sector, []).append(symbol)

    records = {}
    for sector_name, symbols in sectors.items():
        sector_returns = returns_df[symbols]

        if weights_df is not None:
            # 加权：用 weights_df 中对应品种的权重（逐行归一化）
            w = weights_df.reindex(columns=symbols, fill_value=0.0)
            w_sum = w.sum(axis=1).replace(0, np.nan)
            w_norm = w.div(w_sum, axis=0).fillna(0.0)
            sector_ret_series = (sector_returns * w_norm).sum(axis=1)
        else:
            # 等权合成
            sector_ret_series = sector_returns.mean(axis=1)

        # 从收益率序列构造 NAV
        nav = (1 + sector_ret_series).cumprod()
        nav = nav / nav.iloc[0]  # 从 1.0 开始

        summary = performance_summary(nav)
        records[sector_name] = summary

    if not records:
        return pd.DataFrame()

    result = pd.DataFrame(records).T
    result.index.name = "sector"
    return result
