"""品种收益归因模块。"""

from __future__ import annotations

import pandas as pd


def asset_contribution(
    returns_df: pd.DataFrame,    # shape: (dates, symbols)，品种日收益率
    weights_df: pd.DataFrame,    # shape: (dates, symbols)，持仓权重
) -> pd.Series:
    """计算各品种对组合收益的总贡献（汇总全期）。

    contribution_i = Σ(w_{i,t-1} × r_{i,t})

    Parameters
    ----------
    returns_df:
        品种日收益率矩阵。
    weights_df:
        持仓权重矩阵（使用 t-1 权重乘以 t 收益率）。

    Returns
    -------
    pd.Series，index=symbols，按贡献降序排列。
    """
    # 对齐列
    symbols = returns_df.columns.intersection(weights_df.columns)
    r = returns_df[symbols]
    w = weights_df[symbols]

    # 用 t-1 权重（shift(1)）× t 收益率
    w_lagged = w.shift(1)

    # 逐日逐品种贡献，汇总全期
    daily_contrib = w_lagged * r
    total_contrib = daily_contrib.sum(skipna=True)

    return total_contrib.sort_values(ascending=False)


def annual_contribution(
    returns_df: pd.DataFrame,
    weights_df: pd.DataFrame,
) -> pd.DataFrame:
    """计算各品种各年的收益贡献矩阵。

    Parameters
    ----------
    returns_df:
        品种日收益率矩阵。
    weights_df:
        持仓权重矩阵。

    Returns
    -------
    DataFrame，index=year，columns=symbols，用于绘制年度热图。
    """
    years = returns_df.index.year.unique()
    records = []
    for year in sorted(years):
        year_returns = returns_df[returns_df.index.year == year]
        year_weights = weights_df[weights_df.index.year == year]
        contrib = asset_contribution(year_returns, year_weights)
        contrib.name = year
        records.append(contrib)

    if not records:
        return pd.DataFrame()

    result = pd.DataFrame(records)
    result.index.name = "year"
    return result
