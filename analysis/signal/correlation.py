"""策略相关性分析模块。"""

from __future__ import annotations

import pandas as pd


def correlation_analysis(
    nav_dict: dict[str, pd.Series],                        # {name: nav_series}
    benchmark_returns: dict[str, pd.Series] | None = None, # {name: return_series}
) -> pd.DataFrame:
    """计算各 NAV 序列（及基准）之间的收益相关矩阵。

    Parameters
    ----------
    nav_dict:
        策略名称到 NAV 序列的映射。
    benchmark_returns:
        可选，基准名称到日收益率序列的映射。

    Returns
    -------
    DataFrame，对称相关矩阵，index=columns=名称。
    """
    returns_dict: dict[str, pd.Series] = {}

    # NAV 转日收益率
    for name, nav in nav_dict.items():
        returns_dict[name] = nav.pct_change().dropna()

    # 加入基准收益率
    if benchmark_returns is not None:
        for name, ret in benchmark_returns.items():
            returns_dict[name] = ret.dropna()

    if not returns_dict:
        return pd.DataFrame()

    # 合并成 DataFrame，取公共日期
    combined = pd.DataFrame(returns_dict)
    corr_matrix = combined.corr()

    return corr_matrix
