"""危机凸性（微笑曲线）分析模块。"""

from __future__ import annotations

import numpy as np
import pandas as pd


def convexity_analysis(
    strategy_returns: pd.Series,
    benchmark_returns: pd.Series,
    n_bins: int = 20,
) -> pd.DataFrame:
    """微笑曲线：按基准收益分位数分组，计算各组策略平均收益。

    用于观察策略在极端行情下是否有凸性（两端收益高于中间）。

    Parameters
    ----------
    strategy_returns:
        策略日收益率序列。
    benchmark_returns:
        基准日收益率序列。
    n_bins:
        分位数分组数量，默认 20。

    Returns
    -------
    DataFrame，columns=[bin_mid, strategy_mean, benchmark_mean]
    """
    # 对齐索引
    common_idx = strategy_returns.index.intersection(benchmark_returns.index)
    strat = strategy_returns.loc[common_idx].dropna()
    bench = benchmark_returns.loc[common_idx].dropna()

    common_idx2 = strat.index.intersection(bench.index)
    strat = strat.loc[common_idx2]
    bench = bench.loc[common_idx2]

    if len(bench) < n_bins:
        n_bins = max(2, len(bench))

    # 按基准收益率分位数分组
    try:
        bins = pd.qcut(bench, q=n_bins, duplicates="drop")
    except ValueError:
        # 极端情况：数据量太少或重复太多
        bins = pd.cut(bench, bins=n_bins)

    records = []
    for interval in bins.cat.categories:
        mask = bins == interval
        if mask.sum() == 0:
            continue
        bin_mid = float(interval.mid)
        strat_mean = float(strat[mask].mean())
        bench_mean = float(bench[mask].mean())
        records.append({
            "bin_mid": bin_mid,
            "strategy_mean": strat_mean,
            "benchmark_mean": bench_mean,
        })

    result = pd.DataFrame(records).sort_values("bin_mid").reset_index(drop=True)
    return result
