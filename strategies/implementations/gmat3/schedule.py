"""GMAT3 指数计算日与错峰调度。"""

from __future__ import annotations

import pandas as pd

from portfolio.scheduler.staggered import StaggeredScheduler


def build_index_calc_days(
    base_date: str | pd.Timestamp,
    end_date: str | pd.Timestamp | None = None,
    warmup_days: int = 300 + 260 + 50,
) -> tuple[list[pd.Timestamp], list[pd.Timestamp]]:
    """构建 GMAT3 口径的全量计算日与正式指数计算日。

    当前沿用 `ddb/run.py` 的定义：
    - 指数计算日 = 所有工作日（周一至周五）
    - 正式期从 `base_date` 开始
    - 全量计算日额外向前保留 `warmup_days` 个工作日余量
    """
    base_ts = pd.Timestamp(base_date)
    end_ts = pd.Timestamp(end_date) if end_date is not None else pd.Timestamp.today().normalize()

    all_weekdays = pd.bdate_range(start="2000-01-01", end=end_ts).tolist()
    first_base_idx = next(i for i, d in enumerate(all_weekdays) if d >= base_ts)
    warmup_start = all_weekdays[max(0, first_base_idx - warmup_days)]

    full_calc_days = [d for d in all_weekdays if d >= warmup_start]
    calc_days = [d for d in all_weekdays if d >= base_ts]
    return full_calc_days, calc_days


def build_value_matrices(
    value_series_dict: dict[str, pd.Series],
    full_calc_days: list[pd.Timestamp],
    calc_days: list[pd.Timestamp],
) -> tuple[pd.DataFrame, pd.DataFrame]:
    """构建 `value_df_full` 与正式期 `value_df`。"""
    full_idx = pd.DatetimeIndex(full_calc_days)
    value_df_full = pd.DataFrame(index=full_idx, columns=sorted(value_series_dict), dtype=float)
    for variety, series in value_series_dict.items():
        reindexed = series.reindex(full_idx.union(series.index)).sort_index().ffill()
        value_df_full[variety] = reindexed.reindex(full_idx)

    value_df = value_df_full.reindex(pd.DatetimeIndex(calc_days))
    return value_df_full, value_df


def build_staggered_schedule(
    calendar,
    start: str | pd.Timestamp,
    end: str | pd.Timestamp,
    n_sub_portfolios: int = 4,
):
    """构建 GMAT3 4 子组合错峰调仓 schedule。"""
    scheduler = StaggeredScheduler(n_sub=n_sub_portfolios)
    return scheduler.produce_schedule(calendar, start, end)
