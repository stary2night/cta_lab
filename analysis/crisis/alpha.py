"""危机期间 Alpha 分析模块。"""

from __future__ import annotations

import pandas as pd

# 默认危机事件表（国内历史事件，9 个）
DEFAULT_CRISIS_EVENTS: dict[str, tuple[str, str]] = {
    "2008全球金融危机": ("2008-01-01", "2008-12-31"),
    "2010通胀熊市":     ("2010-11-11", "2012-01-06"),
    "2013信用危机":     ("2013-06-13", "2013-07-05"),
    "2015A股股灾I":     ("2015-06-12", "2015-08-26"),
    "2015A股股灾II":    ("2015-12-22", "2016-02-29"),
    "2016熔断":         ("2016-01-04", "2016-01-07"),
    "2018中美贸易战":   ("2018-01-29", "2019-01-04"),
    "2020新冠疫情":     ("2020-01-23", "2020-03-23"),
    "2022年广泛抛售":   ("2022-01-01", "2022-10-31"),
}


def crisis_alpha_analysis(
    strategy_nav: pd.Series,
    benchmark_returns: pd.Series,           # 基准日收益率（如沪深300）
    crisis_events: dict[str, tuple[str, str]] | None = None,  # None 时使用 DEFAULT_CRISIS_EVENTS
) -> pd.DataFrame:
    """计算各危机事件期间策略与基准的收益对比。

    Parameters
    ----------
    strategy_nav:
        策略 NAV 序列，DatetimeIndex。
    benchmark_returns:
        基准日收益率序列，DatetimeIndex。
    crisis_events:
        危机事件字典 {事件名: (start_date, end_date)}。
        None 时使用 DEFAULT_CRISIS_EVENTS。

    Returns
    -------
    DataFrame，index=事件名，columns=[strategy_return, benchmark_return, alpha]
    crisis_return = nav[end] / nav[start] - 1（复利）
    """
    if crisis_events is None:
        crisis_events = DEFAULT_CRISIS_EVENTS

    records = []
    for event_name, (start_str, end_str) in crisis_events.items():
        start = pd.Timestamp(start_str)
        end = pd.Timestamp(end_str)

        # 截取策略 NAV 区间
        nav_slice = strategy_nav.loc[
            (strategy_nav.index >= start) & (strategy_nav.index <= end)
        ]
        if len(nav_slice) < 2:
            records.append({
                "event": event_name,
                "strategy_return": float("nan"),
                "benchmark_return": float("nan"),
                "alpha": float("nan"),
            })
            continue

        strategy_ret = nav_slice.iloc[-1] / nav_slice.iloc[0] - 1

        # 截取基准收益率区间，复利累计
        bm_slice = benchmark_returns.loc[
            (benchmark_returns.index >= start) & (benchmark_returns.index <= end)
        ]
        if len(bm_slice) == 0:
            bm_ret = float("nan")
        else:
            bm_ret = float((1 + bm_slice).prod() - 1)

        alpha = strategy_ret - bm_ret if not (
            strategy_ret != strategy_ret or bm_ret != bm_ret
        ) else float("nan")

        records.append({
            "event": event_name,
            "strategy_return": float(strategy_ret),
            "benchmark_return": bm_ret,
            "alpha": alpha,
        })

    result = pd.DataFrame(records).set_index("event")
    result.index.name = "event"
    return result
