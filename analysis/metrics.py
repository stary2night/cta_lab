"""标准绩效指标计算模块。"""

from __future__ import annotations

import numpy as np
import pandas as pd

_TRADING_DAYS = 252


def underwater_series(nav: pd.Series) -> pd.Series:
    """每日回撤水位序列（0 到负数）。

    underwater[t] = nav[t] / nav[:t+1].max() - 1
    """
    rolling_max = nav.expanding().max()
    return nav / rolling_max - 1


def performance_summary(nav: pd.Series, rf: float = 0.02) -> dict:
    """计算标准绩效指标字典。

    Parameters
    ----------
    nav:
        NAV 序列，DatetimeIndex，从 1.0 开始。
    rf:
        无风险利率（年化），默认 0.02。

    Returns
    -------
    dict，键：
      annual_return   # 年化收益率（几何）
      annual_vol      # 年化波动率
      sharpe          # Sharpe 比率
      sortino         # Sortino 比率（下行波动）
      max_drawdown    # 最大回撤（负值，如 -0.20）
      max_dd_duration # 最大回撤持续天数（int）
      calmar          # Calmar = annual_return / abs(max_drawdown)
      win_rate        # 胜率（日收益 > 0 的比例）
      profit_loss     # 盈亏比（平均盈利日 / 平均亏损日绝对值）
    """
    returns = nav.pct_change().dropna()
    n = len(returns)
    if n == 0:
        return {}

    annual_return = (nav.iloc[-1] / nav.iloc[0]) ** (252 / n) - 1
    annual_vol = returns.std() * np.sqrt(252)
    sharpe = (annual_return - rf) / annual_vol if annual_vol > 0 else np.nan

    downside = returns[returns < 0].std() * np.sqrt(252)
    sortino = (annual_return - rf) / downside if downside > 0 else np.nan

    # 最大回撤
    uw = underwater_series(nav)
    max_drawdown = float(uw.min())

    # 最大回撤持续天数：最长连续水下天数
    is_underwater = (uw < 0).astype(int)
    max_dd_duration = 0
    current_streak = 0
    for val in is_underwater:
        if val:
            current_streak += 1
            max_dd_duration = max(max_dd_duration, current_streak)
        else:
            current_streak = 0

    # Calmar
    calmar = annual_return / abs(max_drawdown) if max_drawdown != 0 else np.nan

    # 胜率
    win_rate = float((returns > 0).mean())

    # 盈亏比
    pos_mean = returns[returns > 0].mean()
    neg_mean = returns[returns < 0].mean()
    if len(returns[returns < 0]) > 0 and neg_mean != 0:
        profit_loss = float(pos_mean / abs(neg_mean))
    else:
        profit_loss = np.nan

    return {
        "annual_return": annual_return,
        "annual_vol": annual_vol,
        "sharpe": sharpe,
        "sortino": sortino,
        "max_drawdown": max_drawdown,
        "max_dd_duration": max_dd_duration,
        "calmar": calmar,
        "win_rate": win_rate,
        "profit_loss": profit_loss,
    }


def rolling_metrics(nav: pd.Series, window: int = 252) -> pd.DataFrame:
    """滚动窗口绩效指标。

    返回 DataFrame，columns: [rolling_sharpe, rolling_vol, rolling_max_dd]
    前 window-1 行为 NaN。
    """
    returns = nav.pct_change()

    rolling_vol = returns.rolling(window).std() * np.sqrt(252)

    # 滚动 Sharpe（使用 rf=0 简化，外部可调整）
    rolling_mean = returns.rolling(window).mean() * 252
    rolling_sharpe = rolling_mean / rolling_vol

    # 滚动最大回撤：对每个窗口末尾，取该窗口内的 NAV 子序列计算最大回撤
    def _roll_max_dd(window_nav_vals: np.ndarray) -> float:
        """计算给定 NAV 窗口的最大回撤。"""
        peak = np.maximum.accumulate(window_nav_vals)
        dd = window_nav_vals / peak - 1.0
        return float(dd.min())

    # 用 nav 的滚动窗口计算最大回撤
    rolling_max_dd = nav.rolling(window).apply(_roll_max_dd, raw=True)

    result = pd.DataFrame({
        "rolling_sharpe": rolling_sharpe,
        "rolling_vol": rolling_vol,
        "rolling_max_dd": rolling_max_dd,
    }, index=nav.index)

    return result


# ── 日收益率序列统计（脚本层格式）────────────────────────────────────────────────

def pnl_stats(pnl: pd.Series, include_skew: bool = False) -> dict:
    """从日收益率序列计算格式化统计字典。

    与 performance_summary() 的区别：输入为日收益率（非 NAV），
    输出值已转换为百分比并保留适当精度，便于直接打印和写入 CSV。

    Parameters
    ----------
    pnl:
        日收益率序列（非对数），DatetimeIndex。
    include_skew:
        是否包含偏度，默认 False。

    Returns
    -------
    dict，键：Return(%), Vol(%), Sharpe, MaxDD(%), Calmar, HitRate(%)
          可选：Skewness
    """
    ann_r = pnl.mean() * _TRADING_DAYS
    ann_v = pnl.std() * np.sqrt(_TRADING_DAYS)
    sharpe = ann_r / ann_v if ann_v > 0 else float("nan")
    nav = (1 + pnl).cumprod()
    mdd = float(((nav - nav.cummax()) / nav.cummax()).min())
    calmar = ann_r / abs(mdd) if mdd != 0 else float("nan")
    hit = float((pnl > 0).mean())
    result = {
        "Return(%)": round(ann_r * 100, 2),
        "Vol(%)":    round(ann_v * 100, 2),
        "Sharpe":    round(sharpe, 3),
        "MaxDD(%)":  round(mdd * 100, 2),
        "Calmar":    round(calmar, 3),
        "HitRate(%)": round(hit * 100, 1),
    }
    if include_skew:
        result["Skewness"] = round(float(pnl.skew()), 3)
    return result


def annual_stats(pnl: pd.Series) -> pd.DataFrame:
    """按年度分组计算绩效统计。

    Parameters
    ----------
    pnl:
        日收益率序列，DatetimeIndex。

    Returns
    -------
    pd.DataFrame，index=Year，columns=[Return(%), Vol(%), Sharpe, MaxDD(%), Days]
    """
    rows = []
    for year, grp in pnl.groupby(pnl.index.year):
        ann_r = grp.mean() * _TRADING_DAYS
        ann_v = grp.std() * np.sqrt(_TRADING_DAYS)
        sharpe = ann_r / ann_v if ann_v > 0 else float("nan")
        nav = (1 + grp).cumprod()
        mdd = float(((nav - nav.cummax()) / nav.cummax()).min())
        rows.append({
            "Year":      year,
            "Return(%)": round(ann_r * 100, 2),
            "Vol(%)":    round(ann_v * 100, 2),
            "Sharpe":    round(sharpe, 3),
            "MaxDD(%)":  round(mdd * 100, 2),
            "Days":      len(grp),
        })
    return pd.DataFrame(rows).set_index("Year")


def decade_stats(
    pnl: pd.Series,
    starts: list[int] | None = None,
    min_obs: int = 63,
) -> pd.DataFrame:
    """按十年期分段计算绩效统计。

    Parameters
    ----------
    pnl:
        日收益率序列，DatetimeIndex。
    starts:
        各段起始年份，每段覆盖 [start, start+9]。
        默认 [1995, 2000, 2005, 2010, 2015, 2020]。
    min_obs:
        段内最少交易日数，不足则跳过，默认 63。

    Returns
    -------
    pd.DataFrame，index=Period（如"1995s"），
    columns=[Start, End, Return(%), Vol(%), Sharpe, MaxDD(%)]
    """
    if starts is None:
        starts = [1995, 2000, 2005, 2010, 2015, 2020]
    rows = []
    for ds in starts:
        mask = (pnl.index.year >= ds) & (pnl.index.year <= ds + 9)
        grp = pnl[mask]
        if len(grp) < min_obs:
            continue
        ann_r = grp.mean() * _TRADING_DAYS
        ann_v = grp.std() * np.sqrt(_TRADING_DAYS)
        sharpe = ann_r / ann_v if ann_v > 0 else float("nan")
        nav = (1 + grp).cumprod()
        mdd = float(((nav - nav.cummax()) / nav.cummax()).min())
        rows.append({
            "Period":    f"{ds}s",
            "Start":     grp.index[0].date(),
            "End":       grp.index[-1].date(),
            "Return(%)": round(ann_r * 100, 2),
            "Vol(%)":    round(ann_v * 100, 2),
            "Sharpe":    round(sharpe, 3),
            "MaxDD(%)":  round(mdd * 100, 2),
        })
    return pd.DataFrame(rows).set_index("Period")


def monthly_pivot(pnl: pd.Series) -> pd.DataFrame:
    """计算月度收益透视表（年×月）。

    Parameters
    ----------
    pnl:
        日收益率序列，DatetimeIndex。

    Returns
    -------
    pd.DataFrame，index=Year，columns=[Jan…Dec, Annual(%)]，单位 %。
    """
    mr = pnl.groupby([pnl.index.year, pnl.index.month]).apply(
        lambda g: (1 + g).prod() - 1
    )
    mr.index = pd.MultiIndex.from_tuples(mr.index, names=["Year", "Month"])
    piv = mr.unstack("Month") * 100
    month_names = ["Jan", "Feb", "Mar", "Apr", "May", "Jun",
                   "Jul", "Aug", "Sep", "Oct", "Nov", "Dec"]
    piv.columns = month_names[: len(piv.columns)]
    piv["Annual(%)"] = (
        (piv / 100 + 1).prod(axis=1, skipna=True).subtract(1).mul(100).round(2)
    )
    return piv.round(2)


def sector_stats(
    pos_df: pd.DataFrame,
    ret_df: pd.DataFrame,
    sector_map: dict[str, str],
    bt,
    include_avg_pos: bool = False,
) -> pd.DataFrame:
    """按板块拆分回测，返回各板块绩效汇总。

    Parameters
    ----------
    pos_df:
        头寸权重矩阵，columns=symbols。
    ret_df:
        日收益率矩阵，columns=symbols。
    sector_map:
        symbol → sector 映射字典。
    bt:
        VectorizedBacktest 实例，用于逐板块回测。
    include_avg_pos:
        是否附加 AvgAbsPos 列（平均绝对头寸大小）。

    Returns
    -------
    pd.DataFrame，index=Sector，包含各板块绩效指标。
    """
    sectors: dict[str, list[str]] = {}
    for sym in ret_df.columns:
        sec = sector_map.get(sym, "Other")
        sectors.setdefault(sec, []).append(sym)

    rows = []
    for sec, syms in sorted(sectors.items()):
        avail = [s for s in syms if s in pos_df.columns]
        if not avail:
            continue
        pnl_s = bt.run(pos_df[avail], ret_df[avail]).returns.iloc[1:]
        s = pnl_stats(pnl_s)
        row: dict = {"Sector": sec, "Symbols": len(avail), **s}
        if include_avg_pos:
            row["AvgAbsPos"] = round(pos_df[avail].abs().mean().mean(), 3)
        rows.append(row)

    return pd.DataFrame(rows).set_index("Sector")


def asset_stats(
    pos_df: pd.DataFrame,
    ret_df: pd.DataFrame,
    sector_map: dict[str, str],
    bt,
) -> pd.DataFrame:
    """逐品种独立回测，返回各品种 Sharpe / MaxDD 汇总。

    Parameters
    ----------
    pos_df:
        头寸权重矩阵，columns=symbols。
    ret_df:
        日收益率矩阵，columns=symbols。
    sector_map:
        symbol → sector 映射字典。
    bt:
        VectorizedBacktest 实例，用于逐品种回测。

    Returns
    -------
    pd.DataFrame，index=Symbol，按 StandaloneSR 降序排列。
    """
    rows = []
    for sym in ret_df.columns:
        pnl_a = bt.run(pos_df[[sym]], ret_df[[sym]]).returns.iloc[1:]
        s = pnl_stats(pnl_a)
        rows.append({
            "Symbol": sym,
            "Sector": sector_map.get(sym, "Other"),
            "StandaloneSR": s["Sharpe"],
            "MaxDD(%)": s["MaxDD(%)"],
            "Start": ret_df[sym].first_valid_index().date(),
        })
    return (
        pd.DataFrame(rows)
        .set_index("Symbol")
        .sort_values("StandaloneSR", ascending=False)
    )
