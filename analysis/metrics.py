"""标准绩效指标计算模块。"""

from __future__ import annotations

import numpy as np
import pandas as pd


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
