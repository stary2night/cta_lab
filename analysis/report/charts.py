"""图表生成模块。

所有函数返回 matplotlib.figure.Figure，不调用 plt.show()。
"""

from __future__ import annotations

import numpy as np
import pandas as pd
import matplotlib
import matplotlib.pyplot as plt
import matplotlib.ticker as mticker
from matplotlib.figure import Figure

matplotlib.use("Agg")  # 非交互后端，避免 GUI 依赖


def plot_nav(
    nav_dict: dict[str, pd.Series],
    log_scale: bool = True,
) -> Figure:
    """NAV 曲线，支持多策略对比。

    Parameters
    ----------
    nav_dict:
        {策略名: NAV Series}。
    log_scale:
        是否使用对数纵轴，默认 True。

    Returns
    -------
    matplotlib Figure。
    """
    fig, ax = plt.subplots(figsize=(12, 6))

    for name, nav in nav_dict.items():
        ax.plot(nav.index, nav.values, label=name, linewidth=1.2)

    if log_scale:
        ax.set_yscale("log")
    ax.set_title("NAV Curve")
    ax.set_xlabel("Date")
    ax.set_ylabel("NAV" + (" (log scale)" if log_scale else ""))
    ax.legend(loc="best")
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    return fig


def plot_performance_table(summary: dict) -> Figure:
    """绩效指标汇总表格图。

    Parameters
    ----------
    summary:
        performance_summary() 返回的字典。

    Returns
    -------
    matplotlib Figure（表格形式）。
    """
    # 格式化指标
    format_map = {
        "annual_return": "{:.2%}",
        "annual_vol":    "{:.2%}",
        "sharpe":        "{:.2f}",
        "sortino":       "{:.2f}",
        "max_drawdown":  "{:.2%}",
        "max_dd_duration": "{:.0f} days",
        "calmar":        "{:.2f}",
        "win_rate":      "{:.2%}",
        "profit_loss":   "{:.2f}",
    }
    label_map = {
        "annual_return":   "Annual Return",
        "annual_vol":      "Annual Volatility",
        "sharpe":          "Sharpe Ratio",
        "sortino":         "Sortino Ratio",
        "max_drawdown":    "Max Drawdown",
        "max_dd_duration": "Max DD Duration",
        "calmar":          "Calmar Ratio",
        "win_rate":        "Win Rate",
        "profit_loss":     "Profit/Loss Ratio",
    }

    rows = []
    for key, fmt in format_map.items():
        val = summary.get(key, float("nan"))
        if val != val:  # NaN check
            formatted = "N/A"
        else:
            try:
                formatted = fmt.format(val)
            except (ValueError, TypeError):
                formatted = str(val)
        rows.append([label_map.get(key, key), formatted])

    fig, ax = plt.subplots(figsize=(6, len(rows) * 0.5 + 1))
    ax.axis("off")

    table = ax.table(
        cellText=rows,
        colLabels=["Metric", "Value"],
        loc="center",
        cellLoc="left",
    )
    table.auto_set_font_size(False)
    table.set_fontsize(11)
    table.scale(1.2, 1.5)

    ax.set_title("Performance Summary", fontsize=13, pad=12)
    fig.tight_layout()
    return fig


def plot_crisis_alpha(crisis_df: pd.DataFrame) -> Figure:
    """危机 Alpha 柱状图：策略 vs 基准。

    Parameters
    ----------
    crisis_df:
        crisis_alpha_analysis() 返回的 DataFrame，
        columns=[strategy_return, benchmark_return, alpha]。

    Returns
    -------
    matplotlib Figure。
    """
    events = crisis_df.index.tolist()
    x = np.arange(len(events))
    width = 0.25

    fig, ax = plt.subplots(figsize=(max(8, len(events) * 1.5), 6))

    bars1 = ax.bar(x - width, crisis_df["strategy_return"].values, width,
                   label="Strategy", color="steelblue", alpha=0.8)
    bars2 = ax.bar(x, crisis_df["benchmark_return"].values, width,
                   label="Benchmark", color="coral", alpha=0.8)
    bars3 = ax.bar(x + width, crisis_df["alpha"].values, width,
                   label="Alpha", color="seagreen", alpha=0.8)

    ax.axhline(0, color="black", linewidth=0.8, linestyle="--")
    ax.set_xticks(x)
    ax.set_xticklabels(events, rotation=20, ha="right", fontsize=9)
    ax.yaxis.set_major_formatter(mticker.PercentFormatter(xmax=1.0))
    ax.set_title("Crisis Alpha Analysis")
    ax.set_ylabel("Return")
    ax.legend(loc="best")
    ax.grid(True, axis="y", alpha=0.3)
    fig.tight_layout()
    return fig


def plot_long_short(nav_df: pd.DataFrame) -> Figure:
    """多空不对称三条 NAV 曲线。

    Parameters
    ----------
    nav_df:
        long_short_asymmetry() 返回的 DataFrame，
        columns=[long_only, short_only, long_short]。

    Returns
    -------
    matplotlib Figure。
    """
    fig, ax = plt.subplots(figsize=(12, 6))

    colors = {"long_only": "steelblue", "short_only": "coral", "long_short": "seagreen"}
    labels = {"long_only": "Long Only", "short_only": "Short Only", "long_short": "Long-Short"}

    for col in ["long_only", "short_only", "long_short"]:
        if col in nav_df.columns:
            ax.plot(nav_df.index, nav_df[col].values,
                    label=labels[col], color=colors[col], linewidth=1.2)

    ax.set_title("Long/Short Asymmetry")
    ax.set_xlabel("Date")
    ax.set_ylabel("NAV")
    ax.legend(loc="best")
    ax.grid(True, alpha=0.3)
    ax.axhline(1.0, color="black", linewidth=0.6, linestyle="--")
    fig.tight_layout()
    return fig


def plot_sector_heatmap(sector_df: pd.DataFrame) -> Figure:
    """板块绩效热图。

    Parameters
    ----------
    sector_df:
        sector_performance() 返回的 DataFrame，
        index=sector，columns=绩效指标。

    Returns
    -------
    matplotlib Figure。
    """
    # 选取数值型列用于热图
    numeric_df = sector_df.select_dtypes(include=[float, int])
    if numeric_df.empty:
        fig, ax = plt.subplots()
        ax.text(0.5, 0.5, "No data", ha="center", va="center")
        return fig

    data = numeric_df.values.astype(float)
    fig, ax = plt.subplots(figsize=(max(8, len(numeric_df.columns) * 1.2),
                                    max(4, len(numeric_df.index) * 0.6)))

    im = ax.imshow(data, aspect="auto", cmap="RdYlGn")
    plt.colorbar(im, ax=ax)

    ax.set_xticks(range(len(numeric_df.columns)))
    ax.set_xticklabels(numeric_df.columns, rotation=30, ha="right", fontsize=9)
    ax.set_yticks(range(len(numeric_df.index)))
    ax.set_yticklabels(numeric_df.index, fontsize=9)
    ax.set_title("Sector Performance Heatmap")

    # 在格子内添加数值
    for i in range(data.shape[0]):
        for j in range(data.shape[1]):
            val = data[i, j]
            if not np.isnan(val):
                ax.text(j, i, f"{val:.2f}", ha="center", va="center", fontsize=7)

    fig.tight_layout()
    return fig


def plot_momentum_persistence(persistence_df: pd.DataFrame) -> Figure:
    """动量持续性：beta vs 滞后期折线图（含 t-stat 辅助轴）。

    Parameters
    ----------
    persistence_df:
        momentum_persistence() 返回的 DataFrame，
        index=lag，columns=[beta, t_stat, r_squared]。

    Returns
    -------
    matplotlib Figure。
    """
    fig, ax1 = plt.subplots(figsize=(10, 6))

    lags = persistence_df.index.tolist()
    betas = persistence_df["beta"].values
    t_stats = persistence_df["t_stat"].values if "t_stat" in persistence_df.columns else None

    ax1.plot(lags, betas, "o-", color="steelblue", linewidth=1.5, label="Beta")
    ax1.axhline(0, color="black", linewidth=0.8, linestyle="--")
    ax1.set_xlabel("Lag (months)")
    ax1.set_ylabel("Beta", color="steelblue")
    ax1.tick_params(axis="y", labelcolor="steelblue")
    ax1.set_title("Momentum Persistence")

    if t_stats is not None:
        ax2 = ax1.twinx()
        ax2.bar(lags, t_stats, alpha=0.3, color="coral", label="t-stat")
        ax2.axhline(1.96, color="red", linewidth=0.8, linestyle=":", label="t=1.96")
        ax2.axhline(-1.96, color="red", linewidth=0.8, linestyle=":")
        ax2.set_ylabel("t-stat", color="coral")
        ax2.tick_params(axis="y", labelcolor="coral")
        lines1, labels1 = ax1.get_legend_handles_labels()
        lines2, labels2 = ax2.get_legend_handles_labels()
        ax1.legend(lines1 + lines2, labels1 + labels2, loc="best")
    else:
        ax1.legend(loc="best")

    ax1.grid(True, alpha=0.3)
    fig.tight_layout()
    return fig


def plot_convexity(convexity_df: pd.DataFrame) -> Figure:
    """危机凸性微笑曲线。

    Parameters
    ----------
    convexity_df:
        convexity_analysis() 返回的 DataFrame，
        columns=[bin_mid, strategy_mean, benchmark_mean]。

    Returns
    -------
    matplotlib Figure。
    """
    fig, ax = plt.subplots(figsize=(10, 6))

    ax.plot(convexity_df["bin_mid"], convexity_df["strategy_mean"],
            "o-", color="steelblue", linewidth=1.5, label="Strategy")
    ax.plot(convexity_df["bin_mid"], convexity_df["benchmark_mean"],
            "s--", color="coral", linewidth=1.2, label="Benchmark")

    ax.axhline(0, color="black", linewidth=0.6, linestyle="--")
    ax.axvline(0, color="gray", linewidth=0.6, linestyle=":")
    ax.xaxis.set_major_formatter(mticker.PercentFormatter(xmax=1.0))
    ax.yaxis.set_major_formatter(mticker.PercentFormatter(xmax=1.0))
    ax.set_xlabel("Benchmark Return Bin Midpoint")
    ax.set_ylabel("Average Return")
    ax.set_title("Convexity / Smile Curve")
    ax.legend(loc="best")
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    return fig


def plot_nav_with_drawdown(
    pnl_dict: dict[str, pd.Series],
    title: str = "",
    log_scale: bool = True,
    colors: dict[str, str] | None = None,
) -> Figure:
    """NAV 曲线 + 回撤面板（双 subplot）。

    Parameters
    ----------
    pnl_dict:
        {策略名: 日收益率 Series}（非 NAV，函数内部自动累乘）。
    title:
        图表标题，默认空字符串。
    log_scale:
        NAV 面板是否使用对数纵轴，默认 True。
    colors:
        {策略名: 颜色}，None 时使用 tab10 调色板。

    Returns
    -------
    matplotlib Figure（上：NAV，下：回撤）。
    """
    _TRADING_DAYS = 252
    _tab10 = plt.cm.tab10.colors  # type: ignore[attr-defined]

    fig, axes = plt.subplots(
        2, 1, figsize=(14, 9), gridspec_kw={"height_ratios": [3, 1]}
    )
    ax_nav, ax_dd = axes

    for i, (name, pnl) in enumerate(pnl_dict.items()):
        color = (colors or {}).get(name, _tab10[i % 10])
        nav = (1 + pnl).cumprod()
        # 计算全期 Sharpe / MDD 用于图例
        ann_r = pnl.mean() * _TRADING_DAYS
        ann_v = pnl.std() * np.sqrt(_TRADING_DAYS)
        sr = ann_r / ann_v if ann_v > 0 else float("nan")
        mdd = float(((nav - nav.cummax()) / nav.cummax()).min())
        label = f"{name}  SR={sr:.3f}  MDD={mdd*100:.1f}%"

        plot_fn = ax_nav.semilogy if log_scale else ax_nav.plot
        plot_fn(nav.index, nav.values, color=color, linewidth=1.5, label=label)

        dd = (nav - nav.cummax()) / nav.cummax() * 100
        ax_dd.fill_between(dd.index, dd.values, 0, color=color, alpha=0.25)
        ax_dd.plot(dd.index, dd.values, color=color, linewidth=0.7)

    ax_nav.set_ylabel("NAV" + (" (log scale)" if log_scale else ""), fontsize=11)
    if title:
        ax_nav.set_title(title, fontsize=12)
    ax_nav.legend(fontsize=9)
    ax_nav.grid(True, alpha=0.3, which="both")

    ax_dd.set_ylabel("Drawdown (%)", fontsize=10)
    ax_dd.set_xlabel("Date", fontsize=10)
    ax_dd.grid(True, alpha=0.3)
    fig.tight_layout()
    return fig


def plot_annual_bar(
    annual_df_dict: dict[str, pd.DataFrame],
    value_col: str = "Return(%)",
    title: str = "",
    colors: dict[str, str] | None = None,
) -> Figure:
    """年度收益柱状图，支持单策略（正负配色）和多策略（分组）。

    Parameters
    ----------
    annual_df_dict:
        {策略名: annual_stats() 返回的 DataFrame}。
        单策略时 dict 只有一个 key，柱子按正负着色；
        多策略时生成分组柱状图。
    value_col:
        取哪一列作为柱高，默认 "Return(%)"，也可用 "Sharpe"。
    title:
        图表标题。
    colors:
        {策略名: 颜色}，None 时使用 tab10。

    Returns
    -------
    matplotlib Figure。
    """
    _tab10 = plt.cm.tab10.colors  # type: ignore[attr-defined]
    names = list(annual_df_dict.keys())
    # 取所有策略年份的并集并排序
    all_years = sorted(set().union(*[set(df.index) for df in annual_df_dict.values()]))
    x = np.arange(len(all_years))

    fig, ax = plt.subplots(figsize=(max(14, len(all_years) * 0.6), 5))

    if len(names) == 1:
        # 单策略：按正负值着色
        name = names[0]
        vals = [annual_df_dict[name][value_col].get(y, float("nan")) for y in all_years]
        bar_colors = ["steelblue" if (v == v and v >= 0) else "tomato" for v in vals]
        ax.bar(x, vals, color=bar_colors, alpha=0.85)
    else:
        # 多策略：分组柱状图
        n = len(names)
        width = min(0.8 / n, 0.3)
        offsets = np.linspace(-(n - 1) * width / 2, (n - 1) * width / 2, n)
        for i, name in enumerate(names):
            color = (colors or {}).get(name, _tab10[i % 10])
            vals = [annual_df_dict[name][value_col].get(y, float("nan"))
                    for y in all_years]
            ax.bar(x + offsets[i], vals, width=width, color=color, alpha=0.82, label=name)
        ax.legend(fontsize=9)

    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_xticks(x)
    ax.set_xticklabels([str(y) for y in all_years], rotation=45, fontsize=8)
    ax.set_ylabel(value_col, fontsize=11)
    if title:
        ax.set_title(title, fontsize=12)
    ax.grid(True, alpha=0.2, axis="y")
    fig.tight_layout()
    return fig


def plot_rolling_sharpe(
    pnl_dict: dict[str, pd.Series],
    window: int = 252,
    title: str = "",
    colors: dict[str, str] | None = None,
) -> Figure:
    """滚动 Sharpe 曲线，含全期均值参考虚线。

    Parameters
    ----------
    pnl_dict:
        {策略名: 日收益率 Series}。
    window:
        滚动窗口（交易日），默认 252。
    title:
        图表标题。
    colors:
        {策略名: 颜色}，None 时使用 tab10。

    Returns
    -------
    matplotlib Figure。
    """
    _TRADING_DAYS = 252
    _tab10 = plt.cm.tab10.colors  # type: ignore[attr-defined]

    fig, ax = plt.subplots(figsize=(14, 4))

    for i, (name, pnl) in enumerate(pnl_dict.items()):
        color = (colors or {}).get(name, _tab10[i % 10])
        roll_mean = pnl.rolling(window).mean() * _TRADING_DAYS
        roll_vol  = pnl.rolling(window).std() * np.sqrt(_TRADING_DAYS)
        roll_sr   = roll_mean / roll_vol
        # 全期 Sharpe 参考线
        ann_r = pnl.mean() * _TRADING_DAYS
        ann_v = pnl.std() * np.sqrt(_TRADING_DAYS)
        full_sr = ann_r / ann_v if ann_v > 0 else float("nan")

        ax.plot(roll_sr.index, roll_sr.values, color=color, linewidth=1.2,
                label=f"{name} (SR={full_sr:.3f})")
        ax.axhline(full_sr, color=color, linestyle="--", linewidth=0.8, alpha=0.55)

    ax.axhline(0, color="black", linewidth=0.8)
    ax.set_ylabel(f"Rolling {window}d Sharpe", fontsize=11)
    if title:
        ax.set_title(title, fontsize=12)
    ax.legend(fontsize=9)
    ax.grid(True, alpha=0.3)
    fig.tight_layout()
    return fig


def plot_monthly_heatmap(
    mpiv: pd.DataFrame,
    title: str = "",
) -> Figure:
    """月度收益热图（年×月），含右侧年度汇总列。

    Parameters
    ----------
    mpiv:
        monthly_pivot() 返回的 DataFrame，
        index=Year，columns=[Jan…Dec, Annual(%)]，单位 %。
    title:
        图表标题。

    Returns
    -------
    matplotlib Figure。
    """
    data = mpiv.drop(columns=["Annual(%)"]).values.astype(float)
    row_labels = mpiv.index.astype(str).tolist()
    col_labels = list(mpiv.drop(columns=["Annual(%)"]).columns)

    fig, ax = plt.subplots(figsize=(13, max(6, len(row_labels) * 0.28)))
    vmax = min(float(np.nanpercentile(np.abs(data), 95)), 15.0)
    im = ax.imshow(data, cmap="RdYlGn", aspect="auto", vmin=-vmax, vmax=vmax)

    ax.set_xticks(range(len(col_labels)))
    ax.set_xticklabels(col_labels, fontsize=9)
    ax.set_yticks(range(len(row_labels)))
    ax.set_yticklabels(row_labels, fontsize=8)

    for i in range(data.shape[0]):
        for j in range(data.shape[1]):
            v = data[i, j]
            if not np.isnan(v):
                ax.text(j, i, f"{v:.1f}", ha="center", va="center",
                        fontsize=6.5,
                        color="black" if abs(v) < vmax * 0.6 else "white")

    ann_vals = mpiv["Annual(%)"].values
    for i, v in enumerate(ann_vals):
        if not np.isnan(v):
            color = "#2a6c2a" if v > 0 else "#8b1a1a"
            ax.text(len(col_labels) + 0.6, i, f"{v:.1f}%",
                    ha="left", va="center", fontsize=8, color=color, fontweight="bold")
    ax.text(len(col_labels) + 0.6, -0.8, "Annual",
            ha="left", va="center", fontsize=8, fontweight="bold")

    plt.colorbar(im, ax=ax, label="Monthly Return (%)", shrink=0.6)
    if title:
        ax.set_title(title, fontsize=12)
    fig.tight_layout()
    return fig


def plot_asset_contribution(
    total_contrib: pd.Series,
    annual_contrib: pd.DataFrame,
) -> Figure:
    """品种贡献：左图条形图 + 右图年度热图。

    Parameters
    ----------
    total_contrib:
        asset_contribution() 返回的 Series，index=symbols。
    annual_contrib:
        annual_contribution() 返回的 DataFrame，index=year，columns=symbols。

    Returns
    -------
    matplotlib Figure（左右两个子图）。
    """
    n_syms = len(total_contrib)
    fig, (ax_bar, ax_heat) = plt.subplots(
        1, 2,
        figsize=(max(14, n_syms * 0.5 + 6), max(6, len(annual_contrib) * 0.5 + 3)),
        gridspec_kw={"width_ratios": [1, 2]},
    )

    # 左图：品种贡献条形图
    colors = ["steelblue" if v >= 0 else "coral" for v in total_contrib.values]
    ax_bar.barh(range(n_syms), total_contrib.values, color=colors, alpha=0.8)
    ax_bar.set_yticks(range(n_syms))
    ax_bar.set_yticklabels(total_contrib.index.tolist(), fontsize=8)
    ax_bar.axvline(0, color="black", linewidth=0.8)
    ax_bar.xaxis.set_major_formatter(mticker.PercentFormatter(xmax=1.0))
    ax_bar.set_xlabel("Total Contribution")
    ax_bar.set_title("Asset Contribution (Full Period)")
    ax_bar.grid(True, axis="x", alpha=0.3)

    # 右图：年度贡献热图
    if not annual_contrib.empty:
        data = annual_contrib.values.astype(float)
        im = ax_heat.imshow(data, aspect="auto", cmap="RdYlGn")
        plt.colorbar(im, ax=ax_heat, label="Contribution")

        ax_heat.set_xticks(range(len(annual_contrib.columns)))
        ax_heat.set_xticklabels(annual_contrib.columns.tolist(), rotation=45, ha="right", fontsize=7)
        ax_heat.set_yticks(range(len(annual_contrib.index)))
        ax_heat.set_yticklabels([str(y) for y in annual_contrib.index], fontsize=8)
        ax_heat.set_title("Annual Asset Contribution Heatmap")
    else:
        ax_heat.axis("off")
        ax_heat.text(0.5, 0.5, "No annual data", ha="center", va="center")

    fig.tight_layout()
    return fig


def plot_sector_nav(
    pos_df: pd.DataFrame,
    returns: pd.DataFrame,
    sector_map: dict[str, str],
    bt,
    title: str = "Sector NAV",
) -> Figure:
    """按板块绘制对数坐标 NAV 曲线，图例附 Sharpe。

    Parameters
    ----------
    pos_df:
        头寸权重矩阵，columns=symbols。
    returns:
        日收益率矩阵，columns=symbols。
    sector_map:
        symbol → sector 映射字典。
    bt:
        VectorizedBacktest 实例，用于逐板块回测。
    title:
        图表标题。

    Returns
    -------
    matplotlib Figure。
    """
    from analysis.metrics import pnl_stats

    sectors: dict[str, list[str]] = {}
    for sym in returns.columns:
        sec = sector_map.get(sym, "Other")
        sectors.setdefault(sec, []).append(sym)

    fig, ax = plt.subplots(figsize=(14, 6))
    colors = plt.cm.tab10.colors  # type: ignore[attr-defined]
    for i, sec in enumerate(sorted(sectors)):
        syms_avail = [s for s in sectors[sec] if s in returns.columns]
        if not syms_avail:
            continue
        pnl_s = bt.run(pos_df[syms_avail], returns[syms_avail]).returns.iloc[1:]
        nav_s = (1 + pnl_s).cumprod()
        sh = pnl_stats(pnl_s)["Sharpe"]
        ax.semilogy(nav_s.index, nav_s.values, color=colors[i % 10],
                    linewidth=1.2, label=f"{sec} (SR={sh:.2f})")

    ax.set_ylabel("NAV (log scale)", fontsize=11)
    ax.set_title(title, fontsize=12)
    ax.legend(fontsize=8, ncol=2)
    ax.grid(True, alpha=0.3, which="both")
    fig.tight_layout()
    return fig
