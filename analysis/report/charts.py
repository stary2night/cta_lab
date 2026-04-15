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
