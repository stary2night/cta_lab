"""时序趋势策略表现研究：中国 vs 中国+海外

研究目标：
  1. 统计中国期货时序趋势策略的历史表现（重点关注 2010 年以来）
  2. 统计全球（中国+海外）时序趋势策略的历史表现
  3. 分析七个因子的逐年 IC/ICIR
  4. 输出对比表格

运行方式：
  cd /home/ubuntu/dengl/my_projects/cta_lab
  python scripts/research_trend_performance.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import matplotlib
matplotlib.use("Agg")
import matplotlib.pyplot as plt
import numpy as np
import pandas as pd

_HERE = Path(__file__).resolve().parent
_CTA_LAB = _HERE.parent
_OUTPUT_ROOT = _CTA_LAB.parent / "research_outputs"
if str(_CTA_LAB) not in sys.path:
    sys.path.insert(0, str(_CTA_LAB))

from backtest.costs import ProportionalCostModel
from backtest.vectorized import VectorizedBacktest
from signals.momentum.multifactor_trend import MultiFactorTrendSignal

# ── 路径 ──────────────────────────────────────────────────────────────────────

CN_RETURNS = _OUTPUT_ROOT / "multifactor_cta_china" / "data" / "returns.parquet"
CN_POSITIONS = _OUTPUT_ROOT / "multifactor_cta_china" / "runs" / "01_trend_only" / "signals" / "positions.parquet"

GL_RETURNS = _OUTPUT_ROOT / "multifactor_cta_global" / "data" / "returns.parquet"
GL_TREND_POS = _OUTPUT_ROOT / "multifactor_cta_global" / "signals" / "trend_positions.parquet"

OUT_DIR = _OUTPUT_ROOT / "trend_research"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ── 参数 ──────────────────────────────────────────────────────────────────────

TARGET_VOL = 0.05
VOL_HALFLIFE = 21
COST_BPS = 5.0
TRADING_DAYS = 252
ANALYSIS_START = "2010-01-01"


# ── 工具函数 ──────────────────────────────────────────────────────────────────

def run_backtest_from_positions(
    positions: pd.DataFrame,
    returns: pd.DataFrame,
    target_vol: float = TARGET_VOL,
    vol_halflife: int = VOL_HALFLIFE,
    cost_bps: float = COST_BPS,
) -> pd.Series:
    """对已有仓位矩阵跑 VectorizedBacktest，返回日度 PnL 序列。"""
    bt = VectorizedBacktest(
        lag=1,
        vol_target=target_vol,
        vol_halflife=vol_halflife,
        trading_days=TRADING_DAYS,
        cost_model=ProportionalCostModel(cost_bps / 10_000.0),
    )
    result = bt.run(positions.reindex(returns.index, fill_value=0.0), returns)
    return result.returns.iloc[1:]


def annual_table(pnl: pd.Series, label: str, start: str = ANALYSIS_START) -> pd.DataFrame:
    """计算逐年统计指标表格。"""
    pnl = pnl[pnl.index >= start].copy()

    rows = []
    for year, grp in pnl.groupby(pnl.index.year):
        r = grp.mean() * TRADING_DAYS
        v = grp.std() * np.sqrt(TRADING_DAYS)
        s = r / v if v > 0 else np.nan
        nav = (1 + grp).cumprod()
        mdd = ((nav - nav.cummax()) / nav.cummax()).min()
        rows.append({
            "Year": year,
            "Return(%)": round(r * 100, 2),
            "Vol(%)": round(v * 100, 2),
            "Sharpe": round(s, 2),
            "MaxDD(%)": round(mdd * 100, 2),
            "Days": len(grp),
        })
    df = pd.DataFrame(rows).set_index("Year")

    # 全样本汇总
    r = pnl.mean() * TRADING_DAYS
    v = pnl.std() * np.sqrt(TRADING_DAYS)
    s = r / v if v > 0 else np.nan
    nav = (1 + pnl).cumprod()
    mdd = ((nav - nav.cummax()) / nav.cummax()).min()
    summary = pd.DataFrame(
        [{
            "Return(%)": round(r * 100, 2),
            "Vol(%)": round(v * 100, 2),
            "Sharpe": round(s, 2),
            "MaxDD(%)": round(mdd * 100, 2),
            "Days": len(pnl),
        }],
        index=pd.Index([f"TOTAL({start[:4]}+)"], name="Year"),
    )
    return pd.concat([df, summary])


def full_period_stats(pnl: pd.Series, label: str) -> dict:
    """全时段统计。"""
    r = pnl.mean() * TRADING_DAYS
    v = pnl.std() * np.sqrt(TRADING_DAYS)
    s = r / v if v > 0 else np.nan
    nav = (1 + pnl).cumprod()
    mdd = ((nav - nav.cummax()) / nav.cummax()).min()
    return {
        "Label": label,
        "Start": str(pnl.index[0].date()),
        "End": str(pnl.index[-1].date()),
        "Return(%)": round(r * 100, 2),
        "Vol(%)": round(v * 100, 2),
        "Sharpe": round(s, 2),
        "MaxDD(%)": round(mdd * 100, 2),
    }


# ── IC 分析 ──────────────────────────────────────────────────────────────────

def compute_factor_ic(
    returns: pd.DataFrame,
    factor_df: pd.DataFrame,
    horizon: int = 1,
) -> pd.Series:
    """计算因子与未来 horizon 日收益的截面 IC（Spearman）日序列。"""
    fwd_ret = returns.shift(-horizon)
    # 按日期对齐：factor_df.index ∩ fwd_ret.index
    common_idx = factor_df.index.intersection(fwd_ret.index)
    f = factor_df.loc[common_idx]
    r = fwd_ret.loc[common_idx]
    ic_list = []
    for date in common_idx:
        fi = f.loc[date].dropna()
        ri = r.loc[date].reindex(fi.index).dropna()
        common = fi.index.intersection(ri.index)
        if len(common) < 5:
            ic_list.append(np.nan)
        else:
            ic_list.append(fi[common].corr(ri[common], method="spearman"))
    return pd.Series(ic_list, index=common_idx, name="IC")


def icir_table(
    returns: pd.DataFrame,
    mts: MultiFactorTrendSignal,
    label: str,
    start: str = ANALYSIS_START,
) -> pd.DataFrame:
    """计算各因子 IC、ICIR（全样本及 2010+ 分年）。"""
    print(f"\n[{label}] Computing factor ICs …")
    factor_dict = mts.factor_dict(returns)

    records = []
    for fname, fdf in factor_dict.items():
        ic_series = compute_factor_ic(returns, fdf, horizon=1)
        ic_2010 = ic_series[ic_series.index >= start]
        ic_mean = ic_2010.mean()
        ic_std = ic_2010.std()
        icir = ic_mean / ic_std * np.sqrt(TRADING_DAYS) if ic_std > 0 else np.nan
        records.append({
            "Factor": fname,
            "IC_Mean(%)": round(ic_mean * 100, 3),
            "IC_Std(%)": round(ic_std * 100, 3),
            "ICIR(ann)": round(icir, 2),
            "IC_pos(%)": round((ic_2010 > 0).mean() * 100, 1),
            "N_days": len(ic_2010),
        })
    df = pd.DataFrame(records).set_index("Factor")
    return df


def icir_annual(
    returns: pd.DataFrame,
    mts: MultiFactorTrendSignal,
    factor_name: str,
    start: str = ANALYSIS_START,
) -> pd.DataFrame:
    """单因子逐年 ICIR。"""
    factor_dict = mts.factor_dict(returns)
    fdf = factor_dict[factor_name]
    ic_series = compute_factor_ic(returns, fdf, horizon=1)
    ic_2010 = ic_series[ic_series.index >= start]

    rows = []
    for year, grp in ic_2010.groupby(ic_2010.index.year):
        m = grp.mean()
        s = grp.std()
        ir = m / s * np.sqrt(TRADING_DAYS) if s > 0 else np.nan
        rows.append({
            "Year": year,
            "IC_Mean(%)": round(m * 100, 3),
            "ICIR(ann)": round(ir, 2),
            "N": len(grp),
        })
    return pd.DataFrame(rows).set_index("Year")


# ── 可视化 ────────────────────────────────────────────────────────────────────

def plot_nav_comparison(pnl_dict: dict[str, pd.Series], title: str, out_path: Path) -> None:
    fig, axes = plt.subplots(2, 1, figsize=(14, 9), gridspec_kw={"height_ratios": [3, 1]})
    ax_nav, ax_dd = axes

    for label, pnl in pnl_dict.items():
        nav = (1 + pnl).cumprod()
        nav_2010 = nav[nav.index >= ANALYSIS_START]
        nav_norm = nav_2010 / nav_2010.iloc[0]
        dd = (nav_2010 - nav_2010.cummax()) / nav_2010.cummax()
        ax_nav.plot(nav_norm.index, nav_norm.values, label=label, lw=1.5)
        ax_dd.fill_between(dd.index, dd.values, 0, alpha=0.35)
        ax_dd.plot(dd.index, dd.values, lw=0.8)

    ax_nav.set_title(title, fontsize=13)
    ax_nav.set_ylabel("Normalized NAV (2010=1)")
    ax_nav.legend(fontsize=9)
    ax_nav.grid(True, alpha=0.3)
    ax_dd.set_ylabel("Drawdown")
    ax_dd.set_xlabel("")
    ax_dd.grid(True, alpha=0.3)

    plt.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {out_path}")


def plot_annual_bar(annual_dict: dict[str, pd.DataFrame], title: str, out_path: Path) -> None:
    fig, ax = plt.subplots(figsize=(14, 5))
    labels = list(annual_dict.keys())
    colors = ["steelblue", "darkorange"]

    years = None
    for label in labels:
        df = annual_dict[label]
        yr = [y for y in df.index if isinstance(y, int)]
        if years is None or len(yr) > len(years):
            years = yr

    x = np.arange(len(years))
    w = 0.35
    for i, (label, df) in enumerate(annual_dict.items()):
        df2 = df.drop(index=[idx for idx in df.index if not isinstance(idx, int)])
        vals = [df2.loc[y, "Return(%)"] if y in df2.index else 0.0 for y in years]
        offset = (i - (len(labels) - 1) / 2) * w
        bars = ax.bar(x + offset, vals, width=w, label=label, color=colors[i % len(colors)], alpha=0.85)

    ax.axhline(0, color="black", lw=0.8)
    ax.set_xticks(x)
    ax.set_xticklabels([str(y) for y in years], rotation=45)
    ax.set_ylabel("Annual Return (%)")
    ax.set_title(title, fontsize=13)
    ax.legend()
    ax.grid(True, axis="y", alpha=0.3)
    plt.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {out_path}")


def plot_factor_icir(icir_df_dict: dict[str, pd.DataFrame], title: str, out_path: Path) -> None:
    """并排 bar 图：各因子 ICIR 对比。"""
    labels = list(icir_df_dict.keys())
    factors = list(next(iter(icir_df_dict.values())).index)
    x = np.arange(len(factors))
    w = 0.35
    colors = ["steelblue", "darkorange"]

    fig, ax = plt.subplots(figsize=(12, 5))
    for i, (label, df) in enumerate(icir_df_dict.items()):
        vals = [df.loc[f, "ICIR(ann)"] if f in df.index else 0.0 for f in factors]
        offset = (i - (len(labels) - 1) / 2) * w
        ax.bar(x + offset, vals, width=w, label=label, color=colors[i % len(colors)], alpha=0.85)

    ax.axhline(0, color="black", lw=0.8)
    ax.set_xticks(x)
    ax.set_xticklabels(factors, rotation=30, ha="right")
    ax.set_ylabel("Annualized ICIR")
    ax.set_title(title, fontsize=13)
    ax.legend()
    ax.grid(True, axis="y", alpha=0.3)
    plt.tight_layout()
    fig.savefig(out_path, dpi=150, bbox_inches="tight")
    plt.close(fig)
    print(f"  Saved: {out_path}")


# ── 主流程 ────────────────────────────────────────────────────────────────────

def main() -> None:
    print("=" * 70)
    print("时序趋势策略表现研究")
    print("=" * 70)

    # ── 1. 中国趋势策略 ─────────────────────────────────────────────────────────
    print("\n[Step 1] 中国期货：加载数据和仓位")
    cn_returns = pd.read_parquet(CN_RETURNS)
    cn_positions = pd.read_parquet(CN_POSITIONS)
    print(f"  Returns: {cn_returns.shape}, {cn_returns.index[0].date()} ~ {cn_returns.index[-1].date()}")
    print(f"  Positions: {cn_positions.shape}")

    print("\n[Step 1b] 中国：运行 VectorizedBacktest …")
    cn_pnl = run_backtest_from_positions(cn_positions, cn_returns)
    cn_annual = annual_table(cn_pnl, "China-Trend")
    cn_full = full_period_stats(cn_pnl[cn_pnl.index >= ANALYSIS_START], "China-Trend-2010+")

    print(f"\n{'='*50}")
    print("中国时序趋势策略 — 逐年表现（{} 起）".format(ANALYSIS_START[:4]))
    print(cn_annual.to_string())

    # ── 2. 全球趋势策略 ─────────────────────────────────────────────────────────
    print("\n[Step 2] 全球（中国+海外）：加载数据和趋势仓位")
    gl_returns = pd.read_parquet(GL_RETURNS)
    gl_trend_pos = pd.read_parquet(GL_TREND_POS)
    print(f"  Returns: {gl_returns.shape}, {gl_returns.index[0].date()} ~ {gl_returns.index[-1].date()}")
    print(f"  Trend positions: {gl_trend_pos.shape}")

    print("\n[Step 2b] 全球：运行 VectorizedBacktest …")
    gl_pnl = run_backtest_from_positions(gl_trend_pos, gl_returns)
    gl_annual = annual_table(gl_pnl, "Global-Trend")
    gl_full = full_period_stats(gl_pnl[gl_pnl.index >= ANALYSIS_START], "Global-Trend-2010+")

    print(f"\n{'='*50}")
    print("全球时序趋势策略 — 逐年表现（{} 起）".format(ANALYSIS_START[:4]))
    print(gl_annual.to_string())

    # ── 3. 汇总对比 ──────────────────────────────────────────────────────────────
    print(f"\n{'='*50}")
    print("策略对比（{}+）".format(ANALYSIS_START[:4]))
    summary_df = pd.DataFrame([cn_full, gl_full]).set_index("Label")
    print(summary_df.to_string())

    # ── 4. 中国因子 IC 分析 ─────────────────────────────────────────────────────
    print(f"\n[Step 3] 中国：因子 IC 分析 …")
    mts_cn = MultiFactorTrendSignal()
    cn_icir = icir_table(cn_returns, mts_cn, "China", start=ANALYSIS_START)
    print(f"\n中国期货 — 七因子 ICIR（{ANALYSIS_START[:4]}+）")
    print(cn_icir.to_string())

    # ── 5. 全球因子 IC 分析 ─────────────────────────────────────────────────────
    print(f"\n[Step 4] 全球：因子 IC 分析 …")
    mts_gl = MultiFactorTrendSignal()
    gl_icir = icir_table(gl_returns, mts_gl, "Global", start=ANALYSIS_START)
    print(f"\n全球期货 — 七因子 ICIR（{ANALYSIS_START[:4]}+）")
    print(gl_icir.to_string())

    # ── 6. 输出图表 ──────────────────────────────────────────────────────────────
    print(f"\n[Step 5] 输出图表 → {OUT_DIR}")
    plot_nav_comparison(
        {"China-Trend (TV5%)": cn_pnl, "Global-Trend (TV5%)": gl_pnl},
        title="Time-series Trend Strategy NAV (2010+, Target Vol=5%, Cost=5bps)",
        out_path=OUT_DIR / "nav_trend_comparison.png",
    )

    plot_annual_bar(
        {"China-Trend": cn_annual, "Global-Trend": gl_annual},
        title="Annual Returns: China vs Global Trend (2010+)",
        out_path=OUT_DIR / "annual_bar_comparison.png",
    )

    plot_factor_icir(
        {"China": cn_icir, "Global": gl_icir},
        title=f"Factor ICIR: China vs Global ({ANALYSIS_START[:4]}+)",
        out_path=OUT_DIR / "factor_icir_comparison.png",
    )

    # ── 7. 保存 CSV ──────────────────────────────────────────────────────────────
    cn_annual.to_csv(OUT_DIR / "cn_trend_annual.csv")
    gl_annual.to_csv(OUT_DIR / "gl_trend_annual.csv")
    cn_icir.to_csv(OUT_DIR / "cn_factor_icir.csv")
    gl_icir.to_csv(OUT_DIR / "gl_factor_icir.csv")
    summary_df.to_csv(OUT_DIR / "trend_strategy_summary.csv")
    print(f"\n所有 CSV 已保存至 {OUT_DIR}")

    # ── 8. 中国各因子逐年 ICIR ───────────────────────────────────────────────────
    print(f"\n[Step 6] 中国各因子逐年 ICIR …")
    factor_names = list(mts_cn.factor_dict(cn_returns).keys())
    annual_icir_frames = {}
    for fname in factor_names:
        df_yr = icir_annual(cn_returns, mts_cn, fname, start=ANALYSIS_START)
        annual_icir_frames[fname] = df_yr

    # 合并展示：factors 为列，年份为行
    icir_pivot = pd.DataFrame(
        {fname: df["ICIR(ann)"] for fname, df in annual_icir_frames.items()}
    )
    print("\n中国期货 — 七因子逐年 ICIR（{}+）".format(ANALYSIS_START[:4]))
    print(icir_pivot.to_string())
    icir_pivot.to_csv(OUT_DIR / "cn_factor_icir_annual.csv")

    print("\n" + "=" * 70)
    print("研究完成。输出目录:", OUT_DIR)
    print("=" * 70)


if __name__ == "__main__":
    main()
