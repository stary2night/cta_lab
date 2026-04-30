"""run_intraday_mom.py：日内时序动量策略端到端运行脚本。

用法
----
    cd /home/ubuntu/dengl/my_projects
    python cta_lab/scripts/run_intraday_mom.py

分阶段输出
----------
1. 信号统计：逐品种首/尾时段相关系数、命中率（复现论文表格）
2. 组合回测：等权信号组合 NAV / Sharpe / 最大回撤（论文表6格式）
3. 品种横截面比较：金属 vs 农产品信号强度对比
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

# ── 确保从项目根目录的 cta_lab 包可以被导入 ────────────────────────────────────
_ROOT = Path(__file__).resolve().parents[2]   # .../my_projects
sys.path.insert(0, str(_ROOT / "cta_lab"))


def main() -> None:
    from strategies.implementations.intraday_mom_backtest import (
        IntradayMomConfig,
        IntradayMomStrategy,
    )

    DATA_DIR = _ROOT / "market_data" / "kline" / "china_minute"

    # ──────────────────────────────────────────────────────────────────────────
    # 实验 A：论文原始4品种，2023–2025
    # ──────────────────────────────────────────────────────────────────────────
    print("\n" + "=" * 65)
    print("实验 A：论文原始4品种  CU/RB/M/A  (2023–2025)")
    print("=" * 65)

    cfg_paper = IntradayMomConfig(
        symbols=["CU", "RB", "M", "A"],
        first_period_minutes=30,
        last_period_minutes=30,
        fee_rate=0.0,          # 先看扣费前原始信号
        vol_target=None,       # 关闭 vol-targeting，直接看等权 PnL
        trading_days=242,
    )
    strat_paper = IntradayMomStrategy(config=cfg_paper, data_dir=DATA_DIR)
    result_a = strat_paper.run_pipeline(start="2023-01-01", end="2025-12-31")

    print("\n── 逐品种信号统计 ───────────────────────────────────────────────")
    per_sym = result_a.per_symbol_stats()
    if not per_sym.empty:
        print(per_sym.to_string(float_format="{:.4f}".format))

    print("\n── 组合绩效（等权，无费）─────────────────────────────────────────")
    summary_a = result_a.summary()
    for k, v in summary_a.items():
        print(f"  {k:20s}: {v}")

    # ──────────────────────────────────────────────────────────────────────────
    # 实验 B：加费用测试（3bps 单边）
    # ──────────────────────────────────────────────────────────────────────────
    print("\n" + "=" * 65)
    print("实验 B：同品种，加 3bps 单边费用")
    print("=" * 65)

    cfg_fee = IntradayMomConfig(
        symbols=["CU", "RB", "M", "A"],
        first_period_minutes=30,
        last_period_minutes=30,
        fee_rate=0.0003,
        vol_target=None,
        trading_days=242,
    )
    strat_fee = IntradayMomStrategy(config=cfg_fee, data_dir=DATA_DIR)
    result_b = strat_fee.run_pipeline(
        start="2023-01-01", end="2025-12-31", verbose=False
    )
    print(f"  [扣费后] ", end="")
    _print_summary(result_b.pnl, "IntradayMom-3bps")

    # ──────────────────────────────────────────────────────────────────────────
    # 实验 C：扩大品种宇宙（所有可用品种，2023–2025）
    # ──────────────────────────────────────────────────────────────────────────
    print("\n" + "=" * 65)
    print("实验 C：全品种宇宙（自动加载所有品种，2023–2025）")
    print("=" * 65)

    cfg_full = IntradayMomConfig(
        symbols=[],             # 空 = 全部
        first_period_minutes=30,
        last_period_minutes=30,
        fee_rate=0.0,
        vol_target=None,
        trading_days=242,
        min_obs=20,
    )
    strat_full = IntradayMomStrategy(config=cfg_full, data_dir=DATA_DIR)
    result_c = strat_full.run_pipeline(
        start="2023-01-01", end="2025-12-31", verbose=True
    )

    print("\n── 全品种逐品种统计（按年化收益排序）─────────────────────────────")
    per_sym_full = result_c.per_symbol_stats()
    if not per_sym_full.empty:
        # 分板块显示
        from data.universe.sectors import get_sector_map  # noqa: F401

        sector_map = {
            "CU": "Metal", "AL": "Metal", "ZN": "Metal", "NI": "Metal",
            "PB": "Metal", "SN": "Metal", "AU": "Metal", "AG": "Metal",
            "RB": "Ferrous", "HC": "Ferrous", "I": "Ferrous",
            "J": "Ferrous", "JM": "Ferrous",
            "M": "Agri", "A": "Agri", "C": "Agri", "CS": "Agri",
            "Y": "Agri", "P": "Agri", "SR": "Agri", "CF": "Agri",
            "OI": "Agri", "RM": "Agri", "JD": "Agri",
            "MA": "Chemical", "TA": "Chemical", "L": "Chemical",
            "PP": "Chemical", "V": "Chemical", "EG": "Chemical",
            "EB": "Chemical", "FG": "Chemical", "BU": "Chemical",
            "RU": "Chemical",
            "IF": "Equity", "IC": "Equity", "IH": "Equity", "IM": "Equity",
            "FU": "Energy", "SC": "Energy",
        }
        per_sym_full["sector"] = per_sym_full.index.map(
            lambda s: sector_map.get(s, "Other")
        )
        cols_show = ["n_days", "hit_rate", "ann_return", "corr_first_last", "sector"]
        available_cols = [c for c in cols_show if c in per_sym_full.columns]
        print(per_sym_full[available_cols].to_string(float_format="{:.4f}".format))

    print("\n── 全品种组合绩效（等权，无费）──────────────────────────────────────")
    summary_c = result_c.summary()
    for k, v in summary_c.items():
        print(f"  {k:20s}: {v}")

    # ──────────────────────────────────────────────────────────────────────────
    # 实验 D：板块对比（金属 vs 农产品）
    # ──────────────────────────────────────────────────────────────────────────
    _sector_comparison(result_c)

    print("\n✓ 全部实验完成。")


# ── 辅助函数 ──────────────────────────────────────────────────────────────────

def _print_summary(pnl: pd.Series, label: str, td: int = 242) -> None:
    if pnl.empty:
        print(f"  [{label}] No PnL.")
        return
    ann_r = pnl.mean() * td
    ann_v = pnl.std() * np.sqrt(td)
    sharpe = ann_r / ann_v if ann_v > 0 else float("nan")
    nav = (1 + pnl).cumprod()
    mdd = ((nav - nav.cummax()) / nav.cummax()).min()
    hit = (pnl > 0).mean()
    print(
        f"[{label}]  Sharpe={sharpe:.3f}  Return={ann_r * 100:.1f}%  "
        f"Vol={ann_v * 100:.1f}%  MaxDD={mdd * 100:.1f}%  Hit={hit:.1%}"
    )


def _sector_comparison(result: "IntradayMomRunResult") -> None:  # noqa: F821
    """按板块分组计算平均 corr(r_first, r_last) 和命中率。"""
    print("\n" + "=" * 65)
    print("板块对比：首/尾时段相关系数")
    print("=" * 65)

    metal = ["CU", "AL", "ZN", "NI", "PB", "SN", "AU", "AG"]
    ferrous = ["RB", "HC", "I", "J", "JM"]
    agri = ["M", "A", "C", "CS", "Y", "P", "SR", "CF", "OI", "RM", "JD"]
    chem = ["MA", "TA", "L", "PP", "V", "EG", "EB", "BU", "RU"]

    sectors = {
        "Metal": metal,
        "Ferrous": ferrous,
        "Agri": agri,
        "Chemical": chem,
    }

    first = result.first_ret
    last = result.last_ret
    avail_cols = set(first.columns)

    for sector_name, syms in sectors.items():
        syms_avail = [s for s in syms if s in avail_cols]
        if not syms_avail:
            continue
        corrs = [
            first[s].corr(last[s])
            for s in syms_avail
            if first[s].notna().sum() > 20
        ]
        hit_rates = []
        for s in syms_avail:
            sig = np.sign(first[s]).replace(0, np.nan)
            pnl_s = sig * last[s]
            pnl_valid = pnl_s.dropna()
            if len(pnl_valid) > 20:
                hit_rates.append((pnl_valid > 0).mean())

        avg_corr = np.nanmean(corrs) if corrs else float("nan")
        avg_hit = np.nanmean(hit_rates) if hit_rates else float("nan")
        print(
            f"  {sector_name:10s}: n={len(syms_avail):2d}  "
            f"avg corr(r_first, r_last)={avg_corr:+.4f}  "
            f"avg hit_rate={avg_hit:.3f}"
        )


if __name__ == "__main__":
    main()
