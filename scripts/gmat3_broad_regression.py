"""GMAT3 更大范围真实数据回归检查。

用法：
    cd /home/ubuntu/dengl/my_projects/cta_lab
    python3 scripts/gmat3_broad_regression.py
    python3 scripts/gmat3_broad_regression.py --end 2016-12-31

默认会使用比单元测试更大的 GMAT3 universe 与更长时间窗口，
对比 `cta_lab` 与旧 `ddb/gmat3` 在以下层面的结果：
1. 子组合价值矩阵 `value_df`
2. 权重矩阵 `weight_df`
3. 最终指数序列 `index_series`
"""

from __future__ import annotations

import argparse
import sys
import time
from pathlib import Path

import numpy as np
import pandas as pd


ROOT = Path(__file__).resolve().parents[2]
CTA_LAB_ROOT = ROOT / "cta_lab"
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))
if str(CTA_LAB_ROOT) not in sys.path:
    sys.path.insert(0, str(CTA_LAB_ROOT))

from strategies.implementations.gmat3.data_access import GMAT3DataAccess
from strategies.implementations.gmat3.index_builder import GMAT3IndexBuilder
from strategies.implementations.gmat3.main_contract import MainContractEngine
from strategies.implementations.gmat3.schedule import build_index_calc_days, build_value_matrices
from strategies.implementations.gmat3.sub_portfolio import SubPortfolioEngine
from strategies.implementations.gmat3.universe import BLACK_COMPONENTS, SUB_PORTFOLIOS
from strategies.implementations.gmat3.weights import WeightCalculator

from ddb.gmat3.data_loader import DataLoader as LegacyDataLoader
from ddb.gmat3.index import IndexCalculator as LegacyIndexCalculator
from ddb.gmat3.main_contract import MainContractEngine as LegacyMainContractEngine
from ddb.gmat3.sub_portfolio import SubPortfolioEngine as LegacySubPortfolioEngine
from ddb.gmat3.weight import WeightCalculator as LegacyWeightCalculator


BROAD_END = "2016-12-31"


def timer(msg: str):
    def deco(fn):
        def wrapper(*args, **kwargs):
            t0 = time.time()
            result = fn(*args, **kwargs)
            print(f"[{time.time() - t0:6.1f}s] {msg}")
            return result

        return wrapper

    return deco


def compare_frame(name: str, ours: pd.DataFrame, legacy: pd.DataFrame, atol: float = 1e-10) -> None:
    pd.testing.assert_index_equal(ours.index, legacy.index)
    pd.testing.assert_index_equal(ours.columns, legacy.columns)
    diff = (ours - legacy).abs()
    max_abs = float(np.nanmax(diff.to_numpy())) if diff.size else 0.0
    print(f"{name}: shape={ours.shape}, max_abs_diff={max_abs:.12g}")
    pd.testing.assert_frame_equal(ours, legacy, check_exact=False, atol=atol, rtol=atol)


def compare_series(name: str, ours: pd.Series, legacy: pd.Series, atol: float = 1e-10) -> None:
    pd.testing.assert_index_equal(ours.index, legacy.index)
    diff = (ours - legacy).abs()
    max_abs = float(np.nanmax(diff.to_numpy())) if len(diff) else 0.0
    print(f"{name}: len={len(ours)}, max_abs_diff={max_abs:.12g}, last={ours.iloc[-1]:.6f}")
    pd.testing.assert_series_equal(ours, legacy, check_exact=False, atol=atol, rtol=atol)


@timer("cta_lab pipeline complete")
def build_cta_lab(end_date: str):
    access = GMAT3DataAccess()
    main_engine = MainContractEngine(access)
    sub_engine = SubPortfolioEngine(access)
    weight_calc = WeightCalculator()
    index_builder = GMAT3IndexBuilder()

    single_varieties = [v for v in SUB_PORTFOLIOS if v != "BLACK"]
    main_varieties = single_varieties + list(BLACK_COMPONENTS.keys())
    main_dfs = {v: main_engine.compute(v, end=end_date) for v in main_varieties}
    val_dict = {v: sub_engine.compute(v, main_dfs) for v in SUB_PORTFOLIOS}

    full_days, calc_days = build_index_calc_days("2009-12-31", end_date)
    value_df_full, value_df = build_value_matrices(val_dict, full_days, calc_days)
    weight_df, schedule = weight_calc.compute(value_df_full, calc_days)
    adjust_date_sets = {sub_n: set(schedule[sub_n]["adjust_dates"]) for sub_n in range(1, 5)}
    index_series = index_builder.compute(
        value_df=value_df,
        weight_df=weight_df,
        index_trading_days=calc_days,
        adjust_date_sets=adjust_date_sets,
        fx_series=access.get_fx_rate(),
    )
    return value_df, weight_df, index_series


@timer("legacy ddb pipeline complete")
def build_legacy(end_date: str):
    loader = LegacyDataLoader()
    main_engine = LegacyMainContractEngine(loader)
    sub_engine = LegacySubPortfolioEngine(loader)
    weight_calc = LegacyWeightCalculator()
    index_builder = LegacyIndexCalculator()

    single_varieties = [v for v in SUB_PORTFOLIOS if v != "BLACK"]
    main_varieties = single_varieties + list(BLACK_COMPONENTS.keys())
    main_dfs = {v: main_engine.compute(v) for v in main_varieties}
    val_dict = {v: sub_engine.compute(v, main_dfs) for v in SUB_PORTFOLIOS}

    full_days, calc_days = build_index_calc_days("2009-12-31", end_date)
    value_df_full, value_df = build_value_matrices(val_dict, full_days, calc_days)
    weight_df, schedule = weight_calc.compute(value_df_full, calc_days)
    adjust_date_sets = {sub_n: set(schedule[sub_n]["adjust_dates"]) for sub_n in range(1, 5)}
    index_series = index_builder.compute(
        value_df=value_df,
        weight_df=weight_df,
        index_trading_days=calc_days,
        adjust_date_sets=adjust_date_sets,
        fx_series=loader.get_fx_rate(),
    )
    return value_df, weight_df, index_series


def main() -> int:
    parser = argparse.ArgumentParser(description="GMAT3 broader real-data regression.")
    parser.add_argument("--end", default=BROAD_END, help="Regression end date, default: 2016-12-31")
    args = parser.parse_args()

    print("GMAT3 broader regression")
    print(f"end_date={args.end}")
    print(f"sub_portfolios={list(SUB_PORTFOLIOS)}")

    ours_value, ours_weight, ours_index = build_cta_lab(args.end)
    legacy_value, legacy_weight, legacy_index = build_legacy(args.end)

    compare_frame("value_df", ours_value, legacy_value)
    compare_frame("weight_df", ours_weight, legacy_weight)
    compare_series("index_series", ours_index, legacy_index)

    print("PASS")
    return 0


if __name__ == "__main__":
    raise SystemExit(main())
