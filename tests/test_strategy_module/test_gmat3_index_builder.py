from __future__ import annotations
import pytest

import sys
from pathlib import Path

import numpy as np
import pandas as pd

from strategies.implementations.gmat3.data_access import GMAT3DataAccess
from strategies.implementations.gmat3.index_builder import GMAT3IndexBuilder
from strategies.implementations.gmat3.main_contract import MainContractEngine
from strategies.implementations.gmat3.schedule import build_index_calc_days, build_value_matrices
from strategies.implementations.gmat3.sub_portfolio import SubPortfolioEngine
from strategies.implementations.gmat3.weights import WeightCalculator


ROOT = Path("/home/ubuntu/dengl/my_projects")
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ddb.gmat3.data_loader import DataLoader as LegacyDataLoader
from ddb.gmat3.index import IndexCalculator as LegacyIndexCalculator
from ddb.gmat3.main_contract import MainContractEngine as LegacyMainContractEngine
from ddb.gmat3.sub_portfolio import SubPortfolioEngine as LegacySubPortfolioEngine
from ddb.gmat3.weight import WeightCalculator as LegacyWeightCalculator


def _build_cta_lab_inputs():
    access = GMAT3DataAccess()
    main_engine = MainContractEngine(access)
    sub_engine = SubPortfolioEngine(access)
    main_dfs = {v: main_engine.compute(v, end="2011-06-30") for v in ("IF", "ES", "AU")}
    value_series = {v: sub_engine.compute(v, main_dfs) for v in ("IF", "ES", "AU")}
    full_days, calc_days = build_index_calc_days("2009-12-31", "2011-06-30")
    value_df_full, value_df = build_value_matrices(value_series, full_days, calc_days)
    weight_df, schedule = WeightCalculator().compute(value_df_full, calc_days)
    adjust_date_sets = {sub_n: set(schedule[sub_n]["adjust_dates"]) for sub_n in range(1, 5)}
    fx_series = access.get_fx_rate()
    return value_df, weight_df, calc_days, adjust_date_sets, fx_series


def _build_legacy_inputs():
    loader = LegacyDataLoader()
    main_engine = LegacyMainContractEngine(loader)
    sub_engine = LegacySubPortfolioEngine(loader)
    main_dfs = {v: main_engine.compute(v) for v in ("IF", "ES", "AU")}
    value_series = {v: sub_engine.compute(v, main_dfs) for v in ("IF", "ES", "AU")}
    full_days, calc_days = build_index_calc_days("2009-12-31", "2011-06-30")
    value_df_full, value_df = build_value_matrices(value_series, full_days, calc_days)
    weight_df, schedule = LegacyWeightCalculator().compute(value_df_full, calc_days)
    adjust_date_sets = {sub_n: set(schedule[sub_n]["adjust_dates"]) for sub_n in range(1, 5)}
    fx_series = loader.get_fx_rate()
    return value_df, weight_df, calc_days, adjust_date_sets, fx_series


@pytest.mark.skip(reason="Legacy ddb/data/raw/ removed after data reorganization; migration G1-G6 complete")
def test_index_builder_matches_legacy_on_small_window() -> None:
    value_df, weight_df, calc_days, adjust_date_sets, fx_series = _build_cta_lab_inputs()
    legacy_value_df, legacy_weight_df, legacy_calc_days, legacy_adjust_date_sets, legacy_fx_series = _build_legacy_inputs()

    ours = GMAT3IndexBuilder().compute(
        value_df=value_df,
        weight_df=weight_df,
        index_trading_days=calc_days,
        adjust_date_sets=adjust_date_sets,
        fx_series=fx_series,
    )
    legacy = LegacyIndexCalculator().compute(
        value_df=legacy_value_df,
        weight_df=legacy_weight_df,
        index_trading_days=legacy_calc_days,
        adjust_date_sets=legacy_adjust_date_sets,
        fx_series=legacy_fx_series,
    )

    pd.testing.assert_index_equal(ours.index, legacy.index)
    pd.testing.assert_series_equal(ours, legacy, check_exact=False, atol=1e-10, rtol=1e-10)


def test_index_builder_outputs_finite_series() -> None:
    value_df, weight_df, calc_days, adjust_date_sets, fx_series = _build_cta_lab_inputs()
    series = GMAT3IndexBuilder().compute(
        value_df=value_df,
        weight_df=weight_df,
        index_trading_days=calc_days,
        adjust_date_sets=adjust_date_sets,
        fx_series=fx_series,
    )

    assert not series.empty
    assert series.index[0] == calc_days[0]
    assert np.isfinite(series.to_numpy()).all()
    assert series.name == "GMAT3"
