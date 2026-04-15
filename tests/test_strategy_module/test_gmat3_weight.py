from __future__ import annotations
import pytest

import sys
from pathlib import Path

import numpy as np
import pandas as pd

from strategies.implementations.gmat3.data_access import GMAT3DataAccess
from strategies.implementations.gmat3.main_contract import MainContractEngine
from strategies.implementations.gmat3.schedule import build_index_calc_days, build_value_matrices
from strategies.implementations.gmat3.sub_portfolio import SubPortfolioEngine
from strategies.implementations.gmat3.weights import WeightCalculator


ROOT = Path("/home/ubuntu/dengl/my_projects")
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ddb.gmat3.data_loader import DataLoader as LegacyDataLoader
from ddb.gmat3.main_contract import MainContractEngine as LegacyMainContractEngine
from ddb.gmat3.sub_portfolio import SubPortfolioEngine as LegacySubPortfolioEngine
from ddb.gmat3.weight import WeightCalculator as LegacyWeightCalculator


def _build_value_df_full() -> tuple[pd.DataFrame, list[pd.Timestamp]]:
    access = GMAT3DataAccess()
    main_engine = MainContractEngine(access)
    sub_engine = SubPortfolioEngine(access)

    main_dfs = {
        variety: main_engine.compute(variety, end="2011-06-30")
        for variety in ("IF", "ES", "AU")
    }
    value_series = {
        variety: sub_engine.compute(variety, main_dfs)
        for variety in ("IF", "ES", "AU")
    }
    full_days, calc_days = build_index_calc_days("2009-12-31", "2011-06-30")
    value_df_full, _ = build_value_matrices(value_series, full_days, calc_days)
    return value_df_full, calc_days


def _build_legacy_value_df_full() -> tuple[pd.DataFrame, list[pd.Timestamp]]:
    loader = LegacyDataLoader()
    main_engine = LegacyMainContractEngine(loader)
    sub_engine = LegacySubPortfolioEngine(loader)

    main_dfs = {variety: main_engine.compute(variety) for variety in ("IF", "ES", "AU")}
    value_series = {variety: sub_engine.compute(variety, main_dfs) for variety in ("IF", "ES", "AU")}

    full_days, calc_days = build_index_calc_days("2009-12-31", "2011-06-30")
    value_df_full, _ = build_value_matrices(value_series, full_days, calc_days)
    return value_df_full, calc_days


@pytest.mark.skip(reason="Legacy ddb/data/raw/ removed after data reorganization; migration G1-G6 complete")
def test_weight_calculator_matches_legacy_on_small_window() -> None:
    value_df_full, calc_days = _build_value_df_full()
    legacy_value_df_full, legacy_calc_days = _build_legacy_value_df_full()

    ours, our_schedule = WeightCalculator().compute(value_df_full, calc_days)
    legacy, legacy_schedule = LegacyWeightCalculator().compute(legacy_value_df_full, legacy_calc_days)

    pd.testing.assert_index_equal(ours.index, legacy.index)
    pd.testing.assert_index_equal(ours.columns, legacy.columns)
    pd.testing.assert_frame_equal(ours, legacy, check_exact=False, atol=1e-10, rtol=1e-10)
    assert our_schedule == legacy_schedule


def test_weight_calculator_outputs_stable_weight_matrix() -> None:
    value_df_full, calc_days = _build_value_df_full()
    weights, schedule = WeightCalculator().compute(value_df_full, calc_days)

    assert not weights.empty
    assert list(weights.columns) == ["AU", "ES", "IF"]
    assert weights.index[0] == calc_days[0]
    assert all(set(item.keys()) == {"calc_dates", "adjust_dates"} for item in schedule.values())
    assert np.isfinite(weights.fillna(0.0).to_numpy()).all()
