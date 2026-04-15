from __future__ import annotations
import pytest

import sys
from pathlib import Path

import pandas as pd

from strategies.implementations.gmat3 import GMAT3DataAccess, MainContractEngine, SubPortfolioEngine


ROOT = Path("/home/ubuntu/dengl/my_projects")
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ddb.gmat3.data_loader import DataLoader as LegacyDataLoader  # noqa: E402
from ddb.gmat3.main_contract import MainContractEngine as LegacyMainContractEngine  # noqa: E402
from ddb.gmat3.sub_portfolio import SubPortfolioEngine as LegacySubPortfolioEngine  # noqa: E402


@pytest.mark.skip(reason="Legacy ddb/data/raw/ removed after data reorganization; migration G1-G6 complete")
def test_sub_portfolio_matches_legacy_for_single_asset_varieties() -> None:
    access = GMAT3DataAccess()
    main_engine = MainContractEngine(access)
    sub_engine = SubPortfolioEngine(access)

    legacy_loader = LegacyDataLoader()
    legacy_main_engine = LegacyMainContractEngine(legacy_loader)
    legacy_sub_engine = LegacySubPortfolioEngine(legacy_loader)

    for variety, end in {
        "IF": "2010-12-31",
        "TF": "2014-06-30",
        "ES": "2007-06-30",
    }.items():
        main_df = main_engine.compute(variety, end=end)
        legacy_main_df = legacy_main_engine.compute(variety)
        legacy_main_df = legacy_main_df[legacy_main_df["trade_date"] <= pd.Timestamp(end)]

        got = sub_engine.compute(variety, {variety: main_df})
        expected = legacy_sub_engine.compute(variety, {variety: legacy_main_df})
        expected = expected[expected.index <= pd.Timestamp(end)]

        pd.testing.assert_series_equal(got, expected, check_dtype=False)


@pytest.mark.skip(reason="Legacy ddb/data/raw/ removed after data reorganization; migration G1-G6 complete")
def test_black_sub_portfolio_matches_legacy_on_short_window() -> None:
    access = GMAT3DataAccess()
    main_engine = MainContractEngine(access)
    sub_engine = SubPortfolioEngine(access)

    legacy_loader = LegacyDataLoader()
    legacy_main_engine = LegacyMainContractEngine(legacy_loader)
    legacy_sub_engine = LegacySubPortfolioEngine(legacy_loader)

    end = "2016-06-30"
    comps = ["RB", "HC", "I", "J", "JM"]

    main_dfs = {comp: main_engine.compute(comp, end=end) for comp in comps}
    legacy_main_dfs = {}
    for comp in comps:
        df = legacy_main_engine.compute(comp)
        legacy_main_dfs[comp] = df[df["trade_date"] <= pd.Timestamp(end)]

    got = sub_engine.compute("BLACK", main_dfs)
    expected = legacy_sub_engine.compute("BLACK", legacy_main_dfs)
    expected = expected[expected.index <= pd.Timestamp(end)]

    pd.testing.assert_series_equal(got, expected, check_dtype=False)
