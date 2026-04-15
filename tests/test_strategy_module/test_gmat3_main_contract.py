from __future__ import annotations
import pytest

import sys
from pathlib import Path

import pandas as pd

from strategies.implementations.gmat3 import GMAT3DataAccess, MainContractEngine, RollReturnCalculator


ROOT = Path("/home/ubuntu/dengl/my_projects")
if str(ROOT) not in sys.path:
    sys.path.insert(0, str(ROOT))

from ddb.gmat3.data_loader import DataLoader as LegacyDataLoader  # noqa: E402
from ddb.gmat3.main_contract import MainContractEngine as LegacyMainContractEngine  # noqa: E402
from ddb.gmat3.roll_return import RollReturnCalculator as LegacyRollReturnCalculator  # noqa: E402


@pytest.mark.skip(reason="Legacy ddb/data/raw/ removed after data reorganization; migration G1-G6 complete")
def test_main_contract_matches_legacy_for_representative_varieties() -> None:
    access = GMAT3DataAccess()
    engine = MainContractEngine(access)

    legacy_loader = LegacyDataLoader()
    legacy_engine = LegacyMainContractEngine(legacy_loader)

    end_dates = {
        "IF": "2010-12-31",
        "TF": "2014-06-30",
        "RB": "2009-12-31",
        "ES": "2007-06-30",
        "LCO": "2007-06-30",
    }

    for variety, end in end_dates.items():
        got = engine.compute(variety, end=end).reset_index(drop=True)
        expected = legacy_engine.compute(variety)
        expected = expected[expected["trade_date"] <= pd.Timestamp(end)].reset_index(drop=True)
        pd.testing.assert_frame_equal(got, expected, check_dtype=False)


@pytest.mark.skip(reason="Legacy ddb/data/raw/ removed after data reorganization; migration G1-G6 complete")
def test_roll_return_matches_legacy_for_if() -> None:
    access = GMAT3DataAccess()
    engine = MainContractEngine(access)
    calc = RollReturnCalculator(access)

    legacy_loader = LegacyDataLoader()
    legacy_engine = LegacyMainContractEngine(legacy_loader)
    legacy_calc = LegacyRollReturnCalculator(legacy_loader)

    main_df = engine.compute("IF", end="2010-12-31")
    legacy_main_df = legacy_engine.compute("IF")
    legacy_main_df = legacy_main_df[legacy_main_df["trade_date"] <= pd.Timestamp("2010-12-31")]

    got = calc.compute("IF", main_df).dropna()
    expected = legacy_calc.compute("IF", legacy_main_df).dropna()
    pd.testing.assert_series_equal(got, expected, check_dtype=False)
