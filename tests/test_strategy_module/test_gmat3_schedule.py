from __future__ import annotations

import pandas as pd

from data.model.calendar import TradingCalendar
from strategies.implementations.gmat3.schedule import (
    build_index_calc_days,
    build_value_matrices,
)
from strategies.implementations.gmat3.weights import WeightCalculator


def test_build_index_calc_days_and_value_matrices() -> None:
    full_days, calc_days = build_index_calc_days("2010-01-04", "2010-02-10", warmup_days=5)
    assert len(full_days) >= len(calc_days)
    assert calc_days[0] == pd.Timestamp("2010-01-04")

    s1 = pd.Series([1.0, 1.1, 1.2], index=pd.to_datetime(["2010-01-04", "2010-01-06", "2010-01-08"]))
    s2 = pd.Series([1.0, 0.9], index=pd.to_datetime(["2010-01-05", "2010-01-07"]))

    value_df_full, value_df = build_value_matrices({"A": s1, "B": s2}, full_days, calc_days)
    assert list(value_df.columns) == ["A", "B"]
    assert value_df_full.index.min() == min(full_days)
    assert value_df.index.min() == min(calc_days)


def test_weight_calculator_exposes_staggered_schedule() -> None:
    dates = pd.bdate_range("2024-01-01", "2024-03-31")
    calendar = TradingCalendar("TEST", dates)
    calc = WeightCalculator()
    schedule = calc.sub_index_schedule(calendar, "2024-01-01", "2024-03-31")
    assert len(schedule) > 0
