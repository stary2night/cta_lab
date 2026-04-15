from __future__ import annotations

import pandas as pd

from strategies.implementations.gmat3 import GMAT3Strategy
from strategies.implementations.gmat3.config import build_gmat3_config


def test_build_gmat3_config_supports_overrides() -> None:
    cfg = build_gmat3_config({"base_risk": 0.2, "n_sub_portfolios": 5})
    assert cfg["base_risk"] == 0.2
    assert cfg["n_sub_portfolios"] == 5
    assert cfg["waf_target"] == 0.040


def test_gmat3_strategy_build_weights_returns_dataframe() -> None:
    idx = pd.date_range("2024-01-01", periods=5, freq="B")
    signal_df = pd.DataFrame(
        {
            "ES": [1.0, 1.0, 0.0, -1.0, -1.0],
            "TY": [0.0, 1.0, 1.0, 1.0, 0.0],
            "GC": [1.0, 0.0, -1.0, -1.0, 1.0],
        },
        index=idx,
    )
    vol_df = pd.DataFrame(
        {
            "ES": [0.20, 0.21, 0.22, 0.23, 0.24],
            "TY": [0.10, 0.10, 0.11, 0.11, 0.12],
            "GC": [0.15, 0.16, 0.17, 0.18, 0.19],
        },
        index=idx,
    )

    strat = GMAT3Strategy()
    weights = strat.build_weights(signal_df, vol_df)

    assert isinstance(weights, pd.DataFrame)
    assert list(weights.columns) == list(signal_df.columns)
    assert list(weights.index) == list(signal_df.index)


def test_gmat3_strategy_run_pipeline_returns_artifacts() -> None:
    strat = GMAT3Strategy()
    result = strat.run_pipeline(
        end_date="2011-06-30",
        sub_portfolios=["IF", "ES", "AU"],
    )

    assert list(result.value_df.columns) == ["AU", "ES", "IF"]
    assert list(result.weight_df.columns) == ["AU", "ES", "IF"]
    assert result.index_series.name == "GMAT3"
    assert result.index_series.index[0] == pd.Timestamp("2009-12-31")
    assert set(result.main_dfs) == {"IF", "ES", "AU"}
    assert set(result.sub_portfolio_values) == {"IF", "ES", "AU"}


def test_gmat3_strategy_run_aliases_pipeline() -> None:
    strat = GMAT3Strategy()
    result = strat.run(
        end_date="2010-06-30",
        sub_portfolios=["IF", "ES"],
    )

    assert result.index_series.index[-1] == pd.Timestamp("2010-06-30")
    assert list(result.value_df.columns) == ["ES", "IF"]
