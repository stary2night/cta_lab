"""Roll component layer bundle 资产最小测试。"""

from __future__ import annotations

import pandas as pd

from strategies.components.roll import (
    BundleRollStrategy,
    BundleRule,
    RollComponentProfile,
    RollStrategyProfile,
    RollStrategyResult,
    build_gmat3_black_bundle_market_data,
    build_gmat3_black_bundle_profile,
    compute_gmat3_black_component_target_weights,
    run_gmat3_black_bundle,
)


def _make_component_result(
    name: str,
    values: list[float],
    contracts: list[str],
    dates: pd.DatetimeIndex,
) -> RollStrategyResult:
    return RollStrategyResult(
        value_series=pd.Series(values, index=dates, name=name),
        lookthrough_book=pd.DataFrame(
            {"trade_date": dates, "contract_id": contracts, "weight": [1.0] * len(dates)}
        ),
    )


class _FakeBlackAccess:
    def __init__(self, dates: pd.DatetimeIndex) -> None:
        self._dates = pd.DatetimeIndex(dates)

    def get_daily(self, variety: str) -> pd.DataFrame:
        base = {"RB": 120.0, "HC": 100.0, "I": 80.0, "J": 60.0, "JM": 40.0}[variety]
        return pd.DataFrame(
            {
                "trade_date": self._dates,
                "settle_price": [base] * len(self._dates),
                "open_interest": [1000.0 + i for i in range(len(self._dates))],
            }
        )


def test_bundle_roll_strategy_static_weights_runs() -> None:
    dates = pd.date_range("2026-01-02", periods=4, freq="D")
    rb_result = _make_component_result("rb", [1.0, 1.01, 1.03, 1.02], ["RB2601"] * 4, dates)
    hc_result = _make_component_result("hc", [1.0, 1.0, 1.01, 1.015], ["HC2601"] * 4, dates)

    profile = RollStrategyProfile(
        name="rb_hc_bundle",
        asset_key="RB_HC_BUNDLE",
        asset_mode="bundle",
        components=[
            RollComponentProfile(component_key="rb", symbol="RB"),
            RollComponentProfile(component_key="hc", symbol="HC"),
        ],
        bundle_rule=BundleRule(
            weight_mode="static",
            static_weights={"rb": 0.6, "hc": 0.4},
        ),
    )
    strategy = BundleRollStrategy(profile)
    result = strategy.run(market_data={"component_results": {"rb": rb_result, "hc": hc_result}})

    assert not result.value_series.empty
    assert list(result.component_values.columns) == ["rb", "hc"]
    assert set(result.component_weights.columns) == {"rb", "hc"}
    assert result.component_weights.iloc[0]["rb"] == 0.6
    assert result.component_weights.iloc[0]["hc"] == 0.4
    assert "component_key" in result.lookthrough_book.columns
    assert "bundle_weight" in result.lookthrough_book.columns


def test_bundle_roll_strategy_equal_weights_runs() -> None:
    dates = pd.date_range("2026-01-02", periods=3, freq="D")
    leg_a = _make_component_result("a", [1.0, 1.01, 1.02], ["A1", "A1", "A2"], dates)
    leg_b = _make_component_result("b", [1.0, 0.99, 1.00], ["B1", "B1", "B2"], dates)

    profile = RollStrategyProfile(
        name="equal_bundle",
        asset_key="EQ_BUNDLE",
        asset_mode="bundle",
        components=[
            RollComponentProfile(component_key="a", symbol="A"),
            RollComponentProfile(component_key="b", symbol="B"),
        ],
        bundle_rule=BundleRule(weight_mode="equal"),
    )
    strategy = BundleRollStrategy(profile)
    result = strategy.run(market_data={"component_results": {"a": leg_a, "b": leg_b}})

    assert (result.component_weights.iloc[0] == 0.5).all()
    assert result.value_series.iloc[0] == 1.0


def test_bundle_roll_strategy_external_weights_runs() -> None:
    dates = pd.date_range("2026-01-02", periods=4, freq="D")
    leg_a = _make_component_result("a", [1.0, 1.01, 1.02, 1.03], ["A1"] * 4, dates)
    leg_b = _make_component_result("b", [1.0, 1.00, 0.99, 1.00], ["B1"] * 4, dates)
    target_weights = pd.DataFrame(
        {
            "a": [0.8, 0.8, 0.3, 0.3],
            "b": [0.2, 0.2, 0.7, 0.7],
        },
        index=dates,
    )

    profile = RollStrategyProfile(
        name="external_bundle",
        asset_key="EXT_BUNDLE",
        asset_mode="bundle",
        components=[
            RollComponentProfile(component_key="a", symbol="A"),
            RollComponentProfile(component_key="b", symbol="B"),
        ],
        bundle_rule=BundleRule(weight_mode="external"),
    )
    strategy = BundleRollStrategy(profile)
    result = strategy.run(
        market_data={
            "component_results": {"a": leg_a, "b": leg_b},
            "component_target_weights": target_weights,
        }
    )

    assert result.component_weights.loc[dates[0], "a"] == 0.8
    assert result.component_weights.loc[dates[2], "a"] == 0.3
    assert result.component_weights.loc[dates[2], "b"] == 0.7


def test_bundle_roll_strategy_external_weights_support_smoothing() -> None:
    dates = pd.to_datetime(
        ["2026-01-30", "2026-02-02", "2026-02-03", "2026-02-04", "2026-02-05"]
    )
    leg_a = _make_component_result("a", [1.0, 1.01, 1.02, 1.03, 1.04], ["A1"] * 5, dates)
    leg_b = _make_component_result("b", [1.0, 1.0, 1.0, 1.0, 1.0], ["B1"] * 5, dates)
    target_weights = pd.DataFrame(
        {
            "a": [0.8, 0.2, 0.2, 0.2, 0.2],
            "b": [0.2, 0.8, 0.8, 0.8, 0.8],
        },
        index=dates,
    )

    profile = RollStrategyProfile(
        name="smooth_bundle",
        asset_key="SMOOTH_BUNDLE",
        asset_mode="bundle",
        components=[
            RollComponentProfile(component_key="a", symbol="A"),
            RollComponentProfile(component_key="b", symbol="B"),
        ],
        bundle_rule=BundleRule(
            weight_mode="external",
            rebalance_frequency="monthly",
            smoothing_window=2,
        ),
    )
    strategy = BundleRollStrategy(profile)
    result = strategy.run(
        market_data={
            "component_results": {"a": leg_a, "b": leg_b},
            "component_target_weights": target_weights,
        }
    )

    assert result.component_weights.loc[dates[0], "a"] == 0.8
    assert 0.2 < result.component_weights.loc[dates[1], "a"] < 0.8
    assert result.component_weights.loc[dates[2], "a"] == 0.2


def test_bundle_roll_strategy_rebalance_sync_annotations() -> None:
    dates = pd.to_datetime(["2026-01-30", "2026-02-02", "2026-02-03"])
    leg_a = _make_component_result("a", [1.0, 1.01, 1.02], ["A1", "A1", "A2"], dates)
    leg_b = _make_component_result("b", [1.0, 1.00, 1.01], ["B1", "B1", "B2"], dates)
    leg_a.roll_schedule = pd.DataFrame(
        {"trade_date": dates, "contract_id": ["A1", "A1", "A2"], "weight": [1.0, 1.0, 1.0]}
    )
    leg_b.roll_schedule = pd.DataFrame(
        {"trade_date": dates, "contract_id": ["B1", "B1", "B2"], "weight": [1.0, 1.0, 1.0]}
    )

    profile = RollStrategyProfile(
        name="sync_bundle",
        asset_key="SYNC_BUNDLE",
        asset_mode="bundle",
        components=[
            RollComponentProfile(component_key="a", symbol="A"),
            RollComponentProfile(component_key="b", symbol="B"),
        ],
        bundle_rule=BundleRule(
            weight_mode="equal",
            sync_mode="rebalance",
            sync_frequency="monthly",
            sync_components=["a", "b"],
        ),
    )
    strategy = BundleRollStrategy(profile)
    result = strategy.run(market_data={"component_results": {"a": leg_a, "b": leg_b}})

    assert "bundle_sync_trigger" in result.roll_schedule.columns
    assert "bundle_sync_mode" in result.roll_schedule.columns
    feb_mask = result.roll_schedule["trade_date"] == pd.Timestamp("2026-02-02")
    assert result.roll_schedule.loc[feb_mask, "bundle_sync_trigger"].all()
    jan_mask = result.roll_schedule["trade_date"] == pd.Timestamp("2026-01-30")
    assert result.roll_schedule.loc[jan_mask, "bundle_sync_trigger"].all()


def test_bundle_rule_rejects_unknown_sync_components() -> None:
    try:
        RollStrategyProfile(
            name="invalid_sync_components",
            asset_key="INVALID",
            asset_mode="bundle",
            components=[RollComponentProfile(component_key="a", symbol="A")],
            bundle_rule=BundleRule(
                weight_mode="equal",
                sync_mode="rebalance",
                sync_components=["missing"],
            ),
        )
    except ValueError as exc:
        assert "unknown components" in str(exc)
    else:
        raise AssertionError("expected ValueError for unknown sync_components")


def test_gmat3_black_bundle_profile_maps_real_structure() -> None:
    profile = build_gmat3_black_bundle_profile()
    assert profile.asset_key == "BLACK"
    assert profile.asset_mode == "bundle"
    assert profile.bundle_rule is not None
    assert profile.bundle_rule.weight_mode == "external"
    assert profile.bundle_rule.sync_mode == "rebalance"
    assert len(profile.components) == 5


def test_compute_gmat3_black_component_target_weights_runs() -> None:
    dates = pd.bdate_range("2024-01-02", periods=380)
    component_results = {
        key.lower(): _make_component_result(
            key.lower(),
            [1.0 + 0.001 * i for i in range(len(dates))],
            [f"{key}2601"] * len(dates),
            dates,
        )
        for key in ["RB", "HC", "I", "J", "JM"]
    }
    access = _FakeBlackAccess(dates)

    weights, rebalance_dates = compute_gmat3_black_component_target_weights(access, component_results)

    assert list(weights.columns) == ["rb", "hc", "i", "j", "jm"]
    assert not weights.empty
    assert (weights.sum(axis=1).round(10) == 1.0).all()
    assert len(rebalance_dates) >= 1


def test_run_gmat3_black_bundle_uses_existing_bundle_strategy_path() -> None:
    dates = pd.bdate_range("2024-01-02", periods=380)
    component_results: dict[str, RollStrategyResult] = {}
    for key in ["RB", "HC", "I", "J", "JM"]:
        lower = key.lower()
        result = _make_component_result(
            lower,
            [1.0 + 0.001 * i for i in range(len(dates))],
            [f"{key}2601"] * len(dates),
            dates,
        )
        result.roll_schedule = pd.DataFrame(
            {"trade_date": dates, "contract_id": [f"{key}2601"] * len(dates), "weight": [1.0] * len(dates)}
        )
        component_results[lower] = result

    access = _FakeBlackAccess(dates)
    market_data = build_gmat3_black_bundle_market_data(access, component_results)
    assert "component_target_weights" in market_data
    assert "bundle_sync_dates" in market_data

    result = run_gmat3_black_bundle(access, component_results)
    assert not result.value_series.empty
    assert set(result.component_weights.columns) == {"rb", "hc", "i", "j", "jm"}
    assert "bundle_sync_trigger" in result.roll_schedule.columns
