"""Roll component layer 单资产最小接入测试。"""

from __future__ import annotations

import re

import pandas as pd

from strategies.components.roll import (
    ExecutionRuleConfig,
    GMAT3SingleAssetRollStrategy,
    LifecycleRuleConfig,
    LinearRollExecutor,
    MarketStateRuleConfig,
    RollStrategyProfile,
    SelectorRuleConfig,
    SingleAssetRollStrategy,
    build_single_asset_strategy_from_profile,
)
from strategies.implementations.gmat3 import GMAT3DataAccess


def test_single_asset_roll_strategy_synthetic_runs() -> None:
    contracts = pd.DataFrame(
        {
            "contract_id": ["RB01", "RB02"],
            "last_trade_date": pd.to_datetime(["2026-01-06", "2026-01-10"]),
            "last_holding_date": pd.to_datetime(["2026-01-06", "2026-01-10"]),
        }
    )
    dates = pd.date_range("2026-01-02", periods=6, freq="D")
    prices = pd.DataFrame(
        {
            "RB01": [100, 101, 102, 103, 104, 105],
            "RB02": [100, 100.5, 101, 101.5, 102, 102.5],
        },
        index=dates,
    )
    open_interest = pd.DataFrame(
        {
            "RB01": [1000, 1100, 900, 100, 50, 10],
            "RB02": [500, 600, 800, 1200, 1300, 1400],
        },
        index=dates,
    )

    strategy = SingleAssetRollStrategy(RollStrategyProfile(name="rb_demo", asset_key="RB"))
    result = strategy.run(
        market_data={
            "contracts": contracts,
            "prices": prices,
            "open_interest": open_interest,
        }
    )

    assert not result.contract_plan.empty
    assert not result.value_series.empty
    assert result.lookthrough_book["contract_id"].nunique() >= 1


def test_gmat3_single_asset_roll_strategy_real_data_smoke() -> None:
    access = GMAT3DataAccess()
    strategy = GMAT3SingleAssetRollStrategy(access, "CU")
    result = strategy.run_from_access(end="2005-12-31")

    assert not result.contract_plan.empty
    assert not result.value_series.empty
    assert result.value_series.index.min() >= pd.Timestamp("2005-01-04")
    assert "contract_id" in result.lookthrough_book.columns
    contract_ids = result.contract_plan["target_contract"].dropna().astype(str).unique().tolist()
    assert all(re.match(r"^CU\d{4}\.SHF$", cid) for cid in contract_ids)


def test_linear_roll_executor_produces_mixed_schedule() -> None:
    target_plan = pd.DataFrame(
        {
            "trade_date": pd.to_datetime(["2026-01-02", "2026-01-03", "2026-01-04", "2026-01-05"]),
            "current_contract": ["RB01", "RB01", "RB01", "RB02"],
            "target_contract": ["RB01", "RB02", "RB02", "RB02"],
        }
    )
    trading_days = pd.date_range("2026-01-02", periods=4, freq="D")
    schedule = LinearRollExecutor(roll_days=3).build_schedule(
        target_plan=target_plan,
        trading_calendar=trading_days,
        context={},
    )

    mixed = schedule.groupby("trade_date")["contract_id"].nunique()
    assert mixed.max() >= 2


def test_profile_driven_builder_uses_gmat3_domestic_commodity_preset() -> None:
    profile = RollStrategyProfile(
        name="cu_profile",
        asset_key="CU",
        rule_profile="gmat3_domestic_commodity",
        roll_days=3,
        lifecycle_date_field="last_holding_date",
        lifecycle_rule_config=LifecycleRuleConfig(
            kind="fixed_days_before_expiry",
            params={"roll_days": 3, "date_field": "last_holding_date"},
        ),
        market_state_rule_config=MarketStateRuleConfig(
            kind="gmat3_domestic_commodity",
            params={},
        ),
        execution_rule_config=ExecutionRuleConfig(
            kind="linear",
            params={"roll_days": 3},
        ),
        selector_rule_config=SelectorRuleConfig(
            kind="prefer_selected",
            params={},
        ),
    )
    strategy = build_single_asset_strategy_from_profile(profile)
    assert strategy.profile.asset_key == "CU"
    assert strategy.executor.__class__.__name__ == "LinearRollExecutor"
    assert strategy.selector.__class__.__name__ == "PreferSelectedContractSelector"


def test_explicit_rule_config_can_drive_builder_without_rule_profile() -> None:
    profile = RollStrategyProfile(
        name="cu_profile_config_only",
        asset_key="CU",
        rule_profile="default_single_asset",
        roll_days=3,
        lifecycle_rule_config=LifecycleRuleConfig(
            kind="fixed_days_before_expiry",
            params={"roll_days": 3, "date_field": "last_holding_date"},
        ),
        market_state_rule_config=MarketStateRuleConfig(
            kind="gmat3_domestic_commodity",
            params={},
        ),
        execution_rule_config=ExecutionRuleConfig(
            kind="linear",
            params={"roll_days": 3},
        ),
        selector_rule_config=SelectorRuleConfig(
            kind="prefer_selected",
            params={},
        ),
    )
    strategy = build_single_asset_strategy_from_profile(profile)
    assert strategy.executor.__class__.__name__ == "LinearRollExecutor"
    assert strategy.selector.__class__.__name__ == "PreferSelectedContractSelector"


def test_selector_rule_config_can_drive_default_builder_path() -> None:
    profile = RollStrategyProfile(
        name="rb_config_only",
        asset_key="RB",
        lifecycle_rule_config=LifecycleRuleConfig(
            kind="fixed_days_before_expiry",
            params={"roll_days": 3, "date_field": "last_holding_date"},
        ),
        market_state_rule_config=MarketStateRuleConfig(
            kind="field_max",
            params={"field_name": "open_interest"},
        ),
        execution_rule_config=ExecutionRuleConfig(
            kind="linear",
            params={"roll_days": 3},
        ),
        selector_rule_config=SelectorRuleConfig(
            kind="hybrid",
            params={},
        ),
    )
    strategy = build_single_asset_strategy_from_profile(profile)
    assert strategy.executor.__class__.__name__ == "LinearRollExecutor"
    assert strategy.selector.__class__.__name__ == "HybridContractSelector"
