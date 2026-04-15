"""GMAT3 单资产到 Roll Strategy Layer 的最小接入。

当前阶段明确只处理 generic contract，不处理 `CU.SHF`、`CU00.SHF`、
`CU01.SHF` 这类连续链 / alias contract。
"""

from __future__ import annotations

from typing import Any

import pandas as pd

from ...implementations.gmat3.data_access import GMAT3DataAccess
from ...implementations.gmat3.universe import BLACK_COMPONENTS, ROLL_PARAMS, SUB_PORTFOLIOS
from .profile import (
    ExecutionRuleConfig,
    LifecycleRuleConfig,
    MarketStateRuleConfig,
    RollStrategyProfile,
    SelectorRuleConfig,
)
from .presets import build_single_asset_strategy_from_profile
from .single_asset import SingleAssetRollStrategy


def _get_variety_cfg(variety: str) -> dict[str, Any]:
    cfg = SUB_PORTFOLIOS.get(variety) or BLACK_COMPONENTS.get(variety)
    if cfg is None:
        raise ValueError(f"Unknown GMAT3 variety: {variety}")
    return cfg


def build_gmat3_single_asset_market_data(
    access: GMAT3DataAccess,
    variety: str,
    *,
    start: str | pd.Timestamp | None = None,
    end: str | pd.Timestamp | None = None,
) -> dict[str, Any]:
    """把 GMAT3DataAccess 数据规范化为单资产 roll layer 输入。

    这里明确使用 generic contract universe，也就是 `contract_info`
    中定义的具体月份合约，如 `CU0503.SHF`、`CU0602.SHF`。
    """
    daily = access.get_daily(variety).copy()
    contracts = access.get_contract_info(variety).copy()

    if "trade_date" in daily.columns:
        daily["trade_date"] = pd.to_datetime(daily["trade_date"])
    start_ts = pd.Timestamp(start) if start is not None else None
    end_ts = pd.Timestamp(end) if end is not None else None
    if start_ts is not None:
        daily = daily[daily["trade_date"] >= start_ts]
    if end_ts is not None:
        daily = daily[daily["trade_date"] <= end_ts]

    if daily.empty:
        prices = pd.DataFrame()
        open_interest = pd.DataFrame()
    else:
        prices = (
            daily.pivot_table(index="trade_date", columns="contract_id", values="settle_price", aggfunc="last")
            .sort_index()
        )
        open_interest = (
            daily.pivot_table(index="trade_date", columns="contract_id", values="open_interest", aggfunc="last")
            .sort_index()
        )
        if not contracts.empty:
            allowed = contracts["contract_id"].astype(str).tolist()
            prices = prices.reindex(columns=allowed, fill_value=pd.NA)
            open_interest = open_interest.reindex(columns=allowed, fill_value=pd.NA)

    return {
        "contracts": contracts,
        "prices": prices,
        "open_interest": open_interest,
        "raw_daily": daily,
        "variety": variety,
        "uses_generic_contract_only": True,
    }


class GMAT3SingleAssetRollStrategy(SingleAssetRollStrategy):
    """基于 GMAT3 原始数据驱动的单资产最小 roll strategy。"""

    def __init__(
        self,
        access: GMAT3DataAccess,
        variety: str,
        *,
        profile: RollStrategyProfile | None = None,
    ) -> None:
        cfg = _get_variety_cfg(variety)
        roll_cfg = ROLL_PARAMS.get(variety, {})
        roll_days = int(roll_cfg.get("roll_days", 3))
        currency = str(cfg.get("currency", "CNY"))
        if profile is None:
            profile = RollStrategyProfile(
                name=f"gmat3_{variety.lower()}_single_asset",
                asset_key=variety,
                asset_mode="single",
                currency=currency,
                rule_profile="gmat3_domestic_commodity",
                roll_days=roll_days,
                lifecycle_date_field="last_holding_date",
                lifecycle_rule_config=LifecycleRuleConfig(
                    kind="fixed_days_before_expiry",
                    params={"roll_days": roll_days, "date_field": "last_holding_date"},
                ),
                market_state_rule_config=MarketStateRuleConfig(
                    kind="gmat3_domestic_commodity",
                    params={},
                ),
                execution_rule_config=ExecutionRuleConfig(
                    kind="linear",
                    params={"roll_days": roll_days},
                ),
                selector_rule_config=SelectorRuleConfig(
                    kind="prefer_selected",
                    params={},
                ),
                metadata={
                    "source": "gmat3",
                    "exchange": cfg.get("exchange"),
                    "contract_type": cfg.get("contract_type"),
                },
            )
        built = build_single_asset_strategy_from_profile(profile)
        super().__init__(
            built.profile,
            lifecycle_rule=built.lifecycle_rule,
            market_state_rule=built.market_state_rule,
            selector=built.selector,
            executor=built.executor,
            composer=built.composer,
            lookthrough_resolver=built.lookthrough_resolver,
        )
        self.access = access
        self.variety = variety
        self.cfg = cfg

    def run_from_access(
        self,
        *,
        start: str | pd.Timestamp | None = None,
        end: str | pd.Timestamp | None = None,
    ):
        cfg_start = self.cfg.get("futures_base_date")
        effective_start = start if start is not None else cfg_start
        market_data = build_gmat3_single_asset_market_data(
            self.access,
            self.variety,
            start=effective_start,
            end=end,
        )
        return self.run(market_data=market_data, start=effective_start, end=end)
