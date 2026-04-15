"""GMAT3 全球多资产配置策略正式入口。"""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import pandas as pd

from strategies.base.cross_sectional import CrossSectionalStrategy

from .config import build_gmat3_config
from .data_access import GMAT3DataAccess
from .index_builder import GMAT3IndexBuilder, INDEX_BASE_DATE
from .main_contract import MainContractEngine
from .schedule import build_index_calc_days, build_value_matrices
from .sub_portfolio import SubPortfolioEngine
from .universe import BLACK_COMPONENTS, SUB_PORTFOLIOS
from .weights import WeightCalculator, build_gmat3_weights


@dataclass
class GMAT3RunResult:
    """GMAT3 端到端运行结果。"""

    main_dfs: dict[str, pd.DataFrame]
    sub_portfolio_values: dict[str, pd.Series]
    full_calc_days: list[pd.Timestamp]
    calc_days: list[pd.Timestamp]
    value_df_full: pd.DataFrame
    value_df: pd.DataFrame
    weight_df: pd.DataFrame
    schedule: dict[int, dict[str, list[pd.Timestamp]]]
    adjust_date_sets: dict[int, set[pd.Timestamp]]
    index_series: pd.Series


class GMAT3Strategy(CrossSectionalStrategy):
    """GMAT3 全球多资产配置策略。

    兼容现有 `CrossSectionalStrategy` 风格接口，同时提供 GMAT3 自己的
    端到端运行入口 `run_pipeline()` / `run()`。
    """

    DEFAULT_CONFIG: dict = build_gmat3_config()

    def __init__(
        self,
        config: dict | None = None,
        currency_map: dict | None = None,
    ) -> None:
        merged = build_gmat3_config(config)
        super().__init__(merged)
        self.currency_map: dict = currency_map or {}

        self._market_data_root = Path(self.config["market_data_root"])
        self._strategy_data_root = Path(self.config["strategy_data_root"])
        self.access = GMAT3DataAccess(self._market_data_root, self._strategy_data_root)
        self.main_engine = MainContractEngine(self.access)
        self.sub_engine = SubPortfolioEngine(self.access)
        self.weight_calc = WeightCalculator()
        self.index_builder = GMAT3IndexBuilder()

    def _reset_access(
        self,
        market_data_root: str | Path | None = None,
        strategy_data_root: str | Path | None = None,
    ) -> None:
        mdr = Path(market_data_root) if market_data_root else self._market_data_root
        sdr = Path(strategy_data_root) if strategy_data_root else self._strategy_data_root
        if mdr == self._market_data_root and sdr == self._strategy_data_root:
            return
        self._market_data_root = mdr
        self._strategy_data_root = sdr
        self.config["market_data_root"] = mdr
        self.config["strategy_data_root"] = sdr
        self.access = GMAT3DataAccess(mdr, sdr)
        self.main_engine = MainContractEngine(self.access)
        self.sub_engine = SubPortfolioEngine(self.access)

    @staticmethod
    def _resolve_sub_portfolios(sub_portfolios: list[str] | None) -> list[str]:
        if sub_portfolios is None:
            return list(SUB_PORTFOLIOS)
        unknown = sorted(set(sub_portfolios) - set(SUB_PORTFOLIOS))
        if unknown:
            raise ValueError(f"Unknown GMAT3 sub_portfolios: {unknown}")
        return list(sub_portfolios)

    @staticmethod
    def _resolve_main_varieties(sub_portfolios: list[str]) -> list[str]:
        main_varieties: list[str] = []
        for variety in sub_portfolios:
            if variety == "BLACK":
                main_varieties.extend(BLACK_COMPONENTS)
            else:
                main_varieties.append(variety)
        deduped: list[str] = []
        seen: set[str] = set()
        for variety in main_varieties:
            if variety not in seen:
                deduped.append(variety)
                seen.add(variety)
        return deduped

    def build_main_dfs(
        self,
        *,
        end_date: str | pd.Timestamp | None = None,
        sub_portfolios: list[str] | None = None,
    ) -> dict[str, pd.DataFrame]:
        selected = self._resolve_sub_portfolios(sub_portfolios)
        main_varieties = self._resolve_main_varieties(selected)
        return {variety: self.main_engine.compute(variety, end=end_date) for variety in main_varieties}

    def build_sub_portfolio_values(
        self,
        main_dfs: dict[str, pd.DataFrame],
        *,
        sub_portfolios: list[str] | None = None,
    ) -> dict[str, pd.Series]:
        selected = self._resolve_sub_portfolios(sub_portfolios)
        return {variety: self.sub_engine.compute(variety, main_dfs) for variety in selected}

    def build_value_inputs(
        self,
        sub_portfolio_values: dict[str, pd.Series],
        *,
        start_date: str | pd.Timestamp | None = None,
        end_date: str | pd.Timestamp | None = None,
    ) -> tuple[list[pd.Timestamp], list[pd.Timestamp], pd.DataFrame, pd.DataFrame]:
        full_calc_days, calc_days = build_index_calc_days(INDEX_BASE_DATE, end_date=end_date)
        if start_date is not None:
            start_ts = pd.Timestamp(start_date)
            calc_days = [d for d in calc_days if d >= start_ts]
        value_df_full, value_df = build_value_matrices(sub_portfolio_values, full_calc_days, calc_days)
        return full_calc_days, calc_days, value_df_full, value_df

    def run_pipeline(
        self,
        *,
        start_date: str | pd.Timestamp | None = None,
        end_date: str | pd.Timestamp | None = None,
        market_data_root: str | Path | None = None,
        strategy_data_root: str | Path | None = None,
        sub_portfolios: list[str] | None = None,
    ) -> GMAT3RunResult:
        self._reset_access(market_data_root, strategy_data_root)
        selected = self._resolve_sub_portfolios(sub_portfolios)

        main_dfs = self.build_main_dfs(end_date=end_date, sub_portfolios=selected)
        sub_portfolio_values = self.build_sub_portfolio_values(main_dfs, sub_portfolios=selected)
        full_calc_days, calc_days, value_df_full, value_df = self.build_value_inputs(
            sub_portfolio_values,
            start_date=start_date,
            end_date=end_date,
        )
        weight_df, schedule = self.weight_calc.compute(value_df_full, calc_days)
        adjust_date_sets = {
            sub_n: set(schedule[sub_n]["adjust_dates"])
            for sub_n in sorted(schedule)
        }
        index_series = self.index_builder.compute(
            value_df=value_df,
            weight_df=weight_df,
            index_trading_days=calc_days,
            adjust_date_sets=adjust_date_sets,
            fx_series=self.access.get_fx_rate(),
        )

        return GMAT3RunResult(
            main_dfs=main_dfs,
            sub_portfolio_values=sub_portfolio_values,
            full_calc_days=full_calc_days,
            calc_days=calc_days,
            value_df_full=value_df_full,
            value_df=value_df,
            weight_df=weight_df,
            schedule=schedule,
            adjust_date_sets=adjust_date_sets,
            index_series=index_series,
        )

    def generate_signals(self, price_df: pd.DataFrame) -> pd.DataFrame:
        """兼容基类接口，GMAT3 正式实现使用 `run_pipeline()`。"""
        return super().generate_signals(price_df)

    def build_weights(
        self,
        signal_df: pd.DataFrame,
        vol_df: pd.DataFrame,
        corr_cache=None,
    ) -> pd.DataFrame:
        """保留简化版 GMAT3 风格定仓桥接。"""
        return build_gmat3_weights(
            signal_df,
            vol_df,
            base_risk=self.config["base_risk"],
            signal_mode=self.config["signal_mode"],
            waf_threshold=self.config["waf_threshold"],
            waf_target=self.config["waf_target"],
        )

    def run(
        self,
        price_df: pd.DataFrame | None = None,
        adjust_dates: set[pd.Timestamp] | None = None,
        engine=None,
        *,
        start_date: str | pd.Timestamp | None = None,
        end_date: str | pd.Timestamp | None = None,
        market_data_root: str | Path | None = None,
        strategy_data_root: str | Path | None = None,
        sub_portfolios: list[str] | None = None,
    ):
        """优先提供 GMAT3 端到端入口，必要时兼容基类通用 run。"""
        if price_df is not None:
            if engine is None:
                raise ValueError("engine is required when using the generic StrategyBase.run path")
            return super().run(price_df, adjust_dates or set(), engine)
        return self.run_pipeline(
            start_date=start_date,
            end_date=end_date,
            market_data_root=market_data_root,
            strategy_data_root=strategy_data_root,
            sub_portfolios=sub_portfolios,
        )
