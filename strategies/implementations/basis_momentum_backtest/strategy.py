"""China futures basis momentum strategy."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from backtest.costs import ProportionalCostModel
from backtest.vectorized import VectorizedBacktest
from strategies.base.strategy import StrategyBase
from strategies.context import StrategyContext

from .config import BasisMomentumConfig, coerce_config
from .data_access import BasisMomentumDataAccess, BasisMomentumMarketData
from .result import BasisMomentumRunResult


class BasisMomentumStrategy(StrategyBase):
    """中国期货基差动量策略。

    参考《全球商品CTA策略整合文档》5.5 / 5A.4 以及《基差动量策略_实现方案》：
    - 近月/活跃远月期限结构变化率滚动均值
    - 远月需通过持仓量占比过滤，默认阈值 5%
    - 截面线性排序权重 + 逆波动调整
    - 20 份轮动调仓
    - 组合层使用 20/60/120 日最大历史波动进行压缩
    """

    def __init__(
        self,
        config: BasisMomentumConfig | dict | None = None,
    ) -> None:
        cfg = coerce_config(config)
        super().__init__(cfg.to_dict())

        self.typed_config = cfg
        self.min_obs = cfg.min_obs
        self.signal_mode = cfg.signal_mode
        self.signal_window = cfg.signal_window
        self.academic_lookback = cfg.academic_lookback
        self.active_oi_pct_threshold = cfg.active_oi_pct_threshold
        self.min_listing_days = cfg.min_listing_days
        self.liquidity_lookback = cfg.liquidity_lookback
        self.liquidity_threshold_pre2017 = cfg.liquidity_threshold_pre2017
        self.liquidity_threshold_post2017 = cfg.liquidity_threshold_post2017
        self.rebalance_buckets = cfg.rebalance_buckets
        self.selection_weighting = cfg.selection_weighting
        self.vol_scale_windows = tuple(cfg.vol_scale_windows)
        self.apply_portfolio_vol_control = cfg.apply_portfolio_vol_scale
        self.max_abs_weight = cfg.max_abs_weight
        self.max_gross_exposure = cfg.max_gross_exposure
        self.vol_halflife = cfg.vol_halflife
        self.target_vol = cfg.target_vol
        self.trading_days = cfg.trading_days
        self.transaction_cost_bps = cfg.transaction_cost_bps
        self.transaction_cost_rate = cfg.transaction_cost_bps / 10_000.0
        self.exclude = set(cfg.exclude)

    def generate_signals(self, price_df: pd.DataFrame) -> pd.DataFrame:
        """StrategyBase-compatible fallback.

        This compatibility path assumes ``price_df`` is already a term-structure
        ratio matrix; the full strategy path uses ``compute_signal_matrices()``.
        """

        term_structure = price_df.replace(0.0, np.nan)
        basis_change = term_structure.div(term_structure.shift(1)).sub(1.0)
        return basis_change.rolling(self.signal_window, min_periods=self.signal_window).mean()

    def build_weights(
        self,
        signal_df: pd.DataFrame,
        vol_df: pd.DataFrame,
        corr_cache: dict | None = None,
    ) -> pd.DataFrame:
        """StrategyBase-compatible weight builder without tradability inputs."""

        del corr_cache
        sigma_max = self._coerce_sigma_max(vol_df)
        raw_positions = self.build_daily_positions(signal_df, sigma_max=sigma_max)
        return self.apply_staggered_rebalance(raw_positions)

    def compute_signal_matrices(
        self,
        near_prices: pd.DataFrame,
        far_prices: pd.DataFrame,
        near_returns: pd.DataFrame | None = None,
        far_returns: pd.DataFrame | None = None,
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Build term structure, daily basis change, and basis momentum signal."""

        aligned_near, aligned_far = near_prices.align(far_prices, join="outer", axis=None)
        term_structure = aligned_near.div(aligned_far.replace(0.0, np.nan))

        if self.signal_mode == "term_structure_change":
            basis_change = term_structure.div(term_structure.shift(1)).sub(1.0)
            signal = basis_change.rolling(
                self.signal_window,
                min_periods=self.signal_window,
            ).mean()
            return (
                term_structure.replace([np.inf, -np.inf], np.nan),
                basis_change.replace([np.inf, -np.inf], np.nan),
                signal.replace([np.inf, -np.inf], np.nan),
            )

        if near_returns is None:
            near_returns = aligned_near.pct_change()
        if far_returns is None:
            far_returns = aligned_far.pct_change()

        near_log = np.log1p(near_returns.fillna(0.0))
        far_log = np.log1p(far_returns.fillna(0.0))
        near_cum = np.exp(near_log.rolling(self.academic_lookback).sum()) - 1.0
        far_cum = np.exp(far_log.rolling(self.academic_lookback).sum()) - 1.0
        basis_change = near_returns - far_returns
        signal = near_cum - far_cum
        return (
            term_structure.replace([np.inf, -np.inf], np.nan),
            basis_change.replace([np.inf, -np.inf], np.nan),
            signal.replace([np.inf, -np.inf], np.nan),
        )

    def compute_sigma_max(self, returns_df: pd.DataFrame) -> pd.DataFrame:
        """Compute max rolling asset volatility across configured windows."""

        rolling_vols = [
            returns_df.rolling(window, min_periods=window).std() * np.sqrt(self.trading_days)
            for window in self.vol_scale_windows
        ]
        return (
            pd.concat(rolling_vols, axis=1, keys=range(len(rolling_vols)))
            .T.groupby(level=1)
            .max()
            .T
            .replace([np.inf, -np.inf], np.nan)
        )

    def _coerce_sigma_max(self, vol_df: pd.DataFrame) -> pd.DataFrame:
        if vol_df.empty:
            return vol_df.copy()
        if float(vol_df.max().max()) <= 2.0:
            return vol_df * np.sqrt(self.trading_days)
        return vol_df.copy()

    def build_tradable_mask(
        self,
        near_prices: pd.DataFrame,
        near_open_interest: pd.DataFrame,
        contract_multiplier: pd.Series | None = None,
    ) -> pd.DataFrame:
        """Build dynamic universe mask from listing age and liquidity."""

        valid_price = near_prices.notna()
        listing_age = valid_price.astype(int).cumsum()
        listing_mask = listing_age >= self.min_listing_days

        multiplier = (
            contract_multiplier.reindex(near_prices.columns).fillna(1.0)
            if contract_multiplier is not None
            else pd.Series(1.0, index=near_prices.columns)
        )
        holding_amount = near_open_interest.mul(near_prices).mul(multiplier, axis=1)
        rolling_min_amount = holding_amount.rolling(
            self.liquidity_lookback,
            min_periods=self.liquidity_lookback,
        ).min()
        threshold = pd.Series(
            np.where(
                rolling_min_amount.index < pd.Timestamp("2017-01-01"),
                self.liquidity_threshold_pre2017,
                self.liquidity_threshold_post2017,
            ),
            index=rolling_min_amount.index,
        )
        liquidity_mask = rolling_min_amount.ge(threshold, axis=0)
        return (listing_mask & liquidity_mask).fillna(False)

    def build_daily_positions(
        self,
        signal_df: pd.DataFrame,
        tradable_mask: pd.DataFrame | None = None,
        sigma_max: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        """Build daily long-short targets before staggered rebalancing."""

        positions = pd.DataFrame(0.0, index=signal_df.index, columns=signal_df.columns)

        for date in signal_df.index:
            row = pd.to_numeric(signal_df.loc[date], errors="coerce").dropna()
            if tradable_mask is not None and date in tradable_mask.index:
                allowed = tradable_mask.loc[date].reindex(row.index).fillna(False)
                row = row[allowed]
            if row.shape[0] < 2:
                continue

            ranked = row.rank(method="first", ascending=True)
            n_assets = float(len(row))
            # Linear long-short weights: (2k - N - 1) / [N(N+1)], zero-sum, symmetric.
            # The equivalent form below avoids an off-by-one that previously produced
            # all-negative weights (pure short portfolio):
            #   WRONG: - 2/(N+1)  →  highest rank = 0, net exposure = -1
            #   RIGHT: - 1/N      →  highest rank = max_long, net exposure = 0
            raw = (2.0 * ranked / (n_assets * (n_assets + 1.0))) - (1.0 / n_assets)

            if self.selection_weighting == "inv_vol" and sigma_max is not None and date in sigma_max.index:
                sigma_row = pd.to_numeric(sigma_max.loc[date].reindex(raw.index), errors="coerce")
                inv_sigma = (1.0 / sigma_row.replace(0.0, np.nan)).replace([np.inf, -np.inf], np.nan)
                raw = raw.mul(inv_sigma).dropna()
                if raw.shape[0] < 2:
                    continue

            gross = raw.abs().sum()
            if gross <= 0:
                continue

            weights = raw / gross
            if self.max_abs_weight > 0:
                weights = weights.clip(lower=-self.max_abs_weight, upper=self.max_abs_weight)
            positions.loc[date, weights.index] = weights.values

        return positions.fillna(0.0)

    def select_far_contract(
        self,
        *,
        near_contract: str | None,
        date: pd.Timestamp,
        contracts: list,
        bar_data: dict[str, object],
    ) -> tuple[str | None, float]:
        """Backward-compatible one-date far-leg selector."""

        return BasisMomentumDataAccess.select_far_contract(
            near_contract=near_contract,
            date=date,
            contracts=contracts,
            bar_data=bar_data,
            active_oi_pct_threshold=self.active_oi_pct_threshold,
        )

    def apply_staggered_rebalance(self, daily_positions: pd.DataFrame) -> pd.DataFrame:
        """Split the book into N tranches and rotate one tranche per day."""

        if self.rebalance_buckets <= 1:
            return daily_positions.fillna(0.0)

        tranches: list[pd.DataFrame] = []
        for bucket in range(self.rebalance_buckets):
            updates = daily_positions.iloc[bucket::self.rebalance_buckets]
            tranche = updates.reindex(daily_positions.index).ffill().fillna(0.0)
            tranches.append(tranche)

        combined = sum(tranches) / float(self.rebalance_buckets)
        return combined.fillna(0.0)

    def compute_portfolio_vol_scale(
        self,
        positions: pd.DataFrame,
        returns_df: pd.DataFrame,
    ) -> pd.Series:
        """Compute portfolio volatility scaling coefficient.

        When ``apply_portfolio_vol_control`` is True, scales positions so that
        realized portfolio vol tracks ``target_vol``.  The scale factor is NOT
        capped at 1.0, allowing positions to be levered up as well as down.
        ``max_gross_exposure`` in ``apply_portfolio_vol_scale`` acts as the
        hard leverage ceiling.
        """

        pnl_proxy = positions.shift(1).fillna(0.0).mul(returns_df.fillna(0.0)).sum(axis=1)
        rolling_vols = [
            pnl_proxy.rolling(window, min_periods=window).std() * np.sqrt(self.trading_days)
            for window in self.vol_scale_windows
        ]
        sigma_max = pd.concat(rolling_vols, axis=1).max(axis=1)
        scale = self.target_vol / sigma_max
        return scale.replace([np.inf, -np.inf], np.nan).fillna(1.0)

    def apply_portfolio_vol_scale(
        self,
        positions: pd.DataFrame,
        scale: pd.Series,
    ) -> pd.DataFrame:
        """Apply portfolio-level volatility compression and gross cap."""

        scaled = positions.mul(scale.reindex(positions.index).fillna(1.0), axis=0)
        gross = scaled.abs().sum(axis=1)
        gross_scale = (self.max_gross_exposure / gross).clip(upper=1.0)
        gross_scale = gross_scale.replace([np.inf, -np.inf], np.nan).fillna(1.0)
        return scaled.mul(gross_scale, axis=0).fillna(0.0)

    def _make_backtest(self) -> VectorizedBacktest:
        return VectorizedBacktest(
            lag=1,
            vol_target=None,
            vol_halflife=self.vol_halflife,
            trading_days=self.trading_days,
            max_gross_exposure=self.max_gross_exposure,
            cost_model=ProportionalCostModel(self.transaction_cost_rate),
        )

    def run_vectorized(
        self,
        returns_df: pd.DataFrame,
        backtest=None,
        *,
        near_prices_df: pd.DataFrame | None = None,
        far_prices_df: pd.DataFrame | None = None,
        near_returns_df: pd.DataFrame | None = None,
        far_returns_df: pd.DataFrame | None = None,
        signal_df: pd.DataFrame | None = None,
        tradable_mask: pd.DataFrame | None = None,
    ):
        """Run the basis momentum strategy on prepared matrices."""

        near_returns = near_returns_df.reindex_like(returns_df) if near_returns_df is not None else returns_df.copy()
        sigma_max = self.compute_sigma_max(near_returns)

        if signal_df is None:
            if near_prices_df is None or far_prices_df is None:
                raise ValueError("near_prices_df and far_prices_df are required when signal_df is not provided")
            _, _, signal_df = self.compute_signal_matrices(
                near_prices_df.reindex_like(returns_df),
                far_prices_df.reindex_like(returns_df),
                near_returns=near_returns,
                far_returns=None if far_returns_df is None else far_returns_df.reindex_like(returns_df),
            )

        raw_positions = self.build_daily_positions(
            signal_df.reindex_like(returns_df),
            tradable_mask=tradable_mask.reindex_like(returns_df).fillna(False) if tradable_mask is not None else None,
            sigma_max=sigma_max.reindex_like(returns_df),
        )
        staggered_positions = self.apply_staggered_rebalance(raw_positions)
        vol_scale = self.compute_portfolio_vol_scale(staggered_positions, near_returns)
        positions = (
            self.apply_portfolio_vol_scale(staggered_positions, vol_scale)
            if self.apply_portfolio_vol_control
            else staggered_positions
        )

        bt = backtest if backtest is not None else self._make_backtest()
        return bt.run(positions, returns_df)

    def run_pipeline(
        self,
        context: StrategyContext | None = None,
        data_dir: str | Path | None = None,
        tickers: list[str] | None = None,
        start: str | None = None,
        end: str | None = None,
        verbose: bool = True,
        contract_info_path: str | Path | None = None,
        lot_size_map: dict[str, float] | pd.Series | None = None,
        prebuilt_dir: str | Path | None = None,
        market_data: BasisMomentumMarketData | None = None,
    ) -> BasisMomentumRunResult:
        """Load data, build basis momentum signals, and run vectorized backtest."""

        if context is None:
            from data.loader import ContractSchema, DataLoader, InstrumentSchema, KlineSchema
            from data.sources.column_keyed_source import ColumnKeyedSource
            from data.sources.parquet_source import ParquetSource

            if data_dir is None:
                raise ValueError("data_dir must be provided via run_pipeline()")
            if contract_info_path is None:
                raise ValueError("contract_info_path must be provided via run_pipeline()")
            contract_source = ColumnKeyedSource(Path(contract_info_path), filter_col="fut_code")
            context = StrategyContext(
                loader=DataLoader(
                    kline_source=ParquetSource(Path(data_dir)),
                    contract_source=contract_source,
                    instrument_source=contract_source,
                    kline_schema=KlineSchema.tushare(),
                    contract_schema=ContractSchema.tushare(),
                    instrument_schema=InstrumentSchema.china_from_contracts(),
                ),
                sector_map={},
                backtest=None,
            )

        if verbose:
            print("=" * 65)
            print("Step 1: Load China futures returns / dominant leg / active far leg")
            print("=" * 65)

        if market_data is None and prebuilt_dir is not None:
            market_data = BasisMomentumMarketData.load(prebuilt_dir)
        if market_data is None:
            market_data = BasisMomentumDataAccess(context).build_market_data(
                tickers=tickers,
                start=start,
                end=end,
                min_obs=self.min_obs,
                exclude=self.exclude,
                lot_size_map=lot_size_map,
                active_oi_pct_threshold=self.active_oi_pct_threshold,
            )

        returns = market_data.returns
        near_returns = market_data.near_returns.reindex_like(returns)
        near_prices = market_data.near_prices.reindex_like(returns)
        near_open_interest = market_data.near_open_interest.reindex_like(returns)
        dominant_contracts = market_data.dominant_contracts.reindex(index=returns.index, columns=returns.columns)
        far_contracts = market_data.far_contracts.reindex(index=returns.index, columns=returns.columns)
        far_prices = market_data.far_prices.reindex_like(returns)
        far_open_interest = market_data.far_open_interest.reindex_like(returns)
        far_oi_share = market_data.far_oi_share.reindex_like(returns)
        contract_multiplier = market_data.contract_multiplier.reindex(returns.columns).fillna(1.0)
        far_returns = far_prices.pct_change().where(far_contracts.eq(far_contracts.shift(1)))
        tradable_mask = self.build_tradable_mask(
            near_prices=near_prices,
            near_open_interest=near_open_interest,
            contract_multiplier=contract_multiplier,
        )
        tradable_mask &= far_prices.notna()

        if verbose:
            print(
                f"\nReturns matrix: {returns.shape} "
                f"({returns.index[0].date()} - {returns.index[-1].date()})"
            )
            print("\n" + "=" * 65)
            print("Step 2: Compute basis momentum signal and cross-sectional targets")
            print("=" * 65)

        term_structure, basis_change, signal = self.compute_signal_matrices(
            near_prices,
            far_prices,
            near_returns=near_returns,
            far_returns=far_returns,
        )
        sigma_max = self.compute_sigma_max(near_returns)
        raw_positions = self.build_daily_positions(
            signal,
            tradable_mask=tradable_mask,
            sigma_max=sigma_max,
        )
        staggered_positions = self.apply_staggered_rebalance(raw_positions)
        portfolio_vol_scale = self.compute_portfolio_vol_scale(staggered_positions, near_returns)
        positions = (
            self.apply_portfolio_vol_scale(staggered_positions, portfolio_vol_scale)
            if self.apply_portfolio_vol_control
            else staggered_positions
        )

        if verbose:
            active_share = raw_positions.ne(0.0).mean().mean()
            gross = positions.abs().sum(axis=1)
            far_coverage = far_prices.notna().mean().mean()
            print(
                f"  Active signal share={active_share:.1%}  "
                f"Far-leg coverage={far_coverage:.1%}"
            )
            print(
                f"  Rebalance buckets={self.rebalance_buckets}  "
                f"Live gross mean/max={gross.mean():.2f}/{gross.max():.2f}"
            )
            print("\n" + "=" * 65)
            print("Step 3: VectorizedBacktest — basis momentum")
            print("=" * 65)

        bt = context.backtest if context.backtest is not None else self._make_backtest()
        bt_result = bt.run(positions, returns)
        pnl = bt_result.returns.iloc[1:]

        if verbose and not pnl.empty:
            self._print_summary(pnl, "BasisMomentum")

        return BasisMomentumRunResult(
            returns=returns,
            near_returns=near_returns,
            far_returns=far_returns,
            near_prices=near_prices,
            far_prices=far_prices,
            near_open_interest=near_open_interest,
            far_open_interest=far_open_interest,
            far_oi_share=far_oi_share,
            dominant_contracts=dominant_contracts,
            far_contracts=far_contracts,
            term_structure=term_structure,
            basis_change=basis_change,
            signal=signal,
            tradable_mask=tradable_mask,
            sigma_max=sigma_max,
            raw_positions=raw_positions,
            positions=positions,
            portfolio_vol_scale=portfolio_vol_scale,
            pnl=pnl,
            backtest_result=bt_result,
            metadata={
                "signal_mode": self.signal_mode,
                "signal_window": self.signal_window,
                "academic_lookback": self.academic_lookback,
                "active_oi_pct_threshold": self.active_oi_pct_threshold,
                "min_listing_days": self.min_listing_days,
                "liquidity_lookback": self.liquidity_lookback,
                "liquidity_threshold_pre2017": self.liquidity_threshold_pre2017,
                "liquidity_threshold_post2017": self.liquidity_threshold_post2017,
                "rebalance_buckets": self.rebalance_buckets,
                "selection_weighting": self.selection_weighting,
                "vol_scale_windows": list(self.vol_scale_windows),
                "apply_portfolio_vol_scale": self.apply_portfolio_vol_control,
                "target_vol": self.target_vol,
                "max_abs_weight": self.max_abs_weight,
                "max_gross_exposure": self.max_gross_exposure,
                "n_symbols": returns.shape[1],
                "start": str(returns.index[0].date()),
                "end": str(returns.index[-1].date()),
                "used_prebuilt_data": bool(prebuilt_dir is not None),
            },
        )

    def _print_summary(self, pnl: pd.Series, label: str) -> None:
        ann_r = pnl.mean() * self.trading_days
        ann_v = pnl.std() * np.sqrt(self.trading_days)
        sharpe = ann_r / ann_v if ann_v > 0 else float("nan")
        nav = (1 + pnl).cumprod()
        mdd = ((nav - nav.cummax()) / nav.cummax()).min()
        print(
            f"  [{label:14s}] Sharpe={sharpe:.3f}  Return={ann_r * 100:.1f}%  "
            f"Vol={ann_v * 100:.1f}%  MaxDD={mdd * 100:.1f}%"
        )
