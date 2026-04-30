"""China futures basis value (basis persistence / momentum) strategy.

Signal construction
-------------------
For each instrument that has a liquid far-leg contract:

1. Compute daily basis = log(near_price / far_price).
   - Positive  → backwardation (near > far)
   - Negative  → contango     (near < far)

2. Compute z-score against its own 504-day rolling history:
       basis_z = (basis - rolling_mean) / rolling_std

3. Cross-sectional signal for the near-leg position:
       signal = +basis_z   (basis persistence / momentum direction)

   IC analysis on Chinese futures (2014–2026) showed that basis deviations
   *persist* rather than mean-revert: ICIR_20d = +0.086 at lb=504 days.
   When basis_z > 0 (near historically expensive), the structural imbalance
   tends to continue → LONG near leg.
   When basis_z < 0 (contango), the imbalance also persists → SHORT near leg.

Portfolio construction is identical to BasisMomentumStrategy:
  - Cross-sectional linear rank weights with inv-vol adjustment
  - Staggered rebalance across N buckets
  - Portfolio-level vol scaling (bidirectional)
  - max_gross_exposure cap
"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from backtest.costs import ProportionalCostModel
from backtest.vectorized import VectorizedBacktest
from strategies.base.strategy import StrategyBase
from strategies.context import StrategyContext
from strategies.implementations.basis_momentum_backtest.data_access import (
    BasisMomentumDataAccess,
    BasisMomentumMarketData,
)

from .config import BasisValueConfig, coerce_config
from .result import BasisValueRunResult


class BasisValueStrategy(StrategyBase):
    """中国期货基差价值（基差持续性）策略。

    当品种基差（log近/远比率）z-score显著偏高时，预期持续偏高，做多近月；
    z-score显著偏低时，预期持续偏低，做空近月。

    IC分析（2014–2026）表明中国期货基差呈持续性而非均值回归：
    signal = +basis_z，lb=504天，ICIR_20d = +0.086。
    """

    def __init__(
        self,
        config: BasisValueConfig | dict | None = None,
    ) -> None:
        cfg = coerce_config(config)
        super().__init__(cfg.to_dict())

        self.typed_config = cfg
        self.min_obs = cfg.min_obs
        self.basis_z_lookback = cfg.basis_z_lookback
        self.z_clip = cfg.z_clip
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

    # ------------------------------------------------------------------
    # StrategyBase compatibility stubs
    # ------------------------------------------------------------------

    def generate_signals(self, price_df: pd.DataFrame) -> pd.DataFrame:
        """StrategyBase-compatible fallback; price_df treated as near/far ratio."""

        term_structure = price_df.replace(0.0, np.nan)
        basis = np.log(term_structure.replace([np.inf, -np.inf], np.nan))
        basis_mean = basis.rolling(self.basis_z_lookback, min_periods=self.basis_z_lookback).mean()
        basis_std = basis.rolling(self.basis_z_lookback, min_periods=self.basis_z_lookback).std()
        basis_z = (basis - basis_mean) / basis_std.replace(0.0, np.nan)
        return basis_z.clip(-self.z_clip, self.z_clip)

    def build_weights(
        self,
        signal_df: pd.DataFrame,
        vol_df: pd.DataFrame,
        corr_cache: dict | None = None,
    ) -> pd.DataFrame:
        del corr_cache
        sigma_max = self._coerce_sigma_max(vol_df)
        raw_positions = self.build_daily_positions(signal_df, sigma_max=sigma_max)
        return self.apply_staggered_rebalance(raw_positions)

    # ------------------------------------------------------------------
    # Core signal computation
    # ------------------------------------------------------------------

    def compute_signal_matrices(
        self,
        near_prices: pd.DataFrame,
        far_prices: pd.DataFrame,
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Build term structure ratio, basis z-score, and basis-persistence signal.

        Returns
        -------
        term_structure : DataFrame
            near_price / far_price (ratio; >1 = backwardation, <1 = contango)
        basis_z : DataFrame
            Rolling z-score of log(near/far) over ``basis_z_lookback`` days.
        signal : DataFrame
            Basis persistence signal = +basis_z (clipped to ±z_clip).
            IC analysis confirmed Chinese futures basis *persists* (not reverts):
            ICIR_20d = +0.086 at lb=504 days.
            Positive signal → LONG near leg (basis abnormally high / backwardation persists).
            Negative signal → SHORT near leg (basis abnormally low / contango persists).
        """

        aligned_near, aligned_far = near_prices.align(far_prices, join="outer", axis=None)
        term_structure = aligned_near.div(aligned_far.replace(0.0, np.nan))

        # Use log ratio for a symmetric, additive basis measure.
        basis = np.log(term_structure.replace([np.inf, -np.inf], np.nan))

        basis_mean = basis.rolling(
            self.basis_z_lookback, min_periods=self.basis_z_lookback
        ).mean()
        basis_std = basis.rolling(
            self.basis_z_lookback, min_periods=self.basis_z_lookback
        ).std()
        basis_z = (basis - basis_mean) / basis_std.replace(0.0, np.nan)
        basis_z = basis_z.clip(-self.z_clip, self.z_clip)

        # Basis persistence: signal follows z-score direction (NOT inverted).
        signal = basis_z

        return (
            term_structure.replace([np.inf, -np.inf], np.nan),
            basis_z.replace([np.inf, -np.inf], np.nan),
            signal.replace([np.inf, -np.inf], np.nan),
        )

    # ------------------------------------------------------------------
    # Volatility helpers (identical logic to BasisMomentumStrategy)
    # ------------------------------------------------------------------

    def compute_sigma_max(self, returns_df: pd.DataFrame) -> pd.DataFrame:
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

    # ------------------------------------------------------------------
    # Universe / tradability filter (identical to BasisMomentumStrategy)
    # ------------------------------------------------------------------

    def build_tradable_mask(
        self,
        near_prices: pd.DataFrame,
        near_open_interest: pd.DataFrame,
        contract_multiplier: pd.Series | None = None,
    ) -> pd.DataFrame:
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

    # ------------------------------------------------------------------
    # Cross-sectional weight building (identical to BasisMomentumStrategy)
    # ------------------------------------------------------------------

    def build_daily_positions(
        self,
        signal_df: pd.DataFrame,
        tradable_mask: pd.DataFrame | None = None,
        sigma_max: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
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

    def apply_staggered_rebalance(self, daily_positions: pd.DataFrame) -> pd.DataFrame:
        if self.rebalance_buckets <= 1:
            return daily_positions.fillna(0.0)

        tranches: list[pd.DataFrame] = []
        for bucket in range(self.rebalance_buckets):
            updates = daily_positions.iloc[bucket::self.rebalance_buckets]
            tranche = updates.reindex(daily_positions.index).ffill().fillna(0.0)
            tranches.append(tranche)

        combined = sum(tranches) / float(self.rebalance_buckets)
        return combined.fillna(0.0)

    # ------------------------------------------------------------------
    # Portfolio vol scaling (identical to BasisMomentumStrategy)
    # ------------------------------------------------------------------

    def compute_portfolio_vol_scale(
        self,
        positions: pd.DataFrame,
        returns_df: pd.DataFrame,
    ) -> pd.Series:
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
        scaled = positions.mul(scale.reindex(positions.index).fillna(1.0), axis=0)
        gross = scaled.abs().sum(axis=1)
        gross_scale = (self.max_gross_exposure / gross).clip(upper=1.0)
        gross_scale = gross_scale.replace([np.inf, -np.inf], np.nan).fillna(1.0)
        return scaled.mul(gross_scale, axis=0).fillna(0.0)

    # ------------------------------------------------------------------
    # VectorizedBacktest factory
    # ------------------------------------------------------------------

    def _make_backtest(self) -> VectorizedBacktest:
        return VectorizedBacktest(
            lag=1,
            vol_target=None,
            vol_halflife=self.vol_halflife,
            trading_days=self.trading_days,
            max_gross_exposure=self.max_gross_exposure,
            cost_model=ProportionalCostModel(self.transaction_cost_rate),
        )

    # ------------------------------------------------------------------
    # Full pipeline
    # ------------------------------------------------------------------

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
    ) -> BasisValueRunResult:
        """Load data, compute basis z-score signals, and run backtest."""

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
        dominant_contracts = market_data.dominant_contracts.reindex(
            index=returns.index, columns=returns.columns
        )
        far_contracts = market_data.far_contracts.reindex(
            index=returns.index, columns=returns.columns
        )
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
            print("Step 2: Compute basis z-score signal (basis persistence)")
            print("=" * 65)

        term_structure, basis_z, signal = self.compute_signal_matrices(
            near_prices, far_prices
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
            print("Step 3: VectorizedBacktest — basis value")
            print("=" * 65)

        bt = context.backtest if context.backtest is not None else self._make_backtest()
        bt_result = bt.run(positions, returns)
        pnl = bt_result.returns.iloc[1:]

        if verbose and not pnl.empty:
            self._print_summary(pnl, "BasisValue")

        return BasisValueRunResult(
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
            basis_z=basis_z,
            signal=signal,
            tradable_mask=tradable_mask,
            sigma_max=sigma_max,
            raw_positions=raw_positions,
            positions=positions,
            portfolio_vol_scale=portfolio_vol_scale,
            pnl=pnl,
            backtest_result=bt_result,
            metadata={
                "basis_z_lookback": self.basis_z_lookback,
                "z_clip": self.z_clip,
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
