"""China futures short-term (1-month) reversal strategy.

Signal construction
-------------------
For each instrument in the tradable universe:

    raw_signal_i = -[log(price_i,t-skip) - log(price_i,t-skip-window)]

The sign is **negative** (contrarian): short recent outperformers,
long recent underperformers.  The raw log-cumulative-return is clipped
to ±signal_clip before cross-sectional ranking.

``skip_days`` (default 1) omits the most recent trading day from the
measurement window to avoid microstructure bid-ask bounce reversal.

Unlike carry/basis strategies, **no far-leg contract data is required**.
The strategy operates entirely on continuous near-leg settle prices and
open interest.

Portfolio construction
----------------------
Identical framework to CarryStrategy / BasisValueStrategy:
  - Cross-sectional linear rank weights with optional inv-vol adjustment
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

from .config import ShortReversalConfig, coerce_config
from .result import ShortReversalRunResult


class ShortReversalStrategy(StrategyBase):
    """中国期货短期（1个月）反转策略。

    做空近期涨幅最大的品种，做多近期跌幅最大的品种。
    信号 = -log(price_{t-skip} / price_{t-skip-window})，截断后截面排序。
    """

    def __init__(
        self,
        config: ShortReversalConfig | dict | None = None,
    ) -> None:
        cfg = coerce_config(config)
        super().__init__(cfg.to_dict())

        self.typed_config = cfg
        self.reversal_window = cfg.reversal_window
        self.skip_days = cfg.skip_days
        self.signal_clip = cfg.signal_clip
        self.min_obs = cfg.min_obs
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
        """StrategyBase-compatible fallback; price_df treated as settle prices."""
        return self.compute_signal(price_df)

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

    def compute_signal(self, settle_prices: pd.DataFrame) -> pd.DataFrame:
        """Compute the contrarian reversal signal.

        Parameters
        ----------
        settle_prices : DataFrame
            Continuous near-leg settle prices, shape (T, N).

        Returns
        -------
        signal : DataFrame
            Negative cumulative log-return over ``reversal_window`` days,
            shifted back by ``skip_days`` to avoid microstructure noise,
            clipped to ±signal_clip.

            Positive signal → instrument has underperformed → LONG.
            Negative signal → instrument has outperformed  → SHORT.
        """
        prices = settle_prices.replace(0.0, np.nan)
        log_prices = np.log(prices)

        total_shift = self.skip_days + self.reversal_window
        # log return over [t - total_shift, t - skip_days]
        log_ret = log_prices.shift(self.skip_days) - log_prices.shift(total_shift)

        # contrarian: invert sign
        signal = -log_ret
        return signal.clip(-self.signal_clip, self.signal_clip).replace(
            [np.inf, -np.inf], np.nan
        )

    # ------------------------------------------------------------------
    # Volatility helpers
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
    # Universe / tradability filter
    # ------------------------------------------------------------------

    def build_tradable_mask(
        self,
        settle_prices: pd.DataFrame,
        open_interest: pd.DataFrame,
        contract_multiplier: pd.Series | None = None,
    ) -> pd.DataFrame:
        valid_price = settle_prices.notna()
        listing_age = valid_price.astype(int).cumsum()
        listing_mask = listing_age >= self.min_listing_days

        multiplier = (
            contract_multiplier.reindex(settle_prices.columns).fillna(1.0)
            if contract_multiplier is not None
            else pd.Series(1.0, index=settle_prices.columns)
        )
        holding_amount = open_interest.mul(settle_prices).mul(multiplier, axis=1)
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
    # Cross-sectional weight building
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
    # Portfolio vol scaling
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
        context: StrategyContext,
        tickers: list[str] | None = None,
        start: str | None = None,
        end: str | None = None,
        verbose: bool = True,
        lot_size_map: dict[str, float] | pd.Series | None = None,
    ) -> ShortReversalRunResult:
        """Load data, compute reversal signal, and run backtest."""

        if verbose:
            print("=" * 65)
            print("Step 1: Load China futures returns / settle prices / OI")
            print("=" * 65)

        returns = context.load_returns_matrix(
            tickers=tickers,
            start=start,
            end=end,
            min_obs=self.min_obs,
            exclude=self.exclude,
        )
        if returns.empty:
            raise RuntimeError("No returns loaded. Check data_dir and contract metadata.")

        symbols = returns.columns.tolist()

        settle_prices = context.load_continuous_field_matrix(
            field_name="settle",
            tickers=symbols,
            start=start,
            end=end,
        ).reindex(index=returns.index, columns=symbols)

        open_interest = context.load_continuous_field_matrix(
            field_name="open_interest",
            tickers=symbols,
            start=start,
            end=end,
        ).reindex(index=returns.index, columns=symbols)

        # Resolve contract multiplier for liquidity calculation
        if lot_size_map is not None:
            if isinstance(lot_size_map, pd.Series):
                contract_multiplier = lot_size_map.reindex(symbols).fillna(1.0).astype(float)
            else:
                contract_multiplier = pd.Series(
                    {s: lot_size_map.get(s, 1.0) for s in symbols}, dtype=float
                )
        else:
            contract_multiplier = None

        if verbose:
            print(
                f"\nReturns matrix: {returns.shape} "
                f"({returns.index[0].date()} - {returns.index[-1].date()})"
            )
            print("\n" + "=" * 65)
            print(
                f"Step 2: Compute reversal signal  "
                f"window={self.reversal_window}d  skip={self.skip_days}d"
            )
            print("=" * 65)

        signal = self.compute_signal(settle_prices)

        tradable_mask = self.build_tradable_mask(
            settle_prices=settle_prices,
            open_interest=open_interest,
            contract_multiplier=contract_multiplier,
        )

        sigma_max = self.compute_sigma_max(returns)
        raw_positions = self.build_daily_positions(
            signal,
            tradable_mask=tradable_mask,
            sigma_max=sigma_max,
        )
        staggered_positions = self.apply_staggered_rebalance(raw_positions)
        portfolio_vol_scale = self.compute_portfolio_vol_scale(staggered_positions, returns)
        positions = (
            self.apply_portfolio_vol_scale(staggered_positions, portfolio_vol_scale)
            if self.apply_portfolio_vol_control
            else staggered_positions
        )

        if verbose:
            active_share = raw_positions.ne(0.0).mean().mean()
            gross = positions.abs().sum(axis=1)
            long_frac = (raw_positions > 0).sum(axis=1).div(
                raw_positions.ne(0).sum(axis=1).replace(0, float("nan"))
            ).mean()
            print(
                f"  Active signal share={active_share:.1%}  "
                f"Avg long frac={long_frac:.1%}"
            )
            print(
                f"  Rebalance buckets={self.rebalance_buckets}  "
                f"Live gross mean/max={gross.mean():.2f}/{gross.max():.2f}"
            )
            print("\n" + "=" * 65)
            print("Step 3: VectorizedBacktest — short reversal")
            print("=" * 65)

        bt = context.backtest if context.backtest is not None else self._make_backtest()
        bt_result = bt.run(positions, returns)
        pnl = bt_result.returns.iloc[1:]

        if verbose and not pnl.empty:
            self._print_summary(pnl, "ShortReversal")

        return ShortReversalRunResult(
            returns=returns,
            settle_prices=settle_prices,
            open_interest=open_interest,
            signal=signal,
            tradable_mask=tradable_mask,
            sigma_max=sigma_max,
            raw_positions=raw_positions,
            positions=positions,
            portfolio_vol_scale=portfolio_vol_scale,
            pnl=pnl,
            backtest_result=bt_result,
            metadata={
                "reversal_window": self.reversal_window,
                "skip_days": self.skip_days,
                "signal_clip": self.signal_clip,
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
