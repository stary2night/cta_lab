"""China futures skew reversal strategy."""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from backtest.costs import ProportionalCostModel
from backtest.vectorized import VectorizedBacktest
from signals.reversal.skew_reversal import SkewReversalSignal
from strategies.base.strategy import StrategyBase
from strategies.context import StrategyContext

from .config import SkewReversalConfig, coerce_config
from .result import SkewReversalRunResult


class SkewReversalStrategy(StrategyBase):
    """中国期货偏度反转策略。

    参考《全球商品CTA策略整合文档》5.4 / 5A.3：
    - 130/195/260 日多窗口偏度均值
    - 截面做多最负偏 25%，做空最正偏 25%
    - 10 日持仓量下降过滤
    - 20 份轮动调仓
    """

    def __init__(
        self,
        config: SkewReversalConfig | dict | None = None,
    ) -> None:
        cfg = coerce_config(config)
        super().__init__(cfg.to_dict())

        self.typed_config = cfg
        self.min_obs = cfg.min_obs
        self.skew_windows = tuple(cfg.skew_windows)
        self.top_pct = cfg.top_pct
        self.bottom_pct = cfg.bottom_pct
        self.oi_lookback = cfg.oi_lookback
        self.rebalance_buckets = cfg.rebalance_buckets
        self.close_settle_blend_alpha = cfg.close_settle_blend_alpha
        self.use_close_settle_correction = cfg.use_close_settle_correction
        self.min_listing_days = cfg.min_listing_days
        self.liquidity_lookback = cfg.liquidity_lookback
        self.liquidity_threshold_pre2017 = cfg.liquidity_threshold_pre2017
        self.liquidity_threshold_post2017 = cfg.liquidity_threshold_post2017
        self.smoothing_window = cfg.smoothing_window
        self.vol_scale_windows = tuple(cfg.vol_scale_windows)
        self.selection_weighting = cfg.selection_weighting
        self.apply_asset_vol_scale = cfg.apply_asset_vol_scale
        self.max_gross_exposure = cfg.max_gross_exposure
        self.vol_halflife = cfg.vol_halflife
        self.target_vol = cfg.target_vol
        self.trading_days = cfg.trading_days
        self.transaction_cost_bps = cfg.transaction_cost_bps
        self.transaction_cost_rate = cfg.transaction_cost_bps / 10_000.0
        self.momentum_filter_window = cfg.momentum_filter_window
        self.momentum_filter_threshold = cfg.momentum_filter_threshold
        self.sector_cap = cfg.sector_cap
        self.exclude = set(cfg.exclude)

    def generate_signals(self, price_df: pd.DataFrame) -> pd.DataFrame:
        """StrategyBase-compatible signal generation from a price matrix."""

        returns_df = price_df.pct_change()
        _, _, skew_factor = self.generate_skew_factor(returns_df, returns_df)
        return skew_factor.fillna(0.0)

    def generate_skew_factor(
        self,
        settle_returns: pd.DataFrame,
        close_returns: pd.DataFrame | None = None,
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        """Compute settle skew, close skew and corrected skew factor."""

        signal = SkewReversalSignal(windows=self.skew_windows)
        settle_skew = signal.compute_from_returns(settle_returns)

        if close_returns is None or close_returns.empty:
            return settle_skew, settle_skew.copy(), settle_skew

        close_skew = signal.compute_from_returns(close_returns)
        blended = (
            self.close_settle_blend_alpha * close_skew
            + (1.0 - self.close_settle_blend_alpha) * settle_skew
        )

        if self.use_close_settle_correction:
            conflict = np.sign(close_skew).mul(np.sign(settle_skew)).lt(0.0)
            blended = blended.where(~conflict, close_skew)

        return settle_skew, close_skew, blended

    def build_daily_positions(
        self,
        skew_factor: pd.DataFrame,
        oi_change: pd.DataFrame | None = None,
        tradable_mask: pd.DataFrame | None = None,
        sigma_max: pd.DataFrame | None = None,
        momentum_df: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        """Build daily long-short reversal targets before staggered rebalancing.

        When ``momentum_df`` is provided and ``momentum_filter_window > 0``:
        - Long candidates (negative skew) are dropped if their rolling return
          is below ``-momentum_filter_threshold`` (strong downtrend — skip recovery bet).
        - Short candidates (positive skew) are dropped if their rolling return
          is above ``+momentum_filter_threshold`` (strong uptrend — skip fading).
        """

        positions = pd.DataFrame(0.0, index=skew_factor.index, columns=skew_factor.columns)
        oi_gate = None if oi_change is None else oi_change.lt(0.0)
        use_mom_filter = (
            momentum_df is not None and self.momentum_filter_window > 0
        )

        for date in skew_factor.index:
            row = skew_factor.loc[date].dropna()
            if tradable_mask is not None and date in tradable_mask.index:
                row = row[tradable_mask.loc[date].reindex(row.index).fillna(False)]
            if row.empty:
                continue

            n_assets = len(row)
            n_long = max(1, int(np.floor(n_assets * self.bottom_pct)))
            n_short = max(1, int(np.floor(n_assets * self.top_pct)))

            long_names = list(row.nsmallest(n_long).index)
            short_names = list(row.nlargest(n_short).index)

            # Momentum confirmation filter: suppress signals fighting strong trends
            if use_mom_filter and date in momentum_df.index:
                mom_row = momentum_df.loc[date]
                thr = self.momentum_filter_threshold
                # Drop long candidates in confirmed strong downtrend (return < -thr)
                long_names = [
                    s for s in long_names
                    if pd.isna(mom_row.get(s, np.nan)) or mom_row.get(s, 0.0) >= -thr
                ]
                # Drop short candidates in confirmed strong uptrend (return > +thr)
                short_names = [
                    s for s in short_names
                    if pd.isna(mom_row.get(s, np.nan)) or mom_row.get(s, 0.0) <= thr
                ]

            if oi_gate is not None and date in oi_gate.index:
                gate = oi_gate.loc[date]
                long_names = [symbol for symbol in long_names if bool(gate.get(symbol, False))]
                short_names = [symbol for symbol in short_names if bool(gate.get(symbol, False))]

            if long_names:
                long_weights = self._bucket_weights(
                    names=long_names,
                    sigma_row=None if sigma_max is None else sigma_max.loc[date],
                    target_sum=0.5,
                )
                positions.loc[date, long_weights.index] = long_weights.values
            if short_names:
                short_weights = self._bucket_weights(
                    names=short_names,
                    sigma_row=None if sigma_max is None else sigma_max.loc[date],
                    target_sum=-0.5,
                )
                positions.loc[date, short_weights.index] = short_weights.values

        return positions.fillna(0.0)

    def _bucket_weights(
        self,
        names: list[str],
        sigma_row: pd.Series | None,
        target_sum: float,
    ) -> pd.Series:
        """Allocate a long or short bucket using equal or inverse-vol weights."""

        if not names:
            return pd.Series(dtype=float)
        if self.selection_weighting == "equal" or sigma_row is None:
            base = pd.Series(1.0, index=names, dtype=float)
        else:
            sigma = pd.to_numeric(sigma_row.reindex(names), errors="coerce")
            base = (1.0 / sigma.replace(0, np.nan)).replace([np.inf, -np.inf], np.nan).dropna()
            if base.empty:
                base = pd.Series(1.0, index=names, dtype=float)
        base = base / base.sum()
        return base * target_sum

    def smooth_positions(self, raw_positions: pd.DataFrame) -> pd.DataFrame:
        """Apply rolling mean smoothing to target weights."""

        if self.smoothing_window <= 1:
            return raw_positions.fillna(0.0)
        return (
            raw_positions
            .rolling(self.smoothing_window, min_periods=1)
            .mean()
            .fillna(0.0)
        )

    def compute_sigma_max(self, returns_df: pd.DataFrame) -> pd.DataFrame:
        """Compute max rolling asset volatility across configured windows."""

        rolling_vols = [
            returns_df.rolling(window, min_periods=window).std()
            for window in self.vol_scale_windows
        ]
        return (
            pd.concat(rolling_vols, axis=1, keys=range(len(rolling_vols)))
            .T.groupby(level=1)
            .max()
            .T
        )

    def compute_vol_scale(self, sigma_max: pd.DataFrame) -> pd.DataFrame:
        """Compute per-asset volatility compression coefficients.

        Following the document-style rule, use the max of multiple rolling
        volatility windows and compress positions when realized vol exceeds
        target daily volatility.
        """

        target_daily_vol = self.target_vol / np.sqrt(self.trading_days)
        scale = (target_daily_vol / sigma_max).clip(upper=1.0)
        return scale.replace([np.inf, -np.inf], np.nan).fillna(0.0)

    def apply_vol_scale(
        self,
        positions: pd.DataFrame,
        vol_scale: pd.DataFrame,
    ) -> pd.DataFrame:
        """Apply per-asset volatility compression to smoothed positions."""

        aligned_scale = vol_scale.reindex_like(positions).fillna(0.0)
        return positions.mul(aligned_scale).fillna(0.0)

    def build_tradable_mask(
        self,
        settle_prices: pd.DataFrame,
        open_interest: pd.DataFrame,
        contract_multiplier: pd.Series | None = None,
    ) -> pd.DataFrame:
        """Build dynamic universe mask from listing age and liquidity."""

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

    @staticmethod
    def resolve_contract_multiplier(
        context: StrategyContext,
        symbols: list[str],
    ) -> pd.Series:
        """Resolve lot-size multipliers when instrument metadata is available."""

        multipliers: dict[str, float] = {}
        for symbol in symbols:
            try:
                instrument = context.loader.load_instrument(symbol)
                multipliers[symbol] = float(instrument.lot_size)
            except Exception:
                multipliers[symbol] = 1.0
        return pd.Series(multipliers, dtype=float)

    def build_weights(
        self,
        signal_df: pd.DataFrame,
        vol_df: pd.DataFrame,
        corr_cache: dict | None = None,
    ) -> pd.DataFrame:
        """StrategyBase-compatible weight build without OI filter input."""

        del vol_df, corr_cache
        daily_positions = self.build_daily_positions(signal_df)
        return self.apply_staggered_rebalance(daily_positions)

    def apply_sector_cap(
        self,
        positions: pd.DataFrame,
        sector_symbol_map: dict[str, str],
    ) -> pd.DataFrame:
        """Scale down any sector whose gross weight exceeds ``sector_cap`` of total gross.

        Operates row-by-row on the raw target positions before smoothing so the
        cap is enforced at each rebalance date independently.  Total gross
        exposure is preserved only approximately (each sector is scaled
        independently; other sectors are not rescaled upward).

        Parameters
        ----------
        positions:
            Daily target weight DataFrame (positive = long, negative = short).
        sector_symbol_map:
            ``{symbol: sector_name}`` dict; unknown symbols fall into "Other".
        """
        if self.sector_cap <= 0 or not sector_symbol_map:
            return positions

        from collections import defaultdict

        sector_cols: dict[str, list[str]] = defaultdict(list)
        for sym in positions.columns:
            sec = sector_symbol_map.get(sym, "Other")
            sector_cols[sec].append(sym)

        result = positions.copy()
        for date in positions.index:
            row = result.loc[date]
            total_gross = row.abs().sum()
            if total_gross == 0.0:
                continue
            for sec, syms in sector_cols.items():
                available = [s for s in syms if s in row.index]
                if not available:
                    continue
                sec_gross = row[available].abs().sum()
                cap_abs = self.sector_cap * total_gross
                if sec_gross > cap_abs and sec_gross > 0.0:
                    result.loc[date, available] *= cap_abs / sec_gross
        return result

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

    def _make_backtest(self) -> VectorizedBacktest:
        return VectorizedBacktest(
            lag=1,
            vol_target=self.target_vol,
            vol_halflife=self.vol_halflife,
            trading_days=self.trading_days,
            max_gross_exposure=self.max_gross_exposure,
            cost_model=ProportionalCostModel(self.transaction_cost_rate),
        )

    def run_vectorized(
        self,
        returns_df: pd.DataFrame,
        backtest=None,
        close_returns_df: pd.DataFrame | None = None,
        oi_change_df: pd.DataFrame | None = None,
        ):
        """StrategyBase-compatible vectorized path on prepared matrices."""

        settle_skew, _, skew_factor = self.generate_skew_factor(returns_df, close_returns_df)
        sigma_max = self.compute_sigma_max(returns_df)
        raw_positions = self.build_daily_positions(
            skew_factor,
            oi_change=oi_change_df,
            sigma_max=sigma_max,
        )
        smoothed_positions = self.smooth_positions(raw_positions)
        vol_scale = self.compute_vol_scale(sigma_max)
        daily_positions = (
            self.apply_vol_scale(smoothed_positions, vol_scale)
            if self.apply_asset_vol_scale
            else smoothed_positions
        )
        positions = self.apply_staggered_rebalance(daily_positions)
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
        lot_size_map: "dict[str, float] | pd.Series | None" = None,
    ) -> SkewReversalRunResult:
        """Load data, compute skew reversal signals, and run vectorized backtest."""

        if context is None:
            from data.loader import DataLoader, KlineSchema
            from data.sources.parquet_source import ParquetSource

            if data_dir is None:
                raise ValueError("data_dir must be provided via run_pipeline()")
            context = StrategyContext(
                loader=DataLoader(
                    kline_source=ParquetSource(Path(data_dir)),
                    kline_schema=KlineSchema.tushare(),
                ),
                sector_map={},
                backtest=None,
            )

        if verbose:
            print("=" * 65)
            print("Step 1: Load China futures returns / close / settle / open_interest")
            print("=" * 65)

        returns = context.load_returns_matrix(
            tickers=tickers,
            start=start,
            end=end,
            min_obs=self.min_obs,
            exclude=self.exclude,
        )
        if returns.empty:
            raise RuntimeError("No returns loaded. Check data_dir and tickers.")

        symbols = returns.columns.tolist()
        close_prices = context.load_continuous_field_matrix(
            field_name="close",
            tickers=symbols,
            start=start,
            end=end,
        ).reindex(index=returns.index, columns=symbols)
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
        close_returns = context.load_continuous_field_returns_matrix(
            field_name="close",
            tickers=symbols,
            start=start,
            end=end,
            zero_on_roll=True,
            clip_abs_return=0.5,
        ).reindex(index=returns.index, columns=symbols)
        settle_returns = context.load_continuous_field_returns_matrix(
            field_name="settle",
            tickers=symbols,
            start=start,
            end=end,
            zero_on_roll=True,
            clip_abs_return=0.5,
        ).reindex(index=returns.index, columns=symbols)
        if close_returns.empty:
            close_returns = returns.copy()
        if settle_returns.empty:
            settle_returns = returns.copy()
        oi_change = (open_interest / open_interest.shift(self.oi_lookback) - 1.0).reindex_like(returns)
        if lot_size_map is not None:
            if isinstance(lot_size_map, pd.Series):
                contract_multiplier = lot_size_map.reindex(symbols).fillna(1.0)
            else:
                contract_multiplier = pd.Series(
                    {s: lot_size_map.get(s, 1.0) for s in symbols}, dtype=float
                )
        else:
            contract_multiplier = self.resolve_contract_multiplier(context, symbols)
        tradable_mask = self.build_tradable_mask(
            settle_prices=settle_prices,
            open_interest=open_interest,
            contract_multiplier=contract_multiplier,
        ).reindex(index=returns.index, columns=symbols).fillna(False)

        if verbose:
            print(
                f"\nReturns matrix: {returns.shape} "
                f"({returns.index[0].date()} - {returns.index[-1].date()})"
            )
            print("\n" + "=" * 65)
            print("Step 2: Compute skew factor, OI filter, and staggered positions")
            print("=" * 65)

        settle_skew, close_skew, skew_factor = self.generate_skew_factor(
            settle_returns,
            close_returns,
        )
        sigma_max = self.compute_sigma_max(settle_returns)

        # Momentum confirmation filter: rolling return over filter window
        momentum_df: pd.DataFrame | None = None
        if self.momentum_filter_window > 0:
            momentum_df = settle_prices.pct_change(self.momentum_filter_window).reindex_like(returns)

        raw_positions = self.build_daily_positions(
            skew_factor,
            oi_change=oi_change,
            tradable_mask=tradable_mask,
            sigma_max=sigma_max,
            momentum_df=momentum_df,
        )

        # Sector concentration cap: limit any single sector's gross weight
        if self.sector_cap > 0:
            try:
                from data.universe.sectors import SECTOR_MAP, build_symbol_sector_map

                sector_symbol_map = build_symbol_sector_map(SECTOR_MAP)
                raw_positions = self.apply_sector_cap(raw_positions, sector_symbol_map)
            except ImportError:
                pass  # sectors module unavailable; skip cap

        smoothed_positions = self.smooth_positions(raw_positions)
        vol_scale = self.compute_vol_scale(sigma_max)
        daily_positions = (
            self.apply_vol_scale(smoothed_positions, vol_scale)
            if self.apply_asset_vol_scale
            else smoothed_positions
        )
        positions = self.apply_staggered_rebalance(daily_positions)

        if verbose:
            active_share = raw_positions.ne(0.0).mean().mean()
            gross_daily = daily_positions.abs().sum(axis=1)
            gross_live = positions.abs().sum(axis=1)
            print(
                f"  Active target share={active_share:.1%}  "
                f"Daily target gross mean/max={gross_daily.mean():.2f}/{gross_daily.max():.2f}"
            )
            print(
                f"  Rebalance buckets={self.rebalance_buckets}  "
                f"Live gross mean/max={gross_live.mean():.2f}/{gross_live.max():.2f}"
            )
            print("\n" + "=" * 65)
            print("Step 3: VectorizedBacktest — skew reversal")
            print("=" * 65)

        bt = context.backtest if context.backtest is not None else self._make_backtest()
        bt_result = bt.run(positions, returns)
        pnl = bt_result.returns.iloc[1:]

        if verbose and not pnl.empty:
            self._print_summary(pnl, "SkewReversal")

        return SkewReversalRunResult(
            returns=returns,
            settle_returns=settle_returns,
            close_returns=close_returns,
            open_interest=open_interest,
            oi_change=oi_change,
            settle_skew=settle_skew,
            close_skew=close_skew,
            skew_factor=skew_factor,
            raw_positions=raw_positions,
            smoothed_positions=smoothed_positions,
            vol_scale=vol_scale,
            daily_positions=daily_positions,
            positions=positions,
            pnl=pnl,
            backtest_result=bt_result,
            metadata={
                "skew_windows": list(self.skew_windows),
                "top_pct": self.top_pct,
                "bottom_pct": self.bottom_pct,
                "oi_lookback": self.oi_lookback,
                "rebalance_buckets": self.rebalance_buckets,
                "close_settle_blend_alpha": self.close_settle_blend_alpha,
                "use_close_settle_correction": self.use_close_settle_correction,
                "min_listing_days": self.min_listing_days,
                "liquidity_lookback": self.liquidity_lookback,
                "liquidity_threshold_pre2017": self.liquidity_threshold_pre2017,
                "liquidity_threshold_post2017": self.liquidity_threshold_post2017,
                "smoothing_window": self.smoothing_window,
                "vol_scale_windows": list(self.vol_scale_windows),
                "selection_weighting": self.selection_weighting,
                "apply_asset_vol_scale": self.apply_asset_vol_scale,
                "target_vol": self.target_vol,
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
