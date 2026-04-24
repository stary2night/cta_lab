"""MultiFactorCTAStrategy：趋势 sleeve + 截面动量 sleeve 的国内期货 CTA。"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from backtest.costs import ProportionalCostModel
from backtest.vectorized import VectorizedBacktest
from signals.momentum.multifactor_crossmom import MultiFactorCrossSectionalMomentumSignal
from signals.momentum.multifactor_trend import MultiFactorTrendSignal
from strategies.base.strategy import StrategyBase
from strategies.context import StrategyContext

from .config import MultiFactorCTAConfig, coerce_config
from .result import MultiFactorCTARunResult


class MultiFactorCTAStrategy(StrategyBase):
    """中国期货多因子 CTA 研究策略。

    第一版只使用现有主力合约收益率矩阵可直接支持的模块：
    - MultiFactorTrendSignal 七因子时序趋势信号
    - MultiFactorCrossSectionalMomentumSignal 四因子板块内截面动量信号
    - 趋势 sleeve 独立做 inverse-vol sizing、单品种上限和 gross exposure 上限
    - 截面动量 sleeve 独立做四因子行业内多空等权组合
    - 组合层按 sleeve 权重混合后统一回测、波控和扣费
    """

    def __init__(
        self,
        config: MultiFactorCTAConfig | dict | None = None,
    ) -> None:
        cfg = coerce_config(config)
        super().__init__(cfg.to_dict())

        self.typed_config = cfg
        self.min_obs = cfg.min_obs
        self.trend_window = cfg.trend_window
        self.trend_short_mean_window = cfg.trend_short_mean_window
        self.trend_vol_window = cfg.trend_vol_window
        self.trend_breakout_windows = cfg.trend_breakout_windows
        self.trend_residual_windows = cfg.trend_residual_windows
        self.cross_lookback = cfg.cross_lookback
        self.cross_short_mean_window = cfg.cross_short_mean_window
        self.cross_vol_window = cfg.cross_vol_window
        self.cross_weighting = cfg.cross_weighting
        self.cross_sector_vol_halflife = cfg.cross_sector_vol_halflife
        self.trend_weight = cfg.trend_weight
        self.cross_weight = cfg.cross_weight
        self.short_filter_mode = cfg.short_filter_mode
        self.short_windows = cfg.short_windows
        self.donchian_window = cfg.donchian_window
        self.donchian_upper = cfg.donchian_upper
        self.donchian_lower = cfg.donchian_lower
        self.smoothing_window = cfg.smoothing_window
        self.max_abs_weight = cfg.max_abs_weight
        self.max_gross_exposure = cfg.max_gross_exposure
        self.top_pct = cfg.top_pct
        self.bottom_pct = cfg.bottom_pct
        self.vol_halflife = cfg.vol_halflife
        self.sigma_halflife = cfg.sigma_halflife
        self.target_vol = cfg.target_vol
        self.trading_days = cfg.trading_days
        self.transaction_cost_bps = cfg.transaction_cost_bps
        self.transaction_cost_rate = cfg.transaction_cost_bps / 10_000.0
        self.exclude = set(cfg.exclude)
        self.sector_map = dict(cfg.sector_map)

    # ── signal layer ────────────────────────────────────────────────────────

    def resolve_sector_map(
        self,
        symbols: list[str] | pd.Index,
        context: StrategyContext | None = None,
        sector_map: dict[str, str] | None = None,
    ) -> dict[str, str]:
        """Map symbols to sectors, falling back to ``Other``."""

        if context is not None:
            return context.resolve_sector_map(symbols, sector_map=sector_map)
        base = sector_map if sector_map is not None else self.sector_map
        return {str(symbol): base.get(str(symbol), "Other") for symbol in symbols}

    def generate_trend_signal(self, returns_df: pd.DataFrame) -> pd.DataFrame:
        """CS-style seven-factor trend signal in (-1, 1)."""

        return MultiFactorTrendSignal(
            trend_window=self.trend_window,
            short_mean_window=self.trend_short_mean_window,
            vol_window=self.trend_vol_window,
            breakout_windows=tuple(self.trend_breakout_windows),
            residual_windows=tuple(self.trend_residual_windows),
        ).compute_from_returns(returns_df)

    def generate_cross_signal(
        self,
        returns_df: pd.DataFrame,
        context: StrategyContext | None = None,
        sector_map: dict[str, str] | None = None,
    ) -> pd.DataFrame:
        """Sector-relative four-factor momentum signal in [-1, 1]."""

        sm = self.resolve_sector_map(returns_df.columns, context=context, sector_map=sector_map)
        signal = MultiFactorCrossSectionalMomentumSignal(
            sector_map=sm,
            lookback=self.cross_lookback,
            short_mean_window=self.cross_short_mean_window,
            vol_window=self.cross_vol_window,
            top_pct=self.top_pct,
            bottom_pct=self.bottom_pct,
        )
        return signal.compute(returns_df).fillna(0.0)

    def build_cross_positions(
        self,
        returns_df: pd.DataFrame,
        context: StrategyContext | None = None,
        sector_map: dict[str, str] | None = None,
    ) -> pd.DataFrame:
        """Build the document-style cross-sectional momentum sleeve.

        The cross sleeve is constructed as four independent factor portfolios:
        sector-relative top/bottom buckets, equal-weight long book (+0.5) and
        short book (-0.5), then averaged across factors.
        """

        sm = self.resolve_sector_map(returns_df.columns, context=context, sector_map=sector_map)
        signal = MultiFactorCrossSectionalMomentumSignal(
            sector_map=sm,
            lookback=self.cross_lookback,
            short_mean_window=self.cross_short_mean_window,
            vol_window=self.cross_vol_window,
            top_pct=self.top_pct,
            bottom_pct=self.bottom_pct,
        )
        if self.cross_weighting == "global_equal":
            positions = signal.compute_factor_portfolio_weights(returns_df)
        elif self.cross_weighting == "global_inv_vol":
            positions = signal.compute_factor_portfolio_weights(returns_df, inv_vol_weighting=True)
        elif self.cross_weighting == "sector_inverse_vol":
            positions = signal.compute_sector_inverse_vol_portfolio_weights(
                returns_df,
                vol_halflife=self.cross_sector_vol_halflife,
            )
        else:
            raise ValueError(f"Unsupported cross_weighting: {self.cross_weighting}")
        return positions.reindex_like(returns_df).fillna(0.0)

    def generate_signals(self, price_df: pd.DataFrame) -> pd.DataFrame:
        """StrategyBase-compatible signal entry from a price matrix."""

        returns_df = price_df.pct_change()
        trend_signal = self.generate_trend_signal(returns_df)
        cross_signal = self.generate_cross_signal(returns_df)
        blended_signal = self.blend_signals(trend_signal, cross_signal)
        short_filter = self.compute_short_filter(returns_df)
        return self.apply_short_filter(blended_signal, short_filter)

    def blend_signals(
        self,
        trend_signal: pd.DataFrame,
        cross_signal: pd.DataFrame,
    ) -> pd.DataFrame:
        """Weighted signal blend, normalized by total absolute module weight."""

        total = self.trend_weight + self.cross_weight
        blended = (self.trend_weight * trend_signal + self.cross_weight * cross_signal) / total
        return blended.fillna(0.0)

    def compute_short_filter(self, returns_df: pd.DataFrame) -> pd.DataFrame:
        """Compute short-horizon direction filter.

        Output convention:
        - +1 means short-term direction is positive
        - -1 means short-term direction is negative
        - 0 means neutral / insufficient evidence
        """

        if self.short_filter_mode == "none":
            return pd.DataFrame(0.0, index=returns_df.index, columns=returns_df.columns)
        if self.short_filter_mode == "momentum_vote":
            votes = []
            log_returns = np.log1p(returns_df)
            for window in self.short_windows:
                votes.append(np.sign(log_returns.rolling(window, min_periods=window).sum()))
            score = sum(votes) / len(votes)  # type: ignore[arg-type]
            return np.sign(score).fillna(0.0)
        if self.short_filter_mode == "donchian":
            price = (1.0 + returns_df.fillna(0.0)).cumprod()
            roll_min = price.rolling(self.donchian_window, min_periods=self.donchian_window).min()
            roll_max = price.rolling(self.donchian_window, min_periods=self.donchian_window).max()
            location = (price - roll_min) / (roll_max - roll_min).replace(0, np.nan)
            filt = pd.DataFrame(0.0, index=returns_df.index, columns=returns_df.columns)
            filt[location >= self.donchian_upper] = 1.0
            filt[location <= self.donchian_lower] = -1.0
            return filt.fillna(0.0)
        raise ValueError(f"Unsupported short_filter_mode: {self.short_filter_mode}")

    @staticmethod
    def apply_short_filter(
        signal_df: pd.DataFrame,
        short_filter: pd.DataFrame,
    ) -> pd.DataFrame:
        """Zero out positions when medium-term and short-term directions conflict."""

        aligned_filter = short_filter.reindex_like(signal_df).fillna(0.0)
        conflict = ((signal_df > 0) & (aligned_filter < 0)) | (
            (signal_df < 0) & (aligned_filter > 0)
        )
        filtered = signal_df.copy()
        filtered[conflict] = 0.0
        return filtered.fillna(0.0)

    def compute_sigma(self, returns_df: pd.DataFrame) -> pd.DataFrame:
        """EWMA annualized asset volatility for inverse-vol sizing."""

        return (
            returns_df.ewm(halflife=self.sigma_halflife, min_periods=30).std()
            * np.sqrt(self.trading_days)
        )

    # ── portfolio construction ──────────────────────────────────────────────

    def build_weights(
        self,
        signal_df: pd.DataFrame,
        vol_df: pd.DataFrame,
        corr_cache: dict | None = None,
    ) -> pd.DataFrame:
        """Build capped and smoothed inverse-vol weights."""

        vol_safe = vol_df.replace(0, np.nan)
        raw = (signal_df / vol_safe).replace([np.inf, -np.inf], np.nan).fillna(0.0)
        raw = raw.clip(lower=-self.max_abs_weight, upper=self.max_abs_weight)

        if self.smoothing_window > 1:
            raw = raw.rolling(self.smoothing_window, min_periods=1).mean()

        gross = raw.abs().sum(axis=1)
        scale = (self.max_gross_exposure / gross).clip(upper=1.0)
        scale = scale.replace([np.inf, -np.inf], np.nan).fillna(0.0)
        return raw.mul(scale, axis=0).fillna(0.0)

    def build_trend_positions(
        self,
        trend_signal: pd.DataFrame,
        sigma: pd.DataFrame,
    ) -> pd.DataFrame:
        """Build the trend-following sleeve from trend signal and asset vol."""

        return self.build_weights(trend_signal, sigma)

    def blend_positions(
        self,
        trend_positions: pd.DataFrame,
        cross_positions: pd.DataFrame,
    ) -> pd.DataFrame:
        """Blend independently constructed strategy sleeves at the position layer."""

        total = self.trend_weight + self.cross_weight
        trend_aligned, cross_aligned = trend_positions.align(
            cross_positions,
            join="outer",
            axis=None,
            fill_value=0.0,
        )
        blended = (
            self.trend_weight * trend_aligned + self.cross_weight * cross_aligned
        ) / total
        return blended.replace([np.inf, -np.inf], np.nan).fillna(0.0)

    def _make_backtest(self) -> VectorizedBacktest:
        return VectorizedBacktest(
            lag=1,
            vol_target=self.target_vol,
            vol_halflife=self.vol_halflife,
            trading_days=self.trading_days,
            cost_model=ProportionalCostModel(self.transaction_cost_rate),
        )

    def run_vectorized(
        self,
        returns_df: pd.DataFrame,
        backtest=None,
        vol_window: int = 20,
    ):
        """Run the multi-factor CTA strategy on a returns matrix."""

        trend_signal = self.generate_trend_signal(returns_df)
        short_filter = self.compute_short_filter(returns_df)
        filtered_trend_signal = self.apply_short_filter(trend_signal, short_filter)
        sigma = self.compute_sigma(returns_df)
        trend_positions = self.build_trend_positions(filtered_trend_signal, sigma)
        cross_positions = self.build_cross_positions(returns_df)
        positions = self.blend_positions(trend_positions, cross_positions)
        bt = backtest if backtest is not None else self._make_backtest()
        return bt.run(positions, returns_df)

    # ── pipeline ────────────────────────────────────────────────────────────

    def run_pipeline(
        self,
        context: StrategyContext | None = None,
        data_dir: str | Path | None = None,
        tickers: list[str] | None = None,
        start: str | None = None,
        end: str | None = None,
        verbose: bool = True,
    ) -> MultiFactorCTARunResult:
        """Load data, build signals/weights, and run vectorized backtest."""

        if verbose:
            print("=" * 65)
            print("Step 1: Load china_daily_full returns")
            print("=" * 65)

        if context is not None:
            returns = context.load_returns_matrix(
                tickers=tickers,
                start=start,
                end=end,
                min_obs=self.min_obs,
                exclude=self.exclude,
            )
        else:
            from data.loader import DataLoader, KlineSchema
            from data.sources.parquet_source import ParquetSource

            if data_dir is None:
                raise ValueError("data_dir must be provided via run_pipeline()")
            loader = DataLoader(
                kline_source=ParquetSource(Path(data_dir)),
                kline_schema=KlineSchema.tushare(),
            )
            if tickers is None:
                tickers = loader.available_symbols(exclude=self.exclude)
            returns = loader.load_returns_matrix(
                tickers,
                start=start,
                end=end,
                min_obs=self.min_obs,
            )

        if returns.empty:
            raise RuntimeError("No returns loaded. Check data_dir and tickers.")

        sector_map = self.resolve_sector_map(returns.columns, context=context)

        if verbose:
            print(
                f"\nReturns matrix: {returns.shape} "
                f"({returns.index[0].date()} - {returns.index[-1].date()})"
            )
            print("\n" + "=" * 65)
            print("Step 2: Compute trend/cross sleeves and short filter")
            print("=" * 65)

        trend_signal = self.generate_trend_signal(returns)
        cross_signal = self.generate_cross_signal(returns, context=context, sector_map=sector_map)
        blended_signal = self.blend_signals(trend_signal, cross_signal)
        short_filter = self.compute_short_filter(returns)
        filtered_signal = self.apply_short_filter(trend_signal, short_filter)
        sigma = self.compute_sigma(returns)
        raw_trend_positions = self.build_trend_positions(trend_signal, sigma)
        trend_positions = self.build_trend_positions(filtered_signal, sigma)
        cross_positions = self.build_cross_positions(
            returns,
            context=context,
            sector_map=sector_map,
        )
        raw_positions = self.blend_positions(raw_trend_positions, cross_positions)
        positions = self.blend_positions(trend_positions, cross_positions)

        if verbose:
            conflict_rate = (filtered_signal.eq(0.0) & trend_signal.ne(0.0)).mean().mean()
            gross = positions.abs().sum(axis=1)
            net = positions.sum(axis=1)
            print(
                f"  Filter mode: {self.short_filter_mode}  "
                f"Filtered non-zero trend signal share: {conflict_rate:.1%}"
            )
            print(
                f"  Sleeve weights: trend={self.trend_weight:g}, "
                f"cross={self.cross_weight:g}  cross_weighting={self.cross_weighting}  "
                f"Position gross mean/max={gross.mean():.2f}/{gross.max():.2f}  "
                f"net abs mean={net.abs().mean():.2f}"
            )
            print("\n" + "=" * 65)
            print("Step 3: VectorizedBacktest — sleeve-blended positions")
            print("=" * 65)

        bt = context.backtest if context is not None and context.backtest is not None else self._make_backtest()
        bt_result = bt.run(positions, returns)
        pnl = bt_result.returns.iloc[1:]

        if verbose and not pnl.empty:
            self._print_summary(pnl, "MultiFactorCTA")

        return MultiFactorCTARunResult(
            returns=returns,
            trend_signal=trend_signal,
            cross_signal=cross_signal,
            blended_signal=blended_signal,
            short_filter=short_filter,
            filtered_signal=filtered_signal,
            raw_positions=raw_positions,
            trend_positions=trend_positions,
            cross_positions=cross_positions,
            positions=positions,
            pnl=pnl,
            sigma=sigma,
            sector_map=sector_map,
            backtest_result=bt_result,
            metadata={
                "trend_window": self.trend_window,
                "trend_short_mean_window": self.trend_short_mean_window,
                "trend_vol_window": self.trend_vol_window,
                "trend_breakout_windows": self.trend_breakout_windows,
                "trend_residual_windows": self.trend_residual_windows,
                "cross_lookback": self.cross_lookback,
                "cross_short_mean_window": self.cross_short_mean_window,
                "cross_vol_window": self.cross_vol_window,
                "cross_weighting": self.cross_weighting,
                "cross_sector_vol_halflife": self.cross_sector_vol_halflife,
                "top_pct": self.top_pct,
                "bottom_pct": self.bottom_pct,
                "trend_weight": self.trend_weight,
                "cross_weight": self.cross_weight,
                "short_filter_mode": self.short_filter_mode,
                "short_windows": self.short_windows,
                "smoothing_window": self.smoothing_window,
                "max_abs_weight": self.max_abs_weight,
                "max_gross_exposure": self.max_gross_exposure,
                "target_vol": self.target_vol,
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
