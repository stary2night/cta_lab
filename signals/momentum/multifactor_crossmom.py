"""Multi-factor cross-sectional momentum signal for commodity futures."""

from __future__ import annotations

import numpy as np
import pandas as pd

from signals.base import CrossSectionalSignal
from signals.momentum.multifactor_trend import MultiFactorTrendSignal


class MultiFactorCrossSectionalMomentumSignal(CrossSectionalSignal):
    """CS-style four-factor sector-relative momentum signal.

    The signal follows the factor families described in the global commodity
    CTA notes:
    F1. Vol-adjusted long-horizon cumulative return
    F2. Long-horizon price breakout location
    F3. Breakout of long-horizon mean return over a long window
    F4. Residual of short mean return vs long mean return

    Each factor is ranked within sectors and converted to {-1, 0, +1}; the
    final signal is the average across factors.
    """

    def __init__(
        self,
        sector_map: dict[str, str],
        lookback: int = 240,
        short_mean_window: int = 120,
        vol_window: int = 20,
        top_pct: float = 0.20,
        bottom_pct: float = 0.20,
        min_periods: int | None = None,
    ) -> None:
        if lookback <= 1:
            raise ValueError("lookback must be > 1")
        if short_mean_window <= 1:
            raise ValueError("short_mean_window must be > 1")
        if vol_window <= 1:
            raise ValueError("vol_window must be > 1")
        if not (0 < top_pct <= 1):
            raise ValueError("top_pct must be in (0, 1]")
        if not (0 < bottom_pct <= 1):
            raise ValueError("bottom_pct must be in (0, 1]")
        if top_pct + bottom_pct > 1:
            raise ValueError("top_pct + bottom_pct must be <= 1")

        self.sector_map = dict(sector_map)
        self.lookback = int(lookback)
        self.short_mean_window = int(short_mean_window)
        self.vol_window = int(vol_window)
        self.top_pct = float(top_pct)
        self.bottom_pct = float(bottom_pct)
        self.min_periods = min_periods if min_periods is not None else max(self.lookback // 2, 1)
        self._trend_helper = MultiFactorTrendSignal(
            trend_window=self.lookback,
            short_mean_window=self.short_mean_window,
            vol_window=self.vol_window,
            breakout_windows=(self.vol_window, max(self.lookback // 4, 2), self.lookback),
            residual_windows=(self.lookback, max(self.short_mean_window, 2)),
        )

    def compute(self, returns_df: pd.DataFrame) -> pd.DataFrame:
        """Compute sector-relative multi-factor momentum signal."""

        factors = self.factor_dict(returns_df)
        ranked = [self._rank_factor_within_sector(factor) for factor in factors.values()]
        combined = sum(ranked) / len(ranked)  # type: ignore[arg-type]
        return combined.replace([np.inf, -np.inf], np.nan).fillna(0.0)

    def compute_factor_portfolio_weights(
        self,
        returns_df: pd.DataFrame,
        inv_vol_weighting: bool = False,
    ) -> pd.DataFrame:
        """Build long/short portfolio for each factor, then average.

        Each raw factor first selects top/bottom buckets within sectors. The
        selected long leg is normalized to +0.5 and the short leg to -0.5 at
        the factor level, preserving the document-style dollar-neutral sleeve.
        The final cross-sectional momentum sleeve is the equal-weight average
        of the four factor portfolios.

        When inv_vol_weighting=True, weights within each long/short bucket are
        proportional to inverse realized volatility (rolling vol_window std)
        instead of equal-weight.
        """

        vol_df: pd.DataFrame | None = None
        if inv_vol_weighting:
            vol_df = returns_df.rolling(
                self.vol_window, min_periods=max(self.vol_window // 2, 1)
            ).std()

        factor_positions = [
            self._signal_to_dollar_neutral_weights(
                self._rank_factor_within_sector(factor).fillna(0.0),
                vol_df=vol_df,
            )
            for factor in self.factor_dict(returns_df).values()
        ]
        combined = sum(factor_positions) / len(factor_positions)  # type: ignore[arg-type]
        return combined.replace([np.inf, -np.inf], np.nan).fillna(0.0)

    def compute_sector_inverse_vol_portfolio_weights(
        self,
        returns_df: pd.DataFrame,
        vol_halflife: int = 21,
        vol_min_periods: int | None = None,
    ) -> pd.DataFrame:
        """Build cross-momentum sleeve via sector-neutral portfolios.

        For each factor, this first builds a dollar-neutral portfolio inside
        each sector. Sector sleeves are then combined with inverse realized
        volatility weights, and the four factor portfolios are averaged. This
        keeps the current global-equal design available while enabling a more
        risk-balanced experimental branch.
        """

        if vol_halflife <= 0:
            raise ValueError("vol_halflife must be > 0")

        min_periods = vol_min_periods if vol_min_periods is not None else vol_halflife
        factor_positions = [
            self._sector_inverse_vol_weights(
                signal=self._rank_factor_within_sector(factor).fillna(0.0),
                returns_df=returns_df,
                vol_halflife=vol_halflife,
                vol_min_periods=min_periods,
            )
            for factor in self.factor_dict(returns_df).values()
        ]
        combined = sum(factor_positions) / len(factor_positions)  # type: ignore[arg-type]
        return combined.replace([np.inf, -np.inf], np.nan).fillna(0.0)

    def factor_dict(self, returns_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
        """Return raw factor matrices before sector-relative ranking."""

        returns = returns_df.replace([np.inf, -np.inf], np.nan)
        price = (1.0 + returns.fillna(0.0)).cumprod()
        long_mean = returns.rolling(self.lookback, min_periods=self.min_periods).mean()
        short_mean = returns.rolling(
            self.short_mean_window,
            min_periods=max(self.short_mean_window // 2, 1),
        ).mean()
        valid_history = returns.notna().rolling(self.lookback, min_periods=1).sum()
        warmup_mask = valid_history < self.min_periods

        factors = {
            "vol_adj_return": self._trend_helper._vol_adjusted_return(returns),
            "price_breakout": self._trend_helper._breakout_location(price, self.lookback),
            "mean_breakout": self._trend_helper._breakout_location(long_mean, self.lookback),
            "mean_residual": self._trend_helper._mean_residual(short_mean, long_mean, self.lookback),
        }
        return {name: factor.mask(warmup_mask) for name, factor in factors.items()}

    @staticmethod
    def _signal_to_dollar_neutral_weights(
        signal: pd.DataFrame,
        vol_df: pd.DataFrame | None = None,
    ) -> pd.DataFrame:
        long_mask = signal > 0
        short_mask = signal < 0

        if vol_df is not None:
            inv_vol = (1.0 / vol_df.reindex_like(signal).replace(0, np.nan)).replace(
                [np.inf, -np.inf], np.nan
            )
            long_inv = inv_vol.where(long_mask, 0.0)
            short_inv = inv_vol.where(short_mask, 0.0)
            long_sum = long_inv.sum(axis=1).replace(0, np.nan)
            short_sum = short_inv.sum(axis=1).replace(0, np.nan)
            long_weights = long_inv.div(long_sum, axis=0).fillna(0.0) * 0.5
            short_weights = short_inv.div(short_sum, axis=0).fillna(0.0) * -0.5
        else:
            n_long = long_mask.sum(axis=1).replace(0, np.nan)
            n_short = short_mask.sum(axis=1).replace(0, np.nan)
            long_weights = long_mask.astype(float).div(n_long, axis=0).fillna(0.0) * 0.5
            short_weights = short_mask.astype(float).div(n_short, axis=0).fillna(0.0) * -0.5

        return (long_weights + short_weights).fillna(0.0)

    def _sector_inverse_vol_weights(
        self,
        signal: pd.DataFrame,
        returns_df: pd.DataFrame,
        vol_halflife: int,
        vol_min_periods: int,
    ) -> pd.DataFrame:
        sector_portfolios = self._sector_neutral_portfolios(signal)
        if not sector_portfolios:
            return pd.DataFrame(0.0, index=signal.index, columns=signal.columns)

        sector_pnls = pd.DataFrame(
            {
                sector: (weights.shift(1).fillna(0.0) * returns_df.reindex_like(weights).fillna(0.0)).sum(axis=1)
                for sector, weights in sector_portfolios.items()
            },
            index=signal.index,
        )
        sector_vol = sector_pnls.ewm(halflife=vol_halflife, min_periods=vol_min_periods).std()
        inv_vol = (1.0 / sector_vol.replace(0.0, np.nan)).replace([np.inf, -np.inf], np.nan)
        active = pd.DataFrame(
            {sector: weights.abs().sum(axis=1) > 0.0 for sector, weights in sector_portfolios.items()},
            index=signal.index,
        )
        inv_vol = inv_vol.where(active)
        sector_budget = inv_vol.div(inv_vol.sum(axis=1).replace(0.0, np.nan), axis=0).fillna(0.0)

        combined = pd.DataFrame(0.0, index=signal.index, columns=signal.columns)
        for sector, weights in sector_portfolios.items():
            combined = combined.add(weights.mul(sector_budget[sector], axis=0), fill_value=0.0)
        return combined.fillna(0.0)

    def _sector_neutral_portfolios(self, signal: pd.DataFrame) -> dict[str, pd.DataFrame]:
        portfolios: dict[str, pd.DataFrame] = {}
        groups: dict[str, list[str]] = {}
        for symbol in signal.columns:
            sector = self.sector_map.get(str(symbol), "Other")
            groups.setdefault(sector, []).append(symbol)

        for sector, symbols in groups.items():
            sector_signal = signal[symbols]
            long_mask = sector_signal > 0
            short_mask = sector_signal < 0
            n_long = long_mask.sum(axis=1).replace(0, np.nan)
            n_short = short_mask.sum(axis=1).replace(0, np.nan)
            long_weights = long_mask.astype(float).div(n_long, axis=0).fillna(0.0) * 0.5
            short_weights = short_mask.astype(float).div(n_short, axis=0).fillna(0.0) * -0.5
            weights = (long_weights + short_weights).reindex(columns=signal.columns, fill_value=0.0)
            portfolios[sector] = weights.fillna(0.0)

        return portfolios

    def _rank_factor_within_sector(self, factor: pd.DataFrame) -> pd.DataFrame:
        symbols = [symbol for symbol in factor.columns if not factor[symbol].isna().all()]
        signal = pd.DataFrame(np.nan, index=factor.index, columns=factor.columns)

        groups: dict[str, list[str]] = {}
        for symbol in symbols:
            sector = self.sector_map.get(str(symbol), "Other")
            groups.setdefault(sector, []).append(symbol)

        for sector_symbols in groups.values():
            if not sector_symbols:
                continue
            values = factor[sector_symbols]
            counts = values.notna().sum(axis=1)
            max_side = (counts // 2).clip(lower=1)
            n_long = pd.Series(
                np.ceil(counts * self.top_pct).astype(int),
                index=values.index,
            )
            n_short = pd.Series(
                np.ceil(counts * self.bottom_pct).astype(int),
                index=values.index,
            )
            n_long = n_long.where(counts >= 2, 0).mask((counts >= 2) & (n_long < 1), 1)
            n_short = n_short.where(counts >= 2, 0).mask((counts >= 2) & (n_short < 1), 1)
            n_long = n_long.clip(upper=max_side)
            n_short = n_short.clip(upper=max_side)

            rank_asc = values.rank(axis=1, method="first", ascending=True, na_option="keep")
            rank_desc = values.rank(axis=1, method="first", ascending=False, na_option="keep")
            long_mask = rank_desc.le(n_long, axis=0)
            short_mask = rank_asc.le(n_short, axis=0)
            overlap = long_mask & short_mask
            long_mask = long_mask & ~overlap
            short_mask = short_mask & ~overlap
            sector_signal = pd.DataFrame(0.0, index=factor.index, columns=sector_symbols)
            sector_signal[short_mask] = -1.0
            sector_signal[long_mask] = 1.0
            sector_signal[counts == 0] = np.nan
            signal[sector_symbols] = sector_signal

        return signal
