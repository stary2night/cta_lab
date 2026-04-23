"""Multi-factor trend signal inspired by commodity CTA index methodology.

This is a research implementation of the trend module described in the
``全球商品CTA策略整合文档`` notes. The source document gives factor families
but not exact formulas for every normalization detail, so this class keeps the
implementation explicit and auditable:

F1. Vol-adjusted long-horizon cumulative return
F2. Long-horizon price breakout location
F3. Breakout of long-horizon mean return over a short window
F4. Breakout of long-horizon mean return over a medium window
F5. Breakout of long-horizon mean return over a long window
F6. Residual of short mean return vs long mean return over a long regression window
F7. Residual of short mean return vs long mean return over a medium regression window
"""

from __future__ import annotations

import numpy as np
import pandas as pd

from signals.base import CrossSectionalSignal


class MultiFactorTrendSignal(CrossSectionalSignal):
    """Seven-factor time-series trend signal for futures returns matrices.

    Parameters
    ----------
    trend_window:
        Main long-horizon window. The CS-style notes use 240 trading days.
    short_mean_window:
        Short mean-return window used by residual factors.
    vol_window:
        Short realized volatility window for vol-adjusted cumulative return.
    breakout_windows:
        Windows applied to the long-horizon mean-return series for F3/F4/F5.
    residual_windows:
        Rolling regression windows for F6/F7.
    squash:
        ``tanh`` divisor used to map unbounded z-style factors into [-1, 1].
    """

    def __init__(
        self,
        trend_window: int = 240,
        short_mean_window: int = 120,
        vol_window: int = 20,
        breakout_windows: tuple[int, int, int] = (20, 60, 240),
        residual_windows: tuple[int, int] = (240, 120),
        squash: float = 2.0,
    ) -> None:
        if trend_window <= 1:
            raise ValueError("trend_window must be > 1")
        if short_mean_window <= 1:
            raise ValueError("short_mean_window must be > 1")
        if vol_window <= 1:
            raise ValueError("vol_window must be > 1")
        if any(w <= 1 for w in breakout_windows):
            raise ValueError("breakout_windows must contain values > 1")
        if any(w <= 1 for w in residual_windows):
            raise ValueError("residual_windows must contain values > 1")
        if squash <= 0:
            raise ValueError("squash must be > 0")

        self.trend_window = int(trend_window)
        self.short_mean_window = int(short_mean_window)
        self.vol_window = int(vol_window)
        self.breakout_windows = tuple(int(w) for w in breakout_windows)
        self.residual_windows = tuple(int(w) for w in residual_windows)
        self.squash = float(squash)

    def compute(self, price_matrix: pd.DataFrame) -> pd.DataFrame:
        """Compute signal from a price matrix."""

        return self.compute_from_returns(price_matrix.pct_change())

    def compute_from_returns(self, returns_df: pd.DataFrame) -> pd.DataFrame:
        """Compute seven-factor trend signal from a returns matrix."""

        returns = returns_df.replace([np.inf, -np.inf], np.nan)
        price = (1.0 + returns.fillna(0.0)).cumprod()
        long_mean = returns.rolling(self.trend_window, min_periods=self.trend_window // 2).mean()
        short_mean = returns.rolling(
            self.short_mean_window, min_periods=self.short_mean_window // 2
        ).mean()

        factors = [
            self._vol_adjusted_return(returns),
            self._breakout_location(price, self.trend_window),
        ]
        factors.extend(
            self._breakout_location(long_mean, window) for window in self.breakout_windows
        )
        factors.extend(
            self._mean_residual(short_mean, long_mean, window) for window in self.residual_windows
        )

        combined = sum(factors) / len(factors)  # type: ignore[arg-type]
        return combined.replace([np.inf, -np.inf], np.nan).fillna(0.0).clip(-1.0, 1.0)

    def factor_dict(self, returns_df: pd.DataFrame) -> dict[str, pd.DataFrame]:
        """Return individual factor matrices for diagnostics."""

        returns = returns_df.replace([np.inf, -np.inf], np.nan)
        price = (1.0 + returns.fillna(0.0)).cumprod()
        long_mean = returns.rolling(self.trend_window, min_periods=self.trend_window // 2).mean()
        short_mean = returns.rolling(
            self.short_mean_window, min_periods=self.short_mean_window // 2
        ).mean()

        return {
            "vol_adj_return": self._vol_adjusted_return(returns),
            "price_breakout": self._breakout_location(price, self.trend_window),
            "mean_breakout_short": self._breakout_location(long_mean, self.breakout_windows[0]),
            "mean_breakout_medium": self._breakout_location(long_mean, self.breakout_windows[1]),
            "mean_breakout_long": self._breakout_location(long_mean, self.breakout_windows[2]),
            "mean_residual_long": self._mean_residual(short_mean, long_mean, self.residual_windows[0]),
            "mean_residual_medium": self._mean_residual(short_mean, long_mean, self.residual_windows[1]),
        }

    def _vol_adjusted_return(self, returns: pd.DataFrame) -> pd.DataFrame:
        cum_ret = np.log1p(returns).rolling(
            self.trend_window, min_periods=self.trend_window // 2
        ).sum()
        vol = returns.rolling(self.vol_window, min_periods=self.vol_window).std()
        z = cum_ret / vol.replace(0, np.nan)
        return self._squash(z)

    @staticmethod
    def _breakout_location(values: pd.DataFrame, window: int) -> pd.DataFrame:
        roll_min = values.rolling(window, min_periods=max(window // 2, 1)).min()
        roll_max = values.rolling(window, min_periods=max(window // 2, 1)).max()
        location = (values - roll_min) / (roll_max - roll_min).replace(0, np.nan)
        return (2.0 * location - 1.0).replace([np.inf, -np.inf], np.nan).fillna(0.0)

    def _mean_residual(
        self,
        short_mean: pd.DataFrame,
        long_mean: pd.DataFrame,
        window: int,
    ) -> pd.DataFrame:
        cov = short_mean.rolling(window, min_periods=max(window // 2, 1)).cov(long_mean)
        var = long_mean.rolling(window, min_periods=max(window // 2, 1)).var()
        beta = cov / var.replace(0, np.nan)
        alpha = short_mean.rolling(window, min_periods=max(window // 2, 1)).mean() - beta * long_mean.rolling(
            window, min_periods=max(window // 2, 1)
        ).mean()
        residual = short_mean - (alpha + beta * long_mean)
        residual_vol = residual.rolling(window, min_periods=max(window // 2, 1)).std()
        return self._squash(residual / residual_vol.replace(0, np.nan))

    def _squash(self, values: pd.DataFrame) -> pd.DataFrame:
        return pd.DataFrame(
            np.tanh(values / self.squash),
            index=values.index,
            columns=values.columns,
        ).replace([np.inf, -np.inf], np.nan).fillna(0.0)
