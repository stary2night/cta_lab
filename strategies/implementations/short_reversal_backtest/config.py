"""Short-term (1-month) reversal strategy configuration."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from strategies.implementations.jpm_trend_trade.config import (
    EXCLUDE,
    TARGET_VOL,
    TRADING_DAYS,
    VOL_HALFLIFE,
)


REVERSAL_WINDOW: int = 21          # lookback trading days for cumulative return signal (~1 month)
SKIP_DAYS: int = 1                 # skip the most recent N days before the window (microstructure buffer)
SIGNAL_CLIP: float = 0.30          # clip raw log-return signal at ±30% before cross-sectional ranking
MIN_OBS: int = 65
MIN_LISTING_DAYS: int = 65
LIQUIDITY_LOOKBACK: int = 20
LIQUIDITY_THRESHOLD_PRE2017: float = 1.0e9
LIQUIDITY_THRESHOLD_POST2017: float = 2.0e9
REBALANCE_BUCKETS: int = 20
SELECTION_WEIGHTING: str = "inv_vol"
VOL_SCALE_WINDOWS: list[int] = [20, 60, 120]
APPLY_PORTFOLIO_VOL_SCALE: bool = True
MAX_ABS_WEIGHT: float = 0.06
MAX_GROSS_EXPOSURE: float = 1.0
TRANSACTION_COST_BPS: float = 5.0


@dataclass(slots=True)
class ShortReversalConfig:
    """Typed configuration for the China futures short-term reversal strategy.

    Signal construction
    -------------------
    For each instrument in the tradable universe:

        raw_signal_i = -[log(price_i,t-skip) - log(price_i,t-skip-window)]

    The sign is **negative** because the strategy is *contrarian*: short
    recent outperformers, long recent underperformers.  The raw log-return
    is clipped to ±signal_clip before cross-sectional ranking.

    ``skip_days`` (default 1) drops the most recent trading day before the
    measurement window.  This avoids microstructure bid-ask bounce effects
    where the same day's closing price move is spuriously reversed next day.

    Unlike carry / basis strategies, no far-leg contract data is required;
    the strategy operates entirely on continuous near-leg prices and OI.
    """

    reversal_window: int = REVERSAL_WINDOW
    skip_days: int = SKIP_DAYS
    signal_clip: float = SIGNAL_CLIP
    min_obs: int = MIN_OBS
    min_listing_days: int = MIN_LISTING_DAYS
    liquidity_lookback: int = LIQUIDITY_LOOKBACK
    liquidity_threshold_pre2017: float = LIQUIDITY_THRESHOLD_PRE2017
    liquidity_threshold_post2017: float = LIQUIDITY_THRESHOLD_POST2017
    rebalance_buckets: int = REBALANCE_BUCKETS
    selection_weighting: str = SELECTION_WEIGHTING
    vol_scale_windows: list[int] = field(default_factory=lambda: list(VOL_SCALE_WINDOWS))
    apply_portfolio_vol_scale: bool = APPLY_PORTFOLIO_VOL_SCALE
    max_abs_weight: float = MAX_ABS_WEIGHT
    max_gross_exposure: float = MAX_GROSS_EXPOSURE
    vol_halflife: int = VOL_HALFLIFE
    target_vol: float = TARGET_VOL / 2.0
    trading_days: int = TRADING_DAYS
    transaction_cost_bps: float = TRANSACTION_COST_BPS
    exclude: list[str] = field(default_factory=lambda: sorted(EXCLUDE))

    def __post_init__(self) -> None:
        if self.reversal_window <= 0:
            raise ValueError("reversal_window must be > 0")
        if self.skip_days < 0:
            raise ValueError("skip_days must be >= 0")
        if self.signal_clip <= 0:
            raise ValueError("signal_clip must be > 0")
        if self.min_obs <= 0:
            raise ValueError("min_obs must be > 0")
        if self.min_listing_days <= 0:
            raise ValueError("min_listing_days must be > 0")
        if self.liquidity_lookback <= 0:
            raise ValueError("liquidity_lookback must be > 0")
        if self.liquidity_threshold_pre2017 < 0 or self.liquidity_threshold_post2017 < 0:
            raise ValueError("liquidity thresholds must be >= 0")
        if self.rebalance_buckets <= 0:
            raise ValueError("rebalance_buckets must be > 0")
        if self.selection_weighting not in {"equal", "inv_vol"}:
            raise ValueError("selection_weighting must be equal/inv_vol")
        if not self.vol_scale_windows or any(w <= 1 for w in self.vol_scale_windows):
            raise ValueError("vol_scale_windows must be non-empty and all > 1")
        if self.max_abs_weight <= 0:
            raise ValueError("max_abs_weight must be > 0")
        if self.max_gross_exposure <= 0:
            raise ValueError("max_gross_exposure must be > 0")
        if self.vol_halflife <= 0:
            raise ValueError("vol_halflife must be > 0")
        if self.target_vol <= 0:
            raise ValueError("target_vol must be > 0")
        if self.trading_days <= 0:
            raise ValueError("trading_days must be > 0")
        if self.transaction_cost_bps < 0:
            raise ValueError("transaction_cost_bps must be >= 0")

        self.vol_scale_windows = [int(w) for w in self.vol_scale_windows]
        self.exclude = sorted({str(s) for s in self.exclude})

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def coerce_config(
    config: ShortReversalConfig | dict[str, Any] | None = None,
) -> ShortReversalConfig:
    if config is None:
        return ShortReversalConfig()
    if isinstance(config, ShortReversalConfig):
        return config
    return ShortReversalConfig(**config)
