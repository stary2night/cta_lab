"""Skew reversal strategy configuration."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from strategies.implementations.jpm_trend_trade.config import (
    EXCLUDE,
    TARGET_VOL,
    TRADING_DAYS,
    VOL_HALFLIFE,
)


MIN_OBS: int = 260
SKEW_WINDOWS: list[int] = [130, 195, 260]
TOP_PCT: float = 0.25
BOTTOM_PCT: float = 0.25
OI_LOOKBACK: int = 10
REBALANCE_BUCKETS: int = 20
CLOSE_SETTLE_BLEND_ALPHA: float = 0.70
USE_CLOSE_SETTLE_CORRECTION: bool = True
MIN_LISTING_DAYS: int = 130
LIQUIDITY_LOOKBACK: int = 19
LIQUIDITY_THRESHOLD_PRE2017: float = 1.0e9
LIQUIDITY_THRESHOLD_POST2017: float = 2.0e9
SMOOTHING_WINDOW: int = 20
VOL_SCALE_WINDOWS: list[int] = [22, 65, 130]
SELECTION_WEIGHTING: str = "inv_vol"
APPLY_ASSET_VOL_SCALE: bool = False
MAX_GROSS_EXPOSURE: float = 1.50
TRANSACTION_COST_BPS: float = 5.0


@dataclass(slots=True)
class SkewReversalConfig:
    """Typed configuration for the China futures skew reversal strategy."""

    min_obs: int = MIN_OBS
    skew_windows: list[int] = field(default_factory=lambda: list(SKEW_WINDOWS))
    top_pct: float = TOP_PCT
    bottom_pct: float = BOTTOM_PCT
    oi_lookback: int = OI_LOOKBACK
    rebalance_buckets: int = REBALANCE_BUCKETS
    close_settle_blend_alpha: float = CLOSE_SETTLE_BLEND_ALPHA
    use_close_settle_correction: bool = USE_CLOSE_SETTLE_CORRECTION
    min_listing_days: int = MIN_LISTING_DAYS
    liquidity_lookback: int = LIQUIDITY_LOOKBACK
    liquidity_threshold_pre2017: float = LIQUIDITY_THRESHOLD_PRE2017
    liquidity_threshold_post2017: float = LIQUIDITY_THRESHOLD_POST2017
    smoothing_window: int = SMOOTHING_WINDOW
    vol_scale_windows: list[int] = field(default_factory=lambda: list(VOL_SCALE_WINDOWS))
    selection_weighting: str = SELECTION_WEIGHTING
    apply_asset_vol_scale: bool = APPLY_ASSET_VOL_SCALE
    max_gross_exposure: float = MAX_GROSS_EXPOSURE
    vol_halflife: int = VOL_HALFLIFE
    target_vol: float = TARGET_VOL / 2.0
    trading_days: int = TRADING_DAYS
    transaction_cost_bps: float = TRANSACTION_COST_BPS
    exclude: list[str] = field(default_factory=lambda: sorted(EXCLUDE))

    def __post_init__(self) -> None:
        if self.min_obs <= 0:
            raise ValueError("min_obs must be > 0")
        if not self.skew_windows or any(window <= 2 for window in self.skew_windows):
            raise ValueError("skew_windows must be non-empty and > 2")
        if not (0 < self.top_pct <= 1) or not (0 < self.bottom_pct <= 1):
            raise ValueError("top_pct and bottom_pct must be in (0, 1]")
        if self.oi_lookback <= 0:
            raise ValueError("oi_lookback must be > 0")
        if self.rebalance_buckets <= 0:
            raise ValueError("rebalance_buckets must be > 0")
        if not (0.0 <= self.close_settle_blend_alpha <= 1.0):
            raise ValueError("close_settle_blend_alpha must be in [0, 1]")
        if self.min_listing_days <= 0:
            raise ValueError("min_listing_days must be > 0")
        if self.liquidity_lookback <= 0:
            raise ValueError("liquidity_lookback must be > 0")
        if self.liquidity_threshold_pre2017 < 0 or self.liquidity_threshold_post2017 < 0:
            raise ValueError("liquidity thresholds must be >= 0")
        if self.smoothing_window <= 0:
            raise ValueError("smoothing_window must be > 0")
        if not self.vol_scale_windows or any(window <= 1 for window in self.vol_scale_windows):
            raise ValueError("vol_scale_windows must be non-empty and > 1")
        if self.selection_weighting not in {"equal", "inv_vol"}:
            raise ValueError("selection_weighting must be equal/inv_vol")
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

        self.skew_windows = [int(window) for window in self.skew_windows]
        self.vol_scale_windows = [int(window) for window in self.vol_scale_windows]
        self.exclude = sorted({str(symbol) for symbol in self.exclude})

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def coerce_config(
    config: SkewReversalConfig | dict[str, Any] | None = None,
) -> SkewReversalConfig:
    if config is None:
        return SkewReversalConfig()
    if isinstance(config, SkewReversalConfig):
        return config
    return SkewReversalConfig(**config)
