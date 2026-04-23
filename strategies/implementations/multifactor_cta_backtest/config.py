"""MultiFactor CTA strategy configuration."""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from strategies.implementations.jpm_trend_trade.config import (
    EXCLUDE,
    SECTOR_MAP,
    SIGMA_HALFLIFE,
    TARGET_VOL,
    TRADING_DAYS,
    VOL_HALFLIFE,
)


MIN_OBS: int = 252
TREND_WINDOW: int = 240
TREND_SHORT_MEAN_WINDOW: int = 120
TREND_VOL_WINDOW: int = 20
TREND_BREAKOUT_WINDOWS: list[int] = [20, 60, 240]
TREND_RESIDUAL_WINDOWS: list[int] = [240, 120]
CROSS_LOOKBACK: int = 240
CROSS_SHORT_MEAN_WINDOW: int = 120
CROSS_VOL_WINDOW: int = 20
CROSS_WEIGHTING: str = "global_equal"
CROSS_SECTOR_VOL_HALFLIFE: int = VOL_HALFLIFE
TREND_WEIGHT: float = 2.0
CROSS_WEIGHT: float = 1.0
SHORT_FILTER_MODE: str = "momentum_vote"
SHORT_WINDOWS: list[int] = [5, 10, 20]
DONCHIAN_WINDOW: int = 20
DONCHIAN_UPPER: float = 0.60
DONCHIAN_LOWER: float = 0.40
SMOOTHING_WINDOW: int = 20
MAX_ABS_WEIGHT: float = 0.10
MAX_GROSS_EXPOSURE: float = 1.50
TOP_PCT: float = 0.20
BOTTOM_PCT: float = 0.20
TRANSACTION_COST_BPS: float = 5.0


@dataclass(slots=True)
class MultiFactorCTAConfig:
    """Typed configuration for the China multi-factor CTA research strategy."""

    min_obs: int = MIN_OBS
    trend_window: int = TREND_WINDOW
    trend_short_mean_window: int = TREND_SHORT_MEAN_WINDOW
    trend_vol_window: int = TREND_VOL_WINDOW
    trend_breakout_windows: list[int] = field(default_factory=lambda: list(TREND_BREAKOUT_WINDOWS))
    trend_residual_windows: list[int] = field(default_factory=lambda: list(TREND_RESIDUAL_WINDOWS))
    cross_lookback: int = CROSS_LOOKBACK
    cross_short_mean_window: int = CROSS_SHORT_MEAN_WINDOW
    cross_vol_window: int = CROSS_VOL_WINDOW
    cross_weighting: str = CROSS_WEIGHTING
    cross_sector_vol_halflife: int = CROSS_SECTOR_VOL_HALFLIFE
    trend_weight: float = TREND_WEIGHT
    cross_weight: float = CROSS_WEIGHT
    short_filter_mode: str = SHORT_FILTER_MODE
    short_windows: list[int] = field(default_factory=lambda: list(SHORT_WINDOWS))
    donchian_window: int = DONCHIAN_WINDOW
    donchian_upper: float = DONCHIAN_UPPER
    donchian_lower: float = DONCHIAN_LOWER
    smoothing_window: int = SMOOTHING_WINDOW
    max_abs_weight: float = MAX_ABS_WEIGHT
    max_gross_exposure: float = MAX_GROSS_EXPOSURE
    top_pct: float = TOP_PCT
    bottom_pct: float = BOTTOM_PCT
    vol_halflife: int = VOL_HALFLIFE
    sigma_halflife: int = SIGMA_HALFLIFE
    target_vol: float = TARGET_VOL
    trading_days: int = TRADING_DAYS
    transaction_cost_bps: float = TRANSACTION_COST_BPS
    exclude: list[str] = field(default_factory=lambda: sorted(EXCLUDE))
    sector_map: dict[str, str] = field(default_factory=lambda: dict(SECTOR_MAP))

    def __post_init__(self) -> None:
        if self.min_obs <= 0:
            raise ValueError("min_obs must be > 0")
        if self.trend_window <= 1:
            raise ValueError("trend_window must be > 1")
        if self.trend_short_mean_window <= 1:
            raise ValueError("trend_short_mean_window must be > 1")
        if self.trend_vol_window <= 1:
            raise ValueError("trend_vol_window must be > 1")
        if len(self.trend_breakout_windows) != 3 or any(w <= 1 for w in self.trend_breakout_windows):
            raise ValueError("trend_breakout_windows must contain three values > 1")
        if len(self.trend_residual_windows) != 2 or any(w <= 1 for w in self.trend_residual_windows):
            raise ValueError("trend_residual_windows must contain two values > 1")
        if self.cross_lookback <= 1:
            raise ValueError("cross_lookback must be > 1")
        if self.cross_short_mean_window <= 1:
            raise ValueError("cross_short_mean_window must be > 1")
        if self.cross_vol_window <= 1:
            raise ValueError("cross_vol_window must be > 1")
        if self.cross_weighting not in {"global_equal", "sector_inverse_vol"}:
            raise ValueError("cross_weighting must be global_equal/sector_inverse_vol")
        if self.cross_sector_vol_halflife <= 0:
            raise ValueError("cross_sector_vol_halflife must be > 0")
        if self.trend_weight < 0 or self.cross_weight < 0:
            raise ValueError("blend weights must be >= 0")
        if self.trend_weight + self.cross_weight <= 0:
            raise ValueError("at least one blend weight must be positive")
        if self.short_filter_mode not in {"none", "momentum_vote", "donchian"}:
            raise ValueError("short_filter_mode must be none/momentum_vote/donchian")
        if not self.short_windows or any(w <= 0 for w in self.short_windows):
            raise ValueError("short_windows must be non-empty positive ints")
        if self.donchian_window <= 1:
            raise ValueError("donchian_window must be > 1")
        if not (0 <= self.donchian_lower < self.donchian_upper <= 1):
            raise ValueError("donchian thresholds must satisfy 0 <= lower < upper <= 1")
        if self.smoothing_window <= 0:
            raise ValueError("smoothing_window must be > 0")
        if self.max_abs_weight <= 0:
            raise ValueError("max_abs_weight must be > 0")
        if self.max_gross_exposure <= 0:
            raise ValueError("max_gross_exposure must be > 0")
        if not (0 < self.top_pct <= 1) or not (0 < self.bottom_pct <= 1):
            raise ValueError("top_pct and bottom_pct must be in (0, 1]")
        if self.vol_halflife <= 0 or self.sigma_halflife <= 0:
            raise ValueError("halflife parameters must be > 0")
        if self.target_vol <= 0:
            raise ValueError("target_vol must be > 0")
        if self.trading_days <= 0:
            raise ValueError("trading_days must be > 0")
        if self.transaction_cost_bps < 0:
            raise ValueError("transaction_cost_bps must be >= 0")

        self.trend_window = int(self.trend_window)
        self.trend_short_mean_window = int(self.trend_short_mean_window)
        self.trend_vol_window = int(self.trend_vol_window)
        self.trend_breakout_windows = [int(w) for w in self.trend_breakout_windows]
        self.trend_residual_windows = [int(w) for w in self.trend_residual_windows]
        self.cross_lookback = int(self.cross_lookback)
        self.cross_short_mean_window = int(self.cross_short_mean_window)
        self.cross_vol_window = int(self.cross_vol_window)
        self.cross_sector_vol_halflife = int(self.cross_sector_vol_halflife)
        self.short_windows = sorted(int(w) for w in self.short_windows)
        self.exclude = sorted({str(symbol) for symbol in self.exclude})
        self.sector_map = dict(self.sector_map)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def coerce_config(
    config: MultiFactorCTAConfig | dict[str, Any] | None = None,
) -> MultiFactorCTAConfig:
    """Coerce None/dict/config objects into ``MultiFactorCTAConfig``."""

    if config is None:
        return MultiFactorCTAConfig()
    if isinstance(config, MultiFactorCTAConfig):
        return config
    return MultiFactorCTAConfig(**config)
