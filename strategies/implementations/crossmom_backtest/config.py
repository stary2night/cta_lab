"""CrossMOM 策略配置。

参考：
  - Jegadeesh & Titman (1993)                — 横截面动量
  - Demystifying Momentum (du Plessis, 2013) — XS 动量与波动率加权

当前实现路径：
  - 相对动量信号复用 DualMomentumSignal(mode="relative")
  - 基准定仓：signal / sigma_ewma
  - 组合回测：VectorizedBacktest(vol-targeting)
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

from strategies.implementations.dual_momentum_backtest.config import (
    EXCLUDE,
    MIN_OBS,
    SECTOR_MAP,
    SIGMA_HALFLIFE,
    TARGET_VOL,
    TRADING_DAYS,
    VOL_HALFLIFE,
)


LOOKBACK: int = 252
TOP_PCT: float = 0.30
BOTTOM_PCT: float = 0.30


@dataclass(slots=True)
class CrossMOMConfig:
    """CrossMOMStrategy 类型化配置。"""

    lookback: int = LOOKBACK
    min_obs: int = MIN_OBS
    vol_halflife: int = VOL_HALFLIFE
    sigma_halflife: int = SIGMA_HALFLIFE
    target_vol: float = TARGET_VOL
    trading_days: int = TRADING_DAYS
    top_pct: float = TOP_PCT
    bottom_pct: float = BOTTOM_PCT
    exclude: list[str] = field(default_factory=lambda: sorted(EXCLUDE))
    sector_map: dict[str, str] = field(default_factory=lambda: dict(SECTOR_MAP))

    def __post_init__(self) -> None:
        if self.lookback <= 0:
            raise ValueError("lookback must be > 0")
        if self.min_obs <= 0:
            raise ValueError("min_obs must be > 0")
        if self.target_vol <= 0:
            raise ValueError("target_vol must be > 0")
        if self.trading_days <= 0:
            raise ValueError("trading_days must be > 0")
        if not (0 < self.top_pct <= 1):
            raise ValueError("top_pct must be in (0, 1]")
        if not (0 < self.bottom_pct <= 1):
            raise ValueError("bottom_pct must be in (0, 1]")

        self.lookback = int(self.lookback)
        self.exclude = sorted({str(s) for s in self.exclude})
        self.sector_map = dict(self.sector_map)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def coerce_config(
    config: CrossMOMConfig | dict[str, Any] | None = None,
) -> CrossMOMConfig:
    """将 None / dict / 配置对象统一转成 CrossMOMConfig。"""
    if config is None:
        return CrossMOMConfig()
    if isinstance(config, CrossMOMConfig):
        return config
    return CrossMOMConfig(**config)
