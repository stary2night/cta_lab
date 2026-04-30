"""IntradayMomStrategy 策略配置。

参考：Jin et al. (SSRN #3493927, 2019)
  - 首30分钟收益 sign → 尾30分钟收益
  - 品种：铜(CU)、螺纹钢(RB)、豆粕(M)、豆一(A)（论文原始4个品种）
  - 可扩展至全品种宇宙
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

# ── 默认参数 ──────────────────────────────────────────────────────────────────

# 论文原始4个品种（铜/螺纹钢/豆粕/豆一）+ 常用金属对照
PAPER_SYMBOLS: list[str] = ["CU", "RB", "M", "A"]

# 时段设置（分钟数）
FIRST_PERIOD_MINUTES: int = 30
LAST_PERIOD_MINUTES: int = 30

# 成交量过滤（主力合约日总成交量低于此值的交易日排除）
MIN_DAILY_VOLUME: int = 500

# 回测设置
TRADING_DAYS: int = 242    # 中国期货年交易日（约242天，含夜盘非全年）
TARGET_VOL: float = 0.10   # 组合层面目标年化波动率（10%）
VOL_HALFLIFE: int = 21     # EWMA vol-targeting 半衰期
FEE_RATE: float = 0.0003   # 单边换手费率（3bps，保守估计）

# 品种最少有效交易日（不足时从信号矩阵中排除）
MIN_OBS: int = 20


@dataclass(slots=True)
class IntradayMomConfig:
    """IntradayMomStrategy 类型化配置。

    Attributes
    ----------
    symbols : list[str]
        品种列表。空列表时加载 data_dir 下所有可用品种。
    first_period_minutes : int
        日内信号时段（首 N 分钟），默认 30。
    last_period_minutes : int
        目标收益时段（尾 N 分钟），默认 30。
    min_daily_volume : int
        每日最小成交量阈值，低于时排除，默认 500。
    fee_rate : float
        单边换手费率，默认 3bps（0.0003）。
    vol_target : float | None
        组合层面目标年化波动率；None 时跳过 vol-targeting，直接输出原始信号组合 PnL。
    vol_halflife : int
        EWMA vol-targeting 半衰期（交易日），默认 21。
    trading_days : int
        年交易日数，默认 242。
    min_obs : int
        品种最少有效交易日，默认 20。
    volume_scale : bool
        是否按首时段成交量排名条件缩放信号，默认 False。
    vol_scale : bool
        是否按首时段波动率（|r_first|）排名条件缩放信号，默认 False。
    rank_window : int
        条件缩放的滚动分位数窗口，默认 60 天。
    """

    symbols: list[str] = field(default_factory=list)
    first_period_minutes: int = FIRST_PERIOD_MINUTES
    last_period_minutes: int = LAST_PERIOD_MINUTES
    min_daily_volume: int = MIN_DAILY_VOLUME
    fee_rate: float = FEE_RATE
    vol_target: float | None = TARGET_VOL
    vol_halflife: int = VOL_HALFLIFE
    trading_days: int = TRADING_DAYS
    min_obs: int = MIN_OBS
    volume_scale: bool = False
    vol_scale: bool = False
    rank_window: int = 60

    def __post_init__(self) -> None:
        if self.first_period_minutes <= 0:
            raise ValueError("first_period_minutes must be > 0")
        if self.last_period_minutes <= 0:
            raise ValueError("last_period_minutes must be > 0")
        if self.fee_rate < 0:
            raise ValueError("fee_rate must be >= 0")
        if self.vol_target is not None and self.vol_target <= 0:
            raise ValueError("vol_target must be > 0 (or None to disable)")
        if self.trading_days <= 0:
            raise ValueError("trading_days must be > 0")
        if self.min_obs <= 0:
            raise ValueError("min_obs must be > 0")
        self.symbols = [str(s).upper() for s in self.symbols]

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def coerce_config(
    config: IntradayMomConfig | dict[str, Any] | None = None,
) -> IntradayMomConfig:
    if config is None:
        return IntradayMomConfig()
    if isinstance(config, IntradayMomConfig):
        return config
    return IntradayMomConfig(**config)
