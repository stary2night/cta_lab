"""TSMOM 策略配置。

参考：Moskowitz, Ooi & Pedersen (JFE, 2012)
  - 默认回望期 252 交易日（≈12 个月）
  - 组合目标年化波动率 10%（与 JPM 框架对齐；
    论文原始使用品种层面 40% vol-target，由 signal/sigma 单独处理）
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

# ── 回测参数 ──────────────────────────────────────────────────────────────────
TRADING_DAYS: int = 252
TARGET_VOL: float = 0.10          # 组合层面目标波动率（与 JPM 框架一致）
LOOKBACK: int = 252               # TSMOM 回望期（论文默认 12m ≈ 252 交易日）
MIN_OBS: int = 252                # 品种最少有效观测天数
VOL_HALFLIFE: int = 21            # VectorizedBacktest vol-targeting EWMA 半衰期
SIGMA_HALFLIFE: int = 60          # 定仓分母 sigma 的 EWMA 半衰期
CORR_WINDOW: int = 252
CORR_MIN_PERIODS: int = 63
CAP: float = 0.25

# ── 排除品种（与 JPM 框架对齐）──────────────────────────────────────────────
EXCLUDE: set[str] = {"WS", "WT", "EC", "BZ", "LG", "WR", "SP"}

# ── 板块分类（与 JPM config 对齐）──────────────────────────────────────────
SECTOR_MAP: dict[str, str] = {
    "IF": "Equity", "IC": "Equity", "IH": "Equity", "IM": "Equity",
    "T": "FixedIncome", "TF": "FixedIncome", "TL": "FixedIncome", "TS": "FixedIncome",
    "CU": "Metal", "AL": "Metal", "ZN": "Metal", "NI": "Metal",
    "PB": "Metal", "SN": "Metal", "AU": "Metal", "AG": "Metal", "BC": "Metal",
    "RB": "Ferrous", "HC": "Ferrous", "I": "Ferrous", "J": "Ferrous", "JM": "Ferrous",
    "SC": "Energy", "LU": "Energy", "FU": "Energy", "BU": "Chemical",
    "RU": "Chemical", "NR": "Chemical",
    "L": "Chemical", "PP": "Chemical", "V": "Chemical",
    "EG": "Chemical", "EB": "Chemical", "PG": "Chemical",
    "MA": "Chemical", "TA": "Chemical", "FG": "Chemical",
    "A": "Agri", "B": "Agri", "C": "Agri", "CS": "Agri",
    "M": "Agri", "Y": "Agri", "P": "Agri",
    "SR": "Agri", "CF": "Agri", "OI": "Agri", "RM": "Agri",
    "JD": "Agri", "LH": "Agri", "RR": "Agri",
    "JR": "Agri", "LR": "Agri", "WH": "Agri", "RS": "Agri",
    "RI": "Agri", "PM": "Agri", "ZC": "Agri",
    "SF": "Agri", "SM": "Agri", "BB": "Agri", "FB": "Agri",
    "AP": "Agri", "CJ": "Agri", "PK": "Agri",
}


@dataclass(slots=True)
class TSMOMConfig:
    """TSMOMStrategy 类型化配置。"""

    lookback: int = LOOKBACK
    min_obs: int = MIN_OBS
    vol_halflife: int = VOL_HALFLIFE
    sigma_halflife: int = SIGMA_HALFLIFE
    target_vol: float = TARGET_VOL
    trading_days: int = TRADING_DAYS
    corr_window: int = CORR_WINDOW
    corr_min_periods: int = CORR_MIN_PERIODS
    corr_cap: float = CAP
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
        if self.corr_min_periods > self.corr_window:
            raise ValueError("corr_min_periods must be <= corr_window")

        self.lookback = int(self.lookback)
        self.exclude = sorted({str(s) for s in self.exclude})
        self.sector_map = dict(self.sector_map)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def coerce_config(config: TSMOMConfig | dict[str, Any] | None = None) -> TSMOMConfig:
    if config is None:
        return TSMOMConfig()
    if isinstance(config, TSMOMConfig):
        return config
    return TSMOMConfig(**config)
