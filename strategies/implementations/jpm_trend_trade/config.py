"""JPM 趋势策略配置：常量、板块分类、默认参数。"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

# ── 回测参数 ──────────────────────────────────────────────────────────────────
TRADING_DAYS: int = 252
TARGET_VOL: float = 0.10
LOOKBACKS: list[int] = [32, 64, 126, 252, 504]
MIN_OBS: int = 252
VOL_HALFLIFE: int = 21
SIGMA_HALFLIFE: int = 60
CORR_WINDOW: int = 252
CORR_MIN_PERIODS: int = 63
CAP: float = 0.25
TRANSACTION_COST_BPS: float = 0.0

# ── 排除品种 ──────────────────────────────────────────────────────────────────
EXCLUDE: set[str] = {"WS", "WT", "EC", "BZ", "LG", "WR", "SP"}

# ── 板块分类（与 data/universe/sectors.py 中的 SECTOR_MAP 平行，
#    此处使用英文标签以对齐 JPM 研究报告风格）──────────────────────────────────
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
class JPMConfig:
    """JPMTrendStrategy 的类型化配置。"""

    lookbacks: list[int] = field(default_factory=lambda: list(LOOKBACKS))
    min_obs: int = MIN_OBS
    vol_halflife: int = VOL_HALFLIFE
    sigma_halflife: int = SIGMA_HALFLIFE
    target_vol: float = TARGET_VOL
    trading_days: int = TRADING_DAYS
    corr_window: int = CORR_WINDOW
    corr_min_periods: int = CORR_MIN_PERIODS
    corr_cap: float = CAP
    transaction_cost_bps: float = TRANSACTION_COST_BPS
    exclude: list[str] = field(default_factory=lambda: sorted(EXCLUDE))
    sector_map: dict[str, str] = field(default_factory=lambda: dict(SECTOR_MAP))

    def __post_init__(self) -> None:
        if not self.lookbacks or any(lb <= 0 for lb in self.lookbacks):
            raise ValueError("lookbacks must be a non-empty list of positive ints")
        if self.min_obs <= 0:
            raise ValueError("min_obs must be > 0")
        if self.vol_halflife <= 0:
            raise ValueError("vol_halflife must be > 0")
        if self.sigma_halflife <= 0:
            raise ValueError("sigma_halflife must be > 0")
        if self.target_vol <= 0:
            raise ValueError("target_vol must be > 0")
        if self.trading_days <= 0:
            raise ValueError("trading_days must be > 0")
        if self.corr_window <= 0:
            raise ValueError("corr_window must be > 0")
        if self.corr_min_periods <= 0:
            raise ValueError("corr_min_periods must be > 0")
        if self.corr_min_periods > self.corr_window:
            raise ValueError("corr_min_periods must be <= corr_window")
        if self.corr_cap <= 0:
            raise ValueError("corr_cap must be > 0")
        if self.transaction_cost_bps < 0:
            raise ValueError("transaction_cost_bps must be >= 0")

        self.lookbacks = sorted(int(lb) for lb in self.lookbacks)
        self.exclude = sorted({str(symbol) for symbol in self.exclude})
        self.sector_map = dict(self.sector_map)

    def to_dict(self) -> dict[str, Any]:
        """返回与历史实现兼容的字典配置。"""
        return asdict(self)


def default_config() -> JPMConfig:
    """返回 JPMTrendStrategy 默认配置对象。"""
    return JPMConfig()


def coerce_config(config: JPMConfig | dict[str, Any] | None = None) -> JPMConfig:
    """将空值/字典/配置对象统一成 JPMConfig。"""
    if config is None:
        return default_config()
    if isinstance(config, JPMConfig):
        return config
    return JPMConfig(**config)
