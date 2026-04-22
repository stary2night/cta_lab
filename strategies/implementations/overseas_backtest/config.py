"""境外期货策略配置。

三策略对比回测（JPM t-stat / TSMOM Binary / Dual Momentum L/S）共用的
品种配置、板块分类和回测参数。
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

# ── 回测参数 ──────────────────────────────────────────────────────────────────
TRADING_DAYS: int  = 252
TARGET_VOL: float  = 0.10
VOL_HALFLIFE: int  = 21
SIGMA_HALFLIFE: int = 60
TSMOM_LOOKBACK: int = 252
JPM_LOOKBACKS: list[int] = [32, 64, 126, 252, 504]
DUAL_TOP_PCT: float = 0.50

# ── 品种过滤 ──────────────────────────────────────────────────────────────────
EXCLUDE_OVERSEAS: set[str] = {"VX", "BTC"}

# ── 板块分类 ──────────────────────────────────────────────────────────────────
OVERSEAS_SECTOR_MAP: dict[str, str] = {
    # Equity Index
    "ES": "Equity", "NQ": "Equity", "YM": "Equity", "RTY": "Equity",
    "FDAX": "Equity", "FESX": "Equity", "HSI": "Equity", "NIY": "Equity",
    "HTI": "Equity", "CN": "Equity",
    # Fixed Income
    "ZB": "FixedIncome", "ZN": "FixedIncome", "ZF": "FixedIncome",
    "ZT": "FixedIncome", "FGBL": "FixedIncome", "FGBM": "FixedIncome",
    "FGBS": "FixedIncome", "FBTP": "FixedIncome", "FOAT": "FixedIncome",
    "JGB": "FixedIncome", "XT": "FixedIncome", "YT": "FixedIncome",
    "SR3": "FixedIncome", "GE": "FixedIncome", "R": "FixedIncome",
    "JG": "FixedIncome",
    # FX
    "6E": "FX", "6J": "FX", "6B": "FX", "6A": "FX",
    "6C": "FX", "6N": "FX", "6S": "FX",
    # Energy
    "CL": "Energy", "BRN": "Energy", "HO": "Energy",
    "NG": "Energy", "RB": "Energy",
    # Metal
    "GC": "Metal", "SI": "Metal", "HG": "Metal",
    # Agriculture
    "ZS": "Agri", "ZC": "Agri", "KC": "Agri",
    "CC": "Agri", "SB": "Agri", "A01": "Agri",
    # Crypto / Other
    "BTC": "Crypto", "VX": "Other",
}

# ── 对比策略元数据 ─────────────────────────────────────────────────────────────
STRATEGIES: list[str] = ["jpm", "tsmom", "dual_ls"]

LABELS: dict[str, str] = {
    "jpm":     "JPM t-stat (multi-period)",
    "tsmom":   "TSMOM Binary (12m)",
    "dual_ls": "Dual Momentum L/S",
}

COLORS: dict[str, str] = {
    "jpm":     "steelblue",
    "tsmom":   "seagreen",
    "dual_ls": "darkorange",
}


@dataclass(slots=True)
class OverseasTrendSuiteConfig:
    """境外期货趋势策略套件配置。"""

    min_obs: int = 252
    trading_days: int = TRADING_DAYS
    target_vol: float = TARGET_VOL
    vol_halflife: int = VOL_HALFLIFE
    sigma_halflife: int = SIGMA_HALFLIFE
    tsmom_lookback: int = TSMOM_LOOKBACK
    jpm_lookbacks: list[int] = field(default_factory=lambda: list(JPM_LOOKBACKS))
    dual_top_pct: float = DUAL_TOP_PCT
    exclude: list[str] = field(default_factory=lambda: sorted(EXCLUDE_OVERSEAS))
    sector_map: dict[str, str] = field(default_factory=lambda: dict(OVERSEAS_SECTOR_MAP))
    strategies: list[str] = field(default_factory=lambda: list(STRATEGIES))
    labels: dict[str, str] = field(default_factory=lambda: dict(LABELS))
    colors: dict[str, str] = field(default_factory=lambda: dict(COLORS))

    def __post_init__(self) -> None:
        if self.min_obs <= 0:
            raise ValueError("min_obs must be > 0")
        if self.trading_days <= 0:
            raise ValueError("trading_days must be > 0")
        if self.target_vol <= 0:
            raise ValueError("target_vol must be > 0")
        if self.tsmom_lookback <= 0:
            raise ValueError("tsmom_lookback must be > 0")
        if not self.jpm_lookbacks:
            raise ValueError("jpm_lookbacks must not be empty")
        if not 0 < self.dual_top_pct <= 1:
            raise ValueError("dual_top_pct must be in (0, 1]")

        self.min_obs = int(self.min_obs)
        self.trading_days = int(self.trading_days)
        self.tsmom_lookback = int(self.tsmom_lookback)
        self.jpm_lookbacks = [int(x) for x in self.jpm_lookbacks]
        self.exclude = sorted({str(s) for s in self.exclude})
        self.sector_map = dict(self.sector_map)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def coerce_config(
    config: OverseasTrendSuiteConfig | dict[str, Any] | None = None,
) -> OverseasTrendSuiteConfig:
    if config is None:
        return OverseasTrendSuiteConfig()
    if isinstance(config, OverseasTrendSuiteConfig):
        return config
    return OverseasTrendSuiteConfig(**config)
