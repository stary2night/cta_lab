"""JPM 趋势策略配置：常量、板块分类、默认参数。"""

from __future__ import annotations

# ── 回测参数 ──────────────────────────────────────────────────────────────────
TRADING_DAYS: int = 252
TARGET_VOL: float = 0.10          # 组合年化波动率目标
LOOKBACKS: list[int] = [32, 64, 126, 252, 504]   # JPM 多周期信号窗口
MIN_OBS: int = 252                # 品种最少有效观测天数
VOL_HALFLIFE: int = 21            # 组合 PnL 波动率 EWMA 半衰期（vol-targeting）
SIGMA_HALFLIFE: int = 60          # 品种截面波动率 EWMA 半衰期
CORR_WINDOW: int = 252            # 相关性滚动窗口
CORR_MIN_PERIODS: int = 63
CAP: float = 0.25                 # CorrCap 相关性上限

# ── 排除品种 ──────────────────────────────────────────────────────────────────
EXCLUDE: set[str] = {"WS", "WT", "EC", "BZ", "LG", "WR", "SP"}

# ── 板块分类（与 data/universe/sectors.py 中的 SECTOR_MAP 平行，
#    此处使用英文标签以对齐 JPM 研究报告风格）──────────────────────────────────
SECTOR_MAP: dict[str, str] = {
    # Equity Index
    "IF": "Equity", "IC": "Equity", "IH": "Equity", "IM": "Equity",
    # Fixed Income
    "T":  "FixedIncome", "TF": "FixedIncome", "TL": "FixedIncome", "TS": "FixedIncome",
    # Metals
    "CU": "Metal", "AL": "Metal", "ZN": "Metal", "NI": "Metal",
    "PB": "Metal", "SN": "Metal", "AU": "Metal", "AG": "Metal", "BC": "Metal",
    # Ferrous / Steel chain
    "RB": "Ferrous", "HC": "Ferrous", "I":  "Ferrous", "J":  "Ferrous", "JM": "Ferrous",
    # Energy & Chemical
    "SC": "Energy", "LU": "Energy", "FU": "Energy", "BU": "Chemical",
    "RU": "Chemical", "NR": "Chemical",
    "L":  "Chemical", "PP": "Chemical", "V":  "Chemical",
    "EG": "Chemical", "EB": "Chemical", "PG": "Chemical",
    "MA": "Chemical", "TA": "Chemical", "FG": "Chemical",
    # Agricultural
    "A":  "Agri", "B":  "Agri", "C":  "Agri", "CS": "Agri",
    "M":  "Agri", "Y":  "Agri", "P":  "Agri",
    "SR": "Agri", "CF": "Agri", "OI": "Agri", "RM": "Agri",
    "JD": "Agri", "LH": "Agri", "RR": "Agri",
    "JR": "Agri", "LR": "Agri", "WH": "Agri", "RS": "Agri",
    "RI": "Agri", "PM": "Agri", "ZC": "Agri",
    "SF": "Agri", "SM": "Agri", "BB": "Agri", "FB": "Agri",
    "AP": "Agri", "CJ": "Agri", "PK": "Agri",
}


def default_config() -> dict:
    """返回 JPMTrendStrategy 默认配置字典。"""
    return {
        "lookbacks": LOOKBACKS,
        "min_obs": MIN_OBS,
        "vol_halflife": VOL_HALFLIFE,
        "sigma_halflife": SIGMA_HALFLIFE,
        "target_vol": TARGET_VOL,
        "trading_days": TRADING_DAYS,
        "corr_window": CORR_WINDOW,
        "corr_min_periods": CORR_MIN_PERIODS,
        "corr_cap": CAP,
        "exclude": list(EXCLUDE),
        "sector_map": SECTOR_MAP,
    }
