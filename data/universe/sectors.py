"""品种板块分类配置。

国内期货（SECTOR_MAP）和海外期货（SECTOR_MAP_OVERSEAS）的板块归属表，
供信号分析、归因、报告模块使用。
"""

from __future__ import annotations

# ── 国内期货板块分类 ──────────────────────────────────────────────────────────
# 6 个板块，66 个品种
SECTOR_MAP: dict[str, list[str]] = {
    "股指期货": ["IF", "IC", "IH", "IM"],
    "国债期货": ["T", "TF", "TL", "TS"],
    "有色金属": ["AL", "CU", "ZN", "NI", "SN", "PB", "AU", "AG", "BC"],
    "黑色金属": ["I", "J", "JM", "RB", "HC", "WR", "SF", "SM", "BB", "FB"],
    "能源化工": ["SC", "FU", "BU", "ZC", "LU", "BZ", "RU", "NR",
                "TA", "MA", "FG", "V", "L", "PP", "PG", "EB", "EG"],
    "农产品":   ["A", "B", "C", "CS", "M", "P", "Y", "JD",
                "CF", "SR", "OI", "RM", "RS", "RI", "WH", "WT", "JR", "LR", "PM",
                "RR", "LH", "LG", "EC"],
}

# ── 海外期货板块分类 ──────────────────────────────────────────────────────────
# 8 个板块，38 个品种
SECTOR_MAP_OVERSEAS: dict[str, list[str]] = {
    "Equity Index":    ["ES", "NQ", "YM", "FESX", "FDAX", "HSI", "NIY", "CN", "RTY"],
    "Gov. Bond":       ["ZN", "ZF", "ZT", "FGBL", "SR3"],
    "Energy":          ["CL", "BRN", "HO", "NG"],
    "Precious Metals": ["GC", "SI"],
    "Base Metals":     ["HG", "cu", "AH"],
    "Agriculture":     ["C", "ZS", "ZW", "CC", "KC", "SB"],
    "Currency":        ["6B", "6E", "6J"],
    "Alt/Other":       ["VX", "BTC", "TIO", "FEF"],
}


def symbol_to_sector(symbol: str, sector_map: dict[str, list[str]]) -> str | None:
    """返回品种所属板块名称，未找到返回 None。"""
    for sector, symbols in sector_map.items():
        if symbol in symbols:
            return sector
    return None


def build_symbol_sector_map(sector_map: dict[str, list[str]]) -> dict[str, str]:
    """将板块分类字典反转为 {symbol: sector} 形式，便于按品种快速查找。"""
    return {sym: sector for sector, syms in sector_map.items() for sym in syms}
