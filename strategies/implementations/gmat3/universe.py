"""GMAT3 资产宇宙与原始数据分组定义。"""

from __future__ import annotations


SUB_PORTFOLIOS: dict[str, dict] = {
    "IF": {"exchange": "CFF", "currency": "CNY", "contract_type": "domestic_equity", "substitute": "000300.SH", "sub_base_date": "2006-01-04", "futures_base_date": "2010-04-19", "switch_date": "2010-04-19", "weight_ub": 0.10, "direction": 1},
    "IC": {"exchange": "CFF", "currency": "CNY", "contract_type": "domestic_equity", "substitute": "000905.SH", "sub_base_date": "2006-01-04", "futures_base_date": "2015-04-17", "switch_date": "2015-04-17", "weight_ub": 0.10, "direction": 1},
    "IM": {"exchange": "CFF", "currency": "CNY", "contract_type": "domestic_equity", "substitute": "000852.SH", "sub_base_date": "2006-01-04", "futures_base_date": "2022-07-25", "switch_date": "2022-07-25", "weight_ub": 0.10, "direction": 1},
    "ES": {"exchange": "CME", "currency": "USD", "contract_type": "overseas_equity", "substitute": None, "sub_base_date": "2006-12-29", "futures_base_date": "2006-12-29", "switch_date": None, "weight_ub": 0.10, "direction": 1},
    "NQ": {"exchange": "CME", "currency": "USD", "contract_type": "overseas_equity", "substitute": None, "sub_base_date": "2006-12-29", "futures_base_date": "2006-12-29", "switch_date": None, "weight_ub": 0.10, "direction": 1},
    "TS": {"exchange": "CFF", "currency": "CNY", "contract_type": "domestic_bond", "substitute": "CBA00622.CS", "sub_base_date": "2006-11-17", "futures_base_date": "2018-08-20", "switch_date": "2018-08-20", "weight_ub": 0.40, "direction": 1},
    "TF": {"exchange": "CFF", "currency": "CNY", "contract_type": "domestic_bond", "substitute": "000140.SH", "sub_base_date": "2008-01-02", "futures_base_date": "2013-09-09", "switch_date": "2013-09-09", "weight_ub": 0.40, "direction": 1},
    "T": {"exchange": "CFF", "currency": "CNY", "contract_type": "domestic_bond", "substitute": "CBA00652.CS", "sub_base_date": "2006-11-17", "futures_base_date": "2015-03-23", "switch_date": "2015-03-23", "weight_ub": 0.40, "direction": 1},
    "TU": {"exchange": "CME", "currency": "USD", "contract_type": "overseas_bond", "substitute": None, "sub_base_date": "2006-12-29", "futures_base_date": "2006-12-29", "switch_date": None, "weight_ub": 0.60, "direction": 1},
    "FV": {"exchange": "CME", "currency": "USD", "contract_type": "overseas_bond", "substitute": None, "sub_base_date": "2006-12-29", "futures_base_date": "2006-12-29", "switch_date": None, "weight_ub": 0.40, "direction": 1},
    "TY": {"exchange": "CME", "currency": "USD", "contract_type": "overseas_bond", "substitute": None, "sub_base_date": "2006-12-29", "futures_base_date": "2006-12-29", "switch_date": None, "weight_ub": 0.40, "direction": 1},
    "LCO": {"exchange": "ICE", "currency": "USD", "contract_type": "overseas_commodity", "substitute": None, "sub_base_date": "2006-12-29", "futures_base_date": "2006-12-29", "switch_date": None, "weight_ub": 0.10, "direction": 1},
    "AU": {"exchange": "SHF", "currency": "CNY", "contract_type": "domestic_commodity", "substitute": None, "sub_base_date": "2008-01-10", "futures_base_date": "2008-01-10", "switch_date": None, "weight_ub": 0.10, "direction": 1},
    "CU": {"exchange": "SHF", "currency": "CNY", "contract_type": "domestic_commodity", "substitute": None, "sub_base_date": "2005-01-04", "futures_base_date": "2005-01-04", "switch_date": None, "weight_ub": 0.10, "direction": 1},
    "M": {"exchange": "DCE", "currency": "CNY", "contract_type": "domestic_commodity", "substitute": None, "sub_base_date": "2005-01-04", "futures_base_date": "2005-01-04", "switch_date": None, "weight_ub": 0.10, "direction": 1},
    "BLACK": {"exchange": "SHF", "currency": "CNY", "contract_type": "black_series", "substitute": None, "sub_base_date": "2014-04-01", "futures_base_date": "2014-04-01", "switch_date": None, "weight_ub": 0.10, "direction": 1},
}

BLACK_COMPONENTS: dict[str, dict] = {
    "RB": {"exchange": "SHF", "currency": "CNY", "contract_type": "domestic_commodity", "futures_base_date": "2009-03-30"},
    "HC": {"exchange": "SHF", "currency": "CNY", "contract_type": "domestic_commodity", "futures_base_date": "2014-03-24"},
    "I": {"exchange": "DCE", "currency": "CNY", "contract_type": "domestic_commodity", "futures_base_date": "2013-10-21"},
    "J": {"exchange": "DCE", "currency": "CNY", "contract_type": "domestic_commodity", "futures_base_date": "2013-03-25"},
    "JM": {"exchange": "DCE", "currency": "CNY", "contract_type": "domestic_commodity", "futures_base_date": "2011-04-18"},
}

DOMESTIC_DAILY_FILES: dict[str, str] = {
    "index": "domestic/daily_index_futures",
    "bond": "domestic/daily_bond_futures",
    "commodity": "domestic/daily_commodity_futures",
}

DOMESTIC_VARIETY_GROUP: dict[str, str] = {
    **{v: "index" for v in ("IF", "IC", "IM")},
    **{v: "bond" for v in ("TS", "TF", "T")},
    **{v: "commodity" for v in ("AU", "CU", "M", "RB", "HC", "I", "J", "JM")},
}

OVERSEAS_DAILY_FILES: dict[str, str] = {
    v: f"overseas/daily_{v}"
    for v in ("ES", "NQ", "TU", "FV", "TY", "LCO")
}

ROLL_PARAMS: dict[str, dict[str, object]] = {
    "IF": {"roll_days": 3, "roll_window": None, "last_holding_rule": "last_trade_date"},
    "IC": {"roll_days": 3, "roll_window": None, "last_holding_rule": "last_trade_date"},
    "IM": {"roll_days": 3, "roll_window": None, "last_holding_rule": "last_trade_date"},
    "ES": {"roll_days": 3, "roll_window": 6, "last_holding_rule": "last_trade_date"},
    "NQ": {"roll_days": 3, "roll_window": 6, "last_holding_rule": "last_trade_date"},
    "TS": {"roll_days": 5, "roll_window": None, "last_holding_rule": "prev_1_month_last"},
    "TF": {"roll_days": 5, "roll_window": None, "last_holding_rule": "prev_1_month_last"},
    "T": {"roll_days": 5, "roll_window": None, "last_holding_rule": "prev_1_month_last"},
    "TU": {"roll_days": 3, "roll_window": 5, "last_holding_rule": "prev_1_month_last"},
    "FV": {"roll_days": 3, "roll_window": 5, "last_holding_rule": "prev_1_month_last"},
    "TY": {"roll_days": 3, "roll_window": 5, "last_holding_rule": "prev_1_month_last"},
    "LCO": {"roll_days": 5, "roll_window": None, "last_holding_rule": "monthly_calendar"},
    "AU": {"roll_days": 3, "roll_window": None, "last_holding_rule": "prev_2_month_last"},
    "CU": {"roll_days": 3, "roll_window": None, "last_holding_rule": "prev_2_month_last"},
    "M": {"roll_days": 3, "roll_window": None, "last_holding_rule": "prev_1_month_14th"},
    "RB": {"roll_days": 3, "roll_window": None, "last_holding_rule": "prev_2_month_last"},
    "HC": {"roll_days": 3, "roll_window": None, "last_holding_rule": "prev_2_month_last"},
    "I": {"roll_days": 3, "roll_window": None, "last_holding_rule": "prev_1_month_14th"},
    "J": {"roll_days": 3, "roll_window": None, "last_holding_rule": "prev_1_month_14th"},
    "JM": {"roll_days": 3, "roll_window": None, "last_holding_rule": "prev_1_month_14th"},
}

BRENT_MONTHLY_DELIVERY: dict[int, int] = {
    1: 4, 2: 5, 3: 6, 4: 7, 5: 8, 6: 9,
    7: 10, 8: 11, 9: 12, 10: 1, 11: 2, 12: 3,
}

BLACK_REBALANCE_HISTORY_DAYS = 125
BLACK_WEIGHT_WINDOW = 120
BLACK_WEIGHT_MIN = 0.02
BLACK_WEIGHT_MAX = 0.50

MOMENTUM_SELECT_N = 11
MIN_HISTORY_DAYS = 300
VOL_WINDOWS = [22, 65, 130]
WAF_TARGET_VOL = 0.04


def all_varieties() -> list[str]:
    """返回 GMAT3 当前涉及的全部单品种代码。"""
    return sorted(set(DOMESTIC_VARIETY_GROUP) | set(OVERSEAS_DAILY_FILES))
