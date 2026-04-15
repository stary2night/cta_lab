"""GMAT3 默认参数与轻量配置工具。"""

from __future__ import annotations

from pathlib import Path

_PROJECT_ROOT = Path("/home/ubuntu/dengl/my_projects")

# 共享行情数据中心
DEFAULT_MARKET_DATA_ROOT = _PROJECT_ROOT / "market_data"

# GMAT3 策略专属数据（合约信息、参考基准等）
DEFAULT_GMAT3_STRATEGY_DATA_ROOT = _PROJECT_ROOT / "strategy_data" / "gmat3"

DEFAULT_GMAT3_CONFIG: dict = {
    "score_lookbacks": [63, 126, 252],
    "top_pct": 0.30,
    "bottom_pct": 0.00,
    "base_risk": 0.10,
    "waf_threshold": 0.045,
    "waf_target": 0.040,
    "n_sub_portfolios": 4,
    "signal_mode": "direction",
    "market_data_root": DEFAULT_MARKET_DATA_ROOT,
    "strategy_data_root": DEFAULT_GMAT3_STRATEGY_DATA_ROOT,
}


def build_gmat3_config(overrides: dict | None = None) -> dict:
    """返回 GMAT3 配置字典。"""
    merged = {**DEFAULT_GMAT3_CONFIG, **(overrides or {})}
    merged["market_data_root"] = Path(merged["market_data_root"])
    merged["strategy_data_root"] = Path(merged["strategy_data_root"])
    return merged
