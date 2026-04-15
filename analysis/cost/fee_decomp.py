"""费用分解分析模块。"""

from __future__ import annotations

from typing import Callable

import pandas as pd

from backtest import BacktestEngine, ZeroFee, TradingFee, TrackingFee
from backtest.position import PositionTracker


def fee_decomposition(
    weight_df: pd.DataFrame,
    price_df: pd.DataFrame,
    adjust_dates: set,
    position_tracker_factory: Callable[[], PositionTracker],
    trading_fee_rate: float = 0.0005,
    tracking_fee_rate: float = 0.005,
) -> pd.DataFrame:
    """通过三次回测对比，分解费用拖累。

    场景1：ZeroFee（零费用基准）
    场景2：TradingFee only
    场景3：TradingFee + TrackingFee（完整费用）

    Parameters
    ----------
    weight_df:
        权重矩阵，shape: (dates, symbols)。
    price_df:
        价格矩阵，shape: (dates, symbols)。
    adjust_dates:
        调仓日集合。
    position_tracker_factory:
        可调用对象，每次调用返回新的 PositionTracker 实例。
    trading_fee_rate:
        交易费率，默认 0.05%（单边）。
    tracking_fee_rate:
        管理费/追踪费率（年化），默认 0.5%。

    Returns
    -------
    DataFrame，
      index=[zero_fee, trading_only, full_fee]
      columns=[final_nav, annual_return, fee_drag]
    fee_drag = (zero_fee_nav - scenario_nav) / zero_fee_nav
    """
    def _run_scenario(fee_models: list) -> float:
        tracker = position_tracker_factory()
        engine = BacktestEngine(
            position_tracker=tracker,
            fee_models=fee_models,
        )
        result = engine.run(
            weight_df=weight_df,
            price_df=price_df,
            adjust_dates=adjust_dates,
        )
        return result

    # 场景1：零费用
    res_zero = _run_scenario([ZeroFee()])

    # 场景2：仅交易费用
    res_trading = _run_scenario([TradingFee(rate=trading_fee_rate)])

    # 场景3：完整费用
    res_full = _run_scenario([
        TradingFee(rate=trading_fee_rate),
        TrackingFee(annual_rate=tracking_fee_rate),
    ])

    def _calc_annual_return(nav: pd.Series) -> float:
        n = len(nav) - 1
        if n <= 0:
            return float("nan")
        return float((nav.iloc[-1] / nav.iloc[0]) ** (252 / n) - 1)

    zero_final = float(res_zero.nav.iloc[-1])
    trading_final = float(res_trading.nav.iloc[-1])
    full_final = float(res_full.nav.iloc[-1])

    records = {
        "zero_fee": {
            "final_nav": zero_final,
            "annual_return": _calc_annual_return(res_zero.nav),
            "fee_drag": 0.0,
        },
        "trading_only": {
            "final_nav": trading_final,
            "annual_return": _calc_annual_return(res_trading.nav),
            "fee_drag": (zero_final - trading_final) / zero_final if zero_final != 0 else float("nan"),
        },
        "full_fee": {
            "final_nav": full_final,
            "annual_return": _calc_annual_return(res_full.nav),
            "fee_drag": (zero_final - full_final) / zero_final if zero_final != 0 else float("nan"),
        },
    }

    result = pd.DataFrame(records).T
    result.index.name = "scenario"
    return result
