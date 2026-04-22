"""Reporting helpers for backtest turnover and transaction costs."""

from __future__ import annotations

import pandas as pd


def turnover_from_weights(weights: pd.DataFrame) -> pd.Series:
    """Return daily target-weight turnover as sum(abs(delta weight))."""

    turnover = weights.diff().abs().sum(axis=1).fillna(0.0)
    turnover.name = "turnover"
    return turnover


def cost_from_turnover(
    turnover: pd.Series,
    cost_rate: float,
    *,
    lag: int = 1,
) -> pd.Series:
    """Estimate daily transaction cost using vectorized execution lag."""

    cost = (float(cost_rate) * turnover).shift(lag).fillna(0.0)
    cost.name = "transaction_cost"
    return cost


def turnover_cost_frame(
    weights: pd.DataFrame,
    cost_rate: float,
    *,
    lag: int = 1,
) -> pd.DataFrame:
    """Build a daily turnover/cost report from target weights."""

    turnover = turnover_from_weights(weights)
    cost = cost_from_turnover(turnover, cost_rate, lag=lag)
    return pd.DataFrame({"turnover": turnover, "transaction_cost": cost})


def turnover_cost_summary(
    weights: pd.DataFrame,
    cost_rate: float,
    *,
    lag: int = 1,
    trading_days: int = 252,
) -> dict[str, float]:
    """Summarize annualized turnover and transaction-cost drag."""

    frame = turnover_cost_frame(weights, cost_rate, lag=lag)
    return {
        "AvgTurnover(%)": round(float(frame["turnover"].mean() * 100.0), 2),
        "AnnTurnover(x)": round(float(frame["turnover"].mean() * trading_days), 2),
        "TotalCost(%)": round(float(frame["transaction_cost"].sum() * 100.0), 2),
        "AnnCost(%)": round(float(frame["transaction_cost"].mean() * trading_days * 100.0), 2),
    }
