"""动量持续性检验模块。"""

from __future__ import annotations

import numpy as np
import pandas as pd


def momentum_persistence(
    returns_df: pd.DataFrame,    # shape: (dates, symbols)，品种日收益率面板
    max_lag: int = 12,           # 最大滞后期（月）
    freq: str = "ME",            # 聚合频率：月末
) -> pd.DataFrame:
    """动量持续性：用面板 OLS 检验动量信号的持续性。

    对每个滞后期 k，回归：r_{t} = alpha + beta × r_{t-k} + eps

    beta > 0 表示动量持续，beta < 0 表示均值回归。

    Parameters
    ----------
    returns_df:
        品种日收益率面板，shape: (dates, symbols)。
    max_lag:
        最大滞后期（月），默认 12。
    freq:
        时间聚合频率，默认 "ME"（月末）。

    Returns
    -------
    DataFrame，index=lag（1..max_lag），columns=[beta, t_stat, r_squared]
    """
    # 月度化：复利累计
    monthly_returns = (1 + returns_df).resample(freq).prod() - 1

    n_dates, n_symbols = monthly_returns.shape
    records = []

    for lag in range(1, max_lag + 1):
        if lag >= n_dates:
            records.append({"lag": lag, "beta": np.nan, "t_stat": np.nan, "r_squared": np.nan})
            continue

        # 构建面板：每列为一个品种，t 期收益率和 t-lag 期收益率
        # 对所有品种、所有时间点堆叠
        y_list = []
        x_list = []

        for sym in monthly_returns.columns:
            col = monthly_returns[sym].dropna()
            if len(col) <= lag:
                continue
            # t 期收益
            y = col.values[lag:]
            # t-lag 期收益
            x = col.values[:-lag]
            y_list.append(y)
            x_list.append(x)

        if not y_list:
            records.append({"lag": lag, "beta": np.nan, "t_stat": np.nan, "r_squared": np.nan})
            continue

        y_all = np.concatenate(y_list)
        x_all = np.concatenate(x_list)

        # 过滤 NaN
        mask = np.isfinite(y_all) & np.isfinite(x_all)
        y_all = y_all[mask]
        x_all = x_all[mask]

        n = len(y_all)
        if n < 3:
            records.append({"lag": lag, "beta": np.nan, "t_stat": np.nan, "r_squared": np.nan})
            continue

        # OLS：y = alpha + beta * x，用 numpy lstsq
        X = np.column_stack([np.ones(n), x_all])
        coeffs, residuals, rank, sv = np.linalg.lstsq(X, y_all, rcond=None)
        alpha_val, beta_val = coeffs

        # 计算残差
        y_pred = X @ coeffs
        resid = y_all - y_pred
        ss_res = np.sum(resid ** 2)
        ss_tot = np.sum((y_all - y_all.mean()) ** 2)
        r_squared = 1.0 - ss_res / ss_tot if ss_tot > 0 else np.nan

        # t-stat for beta
        # Var(beta) = sigma^2 * (X'X)^{-1}[1,1]
        sigma2 = ss_res / (n - 2) if n > 2 else np.nan
        if sigma2 is not np.nan and sigma2 > 0:
            try:
                XtX_inv = np.linalg.inv(X.T @ X)
                var_beta = sigma2 * XtX_inv[1, 1]
                t_stat = beta_val / np.sqrt(var_beta) if var_beta > 0 else np.nan
            except np.linalg.LinAlgError:
                t_stat = np.nan
        else:
            t_stat = np.nan

        records.append({
            "lag": lag,
            "beta": float(beta_val),
            "t_stat": float(t_stat) if t_stat is not np.nan else np.nan,
            "r_squared": float(r_squared) if r_squared is not np.nan else np.nan,
        })

    result = pd.DataFrame(records).set_index("lag")
    return result
