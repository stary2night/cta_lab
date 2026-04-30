"""NetMOM 策略配置。

参考：Pu et al. (2023) "Network Momentum across Asset Classes"
      伪代码全局参数（Network Momentum 策略伪代码.md）

说明：
  - 与 TSMOMConfig 的命名风格保持一致，便于对比和继承
  - graph_method / mode 等 NetMOM 专有参数额外独立配置
  - sector_map 与 TSMOM 共享，便于板块分析对比
"""

from __future__ import annotations

from dataclasses import asdict, dataclass, field
from typing import Any

# ── 复用 TSMOM 板块映射与排除列表 ───────────────────────────────────────────
from strategies.implementations.tsmom_backtest.config import (
    EXCLUDE,
    SECTOR_MAP,
)


# ── 交易参数 ─────────────────────────────────────────────────────────────────
TRADING_DAYS: int = 252
TARGET_VOL: float = 0.10          # 组合层目标波动率（与其他策略一致）
MIN_OBS: int = 252                # 品种最少有效观测天数
VOL_HALFLIFE: int = 21            # VectorizedBacktest vol-targeting EWMA 半衰期
SIGMA_HALFLIFE: int = 63          # 定仓分母 sigma 的 EWMA 半衰期（特征构建同步）

# ── 网络动量专有参数 ──────────────────────────────────────────────────────────
GRAPH_METHOD: str = "feature_sim"                            # 图构建方法
GRAPH_LOOKBACKS: list[int] = [126, 252, 504, 756]            # 中国市场更重近期结构
TRAIN_WINDOW: int = 756                                       # Ridge 训练窗口（约3年）
RETRAIN_FREQ: int = 10                                        # 再训练频率（约2周）
RIDGE_ALPHA: float = 1.0                                      # Ridge 正则化
MODE: str = "combo"                                           # "net_only" 或 "combo"
TREND_THRESHOLD: float = 0.0005                               # 过滤极弱预测值
FEE_RATE: float = 0.0003                                      # 单边手续费率（3bps）
MAX_ABS_WEIGHT: float = 0.10                                  # 单品种权重上限
MAX_GROSS_EXPOSURE: float = 2.0                               # 组合总杠杆上限


@dataclass(slots=True)
class NetMOMConfig:
    """NetMOMStrategy 类型化配置。

    Parameters
    ----------
    mode:
        信号模式：
        "combo"    — 个体特征 + 网络特征（RegCombo，论文最优，默认）
        "net_only" — 仅网络特征（GMOM）
    graph_method:
        图学习方法：
        "feature_sim"  — 特征 Gaussian kernel（Phase 1b，默认，无需 CVXPY）
        "return_corr"  — 纯收益率相关性（Phase 1，最快）
        "kalofolias"   — 精确凸优化（Phase 2，需要 cvxpy）
    graph_lookbacks:
        ensemble 的多 lookback 窗口（天）。
    train_window:
        Ridge 训练数据窗口（天）。
    retrain_freq:
        重新训练/重新学图的频率（天）。
    ridge_alpha:
        Ridge 回归正则化参数。
    trend_threshold:
        信号方向阈值：仅当 |signal| > threshold 才开仓，0.0 时无过滤。
    fee_rate:
        单边换手费率（如 0.0005 = 5bps），默认 0.0。
    max_abs_weight:
        单品种绝对权重上限。
    max_gross_exposure:
        组合总杠杆上限（vol-targeting 后应用）。
    """

    # 通用参数
    min_obs: int = MIN_OBS
    vol_halflife: int = VOL_HALFLIFE
    sigma_halflife: int = SIGMA_HALFLIFE
    target_vol: float = TARGET_VOL
    trading_days: int = TRADING_DAYS
    exclude: list[str] = field(default_factory=lambda: sorted(EXCLUDE))
    sector_map: dict[str, str] = field(default_factory=lambda: dict(SECTOR_MAP))

    # NetMOM 专有参数
    mode: str = MODE
    graph_method: str = GRAPH_METHOD
    graph_lookbacks: list[int] = field(default_factory=lambda: list(GRAPH_LOOKBACKS))
    train_window: int = TRAIN_WINDOW
    retrain_freq: int = RETRAIN_FREQ
    ridge_alpha: float = RIDGE_ALPHA
    trend_threshold: float = TREND_THRESHOLD
    fee_rate: float = FEE_RATE
    max_abs_weight: float = MAX_ABS_WEIGHT
    max_gross_exposure: float = MAX_GROSS_EXPOSURE

    def __post_init__(self) -> None:
        if self.mode not in {"net_only", "combo"}:
            raise ValueError(f"mode must be 'net_only' or 'combo', got {self.mode!r}")
        if self.graph_method not in {"return_corr", "feature_sim", "kalofolias"}:
            raise ValueError(
                f"graph_method must be 'return_corr', 'feature_sim', or 'kalofolias', "
                f"got {self.graph_method!r}"
            )
        if self.train_window <= 0:
            raise ValueError("train_window must be > 0")
        if self.retrain_freq <= 0:
            raise ValueError("retrain_freq must be > 0")
        if self.target_vol <= 0:
            raise ValueError("target_vol must be > 0")
        if self.max_abs_weight <= 0:
            raise ValueError("max_abs_weight must be > 0")
        if self.max_gross_exposure <= 0:
            raise ValueError("max_gross_exposure must be > 0")

        self.exclude = sorted({str(s) for s in self.exclude})
        self.graph_lookbacks = sorted(set(int(d) for d in self.graph_lookbacks))
        self.sector_map = dict(self.sector_map)

    def to_dict(self) -> dict[str, Any]:
        return asdict(self)


def coerce_config(
    config: NetMOMConfig | dict[str, Any] | None = None,
) -> NetMOMConfig:
    """将 None / dict / 配置对象统一转成 NetMOMConfig。"""
    if config is None:
        return NetMOMConfig()
    if isinstance(config, NetMOMConfig):
        return config
    return NetMOMConfig(**config)
