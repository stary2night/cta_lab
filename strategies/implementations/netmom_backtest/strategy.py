"""NetMOMStrategy — 网络动量策略。

基于 Pu et al. (2023) "Network Momentum across Asset Classes"。

流水线：
  1. 加载 china_daily_full/ 收益率矩阵
  2. NetworkMomentumSignal.compute() → Ridge 预测值矩阵（连续信号）
  3. build_weights()：
        direction = sign(signal)，threshold 过滤
        w = direction / sigma × (target_vol / sqrt(252))
  4. VectorizedBacktest.run() → vol-targeted PnL

支持两种信号模式（通过 config.mode 控制）：
  · "combo"    — 个体特征 + 网络特征（RegCombo，论文最优）
  · "net_only" — 仅网络特征（GMOM）

与 TSMOMStrategy / CrossMOMStrategy 对比：
  · 同：收益率矩阵输入，StrategyBase 接口，VectorizedBacktest 输出
  · 异：信号由 Ridge 回归给出连续预测值，而非 sign/rank；
        build_weights 用 trend_threshold 过滤弱信号

如需与 TSMOM 组合，可直接将两策略的 signal 拼接后输入 Ridge（RegCombo 风格），
或在外层对 pnl 序列做等权/风险平价混合。
"""

from __future__ import annotations

from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from backtest.result import BacktestResult
from backtest.vectorized import VectorizedBacktest
from signals.network.network_momentum_signal import NetworkMomentumSignal
from strategies.base.strategy import StrategyBase
from strategies.context import StrategyContext

from .config import TRADING_DAYS, NetMOMConfig, coerce_config
from .result import NetMOMRunResult


class NetMOMStrategy(StrategyBase):
    """网络动量策略（中国商品期货版）。

    Parameters
    ----------
    config:
        策略配置对象、字典或 None（使用默认值）。
    data_dir:
        china_daily_full/ 数据目录路径。
    verbose:
        打印信号计算进度，默认 False。
    """

    def __init__(
        self,
        config: NetMOMConfig | dict | None = None,
        data_dir: str | Path | None = None,
        verbose: bool = False,
    ) -> None:
        cfg = coerce_config(config)
        super().__init__(cfg.to_dict())
        self.typed_config: NetMOMConfig = cfg
        self.verbose = verbose

        # 复制常用字段到实例，便于访问
        self.min_obs: int = cfg.min_obs
        self.vol_halflife: int = cfg.vol_halflife
        self.sigma_halflife: int = cfg.sigma_halflife
        self.target_vol: float = cfg.target_vol
        self.trading_days: int = cfg.trading_days
        self.trend_threshold: float = cfg.trend_threshold
        self.fee_rate: float = cfg.fee_rate
        self.max_abs_weight: float = cfg.max_abs_weight
        self.max_gross_exposure: float = cfg.max_gross_exposure
        self.exclude: set[str] = set(cfg.exclude)
        self.sector_map: dict[str, str] = dict(cfg.sector_map)

        self._data_dir: Path | None = Path(data_dir) if data_dir else None

        # 信号对象（stateless 参数容器，compute() 是纯函数）
        self._signal = NetworkMomentumSignal(
            mode=cfg.mode,
            graph_method=cfg.graph_method,
            graph_lookbacks=cfg.graph_lookbacks,
            train_window=cfg.train_window,
            retrain_freq=cfg.retrain_freq,
            ridge_alpha=cfg.ridge_alpha,
            sigma_halflife=cfg.sigma_halflife,
            trading_days=cfg.trading_days,
            verbose=verbose,
        )

    # ── StrategyBase 抽象接口 ─────────────────────────────────────────────────

    def generate_signals(self, price_df: pd.DataFrame) -> pd.DataFrame:
        """StrategyBase 价格路径兼容入口（供旧版 BacktestEngine 调用）。"""
        returns_df = price_df.pct_change()
        return self._signal.compute(returns_df)

    def generate_signals_from_returns(
        self, returns_df: pd.DataFrame
    ) -> pd.DataFrame:
        """收益率路径（向量化回测主路径）。"""
        return self._signal.compute(returns_df)

    def build_weights(
        self,
        signal_df: pd.DataFrame,
        vol_df: pd.DataFrame,
        corr_cache: dict | None = None,
    ) -> pd.DataFrame:
        """由预测信号构建波动率缩放仓位。

        Parameters
        ----------
        signal_df:
            Ridge 预测值矩阵（连续）。
        vol_df:
            EWMA 年化波动率矩阵（定仓分母）。

        持仓公式：
            direction = sign(pred) 当 |pred| > trend_threshold 时，否则 0
            raw_w = direction / sigma
            w = raw_w / gross(raw_w)
        """
        direction = self._compute_direction(signal_df)
        # 防线1：vol 下限 5%，避免停板/极低波动期产生天文权重
        vol_safe = vol_df.clip(lower=0.05).replace(0, np.nan)
        raw_weights = (direction / vol_safe).fillna(0.0)
        gross = raw_weights.abs().sum(axis=1).replace(0.0, np.nan)
        weights = raw_weights.div(gross, axis=0).fillna(0.0)
        return weights.clip(lower=-self.max_abs_weight, upper=self.max_abs_weight)

    # ── 内部辅助 ─────────────────────────────────────────────────────────────

    def _compute_direction(self, signal_df: pd.DataFrame) -> pd.DataFrame:
        """将连续预测值转换为方向信号 {-1, 0, +1}，应用 trend_threshold 过滤。"""
        direction = pd.DataFrame(0.0, index=signal_df.index, columns=signal_df.columns)
        direction[signal_df > self.trend_threshold] = 1.0
        direction[signal_df < -self.trend_threshold] = -1.0
        return direction

    def _compute_sigma(self, returns_df: pd.DataFrame) -> pd.DataFrame:
        """EWMA 年化波动率（与信号构建中的 sigma 保持同一半衰期）。"""
        return (
            returns_df.ewm(halflife=self.sigma_halflife, min_periods=30).std()
            * np.sqrt(self.trading_days)
        )

    def _make_backtest(self) -> VectorizedBacktest:
        return VectorizedBacktest(
            lag=1,
            vol_target=self.target_vol,
            vol_halflife=self.vol_halflife,
            trading_days=self.trading_days,
            fee_rate=self.fee_rate,
            max_abs_weight=self.max_abs_weight,
            max_gross_exposure=self.max_gross_exposure,
        )

    def resolve_sector_map(
        self,
        symbols: list[str] | pd.Index,
        context: StrategyContext | None = None,
        sector_map: dict[str, str] | None = None,
    ) -> dict[str, str]:
        """将 symbol 列表映射到板块；缺失项归入 Other。"""
        if context is not None:
            return context.resolve_sector_map(symbols, sector_map=sector_map)
        base = sector_map if sector_map is not None else self.sector_map
        return {str(sym): base.get(str(sym), "Other") for sym in symbols}

    # ── 向量化回测路径 ────────────────────────────────────────────────────────

    def run_vectorized(
        self,
        returns_df: pd.DataFrame,
        backtest: Optional[VectorizedBacktest] = None,
        vol_window: int = 20,
    ) -> BacktestResult:
        """收益率 → 网络动量信号 → 持仓 → VectorizedBacktest。"""
        del vol_window
        signal = self.generate_signals_from_returns(returns_df)
        sigma = self._compute_sigma(returns_df)
        weights = self.build_weights(signal, sigma)
        bt = backtest if backtest is not None else self._make_backtest()
        return bt.run(weights, returns_df)

    # ── 端到端流水线 ──────────────────────────────────────────────────────────

    def run_pipeline(
        self,
        context: StrategyContext | None = None,
        data_dir: str | Path | None = None,
        tickers: list[str] | None = None,
        start: str | None = None,
        end: str | None = None,
        verbose: bool | None = None,
    ) -> NetMOMRunResult:
        """端到端运行：加载数据 → 信号 → 定仓 → VectorizedBacktest PnL。

        Parameters
        ----------
        context:
            注入 StrategyContext（含 DataLoader + sector_map）；
            与 data_dir 二选一。
        data_dir:
            china_daily_full/ 数据目录；context 为 None 时必填。
        tickers:
            指定品种列表；None 时加载全部可用品种。
        start / end:
            数据日期范围过滤。
        verbose:
            覆盖构造时的 verbose 设置。
        """
        verbose = self.verbose if verbose is None else verbose

        # ── Step 1: 加载收益率矩阵 ────────────────────────────────────────────
        if verbose:
            print("=" * 65)
            print("Step 1: Load china_daily_full returns")
            print("=" * 65)

        if context is not None:
            returns = context.load_returns_matrix(
                tickers=tickers,
                start=start,
                end=end,
                min_obs=self.min_obs,
                exclude=self.exclude,
            )
        else:
            from data.loader import DataLoader, KlineSchema
            from data.sources.parquet_source import ParquetSource

            d = Path(data_dir) if data_dir else self._data_dir
            if d is None:
                raise ValueError(
                    "data_dir must be provided via constructor or run_pipeline()"
                )
            loader = DataLoader(
                kline_source=ParquetSource(d),
                kline_schema=KlineSchema.tushare(),
            )
            if tickers is None:
                tickers = loader.available_symbols(exclude=self.exclude)
            returns = loader.load_returns_matrix(
                tickers, start=start, end=end, min_obs=self.min_obs
            )

        if returns.empty:
            raise RuntimeError("No returns loaded. Check data_dir and tickers.")

        sector_map = self.resolve_sector_map(returns.columns, context=context)

        if verbose:
            print(
                f"\nReturns matrix: {returns.shape}  "
                f"({returns.index[0].date()} - {returns.index[-1].date()})"
            )

        # ── Step 2: 计算网络动量信号 ──────────────────────────────────────────
        if verbose:
            cfg = self.typed_config
            print("\n" + "=" * 65)
            print(
                f"Step 2: Network Momentum Signal "
                f"(mode={cfg.mode}, graph={cfg.graph_method}, "
                f"train={cfg.train_window}d, refit={cfg.retrain_freq}d)"
            )
            print("=" * 65)

        signal = self.generate_signals_from_returns(returns)
        sigma = self._compute_sigma(returns)

        if verbose:
            active_frac = (signal.abs() > 0).mean().mean()
            print(f"  Active signal fraction: {active_frac:.1%}")

        # ── Step 3: 构建持仓 ──────────────────────────────────────────────────
        if verbose:
            print("\n" + "=" * 65)
            print(
                f"Step 3: Build positions "
                f"(trend_threshold={self.trend_threshold:.2f}, "
                f"target_vol={self.target_vol:.0%})"
            )
            print("=" * 65)

        positions = self.build_weights(signal, sigma)

        if verbose:
            long_frac = (positions > 0).sum().sum() / max(
                (positions != 0).sum().sum(), 1
            )
            short_frac = (positions < 0).sum().sum() / max(
                (positions != 0).sum().sum(), 1
            )
            gross_mean = positions.abs().sum(axis=1).mean()
            print(
                f"  Long/Short: {long_frac:.1%}/{short_frac:.1%}  "
                f"Avg gross exposure: {gross_mean:.2f}"
            )

        # ── Step 4: 向量化回测 ────────────────────────────────────────────────
        if verbose:
            print("\n" + "=" * 65)
            print(
                f"Step 4: VectorizedBacktest "
                f"(vol-targeted, fee={self.fee_rate * 10000:.1f}bps)"
            )
            print("=" * 65)

        bt = (
            context.backtest
            if context is not None and context.backtest is not None
            else self._make_backtest()
        )
        bt_result = bt.run(positions, returns)
        pnl = bt_result.returns.iloc[1:]

        if verbose and not pnl.empty:
            self._print_summary(pnl, "NetMOM")

        return NetMOMRunResult(
            returns=returns,
            signal=signal,
            sigma=sigma,
            positions=positions,
            pnl=pnl,
            sector_map=sector_map,
            backtest_result=bt_result,
            metadata={
                "mode": self.typed_config.mode,
                "graph_method": self.typed_config.graph_method,
                "graph_lookbacks": self.typed_config.graph_lookbacks,
                "train_window": self.typed_config.train_window,
                "retrain_freq": self.typed_config.retrain_freq,
                "ridge_alpha": self.typed_config.ridge_alpha,
                "trend_threshold": self.trend_threshold,
                "target_vol": self.target_vol,
                "fee_rate": self.fee_rate,
                "max_abs_weight": self.max_abs_weight,
                "max_gross_exposure": self.max_gross_exposure,
                "n_symbols": returns.shape[1],
                "start": str(returns.index[0].date()),
                "end": str(returns.index[-1].date()),
            },
        )

    @staticmethod
    def _print_summary(pnl: pd.Series, label: str) -> None:
        ann_r = pnl.mean() * TRADING_DAYS
        ann_v = pnl.std() * np.sqrt(TRADING_DAYS)
        sharpe = ann_r / ann_v if ann_v > 0 else float("nan")
        nav = (1 + pnl).cumprod()
        mdd = ((nav - nav.cummax()) / nav.cummax()).min()
        print(
            f"  [{label}]  Sharpe={sharpe:.3f}  Return={ann_r*100:.1f}%  "
            f"Vol={ann_v*100:.1f}%  MaxDD={mdd*100:.1f}%  "
            f"({pnl.index[0].date()} - {pnl.index[-1].date()})"
        )
