"""DualMomentumStrategy：绝对动量与相对动量组合策略。

四种信号模式：
  absolute  : sign(cum_log_ret_lookback)        纯绝对动量 = TSMOM Binary
  relative  : 板块内分位排名                     纯相对动量（板块内截面）
  dual_ls   : 相对强 AND 绝对正→多；相对弱 AND 绝对负→空
  dual_lo   : 相对强 AND 绝对正→多；否则平仓（仅做多）

持仓：w = signal / sigma_ewma
回测：VectorizedBacktest，lag=1，vol_target=target_vol
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
import numpy as np
import pandas as pd

from strategies.base.strategy import StrategyBase
from strategies.context import StrategyContext
from strategies.implementations.crossmom_backtest.config import CrossMOMConfig
from strategies.implementations.crossmom_backtest.strategy import CrossMOMStrategy

from .config import MODES, TRADING_DAYS, DualMomentumConfig, coerce_config


@dataclass
class DualMomentumRunResult:
    """DualMomentumStrategy 端到端运行结果。"""

    returns: pd.DataFrame                 # 品种日收益率宽表
    signals: dict[str, pd.DataFrame]     # mode → 信号矩阵
    positions: dict[str, pd.DataFrame]   # mode → 持仓权重矩阵
    pnl: dict[str, pd.Series]            # mode → 组合日收益序列
    sector_map: dict[str, str]           # {symbol: sector}
    metadata: dict = field(default_factory=dict)


class DualMomentumStrategy(StrategyBase):
    """双动量策略（国内期货版）。

    Parameters
    ----------
    config:
        策略配置对象、字典或 None（使用默认值）。
    data_dir:
        china_daily_full/ 数据目录。
    """

    def __init__(
        self,
        config: DualMomentumConfig | dict | None = None,
        data_dir: str | None = None,
    ) -> None:
        cfg = coerce_config(config)
        super().__init__(cfg.to_dict())
        self.lookback: int        = cfg.lookback
        self.min_obs: int         = cfg.min_obs
        self.vol_halflife: int    = cfg.vol_halflife
        self.sigma_halflife: int  = cfg.sigma_halflife
        self.target_vol: float    = cfg.target_vol
        self.trading_days: int    = cfg.trading_days
        self.top_pct: float       = cfg.top_pct
        self.bottom_pct: float    = cfg.bottom_pct
        self.exclude: set[str]    = set(cfg.exclude)
        self.sector_map: dict[str, str] = dict(cfg.sector_map)

        self._data_dir = data_dir
        self._crossmom = CrossMOMStrategy(
            config=CrossMOMConfig(
                lookback=self.lookback,
                min_obs=self.min_obs,
                vol_halflife=self.vol_halflife,
                sigma_halflife=self.sigma_halflife,
                target_vol=self.target_vol,
                trading_days=self.trading_days,
                top_pct=self.top_pct,
                bottom_pct=self.bottom_pct,
                exclude=sorted(self.exclude),
                sector_map=self.sector_map,
            ),
        )

    # ── 信号 ──────────────────────────────────────────────────────────────────

    def generate_signals(
        self,
        returns_df: pd.DataFrame,
        mode: str = "dual_ls",
        context: StrategyContext | None = None,
        sector_map: dict[str, str] | None = None,
    ) -> pd.DataFrame:
        """计算指定模式的双动量信号。

        Parameters
        ----------
        returns_df:
            日收益率宽表。
        mode:
            信号模式（'absolute' / 'relative' / 'dual_ls' / 'dual_lo'）。
        sector_map:
            {symbol: sector}；None 时使用策略配置中的 sector_map。
        """
        if mode == "relative":
            return self._crossmom.generate_signals(returns_df, context=context, sector_map=sector_map)

        from signals.momentum.dual_momentum import DualMomentumSignal

        sm = self._crossmom.resolve_sector_map(returns_df.columns, context=context, sector_map=sector_map)
        sig = DualMomentumSignal(
            sector_map=sm,
            lookback=self.lookback,
            top_pct=self.top_pct,
            bottom_pct=self.bottom_pct,
            mode=mode,
            sigma_halflife=self.sigma_halflife,
            trading_days=self.trading_days,
        )
        return sig.compute(returns_df)

    # ── 定仓 ──────────────────────────────────────────────────────────────────

    def build_weights(
        self,
        signal_df: pd.DataFrame,
        vol_df: pd.DataFrame,
        corr_cache: dict | None = None,
    ) -> pd.DataFrame:
        """基准定仓：w = signal / sigma。"""
        return self._crossmom.build_weights(signal_df, vol_df)

    def run_vectorized(
        self,
        returns_df: pd.DataFrame,
        backtest=None,
        vol_window: int = 20,
        mode: str = "dual_ls",
    ):
        """收益率矩阵 → 指定 Dual Momentum 信号 → 权重 → VectorizedBacktest。"""
        sector_map = self._crossmom.resolve_sector_map(returns_df.columns)
        signal_df = self.generate_signals(returns_df, mode=mode, sector_map=sector_map)
        sigma = self._compute_sigma(returns_df).replace(0, np.nan)
        weight_df = self.build_weights(signal_df, sigma)
        bt = backtest if backtest is not None else self._make_backtest()
        return bt.run(weight_df, returns_df)

    # ── 内部辅助 ──────────────────────────────────────────────────────────────

    def _compute_sigma(self, returns_df: pd.DataFrame) -> pd.DataFrame:
        """EWMA 年化截面波动率（定仓分母）。"""
        return self._crossmom.compute_sigma(returns_df)

    def _make_backtest(self):
        from backtest.vectorized import VectorizedBacktest

        return VectorizedBacktest(
            lag=1,
            vol_target=self.target_vol,
            vol_halflife=self.vol_halflife,
            trading_days=self.trading_days,
        )

    # ── 端到端流水线 ──────────────────────────────────────────────────────────

    def run_pipeline(
        self,
        context: StrategyContext | None = None,
        data_dir: str | Path | None = None,
        tickers: list[str] | None = None,
        modes: list[str] | None = None,
        verbose: bool = True,
    ) -> DualMomentumRunResult:
        """端到端运行：加载数据 → 多模式信号 → 定仓 → VectorizedBacktest PnL。

        Parameters
        ----------
        data_dir:
            china_daily_full/ 数据目录，覆盖构造时传入的路径。
        tickers:
            指定品种列表；None 时加载全部可用品种。
        modes:
            要计算的信号模式列表；None 时使用全部四种模式。
        verbose:
            打印进度。
        """
        run_modes = modes if modes is not None else MODES

        # 1. 加载数据
        if verbose:
            print("=" * 65)
            print("Step 1: Load china_daily_full returns")
            print("=" * 65)

        if context is not None:
            returns = context.load_returns_matrix(
                tickers=tickers,
                min_obs=self.min_obs,
                exclude=self.exclude,
            )
        else:
            from data.loader import DataLoader, KlineSchema
            from data.sources.parquet_source import ParquetSource

            d = data_dir if data_dir else self._data_dir
            if d is None:
                raise ValueError("data_dir must be provided via constructor or run_pipeline()")

            loader = DataLoader(
                kline_source=ParquetSource(d),
                kline_schema=KlineSchema.tushare(),
            )
            if tickers is None:
                tickers = loader.available_symbols(exclude=self.exclude)
            returns = loader.load_returns_matrix(tickers, min_obs=self.min_obs)
        if returns.empty:
            raise RuntimeError("No returns loaded. Check data_dir and tickers.")
        if verbose:
            print(
                f"\nReturns matrix: {returns.shape}  "
                f"({returns.index[0].date()} - {returns.index[-1].date()})"
            )

        # 2. 共用组件
        sigma = self._compute_sigma(returns).replace(0, np.nan)
        bt = context.backtest if context is not None and context.backtest is not None else self._make_backtest()
        sym_sector = self._crossmom.resolve_sector_map(returns.columns, context=context)

        # 3. 多模式信号与回测
        if verbose:
            print("\n" + "=" * 65)
            print(f"Step 2: Signals & backtest ({len(run_modes)} modes)")
            print("=" * 65)

        signals:   dict[str, pd.DataFrame] = {}
        positions: dict[str, pd.DataFrame] = {}
        pnl:       dict[str, pd.Series]    = {}

        for mode in run_modes:
            sig    = self.generate_signals(returns, mode, context=context, sector_map=sym_sector)
            weight = self.build_weights(sig, sigma)
            result = bt.run(weight, returns)
            daily_pnl = result.returns.iloc[1:]

            signals[mode]   = sig
            positions[mode] = weight
            pnl[mode]       = daily_pnl

            if verbose:
                self._print_summary(daily_pnl, mode)

        return DualMomentumRunResult(
            returns=returns,
            signals=signals,
            positions=positions,
            pnl=pnl,
            sector_map=sym_sector,
            metadata={
                "lookback":     self.lookback,
                "modes":        run_modes,
                "top_pct":      self.top_pct,
                "bottom_pct":   self.bottom_pct,
                "target_vol":   self.target_vol,
                "n_symbols":    returns.shape[1],
                "start":        str(returns.index[0].date()),
                "end":          str(returns.index[-1].date()),
            },
        )

    @staticmethod
    def _print_summary(pnl: pd.Series, label: str) -> None:
        ann_r  = pnl.mean() * TRADING_DAYS
        ann_v  = pnl.std() * np.sqrt(TRADING_DAYS)
        sharpe = ann_r / ann_v if ann_v > 0 else float("nan")
        nav    = (1 + pnl).cumprod()
        mdd    = ((nav - nav.cummax()) / nav.cummax()).min()
        print(
            f"  [{label:12s}]  Sharpe={sharpe:.3f}  Return={ann_r*100:.1f}%  "
            f"Vol={ann_v*100:.1f}%  MaxDD={mdd*100:.1f}%  "
            f"({pnl.index[0].date()} - {pnl.index[-1].date()})"
        )
