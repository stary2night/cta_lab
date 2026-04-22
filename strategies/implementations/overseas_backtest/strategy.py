"""OverseasTrendSuite：海外期货趋势/动量策略套件。

该模块承接海外期货三策略的核心研究逻辑：
- JPM t-stat 多周期趋势
- TSMOM Binary 时序动量
- Dual Momentum L/S

脚本层只负责 CLI 与输出，数据依赖、信号、定仓和向量化回测在这里收口。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path

import numpy as np
import pandas as pd

from backtest.vectorized import VectorizedBacktest
from strategies.context import StrategyContext

from .config import TRADING_DAYS, OverseasTrendSuiteConfig, coerce_config


@dataclass
class OverseasTrendSuiteResult:
    """海外趋势策略套件运行结果。"""

    returns: pd.DataFrame
    sigma: pd.DataFrame
    signals: dict[str, pd.DataFrame]
    positions: dict[str, pd.DataFrame]
    pnl: dict[str, pd.Series]
    sector_map: dict[str, str]
    metadata: dict = field(default_factory=dict)


class OverseasTrendSuite:
    """海外期货趋势/动量策略套件。

    这不是单一可交易策略，而是在同一海外期货 universe 与回测口径下，
    并行运行 JPM、TSMOM、Dual Momentum 三条趋势/动量研究路径。
    """

    def __init__(
        self,
        config: OverseasTrendSuiteConfig | dict | None = None,
        data_dir: str | Path | None = None,
    ) -> None:
        cfg = coerce_config(config)
        self.typed_config = cfg
        self.min_obs: int = cfg.min_obs
        self.trading_days: int = cfg.trading_days
        self.target_vol: float = cfg.target_vol
        self.vol_halflife: int = cfg.vol_halflife
        self.sigma_halflife: int = cfg.sigma_halflife
        self.tsmom_lookback: int = cfg.tsmom_lookback
        self.jpm_lookbacks: list[int] = list(cfg.jpm_lookbacks)
        self.dual_top_pct: float = cfg.dual_top_pct
        self.exclude: set[str] = set(cfg.exclude)
        self.sector_map: dict[str, str] = dict(cfg.sector_map)
        self.strategies: list[str] = list(cfg.strategies)
        self.labels: dict[str, str] = dict(cfg.labels)
        self.colors: dict[str, str] = dict(cfg.colors)
        self._data_dir: Path | None = Path(data_dir) if data_dir else None

    def compute_sigma(self, returns_df: pd.DataFrame) -> pd.DataFrame:
        """EWMA 年化截面波动率，用作 signal / sigma 定仓分母。"""
        return (
            returns_df.ewm(halflife=self.sigma_halflife, min_periods=30).std()
            * np.sqrt(self.trading_days)
        ).replace(0, np.nan)

    def build_weights(
        self,
        signal_df: pd.DataFrame,
        vol_df: pd.DataFrame,
    ) -> pd.DataFrame:
        """基准定仓：w = signal / sigma。"""
        return (signal_df / vol_df).fillna(0.0)

    def generate_signals(
        self,
        returns_df: pd.DataFrame,
        strategy: str,
        sector_map: dict[str, str] | None = None,
    ) -> pd.DataFrame:
        """计算指定子策略信号。"""
        if strategy == "jpm":
            from signals.momentum.jpm_tstat import JPMTstatSignal

            return JPMTstatSignal(lookbacks=self.jpm_lookbacks).compute_from_returns(returns_df)

        if strategy == "tsmom":
            from signals.momentum.nltsmom import NLTSMOMSignal, SignalMode

            return NLTSMOMSignal(
                lookback=self.tsmom_lookback,
                sigma_halflife=self.sigma_halflife,
                mode=SignalMode.BINARY,
                trading_days=self.trading_days,
            ).compute(returns_df)

        if strategy == "dual_ls":
            from signals.momentum.dual_momentum import DualMomentumSignal

            sm = self.resolve_sector_map(returns_df.columns, sector_map=sector_map)
            return DualMomentumSignal(
                sector_map=sm,
                lookback=self.tsmom_lookback,
                top_pct=self.dual_top_pct,
                bottom_pct=self.dual_top_pct,
                mode="dual_ls",
                sigma_halflife=self.sigma_halflife,
                trading_days=self.trading_days,
            ).compute(returns_df)

        raise ValueError(f"Unknown overseas trend strategy: {strategy}")

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
        return {str(symbol): base.get(str(symbol), "Other") for symbol in symbols}

    def _make_backtest(self) -> VectorizedBacktest:
        return VectorizedBacktest(
            lag=1,
            vol_target=self.target_vol,
            vol_halflife=self.vol_halflife,
            trading_days=self.trading_days,
        )

    def run_pipeline(
        self,
        context: StrategyContext | None = None,
        data_dir: str | Path | None = None,
        tickers: list[str] | None = None,
        start: str | None = None,
        strategies: list[str] | None = None,
        verbose: bool = True,
    ) -> OverseasTrendSuiteResult:
        """加载海外收益率矩阵，并行运行三条趋势/动量子策略。"""
        run_strategies = strategies if strategies is not None else self.strategies

        if verbose:
            print("=" * 65)
            print("Step 1: Load overseas_daily_full returns")
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

            d = Path(data_dir) if data_dir else self._data_dir
            if d is None:
                raise ValueError("data_dir must be provided via constructor or run_pipeline()")

            loader = DataLoader(
                kline_source=ParquetSource(d),
                kline_schema=KlineSchema.overseas(),
            )
            if tickers is None:
                tickers = loader.available_symbols(exclude=self.exclude)
            returns = loader.load_returns_matrix(tickers, min_obs=self.min_obs)

        if start:
            returns = returns.loc[start:]

        if returns.empty:
            raise RuntimeError("No returns loaded. Check data_dir and tickers.")

        if verbose:
            print(
                f"\nReturns matrix: {returns.shape}  "
                f"({returns.index[0].date()} - {returns.index[-1].date()})"
            )

        sigma = self.compute_sigma(returns)
        bt = context.backtest if context is not None and context.backtest is not None else self._make_backtest()
        sym_sector = self.resolve_sector_map(returns.columns, context=context)

        if verbose:
            print("\n" + "=" * 65)
            print("Step 2: Signals & Backtest")
            print("=" * 65)

        signals: dict[str, pd.DataFrame] = {}
        positions: dict[str, pd.DataFrame] = {}
        pnl: dict[str, pd.Series] = {}

        for name in run_strategies:
            signal = self.generate_signals(returns, name, sector_map=sym_sector)
            weight = self.build_weights(signal, sigma)
            result = bt.run(weight, returns)
            daily_pnl = result.returns.iloc[1:]

            signals[name] = signal
            positions[name] = weight
            pnl[name] = daily_pnl

            if verbose:
                self._print_summary(daily_pnl, self.labels.get(name, name))

        return OverseasTrendSuiteResult(
            returns=returns,
            sigma=sigma,
            signals=signals,
            positions=positions,
            pnl=pnl,
            sector_map=sym_sector,
            metadata={
                "strategies": run_strategies,
                "target_vol": self.target_vol,
                "tsmom_lookback": self.tsmom_lookback,
                "jpm_lookbacks": self.jpm_lookbacks,
                "dual_top_pct": self.dual_top_pct,
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
            f"  [{label:30s}]  SR={sharpe:.3f}  Ret={ann_r*100:.1f}%  "
            f"Vol={ann_v*100:.1f}%  MDD={mdd*100:.1f}%"
        )
