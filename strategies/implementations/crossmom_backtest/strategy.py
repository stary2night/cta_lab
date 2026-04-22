"""CrossMOMStrategy：仅承载相对动量策略逻辑。"""

from __future__ import annotations

from pathlib import Path

import numpy as np
import pandas as pd

from strategies.base.strategy import StrategyBase
from strategies.context import StrategyContext

from .config import TRADING_DAYS, CrossMOMConfig, coerce_config
from .result import CrossMOMRunResult


class CrossMOMStrategy(StrategyBase):
    """相对动量策略核心。

    当前文件只保留与策略本身直接相关的逻辑：
    - relative signal 生成
    - sigma 计算
    - signal -> weights
    """

    def __init__(
        self,
        config: CrossMOMConfig | dict | None = None,
    ) -> None:
        cfg = coerce_config(config)
        super().__init__(cfg.to_dict())
        self.lookback: int = cfg.lookback
        self.min_obs: int = cfg.min_obs
        self.vol_halflife: int = cfg.vol_halflife
        self.sigma_halflife: int = cfg.sigma_halflife
        self.target_vol: float = cfg.target_vol
        self.trading_days: int = cfg.trading_days
        self.top_pct: float = cfg.top_pct
        self.bottom_pct: float = cfg.bottom_pct
        self.exclude: set[str] = set(cfg.exclude)
        self.sector_map: dict[str, str] = dict(cfg.sector_map)

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

    def generate_signals(
        self,
        returns_df: pd.DataFrame,
        context: StrategyContext | None = None,
        sector_map: dict[str, str] | None = None,
    ) -> pd.DataFrame:
        """计算相对动量信号矩阵。"""
        from signals.momentum.dual_momentum import DualMomentumSignal

        sm = self.resolve_sector_map(returns_df.columns, context=context, sector_map=sector_map)
        signal = DualMomentumSignal(
            sector_map=sm,
            lookback=self.lookback,
            top_pct=self.top_pct,
            bottom_pct=self.bottom_pct,
            mode="relative",
            sigma_halflife=self.sigma_halflife,
            trading_days=self.trading_days,
        )
        return signal.compute(returns_df)

    def compute_sigma(self, returns_df: pd.DataFrame) -> pd.DataFrame:
        """EWMA 年化截面波动率（定仓分母）。"""
        return (
            returns_df.ewm(halflife=self.sigma_halflife, min_periods=30).std()
            * np.sqrt(self.trading_days)
        )

    def build_weights(
        self,
        signal_df: pd.DataFrame,
        vol_df: pd.DataFrame,
        corr_cache: dict | None = None,
    ) -> pd.DataFrame:
        """基准定仓：w = signal / sigma。"""
        vol_safe = vol_df.replace(0, np.nan)
        return (signal_df / vol_safe).fillna(0.0)

    def run_vectorized(
        self,
        returns_df: pd.DataFrame,
        backtest=None,
        vol_window: int = 20,
    ):
        """收益率矩阵 → CrossMOM 信号 → 权重 → VectorizedBacktest。"""
        signal_df = self.generate_signals(returns_df)
        sigma = self.compute_sigma(returns_df)
        weight_df = self.build_weights(signal_df, sigma)
        bt = backtest if backtest is not None else self._make_backtest()
        return bt.run(weight_df, returns_df)

    def _make_backtest(self):
        from backtest.vectorized import VectorizedBacktest

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
        verbose: bool = True,
    ) -> CrossMOMRunResult:
        """端到端运行：加载数据 → 相对动量信号 → 定仓 → VectorizedBacktest PnL。

        这里只收口策略运行核心依赖；reports/charts 仍由 scripts 层负责。
        """
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

            if data_dir is None:
                raise ValueError("data_dir must be provided via run_pipeline()")

            loader = DataLoader(
                kline_source=ParquetSource(Path(data_dir)),
                kline_schema=KlineSchema.tushare(),
            )
            if tickers is None:
                tickers = loader.available_symbols(exclude=self.exclude)
            returns = loader.load_returns_matrix(tickers, min_obs=self.min_obs)

        if returns.empty:
            raise RuntimeError("No returns loaded. Check data_dir and tickers.")

        sector_map = self.resolve_sector_map(returns.columns, context=context)

        if verbose:
            print(
                f"\nReturns matrix: {returns.shape}  "
                f"({returns.index[0].date()} - {returns.index[-1].date()})"
            )
            print("\n" + "=" * 65)
            print(
                "Step 2: Compute relative momentum signal "
                f"(lookback={self.lookback}d, top={self.top_pct:.0%}, bottom={self.bottom_pct:.0%})"
            )
            print("=" * 65)

        signal = self.generate_signals(returns, context=context, sector_map=sector_map)
        sigma = self.compute_sigma(returns)
        positions = self.build_weights(signal, sigma)

        if verbose:
            valid_frac = signal.notna().mean().mean()
            long_frac = (signal > 0).sum().sum() / max(signal.notna().sum().sum(), 1)
            short_frac = (signal < 0).sum().sum() / max(signal.notna().sum().sum(), 1)
            print(
                f"  Signal coverage: {valid_frac:.1%}  "
                f"Long fraction: {long_frac:.1%}  Short fraction: {short_frac:.1%}"
            )
            print("\n" + "=" * 65)
            print("Step 3: VectorizedBacktest — vol-targeted PnL")
            print("=" * 65)

        bt = context.backtest if context is not None and context.backtest is not None else self._make_backtest()
        bt_result = bt.run(positions, returns)
        pnl = bt_result.returns.iloc[1:]

        if verbose and not pnl.empty:
            self._print_summary(pnl, "CrossMOM")

        return CrossMOMRunResult(
            returns=returns,
            signal=signal,
            positions=positions,
            pnl=pnl,
            sigma=sigma,
            sector_map=sector_map,
            metadata={
                "lookback": self.lookback,
                "top_pct": self.top_pct,
                "bottom_pct": self.bottom_pct,
                "target_vol": self.target_vol,
                "n_symbols": returns.shape[1],
                "start": str(returns.index[0].date()),
                "end": str(returns.index[-1].date()),
            },
        )

    def _print_summary(self, pnl: pd.Series, label: str) -> None:
        ann_r = pnl.mean() * TRADING_DAYS
        ann_v = pnl.std() * np.sqrt(TRADING_DAYS)
        sharpe = ann_r / ann_v if ann_v > 0 else float("nan")
        nav = (1 + pnl).cumprod()
        mdd = ((nav - nav.cummax()) / nav.cummax()).min()
        print(
            f"  [{label:12s}]  Sharpe={sharpe:.3f}  Return={ann_r * 100:.1f}%  "
            f"Vol={ann_v * 100:.1f}%  MaxDD={mdd * 100:.1f}%"
        )
