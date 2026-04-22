"""TSMOMStrategy：基于 TSMOM 信号的时序动量策略。

信号（Moskowitz et al., 2012）：
    signal_{s,t} = sign( Σ log(P_{s,t-i+1}/P_{s,t-i}), i=1..lookback )
               = sign( log(P_{s,t} / P_{s,t-lookback}) )

头寸：
    w_{s,t} = signal_{s,t} / sigma_{s,t}     （基准，与 JPM baseline 形式相同）

其中 sigma 为 EWMA 年化波动率（halflife=sigma_halflife）。
VectorizedBacktest 在组合层面叠加 vol-targeting，使年化波动趋近 target_vol。

可选 CorrCap 路径（corr_cache 不为 None）：
    与 JPMTrendStrategy 相同，通过 CorrCapSizer 对冲截面相关性。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from backtest.result import BacktestResult
from backtest.vectorized import VectorizedBacktest
from portfolio.sizing.corr_cap import CorrCapSizer
from strategies.base.strategy import StrategyBase
from strategies.context import StrategyContext

from .config import TRADING_DAYS, TSMOMConfig, coerce_config


@dataclass
class TSMOMRunResult:
    """TSMOMStrategy 端到端运行结果。"""

    returns: pd.DataFrame        # 品种日收益率宽表
    signal: pd.DataFrame         # TSMOM 信号（{-1, 0, +1}）
    sigma: pd.DataFrame          # EWMA 年化波动率
    baseline_pos: pd.DataFrame   # 基准持仓（signal / sigma）
    corrcap_pos: pd.DataFrame    # CorrCap 持仓
    pnl_baseline: pd.Series      # 基准组合 PnL（vol-targeted）
    pnl_corrcap: pd.Series       # CorrCap 组合 PnL（vol-targeted）
    sector_map: dict[str, str]   # {symbol: sector}
    metadata: dict = field(default_factory=dict)
    report: dict = field(default_factory=dict)


class TSMOMStrategy(StrategyBase):
    """TSMOM 时序动量策略（国内期货版）。

    Parameters
    ----------
    config:
        策略配置对象、字典或 None（使用默认值）。
    data_dir:
        china_daily_full/ 数据目录。
    fee_rate:
        单边换手成本率，默认 0.0，保持原主线无费用口径。
    """

    def __init__(
        self,
        config: TSMOMConfig | dict | None = None,
        data_dir: str | Path | None = None,
        fee_rate: float = 0.0,
    ) -> None:
        cfg = coerce_config(config)
        super().__init__(cfg.to_dict())
        self.typed_config: TSMOMConfig = cfg
        self.lookback: int = cfg.lookback
        self.min_obs: int = cfg.min_obs
        self.vol_halflife: int = cfg.vol_halflife
        self.sigma_halflife: int = cfg.sigma_halflife
        self.target_vol: float = cfg.target_vol
        self.trading_days: int = cfg.trading_days
        self.corr_window: int = cfg.corr_window
        self.corr_min_periods: int = cfg.corr_min_periods
        self.corr_cap: float = cfg.corr_cap
        self.fee_rate: float = float(fee_rate)
        self.exclude: set[str] = set(cfg.exclude)
        self.sector_map: dict[str, str] = dict(cfg.sector_map)

        self._data_dir: Path | None = Path(data_dir) if data_dir else None

        self._cc_sizer = CorrCapSizer(
            cap=self.corr_cap,
            target_vol=self.target_vol,
            trading_days=self.trading_days,
        )

    # ── 信号 ──────────────────────────────────────────────────────────────────

    def generate_signals(
        self,
        returns_df: pd.DataFrame,
        mode: str = "binary",
    ) -> pd.DataFrame:
        """从收益率矩阵计算 TSMOM 信号。

        Parameters
        ----------
        returns_df:
            日收益率宽表。
        mode:
            信号模式：
            'binary'    → sign(cum_log_ret)，{-1, 0, +1}
            'linear'    → cum_log_ret / sigma（vol 标准化）
            'nonlinear' → f(z) = z/(z²+1)，FS 非线性，z = cum_log_ret/sigma
        """
        from signals.momentum.nltsmom import NLTSMOMSignal, SignalMode

        sig = NLTSMOMSignal(
            lookback=self.lookback,
            sigma_halflife=self.sigma_halflife,
            mode=SignalMode(mode),
            trading_days=self.trading_days,
        )
        return sig.compute(returns_df)

    def generate_signals_from_returns(
        self,
        returns_df: pd.DataFrame,
        mode: str = "binary",
    ) -> pd.DataFrame:
        """直接从收益率矩阵计算信号，供向量化路径显式调用。"""
        return self.generate_signals(returns_df, mode=mode)

    def generate_signals_from_prices(
        self,
        price_df: pd.DataFrame,
        mode: str = "binary",
    ) -> pd.DataFrame:
        """StrategyBase 价格路径兼容入口。"""
        return self.generate_signals_from_returns(price_df.pct_change(), mode=mode)

    # ── 定仓 ──────────────────────────────────────────────────────────────────

    def build_weights(
        self,
        signal_df: pd.DataFrame,
        vol_df: pd.DataFrame,
        corr_cache: dict | None = None,
    ) -> pd.DataFrame:
        """统一定仓入口。

        corr_cache=None  → 基准：w = signal / sigma
        corr_cache=dict  → CorrCapSizer
        """
        if corr_cache is not None:
            return self._cc_sizer.compute(signal_df, vol_df, corr_cache=corr_cache)
        vol_safe = vol_df.replace(0, np.nan)
        return (signal_df / vol_safe).fillna(0.0)

    # ── 内部辅助 ──────────────────────────────────────────────────────────────

    def _compute_sigma(self, returns_df: pd.DataFrame) -> pd.DataFrame:
        """EWMA 年化截面波动率（用于定仓分母）。"""
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
        )

    def run_vectorized(
        self,
        returns_df: pd.DataFrame,
        backtest: Optional[VectorizedBacktest] = None,
        vol_window: int = 20,
        corr_cache: dict | None = None,
    ) -> BacktestResult:
        """向量化回测：收益率 → TSMOM 信号 → 持仓 → VectorizedBacktest。"""
        del vol_window
        sigma = self._compute_sigma(returns_df)
        signal = self.generate_signals_from_returns(returns_df, mode="binary")
        weight = self.build_weights(signal, sigma, corr_cache=corr_cache)
        bt = backtest if backtest is not None else self._make_backtest()
        return bt.run(weight, returns_df)

    def run(
        self,
        price_df: pd.DataFrame,
        adjust_dates: set[pd.Timestamp],
        engine,
    ) -> BacktestResult:
        """事件驱动兼容路径：价格 → 收益率信号 → 权重 → BacktestEngine。"""
        returns_df = price_df.pct_change()
        sigma = self._compute_sigma(returns_df)
        signal = self.generate_signals_from_returns(returns_df, mode="binary")
        weight = self.build_weights(signal, sigma)
        return engine.run(weight, price_df, adjust_dates)

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

    # ── 端到端流水线 ──────────────────────────────────────────────────────────

    def run_pipeline(
        self,
        context: StrategyContext | None = None,
        data_dir: str | Path | None = None,
        tickers: list[str] | None = None,
        compute_corrcap: bool = False,
        verbose: bool = True,
        mode: str = "binary",
        run_analysis: bool = False,
        benchmark_returns: pd.Series | None = None,
        output_dir: str | Path | None = None,
    ) -> TSMOMRunResult:
        """端到端运行：加载数据 → 信号 → 定仓 → VectorizedBacktest PnL。

        Parameters
        ----------
        data_dir:
            china_daily_full/ 数据目录。
        tickers:
            指定品种；None 时加载全部。
        compute_corrcap:
            是否计算 CorrCap 变体（较慢，默认关闭）。
        verbose:
            打印进度。
        run_analysis:
            是否运行 StrategyReport 分析层。
        benchmark_returns:
            可选基准收益，用于分析层危机分解。
        output_dir:
            分析层图表 / 表格输出目录；None 时只返回内存结果。
        """
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

            d = Path(data_dir) if data_dir else self._data_dir
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

        # 2. 计算信号 & 截面波动率
        if verbose:
            print("\n" + "=" * 65)
            print(f"Step 2: Compute TSMOM signal (lookback={self.lookback}d, mode={mode})")
            print("=" * 65)

        signal = self.generate_signals_from_returns(returns, mode=mode)
        sigma = self._compute_sigma(returns)

        if verbose:
            valid_frac = signal.notna().mean().mean()
            long_frac = (signal > 0).sum().sum() / signal.notna().sum().sum()
            print(f"  Signal coverage: {valid_frac:.1%}  Long fraction: {long_frac:.1%}")

        # 3. 基准持仓
        baseline_pos = self.build_weights(signal, sigma)
        if verbose:
            print(f"  Baseline positions: {baseline_pos.shape}")

        # 4. CorrCap 持仓（可选）
        if compute_corrcap:
            if verbose:
                print("\n" + "=" * 65)
                print(f"Step 3: CorrCap (cap={self.corr_cap}) positions")
                print("=" * 65)
                print("  Computing rolling correlations...")

            corr_cache = CorrCapSizer.build_corr_cache(
                returns,
                window=self.corr_window,
                min_periods=self.corr_min_periods,
            )
            if verbose:
                print(f"  Cached {len(corr_cache)} dates.")

            corrcap_pos = self._cc_sizer.compute(
                signal, sigma, corr_cache=corr_cache, verbose=verbose
            )
        else:
            corrcap_pos = pd.DataFrame(0.0, index=signal.index, columns=signal.columns)

        # 5. 向量化回测
        if verbose:
            print("\n" + "=" * 65)
            print(
                "Step 4: VectorizedBacktest — "
                f"vol-targeted PnL, fee={self.fee_rate * 10000:.1f}bps"
            )
            print("=" * 65)

        bt = context.backtest if context is not None and context.backtest is not None else self._make_backtest()
        result_baseline = bt.run(baseline_pos, returns)
        pnl_baseline = result_baseline.returns.iloc[1:]

        if compute_corrcap:
            result_corrcap = bt.run(corrcap_pos, returns)
            pnl_corrcap = result_corrcap.returns.iloc[1:]
        else:
            pnl_corrcap = pd.Series(dtype=float)

        if verbose:
            self._print_summary(pnl_baseline, f"Baseline ({mode})")
            if compute_corrcap:
                self._print_summary(pnl_corrcap, f"CorrCap-{self.corr_cap}")

        sym_sector = self.resolve_sector_map(returns.columns, context=context)

        report: dict = {}
        if run_analysis:
            if verbose:
                print("\n" + "=" * 65)
                print("Step 5: StrategyReport")
                print("=" * 65)

            from analysis.base import AnalysisContext
            from analysis.report.strategy_report import StrategyReport

            analysis_context = AnalysisContext(
                result=result_baseline,
                returns_df=returns,
                weights_df=result_baseline.positions_df,
                signal_df=signal,
                vol_df=sigma,
                sector_map=sym_sector,
                benchmark_returns=benchmark_returns,
            )
            out_d = str(output_dir) if output_dir else None
            report = StrategyReport().run(
                analysis_context,
                output_dir=out_d,
                save_tables=bool(out_d),
            )

            if verbose:
                print(f"  Ran: {sorted(report.keys())}")

        return TSMOMRunResult(
            returns=returns,
            signal=signal,
            sigma=sigma,
            baseline_pos=baseline_pos,
            corrcap_pos=corrcap_pos,
            pnl_baseline=pnl_baseline,
            pnl_corrcap=pnl_corrcap,
            sector_map=sym_sector,
            metadata={
                "lookback": self.lookback,
                "mode": mode,
                "target_vol": self.target_vol,
                "fee_rate": self.fee_rate,
                "corr_cap": self.corr_cap,
                "n_symbols": returns.shape[1],
                "start": str(returns.index[0].date()),
                "end": str(returns.index[-1].date()),
            },
            report=report,
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
