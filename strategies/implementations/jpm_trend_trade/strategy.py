"""JPMTrendStrategy：基于 StrategyBase 的 JPM t-stat 趋势策略。

策略流水线：
  1. 从 china_daily_full/ 加载 OI 主力合约收益率矩阵
  2. 计算 JPM 多周期 t-stat 信号（lookbacks=[32,64,126,252,504]）
  3. build_weights() 统一派发：
       - corr_cache=None  →  基准持仓（signal / sigma_ewma）
       - corr_cache 存在  →  CorrCapSizer 定仓（cap=0.25，目标波动率 10%）
  4. VectorizedBacktest 施加 EWMA vol-targeting（10% 年化）

对外暴露 run_pipeline() 端到端接口，以及符合 StrategyBase 规范的
generate_signals() / build_weights() / run_vectorized()。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from backtest.costs import ProportionalCostModel
from backtest.result import BacktestResult
from backtest.vectorized import VectorizedBacktest
from portfolio.sizing.corr_cap import CorrCapSizer
from strategies.base.strategy import StrategyBase
from strategies.context import StrategyContext

from .config import (
    CORR_MIN_PERIODS,
    CORR_WINDOW,
    JPMConfig,
    SIGMA_HALFLIFE,
    TARGET_VOL,
    TRADING_DAYS,
    VOL_HALFLIFE,
    coerce_config,
)


@dataclass
class JPMRunResult:
    """JPMTrendStrategy 端到端运行结果。"""

    returns: pd.DataFrame             # 品种收益率宽表
    signal: pd.DataFrame              # 多周期 JPM 信号
    sigma: pd.DataFrame               # EWMA 年化波动率
    baseline_pos: pd.DataFrame        # 基准持仓（signal / sigma）
    corrcap_pos: pd.DataFrame         # CorrCap 持仓
    pnl_baseline: pd.Series           # 基准组合 PnL（vol-targeted）
    pnl_corrcap: pd.Series            # CorrCap 组合 PnL（vol-targeted）
    sector_map: dict[str, str]        # {symbol: sector}
    result_baseline: BacktestResult | None = None
    result_corrcap: BacktestResult | None = None
    metadata: dict = field(default_factory=dict)


class JPMTrendStrategy(StrategyBase):
    """JPM t-stat 多周期趋势策略（国内期货版）。

    持有两个 Sizer：
      - 基准：signal / sigma（直接内联，无需额外 Sizer 类）
      - CorrCap：self._cc_sizer（CorrCapSizer 实例，框架级组件）

    build_weights() 根据 corr_cache 是否为 None 自动派发，
    run_vectorized() / run_pipeline() 共用此统一入口。

    Parameters
    ----------
    config:
        策略配置对象或配置字典。
    data_dir:
        china_daily_full/ 数据目录路径。
    """

    def __init__(
        self,
        config: JPMConfig | dict | None = None,
        data_dir: str | Path | None = None,
    ) -> None:
        typed_config = coerce_config(config)
        super().__init__(typed_config.to_dict())

        self.typed_config = typed_config
        self.lookbacks: list[int] = typed_config.lookbacks
        self.min_obs: int = typed_config.min_obs
        self.vol_halflife: int = typed_config.vol_halflife
        self.sigma_halflife: int = typed_config.sigma_halflife
        self.target_vol: float = typed_config.target_vol
        self.trading_days: int = typed_config.trading_days
        self.corr_window: int = typed_config.corr_window
        self.corr_min_periods: int = typed_config.corr_min_periods
        self.corr_cap: float = typed_config.corr_cap
        self.transaction_cost_bps: float = typed_config.transaction_cost_bps
        self.transaction_cost_rate: float = self.transaction_cost_bps / 10_000.0
        self.exclude: set[str] = set(typed_config.exclude)
        self.sector_map: dict[str, str] = dict(typed_config.sector_map)

        self._data_dir: Path | None = Path(data_dir) if data_dir else None

        # CorrCap Sizer：框架级组件，存为实例属性避免重复构造
        self._cc_sizer = CorrCapSizer(
            cap=self.corr_cap,
            target_vol=self.target_vol,
            trading_days=self.trading_days,
        )

    # ── StrategyBase 抽象方法实现 ─────────────────────────────────────────────

    def generate_signals(self, price_df: pd.DataFrame) -> pd.DataFrame:
        """从价格矩阵计算 JPM 多周期 t-stat 信号。"""
        from signals.momentum.jpm_tstat import JPMTstatSignal

        return JPMTstatSignal(lookbacks=self.lookbacks).compute(price_df)

    def generate_signals_from_returns(self, returns_df: pd.DataFrame) -> pd.DataFrame:
        """直接从收益率矩阵计算 JPM 信号（避免价格→收益率往返转换）。"""
        from signals.momentum.jpm_tstat import JPMTstatSignal

        return JPMTstatSignal(lookbacks=self.lookbacks).compute_from_returns(returns_df)

    def build_weights(
        self,
        signal_df: pd.DataFrame,
        vol_df: pd.DataFrame,
        corr_cache: dict | None = None,
    ) -> pd.DataFrame:
        """统一定仓入口：基准 vs CorrCap 由 corr_cache 决定。

        Parameters
        ----------
        signal_df:
            JPM 多周期 t-stat 信号，shape=(dates, symbols)，值域 (-1, 1)。
        vol_df:
            EWMA 年化波动率，shape=(dates, symbols)。
        corr_cache:
            None  →  基准持仓：w_i = signal_i / sigma_i
                     （JPM 原始方式，不按品种数归一化）
            dict  →  CorrCapSizer（self._cc_sizer）：
                     解线性系统 Sigma_adj @ u = |s|，缩放至 target_vol

        Returns
        -------
        shape=(dates, symbols) 的头寸权重矩阵。
        """
        if corr_cache is not None:
            return self._cc_sizer.compute(signal_df, vol_df, corr_cache=corr_cache)
        vol_safe = vol_df.replace(0, np.nan)
        return (signal_df / vol_safe).fillna(0.0)

    # ── 向量化路径（覆盖基类默认实现）────────────────────────────────────────

    def run_vectorized(
        self,
        returns_df: pd.DataFrame,
        backtest: Optional[VectorizedBacktest] = None,
        vol_window: int = 20,
        corr_cache: dict | None = None,
    ) -> BacktestResult:
        """向量化回测：基准或 CorrCap 路径均由此入口触发。

        覆盖基类默认实现，使用 generate_signals_from_returns() 跳过
        pct_change() → cumprod() 的往返转换，信号数值与原始研究完全一致。

        Parameters
        ----------
        returns_df:
            品种日收益率矩阵，shape=(dates, symbols)。
        backtest:
            向量化回测器实例；为 None 时使用策略默认配置。
        vol_window:
            未使用（保留与基类签名兼容），sigma 由 EWMA 计算。
        corr_cache:
            None  →  基准路径；
            dict  →  CorrCap 路径（需提前由 CorrCapSizer.build_corr_cache() 生成）。

        Returns
        -------
        BacktestResult
        """
        if backtest is None:
            backtest = self._make_backtest()

        sigma = self._compute_sigma(returns_df)
        signal = self.generate_signals_from_returns(returns_df)
        weight_df = self.build_weights(signal, sigma, corr_cache=corr_cache)

        return backtest.run(weight_df, returns_df)

    # ── 内部辅助 ──────────────────────────────────────────────────────────────

    def _compute_sigma(self, returns_df: pd.DataFrame) -> pd.DataFrame:
        """计算 EWMA 年化截面波动率（用于 build_weights 的 vol_df）。"""
        return (
            returns_df.ewm(halflife=self.sigma_halflife, min_periods=30).std()
            * np.sqrt(self.trading_days)
        )

    def _make_backtest(self) -> VectorizedBacktest:
        """构造策略默认回测器（vol-targeting 参数与 config 对齐）。"""
        return VectorizedBacktest(
            lag=1,
            vol_target=self.target_vol,
            vol_halflife=self.vol_halflife,
            trading_days=self.trading_days,
            cost_model=ProportionalCostModel(self.transaction_cost_rate),
        )

    @staticmethod
    def _make_corrcap_backtest(base_backtest: VectorizedBacktest) -> VectorizedBacktest:
        """构造 CorrCap 专用回测器。

        CorrCapSizer 已经按目标波动率缩放权重；这里保留执行延迟和成本模型，
        但关闭组合层二次 vol-targeting，避免早期样本下有效杠杆被重复放大。
        """
        return VectorizedBacktest(
            lag=base_backtest.lag,
            vol_target=None,
            vol_halflife=base_backtest.vol_halflife,
            vol_min_periods=base_backtest.vol_min_periods,
            trading_days=base_backtest.trading_days,
            fee_rate=base_backtest.fee_rate,
            cost_model=base_backtest.cost_model,
            trim_inactive=base_backtest.trim_inactive,
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
        return {str(symbol): base.get(str(symbol), "Other") for symbol in symbols}

    # ── 端到端流水线 ──────────────────────────────────────────────────────────

    def run_pipeline(
        self,
        context: StrategyContext | None = None,
        data_dir: str | Path | None = None,
        tickers: list[str] | None = None,
        compute_corrcap: bool = True,
        verbose: bool = True,
    ) -> JPMRunResult:
        """端到端运行：加载数据 → 信号 → 定仓 → VectorizedBacktest PnL。

        两条定仓路径均通过 build_weights() 统一派发：
          - 基准：corr_cache=None
          - CorrCap：corr_cache=<滚动相关性缓存>

        Parameters
        ----------
        data_dir:
            china_daily_full/ 数据目录，覆盖构造时传入的路径。
        tickers:
            指定品种列表；为 None 时加载全部可用品种。
        compute_corrcap:
            True 时计算 CorrCap 变体（较慢）。
        verbose:
            True 时打印进度信息。

        Returns
        -------
        JPMRunResult
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
            print("Step 2: Compute JPM t-stat signal")
            print("=" * 65)

        signal = self.generate_signals_from_returns(returns)
        sigma = self._compute_sigma(returns)

        # 3. 基准持仓（corr_cache=None → signal / sigma）
        baseline_pos = self.build_weights(signal, sigma)
        if verbose:
            print(f"  Baseline positions: {baseline_pos.shape}")

        # 4. CorrCap 持仓（self._cc_sizer via build_weights）
        if compute_corrcap:
            if verbose:
                print("\n" + "=" * 65)
                print("Step 3: CorrCap (cap=0.25) positions")
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

        # 5. 向量化回测：两个持仓方案均经 VectorizedBacktest 统一处理
        if verbose:
            print("\n" + "=" * 65)
            print("Step 4: VectorizedBacktest — vol-targeted PnL")
            print("=" * 65)

        bt = context.backtest if context is not None and context.backtest is not None else self._make_backtest()
        result_baseline = bt.run(baseline_pos, returns)
        pnl_baseline = result_baseline.returns.iloc[1:]   # 去掉起始 0.0

        if compute_corrcap:
            cc_bt = self._make_corrcap_backtest(bt)
            result_corrcap = cc_bt.run(corrcap_pos, returns)
            pnl_corrcap = result_corrcap.returns.iloc[1:]
        else:
            pnl_corrcap = pd.Series(dtype=float)

        if verbose:
            self._print_summary(pnl_baseline, "Baseline")
            if compute_corrcap:
                self._print_summary(pnl_corrcap, "CorrCap-0.25")

        sym_sector = self.resolve_sector_map(returns.columns, context=context)

        return JPMRunResult(
            returns=returns,
            signal=signal,
            sigma=sigma,
            baseline_pos=baseline_pos,
            corrcap_pos=corrcap_pos,
            pnl_baseline=pnl_baseline,
            pnl_corrcap=pnl_corrcap,
            result_baseline=result_baseline,
            result_corrcap=result_corrcap if compute_corrcap else None,
            sector_map=sym_sector,
            metadata={
                "lookbacks": self.lookbacks,
                "target_vol": self.target_vol,
                "corr_cap": self.corr_cap,
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

    # ── 兼容 StrategyBase.run() 的通用路径 ───────────────────────────────────

    def run(  # type: ignore[override]
        self,
        price_df: Optional[pd.DataFrame] = None,
        adjust_dates: Optional[set] = None,
        engine=None,
        *,
        data_dir: str | Path | None = None,
        tickers: list[str] | None = None,
        compute_corrcap: bool = True,
        verbose: bool = True,
    ):
        """统一入口：优先走 run_pipeline()，兼容 StrategyBase.run()。"""
        if price_df is not None:
            if engine is None:
                raise ValueError("engine is required when using StrategyBase.run path")
            return super().run(price_df, adjust_dates or set(), engine)
        return self.run_pipeline(
            context=None,
            data_dir=data_dir,
            tickers=tickers,
            compute_corrcap=compute_corrcap,
            verbose=verbose,
        )
