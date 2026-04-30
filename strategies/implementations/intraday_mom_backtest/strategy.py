"""IntradayMomStrategy：中国期货日内时序动量策略。

策略逻辑（Jin et al., 2019）
----------------------------
每个交易日：
    1. 观察日盘开盘首 N 分钟（首时段）的价格收益 r_first。
    2. signal_t = sign(r_first_t)：首时段涨则做多，跌则做空。
    3. 在首时段结束时建仓，在日盘收盘前最后 N 分钟（尾时段）结束前平仓。
    4. 策略日收益 pnl_t = signal_t × r_last_t。

回测实现
--------
- 信号为日度截面宽表，直接接入 VectorizedBacktest(lag=0)。
  lag=0 表示 signal_t 对应当日收益 r_last_t（日内信号，无隔夜延迟）。
- returns_df = 尾时段收益矩阵（非品种日度涨跌幅）。
- 可选 vol-targeting：对 pnl 序列施加 EWMA 波动率定标。

品种层面权重
-----------
纯二元信号：w_t = signal_t / n_active_t，等权跨品种。
等权设计使得每个有信号的品种对组合贡献相等，便于与论文按品种汇报的结果对照。
"""

from __future__ import annotations

from dataclasses import dataclass, field
from pathlib import Path
from typing import Optional

import numpy as np
import pandas as pd

from backtest.result import BacktestResult
from backtest.vectorized import VectorizedBacktest
from strategies.base.strategy import StrategyBase

from .config import IntradayMomConfig, coerce_config, TRADING_DAYS


@dataclass
class IntradayMomRunResult:
    """IntradayMomStrategy 端到端运行结果。"""

    first_ret: pd.DataFrame    # 首时段收益矩阵（信号原料）
    last_ret: pd.DataFrame     # 尾时段收益矩阵（策略目标收益）
    signal: pd.DataFrame       # sign(r_first)，或条件缩放后信号
    weights: pd.DataFrame      # 组合层面权重（等权规范化）
    pnl: pd.Series             # 组合日度 PnL（已扣费）
    backtest_result: BacktestResult
    metadata: dict = field(default_factory=dict)

    # ── 统计摘要 ──────────────────────────────────────────────────────────────

    def summary(self) -> dict:
        """返回关键绩效指标。"""
        p = self.pnl
        if p.empty:
            return {}
        td = self.metadata.get("trading_days", TRADING_DAYS)
        ann_r = p.mean() * td
        ann_v = p.std() * np.sqrt(td)
        sharpe = ann_r / ann_v if ann_v > 0 else float("nan")
        nav = (1 + p).cumprod()
        mdd = ((nav - nav.cummax()) / nav.cummax()).min()
        hit_rate = (p > 0).mean()
        return {
            "sharpe": round(sharpe, 3),
            "ann_return": round(ann_r, 4),
            "ann_vol": round(ann_v, 4),
            "max_drawdown": round(mdd, 4),
            "hit_rate": round(hit_rate, 4),
            "n_days": len(p),
        }

    def per_symbol_stats(self) -> pd.DataFrame:
        """逐品种统计：信号命中率、平均首/尾收益、相关系数。"""
        results = []
        for sym in self.signal.columns:
            sig = self.signal[sym].dropna()
            fr = self.first_ret[sym].reindex(sig.index).dropna()
            lr = self.last_ret[sym].reindex(sig.index).dropna()
            common = sig.index.intersection(fr.index).intersection(lr.index)
            if len(common) < 10:
                continue
            s = sig.loc[common]
            f = fr.loc[common]
            l = lr.loc[common]
            pnl = s * l
            results.append({
                "symbol": sym,
                "n_days": len(common),
                "signal_long_frac": (s > 0).mean(),
                "first_ret_mean": f.mean(),
                "last_ret_mean": l.mean(),
                "hit_rate": (pnl > 0).mean(),
                "ann_return": pnl.mean() * self.metadata.get("trading_days", TRADING_DAYS),
                "corr_first_last": f.corr(l),
            })
        if not results:
            return pd.DataFrame()
        return pd.DataFrame(results).set_index("symbol").sort_values(
            "ann_return", ascending=False
        )


class IntradayMomStrategy(StrategyBase):
    """中国期货日内时序动量策略。

    Parameters
    ----------
    config : IntradayMomConfig | dict | None
        策略配置对象，字典或 None（使用默认值）。
    data_dir : str | Path
        china_minute/ 数据目录根路径。
    """

    def __init__(
        self,
        config: IntradayMomConfig | dict | None = None,
        data_dir: str | Path | None = None,
    ) -> None:
        cfg = coerce_config(config)
        super().__init__(cfg.to_dict())
        self.typed_config: IntradayMomConfig = cfg
        self._data_dir: Path | None = Path(data_dir) if data_dir else None

    # ── StrategyBase 兼容桩 ───────────────────────────────────────────────────

    def generate_signals(self, price_df: pd.DataFrame) -> pd.DataFrame:
        """StrategyBase 接口兼容桩（日内策略不走价格路径）。"""
        raise NotImplementedError(
            "IntradayMomStrategy uses minute data; call run_pipeline() directly."
        )

    def build_weights(
        self,
        signal_df: pd.DataFrame,
        vol_df: pd.DataFrame,
        corr_cache: dict | None = None,
    ) -> pd.DataFrame:
        """将信号转为等权规范化权重。

        每行（交易日）：有信号的品种按信号方向等权分配，
        使组合多头/空头各方向的毛暴露 ≈ 1.0（跨品种等权）。

        即：w_t = signal_t / n_active_t
        """
        active = signal_df.notna() & (signal_df != 0)
        n_active = active.sum(axis=1).replace(0, np.nan)
        return signal_df.divide(n_active, axis=0).fillna(0.0)

    # ── 全流水线 ──────────────────────────────────────────────────────────────

    def run_pipeline(
        self,
        data_dir: str | Path | None = None,
        symbols: list[str] | None = None,
        start: str | None = None,
        end: str | None = None,
        verbose: bool = True,
    ) -> IntradayMomRunResult:
        """端到端运行：分钟数据 → 信号 → 等权持仓 → VectorizedBacktest PnL。

        Parameters
        ----------
        data_dir : str | Path | None
            china_minute/ 数据目录；None 时使用构造函数传入的值。
        symbols : list[str] | None
            指定品种列表；None 时使用 config.symbols，若为空则加载全部。
        start, end : str | None
            日期范围，格式 'YYYY-MM-DD'。
        verbose : bool
            打印进度信息。
        """
        from data.sources.china_minute_loader import ChinaMinuteLoader
        from signals.momentum.intraday_mom import IntradayMomParams, IntradayMomSignal

        cfg = self.typed_config

        # ── 解析参数 ──────────────────────────────────────────────────────────
        resolved_dir = Path(data_dir) if data_dir else self._data_dir
        if resolved_dir is None:
            raise ValueError(
                "data_dir must be provided via constructor or run_pipeline()."
            )

        target_symbols: list[str] | None = symbols or (cfg.symbols if cfg.symbols else None)

        # ── Step 1: 加载分钟数据 → 首/尾时段收益矩阵 ─────────────────────────
        if verbose:
            print("=" * 65)
            print("Step 1: Load china_minute data → first/last period returns")
            print("=" * 65)

        loader = ChinaMinuteLoader(
            data_dir=resolved_dir,
            first_period_minutes=cfg.first_period_minutes,
            last_period_minutes=cfg.last_period_minutes,
            min_daily_volume=cfg.min_daily_volume,
        )

        first_ret_df, last_ret_df = loader.load_universe(
            symbols=target_symbols,
            start=start,
            end=end,
            verbose=verbose,
        )

        if first_ret_df.empty:
            raise RuntimeError(
                "No data loaded. Check data_dir and symbols."
            )

        # 排除样本量不足的品种
        valid_cols = first_ret_df.notna().sum() >= cfg.min_obs
        first_ret_df = first_ret_df.loc[:, valid_cols]
        last_ret_df = last_ret_df.loc[:, valid_cols]

        if verbose:
            print(
                f"\nFirst period returns: {first_ret_df.shape}  "
                f"({first_ret_df.index[0].date()} - {first_ret_df.index[-1].date()})"
            )
            coverage = first_ret_df.notna().mean().mean()
            print(f"Average coverage: {coverage:.1%}")

        # ── Step 2: 计算信号 ──────────────────────────────────────────────────
        if verbose:
            print("\n" + "=" * 65)
            print(
                f"Step 2: Compute intraday momentum signal  "
                f"(first={cfg.first_period_minutes}min, "
                f"volume_scale={cfg.volume_scale}, vol_scale={cfg.vol_scale})"
            )
            print("=" * 65)

        sig_params = IntradayMomParams(
            volume_scale=cfg.volume_scale,
            vol_scale=cfg.vol_scale,
            rank_window=cfg.rank_window,
        )
        sig_gen = IntradayMomSignal(params=sig_params)
        signal_df = sig_gen.compute(first_ret_df)

        if verbose:
            long_frac = (signal_df > 0).sum().sum() / signal_df.notna().sum().sum()
            print(
                f"  Long fraction: {long_frac:.1%}  "
                f"(expected ≈50% for zero-mean symmetric distribution)"
            )

        # ── Step 3: 构建等权权重 ──────────────────────────────────────────────
        dummy_vol = pd.DataFrame(
            np.ones_like(signal_df.values),
            index=signal_df.index,
            columns=signal_df.columns,
        )
        weights_df = self.build_weights(signal_df, dummy_vol)

        if verbose:
            print(
                f"\n  Weights: shape={weights_df.shape}, "
                f"avg active count={weights_df.ne(0).sum(axis=1).mean():.1f}"
            )

        # ── Step 4: 向量化回测 ────────────────────────────────────────────────
        if verbose:
            print("\n" + "=" * 65)
            print(
                f"Step 3: VectorizedBacktest (lag=0, fee={cfg.fee_rate * 10000:.1f}bps, "
                f"vol_target={cfg.vol_target})"
            )
            print("=" * 65)

        bt = self._make_backtest()
        bt_result = bt.run(weights_df, last_ret_df)
        pnl = bt_result.returns.iloc[1:]  # 去掉初始 0 值行

        if verbose and not pnl.empty:
            self._print_summary(pnl, "IntradayMom")

        return IntradayMomRunResult(
            first_ret=first_ret_df,
            last_ret=last_ret_df,
            signal=signal_df,
            weights=weights_df,
            pnl=pnl,
            backtest_result=bt_result,
            metadata={
                "symbols": list(first_ret_df.columns),
                "first_period_minutes": cfg.first_period_minutes,
                "last_period_minutes": cfg.last_period_minutes,
                "fee_rate": cfg.fee_rate,
                "vol_target": cfg.vol_target,
                "trading_days": cfg.trading_days,
                "start": str(first_ret_df.index[0].date()),
                "end": str(first_ret_df.index[-1].date()),
            },
        )

    # ── 工厂方法 ──────────────────────────────────────────────────────────────

    def _make_backtest(self) -> VectorizedBacktest:
        cfg = self.typed_config
        return VectorizedBacktest(
            lag=0,                         # 日内信号，同日兑现
            vol_target=cfg.vol_target,
            vol_halflife=cfg.vol_halflife,
            trading_days=cfg.trading_days,
            fee_rate=cfg.fee_rate,
        )

    @staticmethod
    def _print_summary(pnl: pd.Series, label: str) -> None:
        td = TRADING_DAYS
        ann_r = pnl.mean() * td
        ann_v = pnl.std() * np.sqrt(td)
        sharpe = ann_r / ann_v if ann_v > 0 else float("nan")
        nav = (1 + pnl).cumprod()
        mdd = ((nav - nav.cummax()) / nav.cummax()).min()
        hit = (pnl > 0).mean()
        print(
            f"  [{label}]  Sharpe={sharpe:.3f}  Return={ann_r * 100:.1f}%  "
            f"Vol={ann_v * 100:.1f}%  MaxDD={mdd * 100:.1f}%  "
            f"HitRate={hit:.1%}  "
            f"({pnl.index[0].date()} - {pnl.index[-1].date()})"
        )
