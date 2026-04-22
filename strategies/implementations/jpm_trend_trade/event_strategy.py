"""Event-driven JPM t-stat trend strategy."""

from __future__ import annotations

from dataclasses import dataclass

import numpy as np
import pandas as pd

from backtest.event import Order, SimulationContext
from portfolio.sizing.corr_cap import CorrCapSizer
from strategies.base import EventDrivenStrategy

from .config import JPMConfig
from .strategy import JPMTrendStrategy


@dataclass(slots=True)
class JPMEventDrivenConfig:
    """Configuration for the event-driven JPM wrapper."""

    mode: str = "baseline"          # "baseline" or "corrcap"
    rebalance_every: int = 1
    min_history: int | None = None
    apply_vol_target: bool = True

    def __post_init__(self) -> None:
        if self.mode not in {"baseline", "corrcap"}:
            raise ValueError("mode must be 'baseline' or 'corrcap'.")
        if self.rebalance_every <= 0:
            raise ValueError("rebalance_every must be positive.")


class JPMEventDrivenStrategy(EventDrivenStrategy):
    """Callback-style JPM strategy using the lightweight event engine.

    The class reuses ``JPMTrendStrategy`` for signal and sizing logic, but emits
    target-weight orders from ``on_bar``. It is intended as the first formal
    implementation-package example of the event-driven strategy paradigm.
    """

    name = "jpm_event_driven"

    def __init__(
        self,
        strategy: JPMTrendStrategy | None = None,
        *,
        jpm_config: JPMConfig | dict | None = None,
        event_config: JPMEventDrivenConfig | dict | None = None,
    ) -> None:
        self.strategy = strategy if strategy is not None else JPMTrendStrategy(config=jpm_config)
        self.event_config = (
            event_config
            if isinstance(event_config, JPMEventDrivenConfig)
            else JPMEventDrivenConfig(**(event_config or {}))
        )
        if self.event_config.min_history is None:
            self.min_history = max(
                max(self.strategy.lookbacks),
                self.strategy.sigma_halflife,
                self.strategy.corr_min_periods,
            )
        else:
            self.min_history = self.event_config.min_history

        self._returns: pd.DataFrame | None = None
        self._signal: pd.DataFrame | None = None
        self._sigma: pd.DataFrame | None = None
        self._corr_cache: dict[pd.Timestamp, np.ndarray] | None = None

    def on_start(self, context: SimulationContext) -> None:
        if context.data_portal is None:
            return
        self._precompute_market_features(self._extract_returns(context))

    def on_bar(self, context: SimulationContext) -> list[Order]:
        if context.now is None or context.data_portal is None:
            return []

        returns = self._ensure_precomputed(context)
        now = pd.Timestamp(context.now)
        if now not in returns.index:
            return []

        loc = returns.index.get_loc(now)
        if not isinstance(loc, int):
            raise ValueError(f"Duplicate timestamp in returns index: {now!s}")
        if loc < self.min_history:
            return []
        if loc % self.event_config.rebalance_every != 0:
            return []

        current_weights = self._target_weights(now)
        if current_weights.empty:
            return []

        context.strategy_state.set("last_signal_date", now)
        context.strategy_state.set("last_mode", self.event_config.mode)

        return [
            Order.target_weight(str(symbol), float(weight), now)
            for symbol, weight in current_weights.items()
        ]

    def _extract_returns(self, context: SimulationContext) -> pd.DataFrame:
        if context.data_portal is None:
            raise ValueError("JPMEventDrivenStrategy requires a data_portal.")

        returns = context.data_portal.returns
        if returns is None:
            returns = context.data_portal.prices.pct_change()
        return returns.sort_index()

    def _ensure_precomputed(self, context: SimulationContext) -> pd.DataFrame:
        returns = self._extract_returns(context)
        if (
            self._returns is None
            or not self._returns.index.equals(returns.index)
            or not self._returns.columns.equals(returns.columns)
        ):
            self._precompute_market_features(returns)
        assert self._returns is not None
        return self._returns

    def _precompute_market_features(self, returns: pd.DataFrame) -> None:
        """Precompute path-independent market features before the event loop.

        Signal, sigma and rolling correlation are functions of historical market
        data only. Computing them once keeps ``on_bar`` focused on event-time
        state and order generation instead of repeatedly rerunning rolling
        vectorized calculations.
        """
        self._returns = returns
        self._signal = self.strategy.generate_signals_from_returns(returns)
        self._sigma = self.strategy._compute_sigma(returns)
        self._corr_cache = None
        if self.event_config.mode == "corrcap":
            self._corr_cache = CorrCapSizer.build_corr_cache(
                returns,
                window=self.strategy.corr_window,
                min_periods=self.strategy.corr_min_periods,
            )

    def _target_weights(self, date: pd.Timestamp) -> pd.Series:
        if self._returns is None or self._signal is None or self._sigma is None:
            raise RuntimeError("Market features are not precomputed. Did on_start run?")

        signal_row = self._signal.loc[[date]]
        sigma_row = self._sigma.loc[[date]]

        if self.event_config.mode == "corrcap":
            weights = self.strategy.build_weights(signal_row, sigma_row, corr_cache=self._corr_cache)
            return weights.loc[date].fillna(0.0)

        weights = self.strategy.build_weights(signal_row, sigma_row)
        target = weights.loc[date].fillna(0.0)
        if self.event_config.apply_vol_target:
            returns_history = self._returns.loc[:date]
            target = self._apply_ex_ante_vol_target(target, returns_history)
        return target

    def _apply_ex_ante_vol_target(
        self,
        weights: pd.Series,
        returns_history: pd.DataFrame,
    ) -> pd.Series:
        active = weights.replace([np.inf, -np.inf], np.nan).dropna()
        active = active[active.abs() > 1e-12]
        if active.empty:
            return weights.fillna(0.0)

        window = returns_history[active.index].tail(self.strategy.corr_window)
        if len(window.dropna(how="all")) < max(2, self.strategy.corr_min_periods):
            return weights.fillna(0.0)

        cov_ann = window.cov().reindex(index=active.index, columns=active.index)
        cov_ann = cov_ann.fillna(0.0).to_numpy(dtype=float) * self.strategy.trading_days
        w = active.to_numpy(dtype=float)
        port_var = float(w @ cov_ann @ w)
        if port_var <= 0:
            return weights.fillna(0.0)

        scale = self.strategy.target_vol / np.sqrt(port_var)
        return (weights.fillna(0.0) * scale).replace([np.inf, -np.inf], 0.0)
