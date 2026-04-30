"""Data access and preprocessing for basis momentum."""

from __future__ import annotations

from dataclasses import dataclass
from pathlib import Path

import numpy as np
import pandas as pd

from strategies.context import StrategyContext


@dataclass
class BasisMomentumMarketData:
    """Prebuilt matrices consumed by the basis momentum strategy layer."""

    returns: pd.DataFrame
    near_returns: pd.DataFrame
    near_prices: pd.DataFrame
    near_open_interest: pd.DataFrame
    dominant_contracts: pd.DataFrame
    far_contracts: pd.DataFrame
    far_prices: pd.DataFrame
    far_open_interest: pd.DataFrame
    far_oi_share: pd.DataFrame
    contract_multiplier: pd.Series

    def save(self, root: str | Path) -> None:
        root_path = Path(root)
        root_path.mkdir(parents=True, exist_ok=True)

        self.returns.to_parquet(root_path / "returns.parquet")
        self.near_returns.to_parquet(root_path / "near_returns.parquet")
        self.near_prices.to_parquet(root_path / "near_prices.parquet")
        self.near_open_interest.to_parquet(root_path / "near_open_interest.parquet")
        self.dominant_contracts.to_parquet(root_path / "dominant_contracts.parquet")
        self.far_contracts.to_parquet(root_path / "far_contracts.parquet")
        self.far_prices.to_parquet(root_path / "far_prices.parquet")
        self.far_open_interest.to_parquet(root_path / "far_open_interest.parquet")
        self.far_oi_share.to_parquet(root_path / "far_oi_share.parquet")
        self.contract_multiplier.rename("contract_multiplier").to_frame().to_parquet(
            root_path / "contract_multiplier.parquet"
        )

        metadata = pd.DataFrame(
            [
                {
                    "start": str(self.returns.index.min().date()) if not self.returns.empty else "",
                    "end": str(self.returns.index.max().date()) if not self.returns.empty else "",
                    "n_dates": int(self.returns.shape[0]),
                    "n_symbols": int(self.returns.shape[1]),
                }
            ]
        )
        metadata.to_parquet(root_path / "metadata.parquet")

    @classmethod
    def load(cls, root: str | Path) -> "BasisMomentumMarketData":
        root_path = Path(root)
        contract_multiplier = pd.read_parquet(root_path / "contract_multiplier.parquet").iloc[:, 0]
        return cls(
            returns=pd.read_parquet(root_path / "returns.parquet"),
            near_returns=pd.read_parquet(root_path / "near_returns.parquet"),
            near_prices=pd.read_parquet(root_path / "near_prices.parquet"),
            near_open_interest=pd.read_parquet(root_path / "near_open_interest.parquet"),
            dominant_contracts=pd.read_parquet(root_path / "dominant_contracts.parquet"),
            far_contracts=pd.read_parquet(root_path / "far_contracts.parquet"),
            far_prices=pd.read_parquet(root_path / "far_prices.parquet"),
            far_open_interest=pd.read_parquet(root_path / "far_open_interest.parquet"),
            far_oi_share=pd.read_parquet(root_path / "far_oi_share.parquet"),
            contract_multiplier=contract_multiplier.astype(float),
        )


class BasisMomentumDataAccess:
    """Build reusable near/far-leg matrices for basis momentum research."""

    def __init__(self, context: StrategyContext) -> None:
        self.context = context
        self.loader = context.loader

    @staticmethod
    def resolve_contract_multiplier(
        context: StrategyContext,
        symbols: list[str],
        lot_size_map: dict[str, float] | pd.Series | None = None,
    ) -> pd.Series:
        if lot_size_map is not None:
            if isinstance(lot_size_map, pd.Series):
                return lot_size_map.reindex(symbols).fillna(1.0).astype(float)
            return pd.Series({symbol: lot_size_map.get(symbol, 1.0) for symbol in symbols}, dtype=float)

        multipliers: dict[str, float] = {}
        for symbol in symbols:
            try:
                instrument = context.loader.load_instrument(str(symbol))
                multipliers[str(symbol)] = float(instrument.lot_size)
            except Exception:
                multipliers[str(symbol)] = 1.0
        return pd.Series(multipliers, dtype=float)

    def build_market_data(
        self,
        *,
        tickers: list[str] | None,
        start: str | None,
        end: str | None,
        min_obs: int,
        exclude: set[str] | None = None,
        lot_size_map: dict[str, float] | pd.Series | None = None,
        active_oi_pct_threshold: float = 0.05,
        cache_dir: str | Path | None = None,
    ) -> BasisMomentumMarketData:
        returns = self.context.load_returns_matrix(
            tickers=tickers,
            start=start,
            end=end,
            min_obs=min_obs,
            exclude=exclude,
        )
        if returns.empty:
            raise RuntimeError("No returns loaded. Check data_dir, tickers, and contract metadata.")

        symbols = returns.columns.tolist()
        near_prices = self.context.load_continuous_field_matrix(
            field_name="settle",
            tickers=symbols,
            start=start,
            end=end,
        ).reindex(index=returns.index, columns=symbols)
        near_open_interest = self.context.load_continuous_field_matrix(
            field_name="open_interest",
            tickers=symbols,
            start=start,
            end=end,
        ).reindex(index=returns.index, columns=symbols)
        near_returns = self.context.load_continuous_field_returns_matrix(
            field_name="settle",
            tickers=symbols,
            start=start,
            end=end,
            zero_on_roll=True,
            clip_abs_return=0.5,
        ).reindex(index=returns.index, columns=symbols)
        if near_returns.empty:
            near_returns = returns.copy()

        (
            dominant_contracts,
            far_contracts,
            far_prices,
            far_open_interest,
            far_oi_share,
        ) = self.build_contract_leg_matrices(
            symbols=symbols,
            dates=returns.index,
            start=start,
            end=end,
            active_oi_pct_threshold=active_oi_pct_threshold,
            cache_dir=cache_dir,
        )
        contract_multiplier = self.resolve_contract_multiplier(
            self.context,
            symbols,
            lot_size_map=lot_size_map,
        )

        return BasisMomentumMarketData(
            returns=returns,
            near_returns=near_returns,
            near_prices=near_prices,
            near_open_interest=near_open_interest,
            dominant_contracts=dominant_contracts,
            far_contracts=far_contracts,
            far_prices=far_prices,
            far_open_interest=far_open_interest,
            far_oi_share=far_oi_share,
            contract_multiplier=contract_multiplier,
        )

    def build_contract_leg_matrices(
        self,
        *,
        symbols: list[str],
        dates: pd.DatetimeIndex,
        start: str | None,
        end: str | None,
        active_oi_pct_threshold: float,
        cache_dir: str | Path | None,
    ) -> tuple[pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame, pd.DataFrame]:
        dominant_contracts = pd.DataFrame(index=dates, columns=symbols, dtype=object)
        far_contracts = pd.DataFrame(index=dates, columns=symbols, dtype=object)
        far_prices = pd.DataFrame(np.nan, index=dates, columns=symbols, dtype=float)
        far_open_interest = pd.DataFrame(np.nan, index=dates, columns=symbols, dtype=float)
        far_oi_share = pd.DataFrame(np.nan, index=dates, columns=symbols, dtype=float)
        cache_root = None if cache_dir is None else Path(cache_dir) / "contract_legs"
        if cache_root is not None:
            cache_root.mkdir(parents=True, exist_ok=True)

        for symbol in symbols:
            cached = self._load_cached_symbol_leg(
                symbol=symbol,
                dates=dates,
                cache_root=cache_root,
                active_oi_pct_threshold=active_oi_pct_threshold,
            )
            if cached is not None:
                dominant_contracts.loc[:, symbol] = cached["dominant_contract"]
                far_contracts.loc[:, symbol] = cached["far_contract"]
                far_prices.loc[:, symbol] = cached["far_price"]
                far_open_interest.loc[:, symbol] = cached["far_open_interest"]
                far_oi_share.loc[:, symbol] = cached["far_oi_share"]
                continue

            try:
                contracts = self.loader.load_contracts(symbol)
            except Exception:
                continue
            if not contracts:
                continue

            try:
                continuous = self.loader.load_continuous(symbol, start=start, end=end)
            except Exception:
                continue
            if not continuous.schedule.events:
                continue

            if hasattr(self.loader, "_load_bar_data_for_continuous"):
                bar_data = self.loader._load_bar_data_for_continuous(symbol, contracts)
            else:
                bar_data = {}
                for contract in contracts:
                    try:
                        bar_data[contract.code] = self.loader.load_bar_series(
                            contract.code,
                            start=start,
                            end=end,
                        )
                    except Exception:
                        continue

            def _safe_active_contract(ts: pd.Timestamp) -> str | None:
                try:
                    return continuous.schedule.get_active_contract(ts)
                except Exception:
                    return None

            near_series = pd.Series([_safe_active_contract(ts) for ts in dates], index=dates, dtype=object)
            dominant_contracts.loc[:, symbol] = near_series
            (
                symbol_far_contracts,
                symbol_far_prices,
                symbol_far_open_interest,
                symbol_far_oi_share,
            ) = self._build_symbol_far_leg_matrices(
                dates=dates,
                contracts=contracts,
                near_series=near_series,
                bar_data=bar_data,
                active_oi_pct_threshold=active_oi_pct_threshold,
            )
            far_contracts.loc[:, symbol] = symbol_far_contracts
            far_prices.loc[:, symbol] = symbol_far_prices
            far_open_interest.loc[:, symbol] = symbol_far_open_interest
            far_oi_share.loc[:, symbol] = symbol_far_oi_share
            self._save_cached_symbol_leg(
                symbol=symbol,
                cache_root=cache_root,
                active_oi_pct_threshold=active_oi_pct_threshold,
                frame=pd.DataFrame(
                    {
                        "dominant_contract": near_series,
                        "far_contract": symbol_far_contracts,
                        "far_price": symbol_far_prices,
                        "far_open_interest": symbol_far_open_interest,
                        "far_oi_share": symbol_far_oi_share,
                    },
                    index=dates,
                ),
            )

        return dominant_contracts, far_contracts, far_prices, far_open_interest, far_oi_share

    @staticmethod
    def _cache_file(
        cache_root: Path | None,
        symbol: str,
        active_oi_pct_threshold: float,
    ) -> Path | None:
        if cache_root is None:
            return None
        threshold_tag = int(round(active_oi_pct_threshold * 10_000))
        return cache_root / f"{symbol}__oi_{threshold_tag}.parquet"

    def _load_cached_symbol_leg(
        self,
        *,
        symbol: str,
        dates: pd.DatetimeIndex,
        cache_root: Path | None,
        active_oi_pct_threshold: float,
    ) -> pd.DataFrame | None:
        cache_file = self._cache_file(cache_root, symbol, active_oi_pct_threshold)
        if cache_file is None or not cache_file.exists():
            return None
        try:
            frame = pd.read_parquet(cache_file)
        except Exception:
            return None
        frame.index = pd.DatetimeIndex(pd.to_datetime(frame.index))
        if not dates.isin(frame.index).all():
            return None
        cols = {
            "dominant_contract",
            "far_contract",
            "far_price",
            "far_open_interest",
            "far_oi_share",
        }
        if not cols.issubset(frame.columns):
            return None
        return frame.reindex(dates)

    def _save_cached_symbol_leg(
        self,
        *,
        symbol: str,
        cache_root: Path | None,
        active_oi_pct_threshold: float,
        frame: pd.DataFrame,
    ) -> None:
        cache_file = self._cache_file(cache_root, symbol, active_oi_pct_threshold)
        if cache_file is None:
            return
        frame.to_parquet(cache_file)

    @staticmethod
    def select_far_contract(
        *,
        near_contract: str | None,
        date: pd.Timestamp,
        contracts: list,
        bar_data: dict[str, object],
        active_oi_pct_threshold: float,
    ) -> tuple[str | None, float]:
        """Compatibility helper for one-date far-leg selection."""

        dates = pd.DatetimeIndex([pd.Timestamp(date)])
        near_series = pd.Series([near_contract], index=dates, dtype=object)
        far_contracts, _, _, far_share = BasisMomentumDataAccess._build_symbol_far_leg_matrices(
            dates=dates,
            contracts=contracts,
            near_series=near_series,
            bar_data=bar_data,
            active_oi_pct_threshold=active_oi_pct_threshold,
        )
        return far_contracts.iloc[0], float(far_share.iloc[0])

    @staticmethod
    def _build_symbol_far_leg_matrices(
        *,
        dates: pd.DatetimeIndex,
        contracts: list,
        near_series: pd.Series,
        bar_data: dict[str, object],
        active_oi_pct_threshold: float,
    ) -> tuple[pd.Series, pd.Series, pd.Series, pd.Series]:
        contract_codes = [contract.code for contract in contracts if contract.code in bar_data]
        if not contract_codes:
            empty_obj = pd.Series(index=dates, dtype=object)
            empty_num = pd.Series(np.nan, index=dates, dtype=float)
            return empty_obj, empty_num.copy(), empty_num.copy(), empty_num.copy()

        last_trade_map = {contract.code: contract.last_trade_date for contract in contracts}
        contract_order = sorted(contract_codes, key=lambda code: (last_trade_map[code], code))
        contract_rank = {code: idx for idx, code in enumerate(contract_order)}

        settle_dict: dict[str, pd.Series] = {}
        oi_dict: dict[str, pd.Series] = {}
        for code in contract_order:
            bs = bar_data.get(code)
            if bs is None or bs.data.empty:
                continue
            data = bs.data.reindex(dates)
            settle_dict[code] = pd.to_numeric(data.get("settle"), errors="coerce")
            oi_dict[code] = pd.to_numeric(data.get("open_interest"), errors="coerce")

        if not settle_dict or not oi_dict:
            empty_obj = pd.Series(index=dates, dtype=object)
            empty_num = pd.Series(np.nan, index=dates, dtype=float)
            return empty_obj, empty_num.copy(), empty_num.copy(), empty_num.copy()

        settle_df = pd.DataFrame(settle_dict, index=dates).reindex(columns=contract_order)
        oi_df = pd.DataFrame(oi_dict, index=dates).reindex(columns=contract_order)

        valid_leg = settle_df.gt(0.0) & oi_df.gt(0.0) & settle_df.notna() & oi_df.notna()
        total_oi = oi_df.where(valid_leg).sum(axis=1)
        oi_share = oi_df.div(total_oi.replace(0.0, np.nan), axis=0)
        eligible = valid_leg & oi_share.ge(active_oi_pct_threshold)

        near_rank = near_series.map(contract_rank)
        rank_values = np.asarray([contract_rank[code] for code in contract_order], dtype=float)
        rank_matrix = np.broadcast_to(rank_values, eligible.shape)
        near_rank_values = near_rank.to_numpy(dtype=float, na_value=np.nan)[:, None]
        eligible_values = eligible.to_numpy(dtype=bool)

        candidate_rank = np.where(
            eligible_values & (rank_matrix > near_rank_values),
            rank_matrix,
            np.inf,
        )
        far_pos = candidate_rank.argmin(axis=1)
        far_rank = candidate_rank[np.arange(candidate_rank.shape[0]), far_pos]
        has_far = np.isfinite(far_rank)

        far_contract_values = np.empty(len(dates), dtype=object)
        far_contract_values[:] = None
        ordered_codes = np.asarray(contract_order, dtype=object)
        far_contract_values[has_far] = ordered_codes[far_pos[has_far]]

        row_idx = np.arange(len(dates))
        settle_values = settle_df.to_numpy(dtype=float)
        oi_values = oi_df.to_numpy(dtype=float)
        oi_share_values = oi_share.to_numpy(dtype=float)

        far_price_values = np.full(len(dates), np.nan, dtype=float)
        far_oi_values = np.full(len(dates), np.nan, dtype=float)
        far_share_values = np.full(len(dates), np.nan, dtype=float)
        far_price_values[has_far] = settle_values[row_idx[has_far], far_pos[has_far]]
        far_oi_values[has_far] = oi_values[row_idx[has_far], far_pos[has_far]]
        far_share_values[has_far] = oi_share_values[row_idx[has_far], far_pos[has_far]]

        return (
            pd.Series(far_contract_values, index=dates, dtype=object),
            pd.Series(far_price_values, index=dates, dtype=float),
            pd.Series(far_oi_values, index=dates, dtype=float),
            pd.Series(far_share_values, index=dates, dtype=float),
        )
