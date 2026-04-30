"""Build preprocessed China futures basis momentum matrices."""

from __future__ import annotations

import argparse
import sys
from pathlib import Path

import pandas as pd

_HERE = Path(__file__).resolve().parent
_CTA_LAB = _HERE.parent
if str(_CTA_LAB) not in sys.path:
    sys.path.insert(0, str(_CTA_LAB))

from data.loader import ContractSchema, DataLoader, InstrumentSchema, KlineSchema
from data.sources.column_keyed_source import ColumnKeyedSource
from data.sources.parquet_source import ParquetSource
from strategies.context import StrategyContext
from strategies.implementations.basis_momentum_backtest.data_access import BasisMomentumDataAccess


def _parse_args() -> argparse.Namespace:
    p = argparse.ArgumentParser(description="预构建基差动量 near/far 矩阵")
    p.add_argument(
        "--data-dir",
        default=str(_CTA_LAB.parent / "market_data" / "kline" / "china_daily_full"),
        help="china_daily_full/ 数据目录",
    )
    p.add_argument(
        "--contract-info",
        default=str(_CTA_LAB.parent / "market_data" / "contracts" / "china" / "contract_info.parquet"),
        help="合约元数据 parquet 路径",
    )
    p.add_argument(
        "--out-dir",
        default=str(_CTA_LAB.parent / "research_outputs" / "basis_momentum_prebuilt"),
        help="输出目录",
    )
    p.add_argument("--start", default=None)
    p.add_argument("--end", default=None)
    p.add_argument("--min-obs", type=int, default=30)
    p.add_argument("--active-oi-pct-threshold", type=float, default=0.05)
    return p.parse_args()


def _load_lot_size_map(contract_info_path: str) -> dict[str, float]:
    try:
        df = pd.read_parquet(contract_info_path)
        return df.groupby("fut_code")["per_unit"].first().dropna().to_dict()
    except Exception:
        return {}


def main() -> None:
    args = _parse_args()

    contract_source = ColumnKeyedSource(args.contract_info, filter_col="fut_code")
    loader = DataLoader(
        kline_source=ParquetSource(args.data_dir),
        contract_source=contract_source,
        instrument_source=contract_source,
        kline_schema=KlineSchema.tushare(),
        contract_schema=ContractSchema.tushare(),
        instrument_schema=InstrumentSchema.china_from_contracts(),
    )
    context = StrategyContext(loader=loader, sector_map={}, backtest=None)
    data_access = BasisMomentumDataAccess(context)
    market_data = data_access.build_market_data(
        tickers=None,
        start=args.start,
        end=args.end,
        min_obs=args.min_obs,
        exclude=None,
        lot_size_map=_load_lot_size_map(args.contract_info),
        active_oi_pct_threshold=args.active_oi_pct_threshold,
        cache_dir=args.out_dir,
    )
    market_data.save(args.out_dir)

    print(f"Saved prebuilt basis momentum dataset to: {Path(args.out_dir).resolve()}")
    print(
        {
            "returns_shape": market_data.returns.shape,
            "far_coverage": round(float(market_data.far_prices.notna().mean().mean()), 4),
            "symbols": int(market_data.returns.shape[1]),
        }
    )


if __name__ == "__main__":
    main()
