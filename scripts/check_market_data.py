import sys
from pathlib import Path

ROOT = Path(__file__).parent.parent
MARKET_DATA = ROOT.parent / "market_data"
sys.path.insert(0, str(ROOT))


from data.sources import ParquetSource, ColumnKeyedSource
from data.loader import DataLoader, KlineSchema, ContractSchema


kline_src = ParquetSource(MARKET_DATA / "kline" / "overseas_daily_full")
# contracts stored as one big parquet with BaseTicker 列:
contract_src = ColumnKeyedSource(
    MARKET_DATA / "contracts" / "overseas" / "contract_info.parquet",
    filter_col="BaseTicker",
)

loader = DataLoader(
    kline_source=kline_src,
    contract_source=contract_src,
    kline_schema=KlineSchema.overseas(),
    contract_schema=ContractSchema.overseas(),
)

cs = loader.load_continuous("ES", start="2000-01-01", end="2005-01-01")
print(cs.prices.head(30))
