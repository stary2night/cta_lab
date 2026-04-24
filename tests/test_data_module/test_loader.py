"""tests/test_data_module/test_loader.py — DataLoader + KlineSchema + ContractSchema 测试"""

import tempfile
from datetime import date
from pathlib import Path

import numpy as np
import pandas as pd
import pytest

from data.loader import ContractSchema, DataLoader, InstrumentSchema, KlineSchema
from data.model.bar import BarSeries
from data.model.calendar import TradingCalendar
from data.model.contract import Contract
from data.model.instrument import InstrumentRegistry
from data.sources.column_keyed_source import ColumnKeyedSource
from data.sources.parquet_source import ParquetSource


# ── fixtures ──────────────────────────────────────────────────────────────

@pytest.fixture(autouse=True)
def clear_registry():
    InstrumentRegistry()._instruments.clear()
    yield
    InstrumentRegistry()._instruments.clear()


@pytest.fixture
def tmp_root(tmp_path):
    return tmp_path


def _write_kline(root: Path, key: str, idx: pd.DatetimeIndex) -> pd.DataFrame:
    """写入标准列名的 K 线 parquet 文件，返回写入的 DataFrame。"""
    df = pd.DataFrame({
        "open": 100.0, "high": 101.0, "low": 99.0, "close": 100.5,
        "settle": np.linspace(100, 110, len(idx)),
        "volume": 1000.0, "open_interest": 5000.0,
    }, index=idx)
    src = ParquetSource(root)
    src.write_dataframe(key, df)
    return df


def _write_tushare_kline(root: Path, symbol: str, contract_code: str, idx: pd.DatetimeIndex):
    """写入 Tushare 格式（多合约混合表）的 K 线数据。"""
    df = pd.DataFrame({
        "contract_code": contract_code,
        "trade_date": idx,
        "open_price": 100.0,
        "high_price": 101.0,
        "low_price": 99.0,
        "close_price": 100.0,
        "settle_price": np.linspace(100, 108, len(idx)),
        "volume": 1000.0,
        "interest": 5000.0,
    })
    src = ParquetSource(root)
    src.write_dataframe(symbol, df)


def _write_contracts(root: Path, symbol: str):
    df = pd.DataFrame([{
        "code": f"{symbol}2410",
        "exchange": "SHFE",
        "list_date": "2024-01-01",
        "expire_date": "2024-10-15",
        "last_trade_date": "2024-10-14",
    }])
    ParquetSource(root).write_dataframe(f"contracts/{symbol}", df)


def _write_calendar(root: Path, exchange: str, dates: pd.DatetimeIndex):
    df = pd.DataFrame({"date": dates})
    ParquetSource(root).write_dataframe(f"calendars/{exchange}", df)


def _write_instrument(root: Path, symbol: str):
    df = pd.DataFrame([{
        "symbol": symbol, "name": "螺纹钢", "exchange": "SHFE",
        "currency": "CNY", "lot_size": 10.0, "tick_size": 1.0, "margin_rate": 0.1,
    }])
    ParquetSource(root).write_dataframe(f"instruments/{symbol}", df)


# ── KlineSchema 预置 ──────────────────────────────────────────────────────

class TestKlineSchema:
    def test_default_fields(self):
        s = KlineSchema.default()
        assert s.settle_col == "settle"
        assert s.oi_col == "open_interest"
        assert s.contract_col is None

    def test_tushare_fields(self):
        s = KlineSchema.tushare()
        assert s.settle_col == "settle_price"
        assert s.oi_col == "interest"
        assert s.contract_col == "contract_code"
        assert s.date_col == "trade_date"


# ── DataLoader.load_bar_series ────────────────────────────────────────────

class TestLoadBarSeries:
    # 标准 schema（contract_col=None）下 key = contract_code 直接传给 DataSource
    def test_standard_schema(self, tmp_root):
        idx = pd.date_range("2024-01-02", periods=30, freq="B")
        _write_kline(tmp_root, "RB2410", idx)  # key = "RB2410"，无 klines/ 前缀
        loader = DataLoader(ParquetSource(tmp_root))
        bs = loader.load_bar_series("RB2410")
        assert isinstance(bs, BarSeries)
        assert len(bs) == 30
        assert set(bs.data.columns) >= {"open", "high", "low", "settle", "open_interest"}

    def test_tushare_schema(self, tmp_root):
        idx = pd.date_range("2024-01-02", periods=20, freq="B")
        _write_tushare_kline(tmp_root, "I", "I2605.DCE", idx)  # key = "I"（品种）
        loader = DataLoader(ParquetSource(tmp_root), kline_schema=KlineSchema.tushare())
        bs = loader.load_bar_series("I2605.DCE")
        assert isinstance(bs, BarSeries)
        assert len(bs) == 20

    def test_file_not_found_raises(self, tmp_root):
        loader = DataLoader(ParquetSource(tmp_root))
        with pytest.raises(FileNotFoundError):
            loader.load_bar_series("NONEXISTENT")

    def test_cache_returns_same_object(self, tmp_root):
        idx = pd.date_range("2024-01-02", periods=10, freq="B")
        _write_kline(tmp_root, "RB2410", idx)
        loader = DataLoader(ParquetSource(tmp_root), cache=True)
        bs1 = loader.load_bar_series("RB2410")
        bs2 = loader.load_bar_series("RB2410")
        assert bs1 is bs2

    def test_no_cache(self, tmp_root):
        idx = pd.date_range("2024-01-02", periods=10, freq="B")
        _write_kline(tmp_root, "RB2410", idx)
        loader = DataLoader(ParquetSource(tmp_root), cache=False)
        bs1 = loader.load_bar_series("RB2410")
        bs2 = loader.load_bar_series("RB2410")
        assert bs1 is not bs2

    def test_date_filter(self, tmp_root):
        idx = pd.date_range("2024-01-02", periods=60, freq="B")
        _write_kline(tmp_root, "RB2410", idx)
        loader = DataLoader(ParquetSource(tmp_root))
        bs = loader.load_bar_series("RB2410", start="2024-02-01", end="2024-02-29")
        assert bs.data.index.min() >= pd.Timestamp("2024-02-01")
        assert bs.data.index.max() <= pd.Timestamp("2024-02-29")


# ── DataLoader.load_contracts ─────────────────────────────────────────────

class TestLoadContracts:
    def test_returns_contract_list(self, tmp_root):
        _write_contracts(tmp_root, "RB")
        loader = DataLoader(ParquetSource(tmp_root))
        contracts = loader.load_contracts("RB")
        assert len(contracts) == 1
        assert isinstance(contracts[0], Contract)
        assert contracts[0].code == "RB2410"
        assert contracts[0].exchange == "SHFE"

    def test_active_only_filter(self, tmp_root):
        df = pd.DataFrame([
            {"code": "RB2409", "exchange": "SHFE",
             "list_date": "2024-01-01", "expire_date": "2024-09-15", "last_trade_date": "2024-09-14"},
            {"code": "RB2410", "exchange": "SHFE",
             "list_date": "2024-01-01", "expire_date": "2024-10-15", "last_trade_date": "2024-10-14"},
        ])
        ParquetSource(tmp_root).write_dataframe("contracts/RB", df)
        loader = DataLoader(ParquetSource(tmp_root))
        # ref_date 在 RB2409 到期后、RB2410 到期前
        contracts = loader.load_contracts("RB", active_only=True, ref_date="2024-09-20")
        assert len(contracts) == 1
        assert contracts[0].code == "RB2410"

    def test_cache(self, tmp_root):
        _write_contracts(tmp_root, "RB")
        loader = DataLoader(ParquetSource(tmp_root), cache=True)
        c1 = loader.load_contracts("RB")
        c2 = loader.load_contracts("RB")
        assert c1 is c2


# ── DataLoader.load_instrument ────────────────────────────────────────────

class TestLoadInstrument:
    def test_returns_instrument(self, tmp_root):
        _write_instrument(tmp_root, "RB")
        loader = DataLoader(ParquetSource(tmp_root))
        inst = loader.load_instrument("RB")
        assert inst.symbol == "RB"
        assert inst.lot_size == 10.0

    def test_auto_registers(self, tmp_root):
        _write_instrument(tmp_root, "RB")
        loader = DataLoader(ParquetSource(tmp_root))
        loader.load_instrument("RB")
        reg = InstrumentRegistry()
        assert reg.get("RB").name == "螺纹钢"

    def test_loads_from_explicit_instrument_source(self, tmp_root):
        df = pd.DataFrame([
            {
                "fut_code": "RB",
                "name": "螺纹钢主数据",
                "exchange": "SHFE",
                "per_unit": 10.0,
            }
        ])
        file_path = tmp_root / "instrument_catalog.parquet"
        df.to_parquet(file_path)

        loader = DataLoader(
            ParquetSource(tmp_root),
            instrument_source=ColumnKeyedSource(file_path, filter_col="fut_code"),
            instrument_schema=InstrumentSchema.china_from_contracts(),
        )
        inst = loader.load_instrument("RB")

        assert inst.symbol == "RB"
        assert inst.name == "螺纹钢主数据"
        assert inst.currency == "CNY"
        assert inst.lot_size == 10.0

    def test_instrument_source_overrides_fallback_file(self, tmp_root):
        _write_instrument(tmp_root, "RB")
        df = pd.DataFrame([
            {
                "fut_code": "RB",
                "name": "显式源优先",
                "exchange": "SHFE",
                "per_unit": 12.0,
            }
        ])
        file_path = tmp_root / "instrument_catalog.parquet"
        df.to_parquet(file_path)

        loader = DataLoader(
            ParquetSource(tmp_root),
            instrument_source=ColumnKeyedSource(file_path, filter_col="fut_code"),
            instrument_schema=InstrumentSchema.china_from_contracts(),
        )
        inst = loader.load_instrument("RB")

        assert inst.name == "显式源优先"
        assert inst.lot_size == 12.0


# ── DataLoader.load_calendar ──────────────────────────────────────────────

class TestLoadCalendar:
    def test_returns_trading_calendar(self, tmp_root):
        dates = pd.date_range("2024-01-02", "2024-06-30", freq="B")
        _write_calendar(tmp_root, "SHFE", dates)
        loader = DataLoader(ParquetSource(tmp_root))
        cal = loader.load_calendar("SHFE")
        assert isinstance(cal, TradingCalendar)
        assert cal.is_trading_day("2024-01-02")
        assert not cal.is_trading_day("2024-01-01")

    def test_load_multi_calendar(self, tmp_root):
        d1 = pd.date_range("2024-01-02", periods=5, freq="B")
        d2 = pd.date_range("2024-01-08", periods=5, freq="B")
        _write_calendar(tmp_root, "SHFE", d1)
        _write_calendar(tmp_root, "DCE", d2)
        loader = DataLoader(ParquetSource(tmp_root))
        mc = loader.load_multi_calendar(["SHFE", "DCE"])
        assert mc.is_trading_day("2024-01-02")
        assert mc.is_trading_day("2024-01-08")


class TestLoadContinuousPrebuilt:
    def test_prebuilt_schedule_from_inline_contract_column(self, tmp_root):
        idx = pd.date_range("2024-01-02", periods=4, freq="B")
        df = pd.DataFrame({
            "settle": [100.0, 101.0, 102.0, 103.0],
            "contract": ["RB2405", "RB2405", "RB2410", "RB2410"],
        }, index=idx)
        ParquetSource(tmp_root).write_dataframe("continuous/RB_nav", df)

        loader = DataLoader(ParquetSource(tmp_root))
        cs = loader.load_continuous("RB")

        assert len(cs.schedule.events) == 2
        assert cs.schedule.events[0].to_contract == "RB2405"
        assert cs.schedule.events[1].to_contract == "RB2410"

    def test_continuous_field_returns_zero_on_roll_day(self, tmp_root):
        idx = pd.date_range("2024-01-02", periods=4, freq="B")
        prebuilt = pd.DataFrame({"settle": [100.0, 101.0, 102.0, 103.0]}, index=idx)
        schedule_df = pd.DataFrame(
            {"to_contract": ["RB2405", "RB2410"]},
            index=pd.DatetimeIndex([idx[0], idx[2]]),
        )
        rb2405 = pd.DataFrame(
            {
                "open": [100.0, 101.0],
                "high": [101.0, 102.0],
                "low": [99.0, 100.0],
                "close": [100.0, 101.0],
                "settle": [100.0, 101.0],
                "volume": [1000.0, 1000.0],
                "open_interest": [5000.0, 4800.0],
            },
            index=idx[:2],
        )
        rb2410 = pd.DataFrame(
            {
                "open": [200.0, 202.0],
                "high": [201.0, 203.0],
                "low": [199.0, 201.0],
                "close": [200.0, 202.0],
                "settle": [200.0, 202.0],
                "volume": [1000.0, 1000.0],
                "open_interest": [5200.0, 5100.0],
            },
            index=idx[2:],
        )

        src = ParquetSource(tmp_root)
        src.write_dataframe("continuous/RB_nav", prebuilt)
        src.write_dataframe("continuous/RB_nav_schedule", schedule_df)
        src.write_dataframe("RB2405", rb2405)
        src.write_dataframe("RB2410", rb2410)

        loader = DataLoader(src)
        ret = loader.load_continuous_field_returns_series("RB", "close")

        assert ret.loc[idx[0]] == 0.0
        assert ret.loc[idx[1]] == pytest.approx(0.01)
        assert ret.loc[idx[2]] == 0.0
        assert ret.loc[idx[3]] == pytest.approx(0.01)

    def test_prebuilt_schedule_from_sidecar_file(self, tmp_root):
        idx = pd.date_range("2024-01-02", periods=4, freq="B")
        df = pd.DataFrame({"settle": [100.0, 101.0, 102.0, 103.0]}, index=idx)
        schedule_df = pd.DataFrame({
            "to_contract": ["RB2405", "RB2410"],
        }, index=pd.DatetimeIndex([idx[0], idx[2]]))
        ParquetSource(tmp_root).write_dataframe("continuous/RB_nav", df)
        ParquetSource(tmp_root).write_dataframe("continuous/RB_nav_schedule", schedule_df)

        loader = DataLoader(ParquetSource(tmp_root))
        cs = loader.load_continuous("RB")

        assert len(cs.schedule.events) == 2
        assert cs.schedule.events[0].to_contract == "RB2405"
        assert cs.schedule.events[1].to_contract == "RB2410"


class _CountingParquetSource(ParquetSource):
    def __init__(self, root_dir):
        super().__init__(root_dir)
        self.read_counts: dict[str, int] = {}

    def read_dataframe(self, key: str, *args, **kwargs):
        self.read_counts[key] = self.read_counts.get(key, 0) + 1
        return super().read_dataframe(key, *args, **kwargs)


class TestLoadContinuousPerformancePath:
    def test_mixed_table_reads_symbol_file_once(self, tmp_root):
        idx = pd.date_range("2024-01-02", periods=6, freq="B")
        df = pd.concat(
            [
                pd.DataFrame(
                    {
                        "contract_code": "RB2405.SHF",
                        "trade_date": idx,
                        "open_price": 100.0,
                        "high_price": 101.0,
                        "low_price": 99.0,
                        "close_price": 100.0,
                        "settle_price": [100, 101, 102, 103, 104, 105],
                        "volume": 1000.0,
                        "interest": [5000, 5000, 4000, 3000, 2000, 1000],
                    }
                ),
                pd.DataFrame(
                    {
                        "contract_code": "RB2410.SHF",
                        "trade_date": idx,
                        "open_price": 100.0,
                        "high_price": 101.0,
                        "low_price": 99.0,
                        "close_price": 100.0,
                        "settle_price": [110, 111, 112, 113, 114, 115],
                        "volume": 1000.0,
                        "interest": [1000, 2000, 6000, 7000, 8000, 9000],
                    }
                ),
            ],
            ignore_index=True,
        )
        ParquetSource(tmp_root).write_dataframe("RB", df)

        contracts_df = pd.DataFrame(
            [
                {
                    "fut_code": "RB",
                    "ts_code": "RB2405.SHF",
                    "exchange": "SHF",
                    "list_date": "2023-10-01",
                    "delist_date": "2024-05-31",
                    "last_ddate": "20240530",
                },
                {
                    "fut_code": "RB",
                    "ts_code": "RB2410.SHF",
                    "exchange": "SHF",
                    "list_date": "2024-01-01",
                    "delist_date": "2024-10-31",
                    "last_ddate": "20241030",
                },
            ]
        )
        contract_path = tmp_root / "contracts.parquet"
        contracts_df.to_parquet(contract_path)

        src = _CountingParquetSource(tmp_root)
        loader = DataLoader(
            src,
            contract_source=ColumnKeyedSource(contract_path, filter_col="fut_code"),
            kline_schema=KlineSchema.tushare(),
            contract_schema=ContractSchema.tushare(),
        )

        cs = loader.load_continuous("RB")
        assert len(cs) > 0
        assert src.read_counts.get("RB", 0) == 1


class TestLoadContinuousMatrix:
    def test_load_continuous_matrix_from_prebuilt_files(self, tmp_root):
        idx = pd.date_range("2024-01-02", periods=4, freq="B")
        src = ParquetSource(tmp_root)
        src.write_dataframe("continuous/RB_nav", pd.DataFrame({"settle": [100.0, 101.0, 102.0, 103.0]}, index=idx))
        src.write_dataframe("continuous/HC_nav", pd.DataFrame({"settle": [200.0, 201.0, 202.0, 203.0]}, index=idx))

        loader = DataLoader(src)
        matrix = loader.load_continuous_matrix(["RB", "HC"])

        assert list(matrix.columns) == ["RB", "HC"]
        assert matrix.loc[idx[0], "RB"] == 100.0
        assert matrix.loc[idx[-1], "HC"] == 203.0


# ── KlineSchema._normalize_kline ─────────────────────────────────────────

class TestNormalizeKline:
    def test_tushare_rename(self, tmp_root):
        idx = pd.date_range("2024-01-02", periods=10, freq="B")
        _write_tushare_kline(tmp_root, "RB", "RB2410", idx)
        loader = DataLoader(ParquetSource(tmp_root), kline_schema=KlineSchema.tushare())
        bs = loader.load_bar_series("RB2410")
        # 列名应已规范化
        assert "settle" in bs.data.columns
        assert "open_interest" in bs.data.columns
        assert "settle_price" not in bs.data.columns
        assert "interest" not in bs.data.columns

    def test_tushare_index_is_datetime(self, tmp_root):
        idx = pd.date_range("2024-01-02", periods=10, freq="B")
        _write_tushare_kline(tmp_root, "RB", "RB2410", idx)
        loader = DataLoader(ParquetSource(tmp_root), kline_schema=KlineSchema.tushare())
        bs = loader.load_bar_series("RB2410")
        assert isinstance(bs.data.index, pd.DatetimeIndex)

    def test_unknown_contract_in_mixed_table_raises(self, tmp_root):
        idx = pd.date_range("2024-01-02", periods=5, freq="B")
        _write_tushare_kline(tmp_root, "RB", "RB2410", idx)
        loader = DataLoader(ParquetSource(tmp_root), kline_schema=KlineSchema.tushare())
        with pytest.raises(KeyError, match="RB9999"):
            loader.load_bar_series("RB9999")
