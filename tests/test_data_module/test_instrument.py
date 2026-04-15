"""tests/test_data_module/test_instrument.py — Instrument & InstrumentRegistry 测试"""

import pytest
from data.model.instrument import Instrument, InstrumentRegistry


@pytest.fixture(autouse=True)
def clear_registry():
    """每个测试前清空注册表，避免单例状态污染。"""
    reg = InstrumentRegistry()
    reg._instruments.clear()
    yield
    reg._instruments.clear()


def make_rb() -> Instrument:
    return Instrument(
        symbol="RB", name="螺纹钢", exchange="SHFE",
        currency="CNY", lot_size=10.0, tick_size=1.0, margin_rate=0.1,
    )


def make_cu() -> Instrument:
    return Instrument(
        symbol="CU", name="铜", exchange="SHFE",
        currency="CNY", lot_size=5.0, tick_size=10.0, margin_rate=0.1,
    )


def make_i() -> Instrument:
    return Instrument(
        symbol="I", name="铁矿石", exchange="DCE",
        currency="CNY", lot_size=100.0, tick_size=0.5, margin_rate=0.08,
    )


# ── 单例行为 ──────────────────────────────────────────────────────────────

class TestRegistrySingleton:
    def test_same_instance(self):
        r1 = InstrumentRegistry()
        r2 = InstrumentRegistry()
        assert r1 is r2

    def test_state_shared_across_instances(self):
        r1 = InstrumentRegistry()
        r1.register(make_rb())
        r2 = InstrumentRegistry()
        assert r2.get("RB").name == "螺纹钢"


# ── register / get ────────────────────────────────────────────────────────

class TestRegisterGet:
    def test_register_and_get(self):
        reg = InstrumentRegistry()
        rb = make_rb()
        reg.register(rb)
        assert reg.get("RB") is rb

    def test_get_unknown_raises_key_error(self):
        reg = InstrumentRegistry()
        with pytest.raises(KeyError, match="RB"):
            reg.get("RB")

    def test_register_overwrite(self):
        reg = InstrumentRegistry()
        reg.register(make_rb())
        new_rb = Instrument("RB", "螺纹钢新", "SHFE", "CNY", 10.0, 1.0, 0.1)
        reg.register(new_rb)
        assert reg.get("RB").name == "螺纹钢新"

    def test_register_multiple(self):
        reg = InstrumentRegistry()
        reg.register(make_rb())
        reg.register(make_cu())
        reg.register(make_i())
        assert len(reg.list_all()) == 3


# ── list_all / list_by_exchange ───────────────────────────────────────────

class TestListMethods:
    def test_list_all_empty(self):
        assert InstrumentRegistry().list_all() == []

    def test_list_all(self):
        reg = InstrumentRegistry()
        reg.register(make_rb())
        reg.register(make_i())
        syms = {inst.symbol for inst in reg.list_all()}
        assert syms == {"RB", "I"}

    def test_list_by_exchange_shfe(self):
        reg = InstrumentRegistry()
        reg.register(make_rb())
        reg.register(make_cu())
        reg.register(make_i())
        shfe = reg.list_by_exchange("SHFE")
        assert len(shfe) == 2
        assert all(i.exchange == "SHFE" for i in shfe)

    def test_list_by_exchange_no_match(self):
        reg = InstrumentRegistry()
        reg.register(make_rb())
        assert reg.list_by_exchange("CFFEX") == []


# ── Instrument dataclass ──────────────────────────────────────────────────

class TestInstrumentDataclass:
    def test_fields_accessible(self):
        rb = make_rb()
        assert rb.symbol == "RB"
        assert rb.lot_size == 10.0
        assert rb.currency == "CNY"

    def test_equality(self):
        assert make_rb() == make_rb()
        assert make_rb() != make_cu()
