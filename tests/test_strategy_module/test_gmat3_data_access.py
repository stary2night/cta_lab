from __future__ import annotations

from strategies.implementations.gmat3 import GMAT3DataAccess


def test_gmat3_data_access_reads_contracts_daily_substitute_and_fx() -> None:
    access = GMAT3DataAccess()

    contracts = access.get_contract_info("IF")
    assert not contracts.empty
    assert {"contract_id", "last_trade_date", "variety", "market"}.issubset(contracts.columns)

    daily = access.get_daily("ES")
    assert not daily.empty
    assert {"contract_id", "trade_date", "settle_price", "open_interest", "variety"}.issubset(daily.columns)

    sub = access.get_substitute_price("000300.SH")
    assert not sub.empty

    fx = access.get_fx_rate()
    assert not fx.empty


def test_gmat3_data_access_trading_days_and_meta() -> None:
    access = GMAT3DataAccess()

    days = access.trading_days("CFF")
    assert len(days) > 10
    assert access.nth_month_trading_day("CFF", 2015, 4, 2) is not None

    meta = access.get_sub_portfolio_meta("TU")
    assert meta["currency"] == "USD"
