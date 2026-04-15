"""tests/test_data_module/test_contract.py — Contract 测试"""

import pytest
from datetime import date
from data.model.contract import Contract


@pytest.fixture
def rb2410() -> Contract:
    return Contract(
        symbol="RB",
        code="RB2410",
        exchange="SHFE",
        list_date=date(2024, 1, 15),
        expire_date=date(2024, 10, 15),
        last_trade_date=date(2024, 10, 14),
    )


# ── days_to_expiry ────────────────────────────────────────────────────────

class TestDaysToExpiry:
    def test_days_before(self, rb2410):
        assert rb2410.days_to_expiry(date(2024, 10, 10)) == 4

    def test_on_last_trade_day(self, rb2410):
        assert rb2410.days_to_expiry(date(2024, 10, 14)) == 0

    def test_after_expiry_negative(self, rb2410):
        assert rb2410.days_to_expiry(date(2024, 10, 20)) < 0

    def test_long_before(self, rb2410):
        # 2024-01-15 到 2024-10-14 = 273 天
        assert rb2410.days_to_expiry(date(2024, 1, 15)) == 273


# ── is_active ─────────────────────────────────────────────────────────────

class TestIsActive:
    def test_on_list_date(self, rb2410):
        assert rb2410.is_active(date(2024, 1, 15)) is True

    def test_on_last_trade_date(self, rb2410):
        assert rb2410.is_active(date(2024, 10, 14)) is True

    def test_before_list_date(self, rb2410):
        assert rb2410.is_active(date(2024, 1, 14)) is False

    def test_after_last_trade_date(self, rb2410):
        assert rb2410.is_active(date(2024, 10, 15)) is False

    def test_middle_of_life(self, rb2410):
        assert rb2410.is_active(date(2024, 5, 1)) is True


# ── month_code ────────────────────────────────────────────────────────────

class TestMonthCode:
    def test_standard(self, rb2410):
        assert rb2410.month_code() == "2410"

    def test_single_letter_symbol(self):
        c = Contract("I", "I2605", "DCE", date(2025, 5, 1), date(2026, 5, 15), date(2026, 5, 14))
        assert c.month_code() == "2605"

    def test_two_letter_symbol(self):
        c = Contract("JM", "JM2412", "DCE", date(2024, 1, 1), date(2024, 12, 15), date(2024, 12, 14))
        assert c.month_code() == "2412"

    def test_with_exchange_suffix(self):
        # 合约代码含交易所后缀时 month_code 不应截断
        c = Contract("RB", "RB2410.SHFE", "SHFE",
                     date(2024, 1, 1), date(2024, 10, 15), date(2024, 10, 14))
        # symbol="RB"，code="RB2410.SHFE"，month_code 返回 "2410.SHFE"
        # 这是当前实现的行为，测试记录实际结果
        result = c.month_code()
        assert result.startswith("2410")


# ── dataclass 基础行为 ────────────────────────────────────────────────────

class TestContractDataclass:
    def test_equality(self, rb2410):
        c2 = Contract("RB", "RB2410", "SHFE",
                      date(2024, 1, 15), date(2024, 10, 15), date(2024, 10, 14))
        assert rb2410 == c2

    def test_inequality(self, rb2410):
        c2 = Contract("RB", "RB2501", "SHFE",
                      date(2024, 1, 15), date(2025, 1, 15), date(2025, 1, 14))
        assert rb2410 != c2
