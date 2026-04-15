"""scripts/verify_data_layer.py — 用真实 market_data 验证 data 层各模块功能。

运行：
    cd /home/ubuntu/dengl/my_projects/cta_lab
    python scripts/verify_data_layer.py
"""

from __future__ import annotations

import sys
from pathlib import Path

import numpy as np
import pandas as pd

ROOT = Path(__file__).parent.parent
MARKET_DATA = ROOT.parent / "market_data"
sys.path.insert(0, str(ROOT))

from data.loader import ContractSchema, DataLoader, KlineSchema
from data.model.bar import BarSeries
from data.model.calendar import TradingCalendar
from data.model.continuous import AdjustMethod, ContinuousSeries
from data.model.roll import OIMaxRoll
from data.sources.column_keyed_source import ColumnKeyedSource
from data.sources.parquet_source import ParquetSource

PASS = "✓"
FAIL = "✗"
SKIP = "○"


def section(title: str):
    print(f"\n{'─'*60}")
    print(f"  {title}")
    print(f"{'─'*60}")


def check(label: str, ok: bool, detail: str = ""):
    mark = PASS if ok else FAIL
    suffix = f"  [{detail}]" if detail else ""
    print(f"  {mark} {label}{suffix}")
    return ok


# ─────────────────────────────────────────────────────────────
# 1. BarSeries 加载（KlineSchema.tushare，china_daily_full）
# ─────────────────────────────────────────────────────────────
section("1. load_bar_series  (KlineSchema.tushare, china_daily_full)")

kline_src = ParquetSource(MARKET_DATA / "kline" / "china_daily_full")
loader = DataLoader(kline_src, kline_schema=KlineSchema.tushare())

SYMBOLS = ["RB", "HC", "CU", "AL", "AU", "I", "J", "JM", "M", "P"]

bar_data_all: dict[str, dict[str, BarSeries]] = {}  # symbol -> {contract_code -> BarSeries}

for sym in SYMBOLS:
    try:
        # 读整张品种大表，取其中一个主力合约来验证
        df_raw = pd.read_parquet(MARKET_DATA / "kline" / "china_daily_full" / f"{sym}.parquet")
        # 找交易量最大的合约代码作为代表
        top_code = df_raw.groupby("contract_code")["volume"].sum().idxmax()
        bs = loader.load_bar_series(top_code)
        ok = isinstance(bs, BarSeries) and len(bs) > 0
        check(f"{sym}: {top_code} → {len(bs)} bars", ok,
              f"settle={bs.data['settle'].iloc[-1]:.1f}")
        bar_data_all[sym] = {"representative": bs}
    except Exception as e:
        check(f"{sym}", False, str(e)[:60])


# ─────────────────────────────────────────────────────────────
# 2. BarSeries 分析方法
# ─────────────────────────────────────────────────────────────
section("2. BarSeries 分析方法  (以 RB 为例)")

try:
    df_raw = pd.read_parquet(MARKET_DATA / "kline" / "china_daily_full" / "RB.parquet")
    top_code = df_raw.groupby("contract_code")["volume"].sum().idxmax()
    bs = loader.load_bar_series(top_code)

    lr = bs.log_returns()
    check("log_returns: 首值 NaN", np.isnan(lr.iloc[0]))
    check("log_returns: 其余非 NaN", lr.iloc[1:].notna().all())

    pr = bs.pct_returns()
    check("pct_returns: 长度一致", len(pr) == len(bs))

    ev = bs.ewm_vol(60)
    check("ewm_vol(60): iloc[2:] 无 NaN", ev.iloc[2:].notna().all())
    check("ewm_vol(60): 全为正值", (ev.dropna() > 0).all())

    rv = bs.rolling_vol(20)
    check("rolling_vol(20): 前20行全 NaN", rv.iloc[:20].isna().all())
    check("rolling_vol(20): 后续非 NaN", rv.iloc[20:].notna().all())

    dd = bs.drawdown()
    check("drawdown: 全部 ≤ 0", (dd <= 0).all())
    check("drawdown: 全部 ≥ -1", (dd >= -1).all())

    # 切片：用该合约数据范围内的日期
    start_dt = bs.data.index[0]
    mid_dt = bs.data.index[len(bs) // 2]
    sub = bs[str(start_dt.date()):str(mid_dt.date())]
    check(f"__getitem__ 切片: {len(sub)} bars", isinstance(sub, BarSeries) and len(sub) > 0)

except Exception as e:
    check("BarSeries 分析方法", False, str(e)[:80])


# ─────────────────────────────────────────────────────────────
# 3. TradingCalendar（从 china_trading_calendar.parquet 提取 SHFE 日历）
# ─────────────────────────────────────────────────────────────
section("3. TradingCalendar  (china_trading_calendar.parquet → SHFE)")

try:
    cal_df = pd.read_parquet(MARKET_DATA / "calendar" / "china_trading_calendar.parquet")
    shfe_dates = pd.DatetimeIndex(
        pd.to_datetime(cal_df[cal_df["exchange"] == "SHF"]["trade_date"])
    )
    cal = TradingCalendar("SHF", shfe_dates)

    check(f"总交易日数: {len(cal._dates)}", len(cal._dates) > 1000)
    check("2024-01-02 是交易日", cal.is_trading_day("2024-01-02"))
    check("2024-01-01 (元旦) 不是交易日", not cal.is_trading_day("2024-01-01"))

    nxt = cal.next_trading_day("2024-01-01")
    check(f"next_trading_day(2024-01-01) = {nxt.date()}", nxt >= pd.Timestamp("2024-01-02"))

    n = cal.trading_days_between("2024-01-01", "2024-12-31")
    check(f"2024年交易日数 = {n}", 240 <= n <= 252)

except Exception as e:
    check("TradingCalendar", False, str(e)[:80])
    cal = None


# ─────────────────────────────────────────────────────────────
# 4. Contract 加载（从 future_basic_info.csv 提取合约列表）
# ─────────────────────────────────────────────────────────────
section("4. Contract 加载  (contract_info.parquet → RB 合约列表)")

from data.model.contract import Contract

basic_df = pd.read_parquet(MARKET_DATA / "contracts" / "china" / "contract_info.parquet")

try:
    rb_df = basic_df[basic_df["fut_code"] == "RB"].copy()

    contracts = []
    for _, row in rb_df.iterrows():
        try:
            c = Contract(
                symbol="RB",
                code=str(row["ts_code"]),
                exchange=str(row["exchange"]),
                list_date=pd.Timestamp(str(row["list_date"])).date(),
                expire_date=pd.Timestamp(str(row["delist_date"])).date(),
                last_trade_date=pd.Timestamp(str(int(row["last_ddate"]))).date(),
            )
            contracts.append(c)
        except Exception:
            continue

    check(f"RB 合约数量: {len(contracts)}", len(contracts) > 20)

    from datetime import date
    active = [c for c in contracts if c.is_active(date(2024, 1, 15))]
    check(f"2024-01-15 活跃合约数: {len(active)}", len(active) >= 1)

    sample = contracts[0]
    dtx = sample.days_to_expiry(date(2024, 1, 1))
    check(f"days_to_expiry 可计算 ({sample.code})", isinstance(dtx, int))

except Exception as e:
    check("Contract 加载", False, str(e)[:80])


# ─────────────────────────────────────────────────────────────
# 5. ContinuousSeries.build（多合约拼接，OIMaxRoll，NAV/ADD/RATIO）
# ─────────────────────────────────────────────────────────────
section("5. ContinuousSeries.build  (RB, OIMaxRoll, 三种调整方式)")

try:
    # 读取 RB 全部合约 K 线
    df_rb = pd.read_parquet(MARKET_DATA / "kline" / "china_daily_full" / "RB.parquet")

    # 只取 2015 年以后、交易量 > 1000 的合约，避免数据噪音
    active_codes = (
        df_rb[df_rb["trade_date"] >= "2015-01-01"]
        .groupby("contract_code")["volume"]
        .sum()
        .pipe(lambda s: s[s > 1000])
        .index.tolist()
    )

    bar_data: dict[str, BarSeries] = {}
    for code in active_codes:
        sub = df_rb[df_rb["contract_code"] == code].copy()
        sub = sub.set_index(pd.DatetimeIndex(pd.to_datetime(sub["trade_date"])))
        sub.index.name = "date"
        sub = sub.rename(columns={
            "open_price": "open", "high_price": "high", "low_price": "low",
            "close_price": "close", "settle_price": "settle", "interest": "open_interest",
        })
        cols = ["open", "high", "low", "close", "settle", "volume", "open_interest"]
        sub = sub[[c for c in cols if c in sub.columns]].sort_index()
        try:
            bar_data[code] = BarSeries(code, sub)
        except Exception:
            continue

    check(f"有效 RB 合约数: {len(bar_data)}", len(bar_data) >= 5)

    # 合约对象（简化：只用合约代码，日期从数据推断）
    from datetime import date as Date
    # bar_data 的 key 格式是 "RB2301.SHF"，构建从 bare code 到 full code 的映射
    bare_to_full = {k.split(".")[0]: k for k in bar_data}

    rb_contracts_raw = basic_df[basic_df["fut_code"] == "RB"].copy()
    contracts_rb = []
    for _, row in rb_contracts_raw.iterrows():
        bare_code = str(row["symbol"])      # e.g. "RB2301"
        full_code = bare_to_full.get(bare_code)  # e.g. "RB2301.SHF"
        if full_code is None:
            continue
        try:
            last_ddate = row["last_ddate"]
            if pd.isna(last_ddate):
                continue
            c = Contract(
                symbol="RB", code=full_code, exchange=str(row["exchange"]),
                list_date=pd.Timestamp(str(row["list_date"])).date(),
                expire_date=pd.Timestamp(str(row["delist_date"])).date(),
                last_trade_date=pd.Timestamp(str(int(last_ddate))).date(),
            )
            contracts_rb.append(c)
        except Exception:
            continue

    check(f"有效 RB Contract 对象: {len(contracts_rb)}", len(contracts_rb) >= 5)

    for method in [AdjustMethod.NONE, AdjustMethod.NAV, AdjustMethod.RATIO, AdjustMethod.ADD]:
        try:
            cs = ContinuousSeries.build(
                "RB", bar_data, contracts_rb, OIMaxRoll(), method, cal
            )
            no_nan = cs.prices.notna().all()
            roll_ok = len(cs.schedule.events) >= 1
            # ADD 方法对大幅上涨品种历史段会出现负价，属预期行为（非 bug）
            if method == AdjustMethod.ADD:
                valid = no_nan and roll_ok
            else:
                valid = no_nan and (cs.prices > 0).all() and roll_ok
            check(
                f"AdjustMethod.{method.name}: {len(cs)} bars, {len(cs.schedule.events)} rolls",
                valid,
                f"首价={cs.prices.iloc[0]:.1f} 末价={cs.prices.iloc[-1]:.1f}"
            )
        except Exception as e:
            check(f"AdjustMethod.{method.name}", False, str(e)[:70])

except Exception as e:
    check("ContinuousSeries.build", False, str(e)[:80])


# ─────────────────────────────────────────────────────────────
# 6. ContinuousSeries 分析方法
# ─────────────────────────────────────────────────────────────
section("6. ContinuousSeries 分析方法  (RB, NAV)")

try:
    cs_nav = ContinuousSeries.build("RB", bar_data, contracts_rb, OIMaxRoll(), AdjustMethod.NAV, cal)

    lr = cs_nav.log_returns()
    check("log_returns: 首值 NaN", np.isnan(lr.iloc[0]))
    check("log_returns: 极端值 < 50%", (lr.dropna().abs() < 0.5).all())

    ev = cs_nav.ewm_vol(60)
    check("ewm_vol(60): 全部正值", (ev.dropna() > 0).all())

    rv = cs_nav.rolling_vol(20)
    check("rolling_vol(20): 有非 NaN 值", rv.dropna().shape[0] > 0)

    dd = cs_nav.drawdown()
    check("drawdown: 全部 ≤ 0", (dd <= 0).all())

    sub = cs_nav["2020-01-01":"2022-12-31"]
    check(f"切片 2020-2022: {len(sub)} bars", isinstance(sub, ContinuousSeries) and len(sub) > 400)

    check("prices property 类型", isinstance(cs_nav.prices, pd.Series))

except Exception as e:
    check("ContinuousSeries 分析", False, str(e)[:80])


# ─────────────────────────────────────────────────────────────
# 7. DataLoader.load_continuous（动态构建，需 contract_source）
# ─────────────────────────────────────────────────────────────
section("7. DataLoader.load_continuous  (使用真实 market_data 动态构建)")

try:
    china_loader = DataLoader(
        kline_source=ParquetSource(MARKET_DATA / "kline" / "china_daily_full"),
        contract_source=ColumnKeyedSource(
            MARKET_DATA / "contracts" / "china" / "contract_info.parquet",
            filter_col="fut_code",
        ),
        calendar_source=ColumnKeyedSource(
            MARKET_DATA / "calendar" / "china_trading_calendar.parquet",
            filter_col="exchange",
        ),
        kline_schema=KlineSchema.tushare(),
        contract_schema=ContractSchema.tushare(),
    )
    cs = china_loader.load_continuous("RB", start="2020-01-01", end="2022-12-31")
    check(
        f"China RB continuous: {len(cs)} bars",
        len(cs) > 400 and cs.prices.notna().all(),
        f"首价={cs.prices.iloc[0]:.4f} 末价={cs.prices.iloc[-1]:.4f}"
    )
except Exception as e:
    check("China load_continuous", False, str(e)[:80])

try:
    overseas_loader = DataLoader(
        kline_source=ParquetSource(MARKET_DATA / "kline" / "overseas_daily_full"),
        contract_source=ColumnKeyedSource(
            MARKET_DATA / "contracts" / "overseas" / "contract_info.parquet",
            filter_col="BaseTicker",
        ),
        kline_schema=KlineSchema.overseas(),
        contract_schema=ContractSchema.overseas(),
    )
    cs = overseas_loader.load_continuous("ES", start="2000-01-01", end="2005-01-01")
    check(
        f"Overseas ES continuous: {len(cs)} bars",
        len(cs) > 1000 and cs.prices.notna().all(),
        f"首价={cs.prices.iloc[0]:.4f} 末价={cs.prices.iloc[-1]:.4f}"
    )
except Exception as e:
    check("Overseas load_continuous", False, str(e)[:80])


print(f"\n{'─'*60}")
print(f"  验证完成")
print(f"{'─'*60}\n")
