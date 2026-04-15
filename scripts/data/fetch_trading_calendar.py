"""
交易日历采集脚本
================

背景
----
GMAT3 策略的调仓日、末日持仓截止日等关键逻辑依赖精确的交易日历。
境内日历可从 Wind DolphinDB 的专用日历表直接获取；境外（CME/ICE）无专用表，
通过已下载的日行情 Parquet 文件推导（有结算价的日期即为交易日）。

功能
----
  Part 1：境内日历（CFFEX/CFF / SHFE/SHF / DCE）
    来源：Wind DolphinDB dfs://WIND.CFUTURESCALENDAR
  Part 2：境外日历（CME / ICE）
    来源：market_data/kline/overseas_daily/ 下的日行情 Parquet 文件推导
  合并输出：market_data/calendar/china_trading_calendar.parquet
    列：exchange（交易所缩写）、trade_date

交易所代码映射
--------------
  Wind S_INFO_EXCHMARKET → 策略使用缩写：
  CFFEX → CFF  （股指、国债期货）
  SHFE  → SHF  （AU/CU/RB/HC 等）
  DCE   → DCE  （M/I/J/JM 等）
  CME（推导）  （ES/NQ/TU/FV/TY）
  ICE（推导）  （LCO）

数据源
------
Wind DolphinDB（境内）+ 本地 Parquet 推导（境外）

前置条件
--------
- dolphindb Python 库：pip install dolphindb
- Wind DolphinDB 内网访问权限
- 境外日行情 Parquet 文件已下载至 market_data/kline/overseas_daily/
- 在 DDB_HOST/DDB_PORT/DDB_USER/DDB_PASS 处填入实际连接信息

运行方式
--------
    cd /home/ubuntu/dengl/my_projects/cta_lab
    python scripts/data/fetch_trading_calendar.py
"""

import dolphindb as ddb
import pandas as pd
from pathlib import Path

# ── DolphinDB 连接配置 ─────────────────────────────────────────────────────────
DDB_HOST = "<wind-ddb-host>"   # Wind DolphinDB IP
DDB_PORT = 0                   # 端口
DDB_USER = "<username>"        # 用户名
DDB_PASS = "<password>"        # 密码

START_DATE = "2004-01-01"

# 路径均相对于本脚本所在目录，避免工作目录不同导致找不到文件
_BASE = Path(__file__).parent
OUTPUT_DIR = _BASE / "raw"
OUTPUT_FILE = OUTPUT_DIR / "trading_calendar.parquet"

# Wind S_INFO_EXCHMARKET 值 → 策略使用的交易所缩写
EXCH_MAP = {
    "CFFEX": "CFF",   # 中国金融期货交易所 → IF/IC/IM/TS/TF/T
    "SHFE":  "SHF",   # 上海期货交易所     → AU/CU/RB/HC
    "DCE":   "DCE",   # 大连商品交易所     → M/I/J/JM
}

# 境外：从已有 Parquet 文件推导（取有结算价的日期集合）
OVERSEAS_FILES = {
    "CME": [_BASE / "raw/overseas/daily_ES.parquet", _BASE / "raw/overseas/daily_NQ.parquet"],
    "ICE": [_BASE / "raw/overseas/daily_LCO.parquet"],
}


def fetch_domestic_calendar(s: ddb.session) -> pd.DataFrame:
    exch_list = "','".join(EXCH_MAP.keys())
    script = f"""
        t = loadTable("dfs://WIND.CFUTURESCALENDAR", "data");
        select
            S_INFO_EXCHMARKET as exchange_code,
            TRADE_DAYS        as trade_date
        from t
        where
            S_INFO_EXCHMARKET in ['{exch_list}']
            and TRADE_DAYS >= {START_DATE}
        order by S_INFO_EXCHMARKET, TRADE_DAYS
    """
    df = s.run(script)
    if df is None or len(df) == 0:
        print("⚠️  境内交易日历查询结果为空")
        return pd.DataFrame()

    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df["exchange"] = df["exchange_code"].map(EXCH_MAP)
    df = df[["exchange", "trade_date"]].dropna().drop_duplicates()

    for exch in sorted(df["exchange"].unique()):
        sub = df[df["exchange"] == exch]
        print(f"  {exch}: {sub['trade_date'].min().date()} ~ {sub['trade_date'].max().date()}, {len(sub)}天")
    return df


def derive_overseas_calendar() -> pd.DataFrame:
    records = []
    for exchange, files in OVERSEAS_FILES.items():
        all_dates = set()
        for fpath in files:
            p = Path(fpath)
            if not p.exists():
                print(f"⚠️  文件不存在: {fpath}，跳过")
                continue
            df = pd.read_parquet(p)
            date_col   = next(c for c in df.columns if "date" in c.lower())
            settle_col = next((c for c in df.columns if "settle" in c.lower()), None)
            df[date_col] = pd.to_datetime(df[date_col])
            valid = df[df[settle_col].notna() & (df[settle_col] > 0)] if settle_col else df
            all_dates |= set(valid[date_col].dt.normalize().unique())

        if all_dates:
            records += [{"exchange": exchange, "trade_date": pd.Timestamp(d)} for d in all_dates]
            print(f"  {exchange}: {min(all_dates).date()} ~ {max(all_dates).date()}, {len(all_dates)}天（Parquet推导）")
        else:
            print(f"  ❌ {exchange}: 无法推导日历")

    return pd.DataFrame(records) if records else pd.DataFrame()


def main():
    print("Part 1: 境内交易日历（Wind CFUTURESCALENDAR）")
    s = ddb.session()
    s.connect(DDB_HOST, DDB_PORT, DDB_USER, DDB_PASS)
    domestic_df = fetch_domestic_calendar(s)
    s.close()

    print("\nPart 2: 境外交易日历（从 Parquet 推导）")
    overseas_df = derive_overseas_calendar()

    frames = [f for f in [domestic_df, overseas_df] if len(f) > 0]
    if not frames:
        print("❌ 无任何日历数据，退出")
        return

    all_cal = (pd.concat(frames, ignore_index=True)
               .drop_duplicates()
               .sort_values(["exchange", "trade_date"])
               .reset_index(drop=True))

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    all_cal.to_parquet(OUTPUT_FILE, index=False)
    print(f"\n✅ 已保存至 {OUTPUT_FILE}，共 {len(all_cal)} 条记录")
    print(all_cal.groupby("exchange").agg(
        start=("trade_date", "min"),
        end=("trade_date", "max"),
        days=("trade_date", "count")
    ).to_string())


if __name__ == "__main__":
    main()
