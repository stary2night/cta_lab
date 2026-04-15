"""
境外交易日历补充脚本
====================

背景
----
fetch_trading_calendar.py 的补充脚本。在境内日历已生成、
但境外日历需要单独更新（例如新增品种或扩展日期范围）时使用。

该脚本不依赖 Wind DolphinDB，仅从本地已有的境外日行情 Parquet 文件
推导 CME/ICE 交易日（有结算价的日期 = 交易日），然后追加到已有的
trading_calendar.parquet 中。

功能
----
1. 读取已有的 market_data/calendar/china_trading_calendar.parquet（境内部分）
2. 从 market_data/kline/overseas_daily/daily_ES/NQ/LCO.parquet 推导 CME/ICE 日历
3. 合并去重后写回 china_trading_calendar.parquet

使用场景
--------
- 境外行情数据更新后，需同步补充日历
- fetch_trading_calendar.py 境外部分推导失败，手动补跑

运行方式
--------
    cd /home/ubuntu/dengl/my_projects/cta_lab
    python scripts/data/append_overseas_calendar.py

注意：脚本内路径（OUTPUT_FILE / OVERSEAS_FILES）需按 market_data 实际结构调整。
"""

import pandas as pd
from pathlib import Path

OUTPUT_FILE = Path("data/raw/trading_calendar.parquet")

OVERSEAS_FILES = {
    "CME": [
        "data/raw/overseas/daily_ES.parquet",
        "data/raw/overseas/daily_NQ.parquet",
    ],
    "ICE": [
        "data/raw/overseas/daily_LCO.parquet",
    ],
}


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
            print(f"  {exchange}: {min(all_dates).date()} ~ {max(all_dates).date()}, {len(all_dates)}天")
        else:
            print(f"  ❌ {exchange}: 无法推导日历")

    return pd.DataFrame(records) if records else pd.DataFrame()


def main():
    # 读取已上传的境内日历
    if not OUTPUT_FILE.exists():
        print(f"❌ 境内日历文件不存在：{OUTPUT_FILE}")
        print("请先从本地上传 trading_calendar.parquet 到服务器")
        return

    domestic_df = pd.read_parquet(OUTPUT_FILE)
    print(f"已读取境内日历：{len(domestic_df)} 条")

    # 推导境外日历
    print("\n推导境外交易日历...")
    overseas_df = derive_overseas_calendar()
    if overseas_df.empty:
        print("❌ 境外日历推导失败，退出")
        return

    # 合并并去重
    all_cal = (pd.concat([domestic_df, overseas_df], ignore_index=True)
               .drop_duplicates()
               .sort_values(["exchange", "trade_date"])
               .reset_index(drop=True))

    all_cal.to_parquet(OUTPUT_FILE, index=False)
    print(f"\n✅ 已保存至 {OUTPUT_FILE}，共 {len(all_cal)} 条记录")
    print(all_cal.groupby("exchange").agg(
        start=("trade_date", "min"),
        end=("trade_date", "max"),
        days=("trade_date", "count")
    ).to_string())


if __name__ == "__main__":
    main()
