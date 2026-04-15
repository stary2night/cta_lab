"""
替代标的指数数据采集脚本
==========================

背景
----
GMAT3 各子组合均有一个"基日"（上市日），基日之前无合约价格数据。
为了将子组合净值回溯到更早期（统一从 2010 年开始），策略规则手册规定
用对应的股票或债券指数作为"替代标的"填充基日前的价格序列。

本脚本负责从 Wind DolphinDB 提取这6个替代标的指数的历史收盘价。

替代标的对应关系（来自 GMAT3 Rule Book 表1）
--------------------------------------------
  - 000300.SH（沪深300指数）  → IF 子组合基日（2010-04-18）前
  - 000905.SH（中证500指数）  → IC 子组合基日（2015-04-16）前
  - 000852.SH（中证1000指数） → IM 子组合基日（2022-07-24）前
  - CBA00622.CS（中债国债净价1-3年） → TS 子组合基日（2018-08-19）前
  - 000140.SH（上证5年期国债净价）   → TF 子组合基日（2013-09-08）前
  - CBA00652.CS（中债国债净价7-10年）→ T  子组合基日（2015-03-22）前

功能
----
从 Wind DolphinDB 提取上述6个指数收盘价，保存为：
  - substitute_indices.parquet（长表，列：wind_code / trade_date / close_price）
  - substitute_indices_wide.parquet（宽表，行：trade_date，列：各指数 wind_code）
输出路径 → market_data/kline/china_daily/substitute_indices*.parquet

数据源
------
Wind DolphinDB（内网访问）：
  - dfs://WIND.AINDEXEODPRICES   股票指数行情
  - dfs://WIND.CBINDEXEODPRICES  债券指数行情

前置条件
--------
- dolphindb Python 库：pip install dolphindb
- Wind DolphinDB 内网访问权限
- 在 DDB_HOST/DDB_PORT/DDB_USER/DDB_PASS 处填入实际连接信息

运行方式
--------
    cd /home/ubuntu/dengl/my_projects/cta_lab
    python scripts/data/fetch_substitute_index.py
"""

import dolphindb as ddb
import pandas as pd
from pathlib import Path

# ── DolphinDB 连接配置 ─────────────────────────────────────────────────────────
DDB_HOST = "<wind-ddb-host>"   # Wind DolphinDB IP
DDB_PORT = 0                   # 端口
DDB_USER = "<username>"        # 用户名
DDB_PASS = "<password>"        # 密码

# ── 替代标的定义（对应 GMAT3 Rule Book 表1）──────────────────────────────────────
# 格式：(wind_code, 名称, 对应子组合, 需要数据截止日, 数据表)
EQUITY_INDICES = [
    ("000300.SH", "沪深300指数",  "IF子组合", "2010.04.18"),
    ("000905.SH", "中证500指数",  "IC子组合", "2015.04.16"),
    ("000852.SH", "中证1000指数", "IM子组合", "2022.07.24"),
]

BOND_INDICES = [
    ("CBA00622.CS", "中债-国债总净价(1-3年)指数",  "TS子组合", "2018.08.19"),
    ("000140.SH",   "上证5年期国债指数(净价)",      "TF子组合", "2013.09.08"),
    ("CBA00652.CS", "中债-国债总净价(7-10年)指数",  "T子组合",  "2015.03.22"),
]

# 合并用于统计输出
ALL_INDICES = EQUITY_INDICES + BOND_INDICES

START_DATE = "2005.01.01"   # 早于所有子组合基日，留足余量

# 输出路径：与本脚本同级的 raw/domestic 目录
# 使用 resolve() 转为绝对路径，运行时会打印实际保存位置
_BASE = Path(__file__).resolve().parent
OUTPUT_DIR = _BASE / "raw" / "domestic"
OUTPUT_FILE = OUTPUT_DIR / "substitute_indices.parquet"


def query_indices(s: ddb.session, indices: list, table_path: str) -> pd.DataFrame:
    """从指定 Wind 表查询指数收盘价"""
    codes_str = "','".join(item[0] for item in indices)
    script = f"""
        t = loadTable("{table_path}", "data");
        select
            S_INFO_WINDCODE as wind_code,
            TRADE_DT        as trade_date,
            S_DQ_CLOSE      as close_price
        from t
        where
            S_INFO_WINDCODE in ['{codes_str}']
            and TRADE_DT >= {START_DATE}
        order by S_INFO_WINDCODE, TRADE_DT
    """
    df = s.run(script)
    return df if df is not None else pd.DataFrame()


def fetch_substitute_indices():
    s = ddb.session()
    s.connect(DDB_HOST, DDB_PORT, DDB_USER, DDB_PASS)
    print(f"已连接 DolphinDB {DDB_HOST}:{DDB_PORT}")

    # 股票指数：AINDEXEODPRICES
    print("正在查询股票指数（AINDEXEODPRICES）...")
    df_equity = query_indices(s, EQUITY_INDICES, "dfs://WIND.AINDEXEODPRICES")
    print(f"  → 获取 {len(df_equity)} 条记录")

    # 债券指数：CBINDEXEODPRICES
    print("正在查询债券指数（CBINDEXEODPRICES）...")
    df_bond = query_indices(s, BOND_INDICES, "dfs://WIND.CBINDEXEODPRICES")
    print(f"  → 获取 {len(df_bond)} 条记录")

    s.close()

    if len(df_equity) == 0 and len(df_bond) == 0:
        print("⚠️  两个表查询结果均为空，请检查表名或字段名是否正确")
        return

    # 合并
    df = pd.concat([df_equity, df_bond], ignore_index=True)

    # 类型整理
    df["trade_date"] = pd.to_datetime(df["trade_date"])
    df["close_price"] = pd.to_numeric(df["close_price"], errors="coerce")
    df = df.sort_values(["wind_code", "trade_date"]).reset_index(drop=True)

    # 统计输出
    print(f"\n=== 查询结果统计 ===")
    for code, name, portfolio, end_dt in ALL_INDICES:
        sub = df[df["wind_code"] == code]
        if len(sub) == 0:
            print(f"  ❌ {code} ({name}): 无数据")
        else:
            needed_end = pd.Timestamp(end_dt)
            actual_end = sub["trade_date"].max()
            actual_start = sub["trade_date"].min()
            covered = "✅" if actual_end >= needed_end else f"⚠️ 数据截止{actual_end.date()}，需要到{end_dt}"
            print(f"  {covered} {code} ({name}): {actual_start.date()} ~ {actual_end.date()}, {len(sub)}条")

    # 空值检查
    null_count = df["close_price"].isna().sum()
    if null_count > 0:
        print(f"\n⚠️  close_price 有 {null_count} 个空值，将在策略层按需前向填充")

    # 保存
    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    df.to_parquet(OUTPUT_FILE, index=False)
    print(f"\n✅ 已保存至 {OUTPUT_FILE.resolve()}，共 {len(df)} 条记录")

    # 同时保存宽表（每个指数一列）方便后续读取
    pivot = df.pivot(index="trade_date", columns="wind_code", values="close_price")
    pivot.to_parquet(OUTPUT_DIR / "substitute_indices_wide.parquet")
    print(f"✅ 宽表已保存至 {OUTPUT_DIR / 'substitute_indices_wide.parquet'}")


if __name__ == "__main__":
    fetch_substitute_indices()
