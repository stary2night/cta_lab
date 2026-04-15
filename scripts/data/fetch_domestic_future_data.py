"""
境内期货数据采集脚本（正式可运行版）
======================================

背景
----
GMAT3 策略所需境内期货数据的正式采集脚本，是 fetch_domestic_data.py 草稿版的
修正版本（使用了正确的 Wind DolphinDB 列名）。覆盖 GMAT3 所有14个境内品种。

功能
----
从 Wind DolphinDB 提取境内期货数据（2004年至今）：
  Step 1：合约静态信息（合并 CFuturesDescription + CFuturesContPro）
    输出：contract_info_domestic.parquet → strategy_data/gmat3/
  Step 2：按类别日行情（3个文件，含开高低收结算价/成交量/持仓量）
    输出：daily_index_futures / daily_bond_futures / daily_commodity_futures.parquet
    → 运行后需通过 merge_kline.py 转换为
      market_data/kline/china_daily_full/{variety}.parquet（按品种单独文件）

数据源
------
Wind DolphinDB（内网访问）：
  - dfs://WIND.CINDEXFUTURESEODPRICES   股指期货行情
  - dfs://WIND.CBONDFUTURESEODPRICES    国债期货行情
  - dfs://WIND.CCOMMODITYFUTURESEODPRICES 商品期货行情
  - dfs://WIND.CFUTURESDESCRIPTION      合约基本信息
  - dfs://WIND.CFUTURESCONTPRO          合约技术参数（乘数等）

前置条件
--------
- dolphindb Python 库：pip install dolphindb
- Wind DolphinDB 内网访问权限
- 在 HOST/PORT/USERID/PASSWORD 处填入实际连接信息

运行方式
--------
    cd /home/ubuntu/dengl/my_projects/cta_lab
    python scripts/data/fetch_domestic_future_data.py
"""

import dolphindb as ddb
import pandas as pd
from pathlib import Path

# ── DolphinDB 连接配置 ────────────────────────────────────────
HOST     = "<wind-ddb-host>"    # Wind DolphinDB IP
PORT     = 0                    # 端口
USERID   = "<username>"         # 用户名
PASSWORD = "<password>"         # 密码

# ── 输出目录 ──────────────────────────────────────────────────
OUT_DIR = Path(__file__).parent / "raw" / "domestic"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ── 数据起始日期 ───────────────────────────────────────────────
START_DATE = "2004.01.01"   # DolphinDB 日期格式

# ── Wind DolphinDB 表路径 ─────────────────────────────────────
TABLES = {
    "index_futures":     ("dfs://WIND.CINDEXFUTURESEODPRICES",     "data"),
    "bond_futures":      ("dfs://WIND.CBONDFUTURESEODPRICES",      "data"),
    "commodity_futures": ("dfs://WIND.CCOMMODITYFUTURESEODPRICES", "data"),
    "fut_desc":          ("dfs://WIND.CFUTURESDESCRIPTION",        "data"),
    "fut_cont_pro":      ("dfs://WIND.CFUTURESCONTPRO",            "data"),
}

# ── 各表需要提取的品种过滤条件 ────────────────────────────────
# DolphinDB 中 code 格式如 "IF2306.CFE"，"M2309.DCE"
# 单字母品种（M/I/J）用 regexFind 避免误匹配 JM/IM 等
VARIETY_FILTERS = {
    "index_futures": """
        startsWith(S_INFO_WINDCODE, 'IF') or
        startsWith(S_INFO_WINDCODE, 'IC') or
        startsWith(S_INFO_WINDCODE, 'IM')
    """,
    "bond_futures": """
        startsWith(S_INFO_WINDCODE, 'TS') or
        startsWith(S_INFO_WINDCODE, 'TF') or
        regexFind(S_INFO_WINDCODE, '^T[0-9]') >= 0
    """,
    "commodity_futures": """
        startsWith(S_INFO_WINDCODE, 'AU') or
        startsWith(S_INFO_WINDCODE, 'CU') or
        startsWith(S_INFO_WINDCODE, 'RB') or
        startsWith(S_INFO_WINDCODE, 'HC') or
        startsWith(S_INFO_WINDCODE, 'JM') or
        regexFind(S_INFO_WINDCODE, '^J[0-9]') >= 0 or
        regexFind(S_INFO_WINDCODE, '^M[0-9]') >= 0 or
        regexFind(S_INFO_WINDCODE, '^I[0-9]') >= 0
    """,
}

# ── 品种代码归属标签（用于后续策略代码识别）─────────────────
VARIETY_MAP = {
    "IF": "index_futures", "IC": "index_futures", "IM": "index_futures",
    "TS": "bond_futures",  "TF": "bond_futures",  "T":  "bond_futures",
    "AU": "commodity_futures", "CU": "commodity_futures",
    "M":  "commodity_futures", "RB": "commodity_futures",
    "HC": "commodity_futures", "I":  "commodity_futures",
    "J":  "commodity_futures", "JM": "commodity_futures",
}

# 所有目标品种前缀（用于合约静态信息过滤）
ALL_VARIETY_PREFIXES = list(VARIETY_MAP.keys())


def connect() -> ddb.session:
    """建立 DolphinDB Session 连接"""
    s = ddb.session()
    s.connect(HOST, PORT, USERID, PASSWORD)
    print(f"已连接 Wind DolphinDB：{HOST}:{PORT}")
    return s


def load_table(s: ddb.session, table_key: str) -> str:
    """返回 DolphinDB loadTable 表达式字符串"""
    db_path, tbl_name = TABLES[table_key]
    return f'loadTable("{db_path}", "{tbl_name}")'


# ═══════════════════════════════════════════════════════════
# Step 1：合约静态信息
# ═══════════════════════════════════════════════════════════
def fetch_contract_info(s: ddb.session) -> pd.DataFrame:
    """
    合并 CFuturesDescription + CFuturesContPro
    提取14个境内品种的合约静态信息
    """
    print("\n>>> [Step 1] 提取合约静态信息 ...")

    # 构建品种过滤条件
    prefix_conditions = " or ".join([
        f"startsWith(S_INFO_WINDCODE, '{v}')" if len(v) > 1
        else f"regexFind(S_INFO_WINDCODE, '^{v}[0-9]') >= 0"
        for v in ALL_VARIETY_PREFIXES
    ])

    # 查询 CFuturesDescription
    script_desc = f"""
        t = {load_table(s, 'fut_desc')}
        select
            S_INFO_WINDCODE   as wind_code,
            S_INFO_NAME       as contract_name,
            S_INFO_EXCHMARKET as exchange,
            S_INFO_LISTDATE   as list_date,
            S_INFO_DELISTDATE as last_trade_date,
            FS_INFO_DLMONTH   as delivery_month,
            FS_INFO_LTDLDATE  as last_delivery_date
        from t
        where {prefix_conditions}
    """
    df_desc = s.run(script_desc)

    # 查询 CFuturesContPro（合约乘数）
    script_pro = f"""
        t = {load_table(s, 'fut_cont_pro')}
        select
            S_INFO_WINDCODE      as wind_code,
            S_INFO_TUNIT         as trade_unit,
            S_INFO_PUNIT         as price_unit,
            S_INFO_CEMULTIPLIER  as contract_multiplier
        from t
        where {prefix_conditions}
    """
    df_pro = s.run(script_pro)

    # 合并两张表
    df = pd.merge(df_desc, df_pro, on="wind_code", how="left")

    # 提取品种代码
    df["variety"] = df["wind_code"].apply(_extract_variety)

    # 类型整理
    for col in ["list_date", "last_trade_date", "last_delivery_date"]:
        df[col] = pd.to_datetime(df[col], errors="coerce")

    out_path = OUT_DIR / "contract_info_domestic.parquet"
    df.to_parquet(out_path, index=False)

    print(f"    合约总数：{len(df)} 条")
    print(df.groupby("variety")[["wind_code"]].count().rename(
        columns={"wind_code": "合约数"}
    ))
    print(f"    已保存 → {out_path}")
    return df


# ═══════════════════════════════════════════════════════════
# Step 2：各类期货日行情
# ═══════════════════════════════════════════════════════════
def fetch_daily_prices(s: ddb.session, table_key: str, output_name: str):
    """
    提取单张行情表的日行情数据

    Parameters
    ----------
    table_key   : TABLES 中的键名
    output_name : 输出文件名（不含扩展名）
    """
    print(f"\n  -- {output_name} --")

    variety_filter = VARIETY_FILTERS[table_key]

    script = f"""
        t = {load_table(s, table_key)}
        select
            S_INFO_WINDCODE   as wind_code,
            TRADE_DT          as trade_date,
            S_DQ_PRESETTLE    as pre_settle_price,
            S_DQ_OPEN         as open_price,
            S_DQ_HIGH         as high_price,
            S_DQ_LOW          as low_price,
            S_DQ_CLOSE        as close_price,
            S_DQ_SETTLE       as settle_price,
            S_DQ_VOLUME       as volume,
            S_DQ_AMOUNT       as amount,
            S_DQ_OI           as open_interest
        from t
        where TRADE_DT >= {START_DATE}
          and ({variety_filter})
        order by S_INFO_WINDCODE, TRADE_DT
    """

    df = s.run(script)

    # 提取品种代码
    df["variety"] = df["wind_code"].apply(_extract_variety)

    # 类型整理
    df["trade_date"] = pd.to_datetime(df["trade_date"])

    # 过滤上市首日无效数据（OI=0 且 Volume=0 且 settle 为空或0）
    invalid_mask = (
        (df["open_interest"] == 0) &
        (df["volume"] == 0) &
        (df["settle_price"].isna() | (df["settle_price"] == 0))
    )
    df = df[~invalid_mask].reset_index(drop=True)

    out_path = OUT_DIR / f"{output_name}.parquet"
    df.to_parquet(out_path, index=False)

    print(f"    行数：{len(df):,}  合约数：{df['wind_code'].nunique()}")
    print(f"    日期：{df['trade_date'].min().date()} ~ {df['trade_date'].max().date()}")
    print(f"    品种：{sorted(df['variety'].unique())}")
    print(f"    已保存 → {out_path}")

    return df


def fetch_all_daily_prices(s: ddb.session):
    """提取三类期货日行情"""
    print("\n>>> [Step 2] 提取日行情数据 ...")

    fetch_daily_prices(s, "index_futures",     "daily_index_futures")
    fetch_daily_prices(s, "bond_futures",      "daily_bond_futures")
    fetch_daily_prices(s, "commodity_futures", "daily_commodity_futures")

    print("\n  日行情提取完成")


# ═══════════════════════════════════════════════════════════
# 工具函数
# ═══════════════════════════════════════════════════════════
def _extract_variety(wind_code: str) -> str:
    """
    从 Wind 合约代码提取品种前缀
    例：'IF2306.CFE' → 'IF'，'M2309.DCE' → 'M'，'JM2309.DCE' → 'JM'
    """
    # 去掉交易所后缀
    base = wind_code.split(".")[0]
    # 提取开头的字母部分
    variety = ""
    for ch in base:
        if ch.isalpha():
            variety += ch
        else:
            break
    return variety


# ═══════════════════════════════════════════════════════════
# 主流程
# ═══════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("=" * 60)
    print("GMAT3 境内期货数据采集")
    print(f"数据起始日期：{START_DATE}")
    print(f"输出目录：{OUT_DIR.resolve()}")
    print("=" * 60)

    s = connect()

    # Step 1：合约静态信息
    contract_info = fetch_contract_info(s)

    # Step 2：各类日行情
    fetch_all_daily_prices(s)

    s.close()

    print("\n" + "=" * 60)
    print("全部数据采集完成")
    print("\n生成文件：")
    for f in sorted(OUT_DIR.glob("*.parquet")):
        size_mb = f.stat().st_size / 1024 / 1024
        print(f"  {f.name:<45} {size_mb:.2f} MB")
