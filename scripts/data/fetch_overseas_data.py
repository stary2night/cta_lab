"""
境外期货数据采集脚本
====================

背景
----
GMAT3 策略所需境外期货数据的采集脚本。数据来源为 QAD（Quantitative Analytics
Database，Refinitiv/LSEG 数据产品），与境内数据使用的 Wind DolphinDB 不同。
覆盖 GMAT3 最初6个境外品种（ES/NQ/TU/FV/TY/LCO），后续扩展品种见 run_data_pipeline.py。

ContrCode 为 QAD 的内部合约类别代码，已通过 QAD 查询界面逐一确认（见 VARIETY_CONFIG）。

功能
----
从 QAD Database 提取境外期货数据（2004年至今）：
  Step 1：合约静态信息（DSFutContrInfo + DSFutClass + DSFutContr）
    输出：contract_info.parquet → strategy_data/gmat3/contract_info_overseas.parquet
  Step 2：各品种日行情（DSFutContrVal，按品种单独文件）
    输出：daily_{variety}.parquet → market_data/kline/overseas_daily/daily_{variety}.parquet
  Step 3：USD/CNY 汇率（DS2FxRate）
    输出：fx_usdcny.parquet → market_data/fx/usdcny.csv（列名转换后）

数据源
------
QAD Database（SQL Server，内网访问）：
  - DSFutContrInfo   合约静态信息
  - DSFutContrVal    合约日行情
  - DSFutClass       合约类别（用于关联 ContrCode）
  - DSFutContr       合约说明（DSContrID、ContrName）
  - DS2FxRate        汇率
  - DS2FxCode        汇率代码

前置条件
--------
- QAD Python 库（`import QAD`，非公开库，需内网环境）
- QAD 数据库访问权限（SQL Server 连接由 QAD 库内部管理）
- 参考文档：docs/how to get data from QAD database.md
            docs/QAD Database Schema - Datastream Data.pdf

运行方式
--------
    cd /home/ubuntu/dengl/my_projects/cta_lab
    python scripts/data/fetch_overseas_data.py
"""

import pandas as pd
from pathlib import Path

# ── 输出目录 ──────────────────────────────────────────────
OUT_DIR = Path(__file__).parent / "raw" / "overseas"
OUT_DIR.mkdir(parents=True, exist_ok=True)

# ── 品种配置（ContrCode 已通过 QAD 查询逐一确认）─────────────
VARIETY_CONFIG = {
    "ES":  {"ContrCode": 1381, "DSContrID": "ISM",  "desc": "E-mini S&P 500"},
    "NQ":  {"ContrCode": 323,  "DSContrID": "CEN",  "desc": "E-mini Nasdaq-100"},
    "TU":  {"ContrCode": 463,  "DSContrID": "CZT",  "desc": "2-Year US Treasury"},
    "FV":  {"ContrCode": 452,  "DSContrID": "CZF",  "desc": "5-Year US Treasury"},
    "TY":  {"ContrCode": 458,  "DSContrID": "CZN",  "desc": "10-Year US Treasury"},
    "LCO": {"ContrCode": 2639, "DSContrID": "LCO",  "desc": "Brent Crude Oil"},
}

START_DATE = "2004-01-01"
ALL_CONTR_CODES = [v["ContrCode"] for v in VARIETY_CONFIG.values()]


# ═══════════════════════════════════════════════════════════
# Step 1：合约静态信息
# ═══════════════════════════════════════════════════════════
def fetch_contract_info():
    """
    提取6个品种的所有历史合约静态信息
    来源：DSFutContrInfo + DSFutClass + DSFutContr
    """
    print(">>> [Step 1] 提取合约静态信息 ...")

    contr_codes_str = ", ".join(str(c) for c in ALL_CONTR_CODES)
    sql = f"""
        SELECT
            ci.FutCode,
            ci.DSMnem,
            ci.ContrDate,
            ci.StartDate,
            ci.LastTrdDate,
            ci.SttlmntDate,
            ci.ISOCurrCode,
            ci.TrdStatCode,
            ci.LDB,
            cls.ContrCode,
            c.DSContrID,
            c.ContrName
        FROM DSFutContrInfo ci
        JOIN DSFutClass cls ON ci.ClsCode = cls.ClsCode
        JOIN DSFutContr  c  ON cls.ContrCode = c.ContrCode
        WHERE cls.ContrCode IN ({contr_codes_str})
          AND ci.StartDate >= '{START_DATE}'
        ORDER BY cls.ContrCode, ci.LastTrdDate
    """

    df = QAD.download_as_df(sql)

    # 打标签：GMAT3品种代码
    code_map = {v["ContrCode"]: k for k, v in VARIETY_CONFIG.items()}
    df["variety"] = df["ContrCode"].map(code_map)

    # 类型整理
    for col in ["StartDate", "LastTrdDate", "SttlmntDate"]:
        df[col] = pd.to_datetime(df[col])

    out_path = OUT_DIR / "contract_info.parquet"
    df.to_parquet(out_path, index=False)

    print(f"    合约数：{len(df)} 条  →  {out_path}")
    print(df.groupby("variety")[["FutCode"]].count().rename(columns={"FutCode": "合约数"}))
    return df


# ═══════════════════════════════════════════════════════════
# Step 2：各品种日行情
# ═══════════════════════════════════════════════════════════
def fetch_daily_prices(contract_info: pd.DataFrame):
    """
    按品种分批提取日行情数据
    来源：DSFutContrVal
    每个品种单独保存，便于管理和增量更新
    """
    print("\n>>> [Step 2] 提取日行情数据 ...")

    for variety, cfg in VARIETY_CONFIG.items():
        print(f"\n  -- {variety} ({cfg['desc']}) --")

        # 取该品种的所有 FutCode
        fut_codes = contract_info.loc[
            contract_info["variety"] == variety, "FutCode"
        ].tolist()

        if not fut_codes:
            print(f"    警告：{variety} 无合约数据，跳过")
            continue

        # SQL Server 的 IN 子句限制，分批处理（每批1000个）
        batch_size = 1000
        all_batches = []

        for i in range(0, len(fut_codes), batch_size):
            batch = fut_codes[i : i + batch_size]
            codes_str = ", ".join(str(c) for c in batch)

            sql = f"""
                SELECT
                    v.FutCode,
                    v.Date_,
                    v.Open_       AS open_price,
                    v.High        AS high_price,
                    v.Low         AS low_price,
                    v.Settlement  AS settle_price,
                    v.OpenInterest AS open_interest,
                    v.Volume      AS volume
                FROM DSFutContrVal v
                WHERE v.FutCode IN ({codes_str})
                  AND v.Date_ >= '{START_DATE}'
                ORDER BY v.FutCode, v.Date_
            """

            batch_df = QAD.download_as_df(sql)
            all_batches.append(batch_df)
            print(f"    批次 {i // batch_size + 1}：{len(batch_df)} 行")

        df = pd.concat(all_batches, ignore_index=True)

        # 类型整理
        df["Date_"] = pd.to_datetime(df["Date_"])
        df["variety"] = variety

        # 过滤掉 StartDate 当天 OI=0 且 Volume=0 的无效首行
        df = df[~((df["open_interest"] == 0) & (df["volume"] == 0) &
                   (df["settle_price"].isna() | (df["settle_price"] == 0)))]

        out_path = OUT_DIR / f"daily_{variety}.parquet"
        df.to_parquet(out_path, index=False)

        print(f"    总行数：{len(df)}  日期范围：{df['Date_'].min().date()} ~ {df['Date_'].max().date()}")
        print(f"    已保存 →  {out_path}")

    print("\n  日行情提取完成")


# ═══════════════════════════════════════════════════════════
# Step 3：USD/CNY 汇率
# ═══════════════════════════════════════════════════════════
def fetch_fx_usdcny():
    """
    提取 USD/CNY 每日中间价
    来源：DS2FxRate + DS2FxCode
    """
    print("\n>>> [Step 3] 提取 USD/CNY 汇率 ...")

    sql = f"""
        SELECT
            r.ExRateDate  AS date_,
            r.MidRate     AS usdcny_mid,
            r.BidRate     AS usdcny_bid,
            r.OfferRate   AS usdcny_offer
        FROM DS2FxRate r
        JOIN DS2FxCode c ON r.ExRateIntCode = c.ExRateIntCode
        WHERE c.FromCurrCode = 'USD'
          AND c.ToCurrCode   = 'CNY'
          AND r.ExRateDate  >= '{START_DATE}'
        ORDER BY r.ExRateDate
    """

    df = QAD.download_as_df(sql)
    df["date_"] = pd.to_datetime(df["date_"])

    out_path = OUT_DIR / "fx_usdcny.parquet"
    df.to_parquet(out_path, index=False)

    print(f"    总行数：{len(df)}  日期范围：{df['date_'].min().date()} ~ {df['date_'].max().date()}")
    print(f"    已保存 →  {out_path}")
    return df


# ═══════════════════════════════════════════════════════════
# 主流程
# ═══════════════════════════════════════════════════════════
if __name__ == "__main__":
    print("=" * 60)
    print("GMAT3 境外期货数据采集")
    print(f"数据起始日期：{START_DATE}")
    print(f"输出目录：{OUT_DIR}")
    print("=" * 60)

    # Step 1：合约静态信息
    contract_info = fetch_contract_info()

    # Step 2：各品种日行情
    fetch_daily_prices(contract_info)

    # Step 3：汇率
    fetch_fx_usdcny()

    print("\n" + "=" * 60)
    print("全部数据采集完成")

    # 输出文件清单
    print("\n生成文件：")
    for f in sorted(OUT_DIR.glob("*.parquet")):
        size_mb = f.stat().st_size / 1024 / 1024
        print(f"  {f.name:<30}  {size_mb:.2f} MB")
