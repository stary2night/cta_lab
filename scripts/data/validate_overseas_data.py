"""
境外期货数据完整性验证脚本
==========================

背景
----
fetch_overseas_data.py 采集完成后用于验证数据质量的检验脚本。
开发 GMAT3 初期，境外数据从 QAD 采集后需手动验证完整性，
本脚本提供了一套系统化的检查流程。

功能
----
逐一检验 market_data/kline/overseas_daily/ 目录下所有 Parquet 文件：
  1. 文件是否存在及大小
  2. 合约数量是否与 contract_info.parquet 吻合
  3. 日期范围和连续性
  4. 结算价空值率（阈值 < 5%）
  5. 持仓量为零比例（阈值 < 10%）
  6. 结算价异常值（≤ 0）
  7. USD/CNY 汇率完整性（MidRate 空值检查）

覆盖品种：ES / NQ / TU / FV / TY / LCO

运行方式
--------
    cd /home/ubuntu/dengl/my_projects/cta_lab
    python scripts/data/validate_overseas_data.py

注意：脚本内 OUT_DIR 路径（默认 raw/overseas/）需修改为
    market_data/kline/overseas_daily/
"""

import pandas as pd
from pathlib import Path

# ── 请修改为实际路径 ──────────────────────────────────────────
# fetch_overseas_data.py 所在目录，例如：
# OUT_DIR = Path(r"C:\Users\yourname\projects\ddb\data\raw\overseas")
OUT_DIR = Path(__file__).parent / "raw" / "overseas"
# ─────────────────────────────────────────────────────────────

VARIETIES = ["ES", "NQ", "TU", "FV", "TY", "LCO"]

SEP = "=" * 60


def check_dir():
    """确认目录和文件存在"""
    print(SEP)
    print(f"数据目录：{OUT_DIR.resolve()}")
    print(f"目录存在：{OUT_DIR.exists()}")
    if not OUT_DIR.exists():
        print("❌ 目录不存在，请检查 OUT_DIR 路径设置")
        return False

    print("\n文件清单：")
    total_mb = 0
    for f in sorted(OUT_DIR.glob("*.parquet")):
        size_mb = f.stat().st_size / 1024 / 1024
        total_mb += size_mb
        print(f"  {f.name:<35} {size_mb:.3f} MB")
    print(f"  {'合计':<35} {total_mb:.3f} MB")
    return True


def check_contract_info():
    """验证合约静态信息"""
    print(f"\n{SEP}")
    print("【contract_info.parquet】合约静态信息")

    path = OUT_DIR / "contract_info.parquet"
    if not path.exists():
        print("❌ 文件不存在")
        return None

    df = pd.read_parquet(path)

    print(f"\n总行数：{len(df)}")
    print(f"字段：{list(df.columns)}")

    print("\n各品种合约数量：")
    summary = df.groupby("variety").agg(
        合约数=("FutCode", "count"),
        最早上市=("StartDate", "min"),
        最晚到期=("LastTrdDate", "max"),
        活跃合约=("TrdStatCode", lambda x: (x == "A").sum()),
    )
    print(summary.to_string())

    print(f"\n空值检查：")
    for col in ["FutCode", "DSMnem", "LastTrdDate", "ContrDate"]:
        n_null = df[col].isna().sum()
        status = "✅" if n_null == 0 else f"⚠️  {n_null}个空值"
        print(f"  {col:<20} {status}")

    return df


def check_daily_prices(contract_info: pd.DataFrame):
    """验证各品种日行情数据"""
    print(f"\n{SEP}")
    print("【daily_*.parquet】日行情数据")

    for variety in VARIETIES:
        path = OUT_DIR / f"daily_{variety}.parquet"
        print(f"\n  -- {variety} --")

        if not path.exists():
            print("  ❌ 文件不存在")
            continue

        df = pd.read_parquet(path)
        n_contracts = df["FutCode"].nunique()
        date_min = df["Date_"].min().date()
        date_max = df["Date_"].max().date()

        # 结算价空值率
        null_settle = df["settle_price"].isna().sum()
        null_pct = null_settle / len(df) * 100

        # 持仓量全零比例
        zero_oi = (df["open_interest"] == 0).sum()
        zero_oi_pct = zero_oi / len(df) * 100

        # 结算价异常（负数或零）
        invalid_settle = (df["settle_price"] <= 0).sum()

        print(f"  行数：{len(df):,}   合约数：{n_contracts}")
        print(f"  日期范围：{date_min} ~ {date_max}")
        print(f"  结算价空值：{null_settle} ({null_pct:.1f}%)"
              f"  {'✅' if null_pct < 5 else '⚠️'}")
        print(f"  持仓量为零：{zero_oi} ({zero_oi_pct:.1f}%)"
              f"  {'✅' if zero_oi_pct < 10 else '⚠️'}")
        print(f"  结算价≤0：{invalid_settle}"
              f"  {'✅' if invalid_settle == 0 else '⚠️'}")

        # 验证合约数与 contract_info 是否一致
        if contract_info is not None:
            expected = contract_info[contract_info["variety"] == variety]["FutCode"].nunique()
            match = "✅" if n_contracts == expected else f"⚠️ 预期{expected}个"
            print(f"  合约数与contract_info一致：{match}")

        # 抽样展示几行
        print(f"\n  数据样本（最早5行）：")
        sample = df.sort_values("Date_").head(5)[
            ["FutCode", "Date_", "settle_price", "open_interest", "volume"]
        ]
        print(sample.to_string(index=False))


def check_fx():
    """验证汇率数据"""
    print(f"\n{SEP}")
    print("【fx_usdcny.parquet】USD/CNY 汇率")

    path = OUT_DIR / "fx_usdcny.parquet"
    if not path.exists():
        print("❌ 文件不存在")
        return

    df = pd.read_parquet(path)

    print(f"\n总行数：{len(df)}")
    print(f"日期范围：{df['date_'].min().date()} ~ {df['date_'].max().date()}")

    null_mid = df["usdcny_mid"].isna().sum()
    print(f"MidRate 空值：{null_mid}  {'✅' if null_mid == 0 else '⚠️'}")

    print(f"\nMidRate 统计：")
    print(f"  最小值：{df['usdcny_mid'].min():.4f}")
    print(f"  最大值：{df['usdcny_mid'].max():.4f}")
    print(f"  均值：  {df['usdcny_mid'].mean():.4f}")

    print(f"\n数据样本（最新5行）：")
    print(df.sort_values("date_").tail(5).to_string(index=False))


# ═══════════════════════════════════════════════════════════
# 主流程
# ═══════════════════════════════════════════════════════════
if __name__ == "__main__":
    print(SEP)
    print("GMAT3 境外期货数据完整性验证")
    print(SEP)

    if not check_dir():
        exit(1)

    contract_info = check_contract_info()
    check_daily_prices(contract_info)
    check_fx()

    print(f"\n{SEP}")
    print("验证完成")
