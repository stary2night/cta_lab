"""
合并 dayKline 和 dayKline_19970630_20190628 两个数据源，
覆盖时间并集，输出到 dayKline_full_period。

策略：
- 两个目录都有的品种：取旧数据中早于新数据起始日期的部分，再拼接新数据（重叠部分经验证完全一致，以新为准）
- 仅新目录有的品种：直接复制
- 仅旧目录有的品种：直接复制
"""
import os
import shutil
import pandas as pd

DIR_NEW = "FutureData/dayKline"
DIR_OLD = "FutureData/dayKline_19970630_20190628"
DIR_OUT = "FutureData/dayKline_full_period"

os.makedirs(DIR_OUT, exist_ok=True)

files_new = {f for f in os.listdir(DIR_NEW) if f.endswith(".parquet")}
files_old = {f for f in os.listdir(DIR_OLD) if f.endswith(".parquet")}

both = sorted(files_new & files_old)
only_new = sorted(files_new - files_old)
only_old = sorted(files_old - files_new)

print(f"共有品种: {len(both)}, 仅新: {len(only_new)}, 仅旧: {len(only_old)}")

# 合并共有品种
merged_ok, merged_fail = [], []
for fname in both:
    try:
        df_new = pd.read_parquet(os.path.join(DIR_NEW, fname))
        df_old = pd.read_parquet(os.path.join(DIR_OLD, fname))

        new_start = df_new["trade_date"].min()
        # 取旧数据中早于新数据起始日期的行（不含起始日，因为重叠一致）
        df_old_pre = df_old[df_old["trade_date"] < new_start]

        df_merged = pd.concat([df_old_pre, df_new], ignore_index=True)
        df_merged = df_merged.sort_values(["trade_date", "contract_code"]).reset_index(drop=True)

        out_path = os.path.join(DIR_OUT, fname)
        df_merged.to_parquet(out_path, index=False)
        merged_ok.append(fname)
        print(f"  [合并] {fname}: {df_old['trade_date'].min().date()} -> {df_new['trade_date'].max().date()} "
              f"({len(df_old_pre)}旧 + {len(df_new)}新 = {len(df_merged)}行)")
    except Exception as e:
        merged_fail.append((fname, str(e)))
        print(f"  [错误] {fname}: {e}")

# 仅新目录有的品种
for fname in only_new:
    try:
        shutil.copy2(os.path.join(DIR_NEW, fname), os.path.join(DIR_OUT, fname))
        df = pd.read_parquet(os.path.join(DIR_OUT, fname))
        print(f"  [复制-新] {fname}: {df['trade_date'].min().date()} -> {df['trade_date'].max().date()} ({len(df)}行)")
    except Exception as e:
        print(f"  [错误] {fname}: {e}")

# 仅旧目录有的品种
for fname in only_old:
    try:
        shutil.copy2(os.path.join(DIR_OLD, fname), os.path.join(DIR_OUT, fname))
        df = pd.read_parquet(os.path.join(DIR_OUT, fname))
        print(f"  [复制-旧] {fname}: {df['trade_date'].min().date()} -> {df['trade_date'].max().date()} ({len(df)}行)")
    except Exception as e:
        print(f"  [错误] {fname}: {e}")

print(f"\n完成。输出目录: {DIR_OUT}")
print(f"总文件数: {len(os.listdir(DIR_OUT))}")
if merged_fail:
    print(f"失败: {merged_fail}")
