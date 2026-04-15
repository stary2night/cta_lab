import numpy as np
import pandas as pd

from pathlib import Path

DATA_DIR = Path(__file__).parent.parent / "dayKline_full_period"
parquet_file = "M.parquet"

df = pd.read_parquet(DATA_DIR / parquet_file)

print(df.head())
print(df.tail())

