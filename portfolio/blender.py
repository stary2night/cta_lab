"""多子组合权重融合模块。"""

from __future__ import annotations

import pandas as pd


def blend(
    sub_weights: dict[int, pd.DataFrame],
    weights: list[float] | None = None,
) -> pd.DataFrame:
    """将多个子组合的权重矩阵加权融合为最终目标权重。

    sub_weights：{sub_index: weight_df(dates × symbols)}
    weights：各子组合融合权重，None 表示等权
    返回：融合后的 DataFrame(dates × symbols)

    对齐日期：取并集，缺失日填充（ffill）后加权。
    """
    keys = sorted(sub_weights.keys())
    n = len(keys)
    if n == 0:
        return pd.DataFrame()

    # 归一化融合权重
    if weights is None:
        w_list = [1.0 / n] * n
    else:
        total = sum(weights)
        w_list = [x / total for x in weights]

    # 取日期并集
    all_dates = pd.DatetimeIndex(
        sorted(
            set().union(*[set(sub_weights[k].index) for k in keys])
        )
    )

    # 取列并集
    all_symbols: list[str] = []
    seen: set[str] = set()
    for k in keys:
        for col in sub_weights[k].columns:
            if col not in seen:
                all_symbols.append(col)
                seen.add(col)

    # 对每个子组合 reindex 到全集日期+列，然后仅在其自身有效区间内 ffill
    blended = pd.DataFrame(0.0, index=all_dates, columns=all_symbols)
    for idx, k in enumerate(keys):
        source = sub_weights[k]
        first_date = source.index.min()
        last_date = source.index.max()
        df = (
            source
            .reindex(index=all_dates, columns=all_symbols)
            .ffill()
        )
        valid_range = pd.Series(
            (df.index >= first_date) & (df.index <= last_date),
            index=df.index,
        )
        df = df.where(valid_range, axis=0).fillna(0.0)
        blended = blended + df * w_list[idx]

    return blended
