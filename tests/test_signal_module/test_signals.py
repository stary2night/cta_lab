"""signals 层测试：接口语义、组合逻辑与关键边界行为。"""

import numpy as np
import pandas as pd
import pytest

from signals import (
    AbsoluteMomentum,
    LinearCombiner,
    PercentileMomentum,
    RankCombiner,
    SharpeMomentum,
    TSMOM,
)


DATES = pd.date_range("2024-01-02", periods=12, freq="B")


class TestSignalBaseSemantics:
    def test_tsmom_direction_values(self):
        prices = pd.Series(np.linspace(100, 120, len(DATES)), index=DATES)
        out = TSMOM(lookback=3).compute(prices).dropna()
        assert set(out.unique()).issubset({-1.0, 0.0, 1.0})

    def test_sharpe_momentum_outputs_float_series(self):
        prices = pd.Series(np.linspace(100, 120, len(DATES)), index=DATES)
        out = SharpeMomentum(lookback=3).compute(prices)
        assert isinstance(out, pd.Series)
        assert out.index.equals(prices.index)


class TestPercentileMomentum:
    def test_flat_prices_are_neutral(self):
        prices = pd.Series(np.full(len(DATES), 100.0), index=DATES)
        out = PercentileMomentum(lookback=5).compute(prices)
        assert out.dropna().eq(0.0).all()

    def test_monotonic_rise_positive(self):
        prices = pd.Series(np.arange(len(DATES), dtype=float) + 100.0, index=DATES)
        out = PercentileMomentum(lookback=5).compute(prices)
        assert (out.dropna() > 0).all()


class TestLinearCombiner:
    def test_skipna_weighted_average(self):
        prices = pd.Series(np.arange(len(DATES), dtype=float) + 100.0, index=DATES)
        comb = LinearCombiner(
            [AbsoluteMomentum(lookback=2), SharpeMomentum(lookback=50)],
            weights=[0.2, 0.8],
        )
        out = comb.compute(prices)

        # 第二个信号全 NaN 时，应退化为第一个信号，而不是被稀释
        ref = AbsoluteMomentum(lookback=2).compute(prices)
        pd.testing.assert_series_equal(out, ref)


class TestRankCombiner:
    def test_skipna_reweights_across_signals(self):
        price_matrix = pd.DataFrame(
            {
                "A": [100, 101, 102, 103, 104, 105],
                "B": [100, 100, 100, 100, 100, 100],
            },
            index=pd.date_range("2024-01-02", periods=6, freq="B"),
        )
        comb = RankCombiner(
            [AbsoluteMomentum(lookback=2), SharpeMomentum(lookback=50)],
            weights=[0.3, 0.7],
        )
        out = comb.compute(price_matrix)

        ref = price_matrix.apply(AbsoluteMomentum(lookback=2).compute, axis=0).rank(pct=True, axis=1)
        pd.testing.assert_frame_equal(out, ref)

    def test_output_nan_when_all_signals_nan(self):
        price_matrix = pd.DataFrame(
            {
                "A": [100.0, 101.0, 102.0],
                "B": [100.0, 101.0, 102.0],
            },
            index=pd.date_range("2024-01-02", periods=3, freq="B"),
        )
        comb = RankCombiner([AbsoluteMomentum(lookback=10)])
        out = comb.compute(price_matrix)
        assert out.isna().all().all()
