"""TimeFrameAggregator 单元测试"""
import pytest
import pandas as pd
import numpy as np
from trend_analyzer import TimeFrameAggregator


@pytest.fixture
def daily_data():
    """生成60个交易日的模拟日K数据（约3个自然月）。"""
    dates = pd.date_range('2024-01-02', periods=60, freq='B')  # 仅交易日
    np.random.seed(42)
    base = 100.0
    closes = base + np.cumsum(np.random.randn(60) * 0.5)
    opens = closes + np.random.randn(60) * 0.3
    highs = np.maximum(opens, closes) + abs(np.random.randn(60)) * 0.5
    lows = np.minimum(opens, closes) - abs(np.random.randn(60)) * 0.5
    volumes = np.random.randint(1000000, 5000000, 60)

    return pd.DataFrame({
        'Open': opens, 'High': highs, 'Low': lows, 'Close': closes,
        'Volume': volumes,
    }, index=dates)


@pytest.fixture
def empty_data():
    return pd.DataFrame()


class TestTimeFrameAggregator:
    """多周期聚合器测试"""

    def setup_method(self):
        self.agg = TimeFrameAggregator()

    def test_none_input(self):
        """空输入应返回None"""
        assert self.agg.aggregate(None, 'weekly') is None

    def test_empty_input(self, empty_data):
        """空DataFrame应返回None"""
        assert self.agg.aggregate(empty_data, 'weekly') is None

    def test_unknown_period(self, daily_data):
        """未知周期应返回None"""
        assert self.agg.aggregate(daily_data, 'yearly') is None

    def test_daily_returns_copy(self, daily_data):
        """daily周期应返回副本而非原数据"""
        result = self.agg.aggregate(daily_data, 'daily')
        assert result is not None
        assert len(result) == len(daily_data)
        # 修改结果不应影响原数据
        result.iloc[0, 0] = 999
        assert daily_data.iloc[0, 0] != 999

    def test_weekly_ohlc_correctness(self, daily_data):
        """周K的OHLC应正确聚合：Open=首周首日开盘，High=周内最高，Low=周内最低，Close=周末收盘，Volume=周内总和"""
        result = self.agg.aggregate(daily_data, 'weekly')
        assert result is not None
        assert len(result) > 0

        # W-FRI 分组的标签为每周五。第一组包含从起始到第一个周五的所有数据。
        first_friday = result.index[0]
        week1 = daily_data[daily_data.index <= first_friday]
        assert len(week1) > 0, f"第一周无数据（截止 {first_friday}）"

        expected_open = week1['Open'].iloc[0]
        expected_high = week1['High'].max()
        expected_low = week1['Low'].min()
        expected_close = week1['Close'].iloc[-1]
        expected_volume = week1['Volume'].sum()

        assert result['Open'].iloc[0] == pytest.approx(expected_open)
        assert result['High'].iloc[0] == pytest.approx(expected_high)
        assert result['Low'].iloc[0] == pytest.approx(expected_low)
        assert result['Close'].iloc[0] == pytest.approx(expected_close)
        assert result['Volume'].iloc[0] == pytest.approx(expected_volume)

    def test_weekly_bar_count(self, daily_data):
        """60个交易日应产生约12根周K线。"""
        result = self.agg.aggregate(daily_data, 'weekly')
        assert result is not None
        # 60交易日 ≈ 12周
        assert 10 <= len(result) <= 15

    def test_monthly_bar_count(self, daily_data):
        """60个交易日应产生3根月K线。"""
        result = self.agg.aggregate(daily_data, 'monthly')
        assert result is not None
        assert len(result) == 3

    def test_monthly_ohlc_correctness(self, daily_data):
        """月K的OHLC应正确聚合。"""
        result = self.agg.aggregate(daily_data, 'monthly')
        assert result is not None

        month1 = daily_data[daily_data.index.month == 1]
        if not month1.empty:
            expected_open = month1['Open'].iloc[0]
            expected_close = month1['Close'].iloc[-1]
            expected_high = month1['High'].max()
            expected_low = month1['Low'].min()

            assert result['Open'].iloc[0] == pytest.approx(expected_open)
            assert result['Close'].iloc[0] == pytest.approx(expected_close)
            assert result['High'].iloc[0] == pytest.approx(expected_high)
            assert result['Low'].iloc[0] == pytest.approx(expected_low)

    def test_missing_columns(self, daily_data):
        """缺少Open列时应返回None。"""
        bad_data = daily_data.drop(columns=['Open'])
        assert self.agg.aggregate(bad_data, 'weekly') is None

    def test_amount_column_aggregated(self, daily_data):
        """如有Amount列，应一并聚合。"""
        daily_data['Amount'] = daily_data['Volume'] * daily_data['Close']
        result = self.agg.aggregate(daily_data, 'weekly')
        assert result is not None
        assert 'Amount' in result.columns
        # 第一周：从起始到第一个周五
        first_friday = result.index[0]
        week1 = daily_data[daily_data.index <= first_friday]
        expected_amount = week1['Amount'].sum()
        assert result['Amount'].iloc[0] == pytest.approx(expected_amount)

    def test_few_data_points(self):
        """仅有2个交易日时，周K仍应产生1根K线。"""
        dates = pd.date_range('2024-01-02', periods=2, freq='B')
        data = pd.DataFrame({
            'Open': [100, 101], 'High': [101, 102], 'Low': [99, 100],
            'Close': [101, 102], 'Volume': [1000, 1000],
        }, index=dates)
        result = self.agg.aggregate(data, 'weekly')
        assert result is not None
        assert len(result) == 1

    def test_sort_index_before_resample(self, daily_data):
        """即使数据索引无序，聚合也应正确。"""
        shuffled = daily_data.sample(frac=1, random_state=42)
        result = self.agg.aggregate(shuffled, 'weekly')
        assert result is not None
        # 结果应与未打乱时一致
        expected = self.agg.aggregate(daily_data, 'weekly')
        pd.testing.assert_frame_equal(result, expected)
