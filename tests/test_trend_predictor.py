"""TrendPredictor 单元测试"""
import pytest
import pandas as pd
import numpy as np
from trend_analyzer import TrendPredictor, TrendConfig


@pytest.fixture
def uptrend_data_with_indicators():
    """持续上升趋势的模拟数据 + 对应的indicators。"""
    np.random.seed(42)
    n = 100
    dates = pd.date_range('2024-01-01', periods=n, freq='B')
    close = 100 + np.arange(n) * 0.3 + np.random.randn(n) * 0.2
    return _make_test_data(dates, close), _make_bullish_indicators(dates, n)


@pytest.fixture
def downtrend_data_with_indicators():
    """持续下降趋势的模拟数据 + 对应的indicators。"""
    np.random.seed(42)
    n = 100
    dates = pd.date_range('2024-01-01', periods=n, freq='B')
    close = 130 - np.arange(n) * 0.3 + np.random.randn(n) * 0.2
    return _make_test_data(dates, close), _make_bearish_indicators(dates, n)


@pytest.fixture
def sideways_data_with_indicators():
    """横盘趋势的模拟数据。"""
    np.random.seed(42)
    n = 100
    dates = pd.date_range('2024-01-01', periods=n, freq='B')
    close = 100 + np.random.randn(n) * 2
    return _make_test_data(dates, close), _make_neutral_indicators(dates, n)


def _make_test_data(dates, close):
    return pd.DataFrame({
        'Open': close - 0.3, 'High': close + 0.5,
        'Low': close - 0.5, 'Close': close,
        'Volume': np.random.randint(1000000, 5000000, len(dates)),
    }, index=dates)


def _make_bullish_indicators(dates, n):
    """看涨的技术指标。"""
    return {
        'ma': {'ma20': pd.Series(100 + np.arange(n) * 0.28, index=dates)},
        'macd': {'histogram': pd.Series(np.linspace(-0.5, 1.5, n), index=dates)},
        'bollinger': {
            'upper': pd.Series(105 + np.arange(n) * 0.3 + 3, index=dates),
            'lower': pd.Series(95 + np.arange(n) * 0.3 - 3, index=dates),
        },
        'support_resistance': {
            'support_level': 95, 'resistance_level': 120,
        },
        'adx': pd.Series([35] * n, index=dates),
    }


def _make_bearish_indicators(dates, n):
    """看跌的技术指标。"""
    return {
        'ma': {'ma20': pd.Series(130 - np.arange(n) * 0.28, index=dates)},
        'macd': {'histogram': pd.Series(np.linspace(0.5, -1.5, n), index=dates)},
        'bollinger': {
            'upper': pd.Series(135 - np.arange(n) * 0.3 + 3, index=dates),
            'lower': pd.Series(125 - np.arange(n) * 0.3 - 3, index=dates),
        },
        'support_resistance': {
            'support_level': 80, 'resistance_level': 95,
        },
        'adx': pd.Series([35] * n, index=dates),
    }


def _make_neutral_indicators(dates, n):
    """中性/震荡的技术指标——MA完全水平，MACD柱在零轴附近来回摆动，布林带宽度稳定。"""
    return {
        'ma': {'ma20': pd.Series(np.full(n, 100.0), index=dates)},  # 完全水平
        'macd': {'histogram': pd.Series(np.sin(np.linspace(0, 4*np.pi, n)) * 0.1, index=dates)},  # 零轴摆动
        'bollinger': {
            'upper': pd.Series(np.full(n, 105.0), index=dates),  # 带宽恒定
            'lower': pd.Series(np.full(n, 95.0), index=dates),
        },
        'support_resistance': {
            'support_level': 95, 'resistance_level': 105,
        },
        'adx': pd.Series([15] * n, index=dates),
    }


class TestTrendPredictor:
    """趋势预测器测试"""

    def setup_method(self):
        self.predictor = TrendPredictor()

    def test_none_data(self):
        """空数据应返回默认预测。"""
        result = self.predictor.predict(None, {})
        assert result['direction'] == 'sideways'
        assert result['confidence_score'] == 0.0
        assert 'reason' in result

    def test_insufficient_data(self):
        """数据不足20条应返回默认预测。"""
        dates = pd.date_range('2024-01-01', periods=5, freq='B')
        data = pd.DataFrame({'Close': [100] * 5, 'High': [101] * 5, 'Low': [99] * 5}, index=dates)
        result = self.predictor.predict(data, {})
        assert result['direction'] == 'sideways'

    def test_uptrend_direction(self, uptrend_data_with_indicators):
        """上升趋势中预测方向应为up。"""
        data, indicators = uptrend_data_with_indicators
        result = self.predictor.predict(data, indicators)
        assert result['direction'] == 'up', \
            f"上升趋势预测应为up，实际: {result['direction']}, 理由: {result['reason']}"

    def test_downtrend_direction(self, downtrend_data_with_indicators):
        """下降趋势中预测方向应为down。"""
        data, indicators = downtrend_data_with_indicators
        result = self.predictor.predict(data, indicators)
        assert result['direction'] == 'down', \
            f"下降趋势预测应为down，实际: {result['direction']}, 理由: {result['reason']}"

    def test_sideways_data(self, sideways_data_with_indicators):
        """横盘行情中置信度应较低（信号冲突时不应给出高确定性判断）。"""
        data, indicators = sideways_data_with_indicators
        result = self.predictor.predict(data, indicators)
        # 横盘/随机数据中置信度应≤0.6（不强推方向）
        assert result['confidence_score'] <= 0.6, \
            f"横盘行情置信度过高: {result['confidence_score']} (方向: {result['direction']})"

    def test_confidence_range(self, uptrend_data_with_indicators):
        """置信度应在0.0-1.0范围。"""
        data, indicators = uptrend_data_with_indicators
        result = self.predictor.predict(data, indicators)
        assert 0.0 <= result['confidence_score'] <= 1.0, \
            f"置信度越界: {result['confidence_score']}"

    def test_target_price_positive(self, uptrend_data_with_indicators):
        """目标价应为正数。"""
        data, indicators = uptrend_data_with_indicators
        result = self.predictor.predict(data, indicators)
        assert result['target_upper'] > 0
        assert result['target_lower'] > 0

    def test_target_upper_greater_than_lower(self, uptrend_data_with_indicators):
        """目标区间上界应大于下界。"""
        data, indicators = uptrend_data_with_indicators
        result = self.predictor.predict(data, indicators)
        assert result['target_upper'] >= result['target_lower'], \
            f"上界({result['target_upper']}) < 下界({result['target_lower']})"

    def test_horizon_bars(self, uptrend_data_with_indicators):
        """预测周期数应等于配置值。"""
        data, indicators = uptrend_data_with_indicators
        result = self.predictor.predict(data, indicators)
        assert result['horizon_bars'] == TrendConfig.PREDICTION_HORIZON_BARS

    def test_reason_not_empty(self, uptrend_data_with_indicators):
        """理由描述不应为空。"""
        data, indicators = uptrend_data_with_indicators
        result = self.predictor.predict(data, indicators)
        assert len(result['reason']) > 0

    def test_atr_calculation(self):
        """ATR计算应返回正值。"""
        dates = pd.date_range('2024-01-01', periods=20, freq='B')
        data = pd.DataFrame({
            'High': 100 + np.random.randn(20) * 2,
            'Low': 100 + np.random.randn(20) * 2 - 1,
            'Close': 100 + np.random.randn(20),
        }, index=dates)
        # ATR应>0
        atr = self.predictor._calc_atr(data)
        assert atr > 0, f"ATR应为正值，实际: {atr}"
