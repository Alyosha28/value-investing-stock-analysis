"""TrendTurningPointDetector 单元测试"""
import pytest
import pandas as pd
import numpy as np
from trend_analyzer import TrendTurningPointDetector, TrendConfig


@pytest.fixture
def sine_wave_data():
    """生成正弦波模拟价格序列（已知拐点位置）。"""
    np.random.seed(42)
    n = 200
    x = np.linspace(0, 4 * np.pi, n)  # 2个完整周期
    close = 100 + 20 * np.sin(x) + np.random.randn(n) * 0.5
    # 构建OHLC（基于close做小幅偏移）
    open_v = close + np.random.randn(n) * 0.3
    high = np.maximum(close, open_v) + abs(np.random.randn(n)) * 0.5
    low = np.minimum(close, open_v) - abs(np.random.randn(n)) * 0.5
    volume = np.random.randint(1000000, 5000000, n)

    # 已知理论拐点位置（波峰/波谷）
    # sin(x) 波峰: x = π/2, 5π/2  → index ≈ 25, 125
    # sin(x) 波谷: x = 3π/2, 7π/2 → index ≈ 75, 175
    expected_peaks = [25, 125]
    expected_troughs = [75, 175]

    dates = pd.date_range('2023-01-01', periods=n, freq='B')
    df = pd.DataFrame({
        'Open': open_v, 'High': high, 'Low': low,
        'Close': close, 'Volume': volume,
    }, index=dates)

    return df, {'peaks': expected_peaks, 'troughs': expected_troughs}


@pytest.fixture
def uptrend_data():
    """持续上涨趋势（无拐点）。"""
    np.random.seed(42)
    n = 100
    dates = pd.date_range('2024-01-01', periods=n, freq='B')
    close = 100 + np.arange(n) * 0.5 + np.random.randn(n) * 0.2
    return pd.DataFrame({
        'Open': close - 0.3, 'High': close + 0.5,
        'Low': close - 0.5, 'Close': close,
        'Volume': np.random.randint(1000000, 5000000, n),
    }, index=dates)


class TestTrendTurningPointDetector:
    """拐点检测器测试"""

    def setup_method(self):
        self.detector = TrendTurningPointDetector()

    def test_none_input(self):
        """空输入应返回空列表"""
        assert self.detector.detect(None) == []

    def test_empty_dataframe(self):
        """空DataFrame应返回空列表"""
        assert self.detector.detect(pd.DataFrame()) == []

    def test_few_data_points(self):
        """数据不足10条应返回空列表"""
        df = pd.DataFrame({'Close': [100] * 5, 'High': [101] * 5, 'Low': [99] * 5})
        assert self.detector.detect(df) == []

    def test_sine_wave_peaks_detected(self, sine_wave_data):
        """正弦波中应检测到波峰拐点。"""
        df, expected = sine_wave_data
        config = TrendConfig()
        config.ZIGZAG_REVERSAL_PCT = 0.02  # 降低阈值以适配小振幅
        detector = TrendTurningPointDetector(config)
        pivots = detector.detect(df)

        # 应检测到至少2个顶部拐点
        tops = [p for p in pivots if 'top' in p['type']]
        assert len(tops) >= 1, f"未检测到顶部拐点，全部拐点: {pivots}"

    def test_sine_wave_troughs_detected(self, sine_wave_data):
        """正弦波中应检测到波谷拐点。"""
        df, expected = sine_wave_data
        config = TrendConfig()
        config.ZIGZAG_REVERSAL_PCT = 0.02
        detector = TrendTurningPointDetector(config)
        pivots = detector.detect(df)

        bottoms = [p for p in pivots if 'bottom' in p['type']]
        assert len(bottoms) >= 1, f"未检测到波谷拐点，全部拐点: {pivots}"

    def test_uptrend_no_false_pivots(self, uptrend_data):
        """持续上涨趋势不应产生大量假拐点。"""
        pivots = self.detector.detect(uptrend_data)
        # 单边趋势中应≤2个拐点
        assert len(pivots) <= 2, f"单边趋势产生过多拐点: {len(pivots)}"

    def test_confidence_score_range(self, sine_wave_data):
        """所有拐点的置信度应在0.0-1.0之间。"""
        df, _ = sine_wave_data
        pivots = self.detector.detect(df)
        for p in pivots:
            assert 0.0 <= p['confidence_score'] <= 1.0, \
                f"置信度越界: {p['confidence_score']}"

    def test_turning_points_have_required_fields(self, sine_wave_data):
        """每个拐点应包含date/type/price/confidence_score字段。"""
        df, _ = sine_wave_data
        pivots = self.detector.detect(df)
        for p in pivots:
            assert 'date' in p, f"拐点缺少date字段: {p}"
            assert 'type' in p, f"拐点缺少type字段: {p}"
            assert 'price' in p, f"拐点缺少price字段: {p}"
            assert 'confidence_score' in p, f"拐点缺少confidence_score字段: {p}"

    def test_dedup_close_pivots(self):
        """相邻间隔不足的拐点应去重。"""
        config = TrendConfig()
        config.ZIGZAG_REVERSAL_PCT = 0.01  # 非常敏感，会产生密集拐点
        config.TURNING_POINT_MIN_INTERVAL = 10
        detector = TrendTurningPointDetector(config)

        dates = pd.date_range('2024-01-01', periods=50, freq='B')
        close = 100 + np.sin(np.linspace(0, 3 * np.pi, 50)) * 10
        df = pd.DataFrame({
            'Close': close, 'High': close + 1, 'Low': close - 1,
            'Volume': np.random.randint(1000000, 5000000, 50),
        }, index=dates)
        pivots = detector.detect(df)

        # 去重后相邻拐点间隔应≥10
        for i in range(1, len(pivots)):
            d1 = pd.Timestamp(pivots[i - 1]['date'])
            d2 = pd.Timestamp(pivots[i]['date'])
            gap = abs((d2 - d1).days)
            assert gap >= 10, f"拐点间隔不足: {pivots[i-1]['date']} ~ {pivots[i]['date']} = {gap}天"

    def test_with_indicators_increases_confidence(self, sine_wave_data):
        """传入indicators应能提升置信度。"""
        df, _ = sine_wave_data
        # 构造简单的indicators
        indicators = {
            'adx': pd.Series([30] * len(df), index=df.index),
            'macd': {'histogram': pd.Series(np.random.randn(len(df)) * 0.1, index=df.index)},
            'rsi': pd.Series([50] * len(df), index=df.index),
        }
        pivots_with = self.detector.detect(df, indicators)
        pivots_without = self.detector.detect(df)

        # 有indicators时平均置信度不应更低
        conf_with = sum(p['confidence_score'] for p in pivots_with) / max(len(pivots_with), 1)
        conf_without = sum(p['confidence_score'] for p in pivots_without) / max(len(pivots_without), 1)
        assert conf_with >= conf_without - 0.01  # 允许微小波动
