"""
趋势分析模块 — 多周期K线分析、拐点检测、趋势预测、信号综合

核心类：
  - TimeFrameAggregator: 日K→周K/月K聚合
  - TrendTurningPointDetector: Zigzag拐点检测（百分比回撤主算法 + 局部极值辅助验证）
  - TrendPredictor: 加权投票趋势预测 + 冲突仲裁
  - TrendAnalyzer: 主协调器（统筹数据获取、分析、图表产出）
"""

import os
import json
import time
import logging
from datetime import datetime
from typing import Dict, List, Optional, Any, Tuple
from functools import lru_cache

import pandas as pd
import numpy as np

from config import TrendConfig
from logger_config import logger

# ═══════════════════════════════════════════════════════════════
# 1. 多周期数据聚合器
# ═══════════════════════════════════════════════════════════════

class TimeFrameAggregator:
    """将日K OHLCV数据聚合为周K或月K。"""

    _PERIOD_RULES = {
        'weekly': 'W-FRI',
        'monthly': 'ME',
    }

    def aggregate(self, df: pd.DataFrame, target_period: str) -> Optional[pd.DataFrame]:
        """聚合到目标周期。target_period: 'weekly' 或 'monthly'"""
        if df is None or df.empty:
            return None
        if target_period == 'daily':
            return df.copy()

        rule = self._PERIOD_RULES.get(target_period)
        if rule is None:
            logger.warning(f"[TimeFrameAggregator] 未知周期: {target_period}")
            return None

        required = {'Open', 'High', 'Low', 'Close', 'Volume'}
        missing = required - set(df.columns)
        if missing:
            logger.warning(f"[TimeFrameAggregator] 缺少列: {missing}")
            return None

        try:
            agg = df.resample(rule).agg({
                'Open': 'first',
                'High': 'max',
                'Low': 'min',
                'Close': 'last',
                'Volume': 'sum',
            })
            if 'Amount' in df.columns:
                amount = df['Amount'].resample(rule).sum()
                agg['Amount'] = amount

            agg = agg.dropna(subset=['Open', 'Close'])
            if agg.empty:
                logger.warning(f"[TimeFrameAggregator] 聚合结果为空")
                return None

            logger.info(f"[TimeFrameAggregator] {target_period}: {len(df)}日K → {len(agg)}K线")
            return agg
        except Exception as e:
            logger.error(f"[TimeFrameAggregator] 聚合失败: {e}")
            return None


# ═══════════════════════════════════════════════════════════════
# 2. Zigzag拐点检测器（双算法融合）
# ═══════════════════════════════════════════════════════════════

class TrendTurningPointDetector:
    """
    趋势拐点检测 — 双算法融合

    主算法：百分比回撤 Zigzag — 从最近拐点起跟踪价格波动，
            反向回撤 ≥ ZIGZAG_REVERSAL_PCT 时确认新拐点。
    辅助验证：局部极值检测 — 验证拐点在其邻域内确为极值。
    """

    def __init__(self, config=TrendConfig):
        self.reversal_pct = config.ZIGZAG_REVERSAL_PCT
        self.left_bars = config.ZIGZAG_LEFT_BARS
        self.right_bars = config.ZIGZAG_RIGHT_BARS
        self.min_interval = config.TURNING_POINT_MIN_INTERVAL
        self.volume_ratio = config.PIVOT_CONFIRM_VOLUME_RATIO

    def detect(self, data: pd.DataFrame, indicators: Dict = None) -> List[Dict]:
        """
        检测趋势拐点。

        参数:
            data: OHLCV DataFrame (DatetimeIndex)
            indicators: TechnicalAnalyzer 输出的 indicators dict（用于背离检测和ADX）

        返回:
            [{'date': str, 'type': str, 'price': float, 'confidence_score': float}, ...]
        """
        if data is None or len(data) < 10:
            return []

        close = data['Close'].values
        high = data['High'].values if 'High' in data.columns else close
        low = data['Low'].values if 'Low' in data.columns else close
        dates = data.index

        # 百分比回撤Zigzag主算法
        zigzag_pivots = self._zigzag_percentage(close, dates)
        if not zigzag_pivots:
            return []

        # 局部极值辅助验证
        confirmed_pivots = []
        for pivot in zigzag_pivots:
            idx = pivot['index']
            left = max(0, idx - self.left_bars)
            right = min(len(close) - 1, idx + self.right_bars)

            is_peak = (high[idx] == high[left:right + 1].max())
            is_trough = (low[idx] == low[left:right + 1].min())
            local_extreme_confirmed = is_peak or is_trough

            # 置信度评分
            confidence = self._compute_confidence(
                pivot, data, indicators, local_extreme_confirmed
            )

            confirmed_pivots.append({
                'date': str(dates[idx].date()) if hasattr(dates[idx], 'date') else str(dates[idx]),
                'type': pivot['type'],
                'price': float(close[idx]),
                'index': idx,
                'confidence_score': round(confidence, 2),
            })

        # 去重：相邻拐点间隔不足
        deduped = self._dedup(confirmed_pivots)

        # 移除内部 index 字段
        for p in deduped:
            del p['index']
        return deduped

    def _zigzag_percentage(self, close: np.ndarray, dates) -> List[Dict]:
        """百分比回撤Zigzag主算法。"""
        pivots = []
        direction = None  # None: 初始, 1: 追踪高, -1: 追踪低
        extreme_idx = 0
        extreme_price = close[0]

        for i in range(1, len(close)):
            if direction is None:
                direction = 1 if close[i] >= close[i - 1] else -1
                extreme_idx = i
                extreme_price = close[i]
                continue

            if direction == 1:
                if close[i] >= extreme_price:
                    extreme_idx = i
                    extreme_price = close[i]
                else:
                    retrace = (extreme_price - close[i]) / extreme_price
                    if retrace >= self.reversal_pct:
                        pivots.append({
                            'index': extreme_idx,
                            'type': 'major_top',
                            'price': extreme_price,
                        })
                        direction = -1
                        extreme_idx = i
                        extreme_price = close[i]
            else:
                if close[i] <= extreme_price:
                    extreme_idx = i
                    extreme_price = close[i]
                else:
                    retrace = (close[i] - extreme_price) / extreme_price
                    if retrace >= self.reversal_pct:
                        pivots.append({
                            'index': extreme_idx,
                            'type': 'major_bottom',
                            'price': extreme_price,
                        })
                        direction = 1
                        extreme_idx = i
                        extreme_price = close[i]

        return pivots

    def _compute_confidence(self, pivot: Dict, data: pd.DataFrame,
                            indicators: Dict, local_extreme: bool) -> float:
        """计算拐点置信度 (0.0-1.0)。"""
        score = 0.0
        idx = pivot['index']
        close = data['Close'].values

        # 1) 局部极值验证 (±20分)
        if local_extreme:
            score += 20

        # 2) 成交量确认 (±30分)
        if 'Volume' in data.columns:
            vol = data['Volume'].values
            avg_vol = np.mean(vol[max(0, idx - 20):idx + 1]) if idx >= 20 else np.mean(vol[:idx + 1])
            if avg_vol > 0 and vol[idx] >= avg_vol * self.volume_ratio:
                score += 30
            elif avg_vol > 0 and vol[idx] >= avg_vol:
                score += 15

        # 3) ADX趋势强度 (±25分)
        if indicators and 'adx' in indicators:
            adx = indicators['adx']
            if isinstance(adx, pd.Series) and idx < len(adx):
                adx_val = adx.iloc[idx] if hasattr(adx, 'iloc') else adx[idx]
                if adx_val >= 40:
                    score += 25
                elif adx_val >= 25:
                    score += 15
                elif adx_val >= 15:
                    score += 5

        # 4) MACD/RSI背离 (±25分)
        divergence_bonus = self._check_divergence(pivot, data, indicators)
        score += divergence_bonus

        return min(100.0, score) / 100.0

    def _check_divergence(self, pivot: Dict, data: pd.DataFrame, indicators: Dict) -> float:
        """检测MACD/RSI背离。返回加分值 (0-25)。"""
        bonus = 0.0
        idx = pivot['index']
        close = data['Close'].values
        is_top = pivot['type'] == 'major_top'

        # 获取MACD柱
        if indicators and 'macd' in indicators:
            macd = indicators['macd']
            histogram = macd.get('histogram')
            if histogram is not None and isinstance(histogram, pd.Series) and idx < len(histogram):
                hist_vals = histogram.values
                lookback = min(10, idx)
                if lookback >= 3:
                    if is_top:
                        # 价格新高但MACD柱更低 → 顶背离
                        price_higher = close[idx] > max(close[idx - lookback:idx])
                        hist_lower = hist_vals[idx] < min(hist_vals[idx - lookback:idx])
                        if price_higher and hist_lower:
                            bonus += 15
                    else:
                        # 价格新低但MACD柱更高 → 底背离
                        price_lower = close[idx] < min(close[idx - lookback:idx])
                        hist_higher = hist_vals[idx] > max(hist_vals[idx - lookback:idx])
                        if price_lower and hist_higher:
                            bonus += 15

        # RSI背离
        if indicators and 'rsi' in indicators:
            rsi = indicators['rsi']
            if isinstance(rsi, pd.Series) and idx < len(rsi):
                rsi_vals = rsi.values
                lookback = min(10, idx)
                if lookback >= 3:
                    if is_top:
                        price_higher = close[idx] > max(close[idx - lookback:idx])
                        rsi_lower = rsi_vals[idx] < min(rsi_vals[idx - lookback:idx])
                        if price_higher and rsi_lower:
                            bonus += 10
                    else:
                        price_lower = close[idx] < min(close[idx - lookback:idx])
                        rsi_higher = rsi_vals[idx] > max(rsi_vals[idx - lookback:idx])
                        if price_lower and rsi_higher:
                            bonus += 10

        return min(bonus, 25)

    def _dedup(self, pivots: List[Dict]) -> List[Dict]:
        """相邻拐点去重：间隔不足则保留置信度高的。"""
        if len(pivots) <= 1:
            return pivots

        result = [pivots[0]]
        for p in pivots[1:]:
            last = result[-1]
            gap = abs(p['index'] - last['index'])
            if gap < self.min_interval:
                if p['confidence_score'] > last['confidence_score']:
                    result[-1] = p
            else:
                result.append(p)
        return result


# ═══════════════════════════════════════════════════════════════
# 3. 趋势预测器（加权投票 + 冲突仲裁）
# ═══════════════════════════════════════════════════════════════

class TrendPredictor:
    """
    统计趋势预测器。非ML模型。

    4个独立信号加权投票：
      - MA斜率 (30%)
      - MACD柱动量 (30%)
      - 布林带宽度 (20%)
      - 支撑/阻力距离 (20%)
    """

    def __init__(self, config=TrendConfig):
        self.horizon = config.PREDICTION_HORIZON_BARS
        self.weights = config.SIGNAL_WEIGHTS

    def predict(self, data: pd.DataFrame, indicators: Dict) -> Dict:
        """
        执行趋势预测。

        返回:
            {'direction': str, 'confidence_score': float,
             'target_upper': float, 'target_lower': float,
             'horizon_bars': int, 'reason': str}
        """
        if data is None or len(data) < 20:
            return self._default_prediction('数据不足')

        close = data['Close'].values
        latest_price = float(close[-1])

        # 4个独立信号
        signals = {}
        signals['ma_slope'] = self._signal_ma_slope(data, indicators)
        signals['macd_histogram'] = self._signal_macd_histogram(data, indicators)
        signals['bollinger_width'] = self._signal_bollinger_width(data, indicators)
        signals['sr_distance'] = self._signal_sr_distance(data, indicators)

        # 加权投票
        weighted_score = 0.0
        vote_counts = {'up': 0, 'down': 0, 'sideways': 0}
        for name, s in signals.items():
            weight = self.weights.get(name, 0.25)
            if s['direction'] == 'up':
                weighted_score += weight * 1.0
                vote_counts['up'] += 1
            elif s['direction'] == 'down':
                weighted_score += weight * (-1.0)
                vote_counts['down'] += 1
            else:
                vote_counts['sideways'] += 1

        # 仲裁
        direction, confidence = self._arbitrate(weighted_score, vote_counts)

        # 目标价位
        atr_val = self._calc_atr(data)
        target_upper = latest_price * (1 + atr_val * 2 / latest_price)
        target_lower = latest_price * (1 - atr_val * 2 / latest_price)

        # 理由
        reason = self._build_reason(direction, signals, vote_counts)

        return {
            'direction': direction,
            'confidence_score': round(confidence, 2),
            'target_upper': round(target_upper, 2),
            'target_lower': round(target_lower, 2),
            'horizon_bars': self.horizon,
            'reason': reason,
        }

    def _signal_ma_slope(self, data: pd.DataFrame, indicators: Dict) -> Dict:
        """MA斜率信号。"""
        ma_key = 'ma20'
        ma_data = indicators.get('ma', {})
        ma = None
        for k in ma_data:
            if '20' in k:
                ma = ma_data[k]
                break
        if ma is None or len(ma) < 5:
            return {'direction': 'sideways', 'strength': 0}

        vals = ma.values[-5:]
        if len(vals) < 2:
            return {'direction': 'sideways', 'strength': 0}

        slope = (vals[-1] - vals[0]) / vals[0] if vals[0] > 0 else 0
        if slope > 0.01:
            return {'direction': 'up', 'strength': min(abs(slope) * 100, 1.0)}
        elif slope < -0.01:
            return {'direction': 'down', 'strength': min(abs(slope) * 100, 1.0)}
        return {'direction': 'sideways', 'strength': 0}

    def _signal_macd_histogram(self, data: pd.DataFrame, indicators: Dict) -> Dict:
        """MACD柱动量信号。"""
        macd = indicators.get('macd', {})
        hist = macd.get('histogram')
        if hist is None or len(hist) < 5:
            return {'direction': 'sideways', 'strength': 0}

        vals = hist.values[-5:]
        if vals[-1] > vals[0] and vals[-1] > 0:
            return {'direction': 'up', 'strength': min(abs(vals[-1]) * 10, 1.0)}
        elif vals[-1] < vals[0] and vals[-1] < 0:
            return {'direction': 'down', 'strength': min(abs(vals[-1]) * 10, 1.0)}
        elif vals[-1] > vals[0]:
            # 柱收缩中
            return {'direction': 'up', 'strength': 0.3}
        elif vals[-1] < vals[0]:
            return {'direction': 'down', 'strength': 0.3}
        return {'direction': 'sideways', 'strength': 0}

    def _signal_bollinger_width(self, data: pd.DataFrame, indicators: Dict) -> Dict:
        """布林带宽度轨迹信号。"""
        bb = indicators.get('bollinger', {})
        upper = bb.get('upper')
        lower = bb.get('lower')
        if upper is None or lower is None or len(upper) < 10:
            return {'direction': 'sideways', 'strength': 0}

        width = (upper - lower).values
        recent_width = np.mean(width[-5:])
        prev_width = np.mean(width[-10:-5]) if len(width) >= 10 else recent_width

        if prev_width > 0:
            width_trend = (recent_width - prev_width) / prev_width
            if width_trend > 0.05:
                return {'direction': 'up', 'strength': min(width_trend * 3, 1.0)}
            elif width_trend < -0.05:
                # 带宽收缩预示变盘
                return {'direction': 'sideways', 'strength': 0.5}
        return {'direction': 'sideways', 'strength': 0}

    def _signal_sr_distance(self, data: pd.DataFrame, indicators: Dict) -> Dict:
        """支撑/阻力距离信号（ATR归一化）。"""
        sr = indicators.get('support_resistance', {})
        close = data['Close'].values[-1]
        atr_val = self._calc_atr(data)
        if atr_val <= 0:
            return {'direction': 'sideways', 'strength': 0}

        resistance = sr.get('resistance_level')
        support = sr.get('support_level')
        if resistance and support:
            dist_to_r = abs(resistance - close) / atr_val
            dist_to_s = abs(close - support) / atr_val
            # 距阻力很近 → 可能回调
            if dist_to_r < 1.0:
                return {'direction': 'down', 'strength': max(0, 1 - dist_to_r)}
            # 距支撑很近 → 可能反弹
            if dist_to_s < 1.0:
                return {'direction': 'up', 'strength': max(0, 1 - dist_to_s)}
        return {'direction': 'sideways', 'strength': 0}

    def _calc_atr(self, data: pd.DataFrame, period: int = 14) -> float:
        """计算ATR（Average True Range）。"""
        high = data['High'].values if 'High' in data.columns else data['Close'].values
        low = data['Low'].values if 'Low' in data.columns else data['Close'].values
        close = data['Close'].values

        if len(close) < period + 1:
            return float(np.std(close)) if len(close) > 1 else 0

        tr = np.zeros(len(close))
        tr[0] = high[0] - low[0]
        for i in range(1, len(close)):
            tr[i] = max(
                high[i] - low[i],
                abs(high[i] - close[i - 1]),
                abs(low[i] - close[i - 1]),
            )
        return float(np.mean(tr[-period:]))

    def _arbitrate(self, weighted_score: float, vote_counts: Dict) -> Tuple[str, float]:
        """信号仲裁。"""
        total_votes = sum(vote_counts.values())
        tie_votes = vote_counts.get('up', 0) == vote_counts.get('down', 0)

        # 平局（2 up : 2 down）
        if tie_votes and vote_counts.get('up', 0) + vote_counts.get('down', 0) == 4:
            return 'sideways', 0.5

        # 加权得分判定
        if weighted_score > 0.2:
            confidence = min(abs(weighted_score) + 0.2, 1.0)
            # 4:0 全票 → 置信度加成
            if vote_counts.get('up', 0) == 4 and vote_counts.get('down', 0) == 0:
                confidence = min(confidence * 1.2, 1.0)
            return 'up', confidence
        elif weighted_score < -0.2:
            confidence = min(abs(weighted_score) + 0.2, 1.0)
            if vote_counts.get('down', 0) == 4 and vote_counts.get('up', 0) == 0:
                confidence = min(confidence * 1.2, 1.0)
            return 'down', confidence
        else:
            return 'sideways', 0.3

    def _build_reason(self, direction: str, signals: Dict, vote_counts: Dict) -> str:
        """生成可读的预测理由（中文）。"""
        dir_map = {'up': '上涨', 'down': '下跌', 'sideways': '震荡'}
        parts = [f"综合{dir_map.get(direction, '未知')}信号"]
        parts.append(f"(多:{vote_counts.get('up',0)} 空:{vote_counts.get('down',0)} 平:{vote_counts.get('sideways',0)})")

        details = []
        for name, s in signals.items():
            label = {
                'ma_slope': 'MA斜率', 'macd_histogram': 'MACD柱',
                'bollinger_width': '布林带宽度', 'sr_distance': '支撑/阻力',
            }.get(name, name)
            d = {'up': '看涨', 'down': '看跌', 'sideways': '中性'}.get(s['direction'], '中性')
            details.append(f"{label}:{d}")
        parts.append(" | ".join(details))
        return " ".join(parts)

    def _default_prediction(self, reason: str) -> Dict:
        return {
            'direction': 'sideways',
            'confidence_score': 0.0,
            'target_upper': 0.0,
            'target_lower': 0.0,
            'horizon_bars': self.horizon,
            'reason': reason,
        }


# ═══════════════════════════════════════════════════════════════
# 4. 多周期信号对齐
# ═══════════════════════════════════════════════════════════════

class MultiTimeframeAligner:
    """对齐日/周/月K信号并输出共振强度。"""

    def __init__(self, config=TrendConfig):
        self.strong_threshold = config.MULTI_TF_STRONG_THRESHOLD
        self.weak_threshold = config.MULTI_TF_WEAK_THRESHOLD
        self.conflict_multiplier = config.MULTI_TF_CONFLICT_MULTIPLIER

    def align(self, tf_results: Dict[str, Dict]) -> Dict:
        """
        对齐多周期信号。

        参数:
            tf_results: {'daily': {...}, 'weekly': {...}, 'monthly': {...}}
                        每个值含 'direction' 和 'score'

        返回:
            {'alignment': {'daily': str, 'weekly': str, 'monthly': str,
                           'consensus_count': int, 'signal_strength': str},
             'details': [...]}
        """
        directions = {}
        details = []
        for period, result in tf_results.items():
            if result and 'trend_summary' in result:
                d = result['trend_summary'].get('direction', 'sideways')
                directions[period] = d
                details.append({
                    'period': period,
                    'direction': d,
                    'score': result['trend_summary'].get('score', 50),
                    'ma_alignment': self._classify_ma_alignment(result),
                    'adx': self._get_adx(result),
                })

        if not directions:
            return {
                'alignment': {'signal_strength': 'unknown', 'consensus_count': 0},
                'details': [],
            }

        # 统计同向数量（排除sideways）
        non_sideways = {k: v for k, v in directions.items() if v != 'sideways'}
        if not non_sideways:
            consensus_count = 0
            signal_strength = 'conflict'
        else:
            majority_dir = max(set(non_sideways.values()), key=list(non_sideways.values()).count)
            consensus_count = sum(1 for v in non_sideways.values() if v == majority_dir)

            if consensus_count >= self.strong_threshold:
                signal_strength = 'strong'
            elif consensus_count >= self.weak_threshold:
                signal_strength = 'moderate'
            elif consensus_count == 1:
                signal_strength = 'weak'
            else:
                signal_strength = 'conflict'

        # 补齐未获取到的周期
        for p in ['daily', 'weekly', 'monthly']:
            if p not in directions:
                directions[p] = 'unknown'

        return {
            'alignment': {
                'daily': directions.get('daily', 'unknown'),
                'weekly': directions.get('weekly', 'unknown'),
                'monthly': directions.get('monthly', 'unknown'),
                'consensus_count': consensus_count,
                'signal_strength': signal_strength,
            },
            'details': details,
        }

    def _classify_ma_alignment(self, result: Dict) -> str:
        """从trend_summary归类MA排列状态。"""
        regime = result.get('trend_summary', {}).get('regime', '')
        if '多头' in regime or 'uptrend' in regime:
            return '多头排列'
        if '空头' in regime or 'downtrend' in regime:
            return '空头排列'
        return '交叉缠绕'

    def _get_adx(self, result: Dict) -> float:
        """提取ADX值。"""
        indicators = result.get('indicators', {})
        adx = indicators.get('adx', {})
        if isinstance(adx, pd.Series) and not adx.empty:
            return round(float(adx.iloc[-1]), 1)
        if isinstance(adx, dict):
            return adx.get('value', 0)
        return 0


# ═══════════════════════════════════════════════════════════════
# 5. 主协调器
# ═══════════════════════════════════════════════════════════════

class TrendAnalyzer:
    """
    趋势分析主协调器。

    统筹 K线数据获取 → 多周期聚合 → 指标计算(复用TechnicalAnalyzer)
    → 拐点检测 → 趋势预测 → 多周期对齐 → 图表生成 → 结果缓存
    """

    def __init__(self, data_hub=None, tech_analyzer=None, chart_engine=None):
        self.data_hub = data_hub
        self._local_hub = None
        self.tech_analyzer = tech_analyzer
        self._local_tech = None
        self.chart_engine = chart_engine

        self.aggregator = TimeFrameAggregator()
        self.turning_point_detector = TrendTurningPointDetector()
        self.predictor = TrendPredictor()
        self.aligner = MultiTimeframeAligner()

    def _get_hub(self):
        if self.data_hub is None:
            if self._local_hub is None:
                from data_hub import DataHub
                self._local_hub = DataHub.get_instance()
            return self._local_hub
        return self.data_hub

    def _get_tech(self):
        if self.tech_analyzer is None:
            if self._local_tech is None:
                from technical_analyzer import TechnicalAnalyzer
                self._local_tech = TechnicalAnalyzer()
            return self._local_tech
        return self.tech_analyzer

    def analyze(self, stock_code: str, period: str = 'daily',
                start_date: str = None, end_date: str = None,
                output_format: str = 'png') -> Dict:
        """
        执行完整的趋势分析流程。

        参数:
            stock_code: A股代码
            period: 'daily' | 'weekly' | 'monthly'
            start_date: 起始日期 YYYY-MM-DD
            end_date: 结束日期 YYYY-MM-DD
            output_format: 'png' | 'html'

        返回:
            综合结果 dict
        """
        logger.info(f"[TrendAnalyzer] 开始分析 {stock_code}, 周期={period}")

        # 1. 数据获取
        hub = self._get_hub()
        df = self._get_kline_data(hub, stock_code, period, start_date, end_date)
        if df is None or df.empty:
            return self._error_result(stock_code, '无法获取K线数据')

        # 2. 基础数据（股票名称）
        stock_data = hub.get_stock_data(stock_code)
        stock_name = '未知'
        if stock_data:
            info = stock_data.get('info', {})
            stock_name = info.get('stock_name', stock_code)

        # 3. 计算技术指标
        tech_input = {
            'historical': df,
            'info': (stock_data or {}).get('info', {}),
            'financial': (stock_data or {}).get('financial', {}),
        }
        tech_result = self._get_tech().analyze(tech_input)
        if not tech_result or 'indicators' not in tech_result:
            return self._error_result(stock_code, '技术指标计算失败')

        # 4. 拐点检测
        turning_points = self.turning_point_detector.detect(df, tech_result.get('indicators'))

        # 5. 趋势预测
        prediction = self.predictor.predict(df, tech_result.get('indicators', {}))

        # 6. 多周期分析（如周期为daily仍尝试获取周K/月K）
        multi_tf = self._analyze_multi_timeframe(hub, stock_code, stock_data,
                                                  start_date, end_date, tech_result)

        # 7. 生成图表
        chart_path = self._generate_chart(df, tech_result, turning_points, prediction,
                                           stock_name, stock_code, period, output_format)

        # 8. 构建趋势摘要
        summary = self._build_trend_summary(tech_result)

        # 9. 综合结果
        result = {
            'stock_code': stock_code,
            'stock_name': stock_name,
            'period': period,
            'analysis_date': datetime.now().strftime('%Y-%m-%d'),
            'current_price': float(df['Close'].iloc[-1]),
            'trend_summary': summary,
            'multi_timeframe': multi_tf,
            'indicators': tech_result.get('indicators', {}),
            'turning_points': turning_points,
            'prediction': prediction,
            'signals': self._consolidate_signals(tech_result, prediction),
            'support_resistance': tech_result.get('support_resistance', {}),
            'chart_path': chart_path or '',
        }

        logger.info(f"[TrendAnalyzer] {stock_code} 分析完成: "
                     f"趋势={summary['direction']}, 信号={result['signals']['composite']}")
        return result

    # ── 内部方法 ──

    def _get_kline_data(self, hub, stock_code: str, period: str,
                        start_date: str, end_date: str) -> Optional[pd.DataFrame]:
        """获取K线数据（尝试原生周期，失败则日K+聚合）。"""
        df = hub.get_stock_kline(stock_code, period=period,
                                  start_date=start_date, end_date=end_date)
        if df is not None and not df.empty:
            return df

        # 回退：日K + 聚合
        if period != 'daily':
            daily_df = hub.get_stock_kline(stock_code, period='daily',
                                            start_date=start_date, end_date=end_date,
                                            limit=1500)
            if daily_df is not None and not daily_df.empty:
                return self.aggregator.aggregate(daily_df, period)

        return df  # 返回None或空df

    def _analyze_multi_timeframe(self, hub, stock_code, stock_data,
                                  start_date, end_date,
                                  primary_result) -> Dict:
        """对多周期运行分析并对齐信号。"""
        tf_results = {}
        periods_to_check = ['daily', 'weekly', 'monthly']

        # 填入主周期的结果
        period = 'daily'  # 默认主周期
        if primary_result and 'indicators' in primary_result:
            summary = self._build_trend_summary(primary_result)
            tf_results['daily'] = {
                'trend_summary': summary,
                'indicators': primary_result.get('indicators', {}),
            }

        # 尝试获取其他周期
        for p in periods_to_check:
            if p == 'daily' or p in tf_results:
                continue
            try:
                df_p = self._get_kline_data(hub, stock_code, p, start_date, end_date)
                if df_p is not None and len(df_p) >= 20:
                    tech_input = {
                        'historical': df_p,
                        'info': (stock_data or {}).get('info', {}),
                        'financial': (stock_data or {}).get('financial', {}),
                    }
                    result_p = self._get_tech().analyze(tech_input)
                    if result_p and 'indicators' in result_p:
                        sub_summary = self._build_trend_summary(result_p)
                        tf_results[p] = {
                            'trend_summary': sub_summary,
                            'indicators': result_p.get('indicators', {}),
                        }
            except Exception as e:
                logger.warning(f"[TrendAnalyzer] {p}周期分析失败: {e}")

        return self.aligner.align(tf_results)

    def _build_trend_summary(self, tech_result: Dict) -> Dict:
        """从TechnicalAnalyzer结果构建趋势摘要。"""
        cs = tech_result.get('composite_score', 50)
        mr = tech_result.get('market_regime', 'unknown')

        # 方向判定
        if cs >= 65:
            direction = 'uptrend'
        elif cs >= 45:
            direction = 'sideways'
        else:
            direction = 'downtrend'

        # 强度
        if cs >= 75:
            strength = 'strong'
        elif cs >= 55:
            strength = 'moderate'
        else:
            strength = 'weak'

        return {
            'direction': direction,
            'strength': strength,
            'regime': mr,
            'score': round(cs, 1),
        }

    def _consolidate_signals(self, tech_result: Dict, prediction: Dict) -> Dict:
        """综合所有信号生成最终信号报告。"""
        strength = tech_result.get('signal_strength', '持有观望')
        direction = prediction.get('direction', 'sideways')

        composite_map = {
            ('强烈买入', 'up'): 'strong_buy',
            ('建议买入', 'up'): 'buy',
            ('谨慎买入', 'up'): 'buy',
            ('持有观望', 'up'): 'neutral',
            ('持有观望', 'down'): 'neutral',
            ('谨慎卖出', 'down'): 'sell',
            ('建议卖出', 'down'): 'sell',
            ('强烈卖出', 'down'): 'strong_sell',
        }
        composite = composite_map.get((strength, direction), 'neutral')

        details = []
        for name, desc in [('macd_analysis', 'MACD'), ('kdj_analysis', 'KDJ'),
                            ('rsi_analysis', 'RSI'), ('bollinger_analysis', '布林带')]:
            analysis = tech_result.get(name, {})
            signal = analysis.get('signal', 0)
            desc_text = analysis.get('description', f'{desc}信号')
            details.append({
                'indicator': desc,
                'signal': signal,
                'description': desc_text,
            })

        return {
            'composite': composite,
            'details': details,
        }

    def _generate_chart(self, df, tech_result, turning_points, prediction,
                         stock_name, stock_code, period, output_format) -> Optional[str]:
        """生成图表。"""
        try:
            from chart_engine import ChartEngine
            engine = ChartEngine()
            interactive = (output_format == 'html')
            return engine.generate(
                stock_data=df,
                indicators=tech_result.get('indicators', {}),
                signals=tech_result.get('signals', {}),
                turning_points=turning_points,
                sr_levels=tech_result.get('support_resistance', {}),
                stock_name=stock_name,
                stock_code=stock_code,
                period=period,
                interactive=interactive,
            )
        except Exception as e:
            logger.error(f"[TrendAnalyzer] 图表生成失败: {e}")
            return None

    def _error_result(self, stock_code: str, error: str) -> Dict:
        logger.error(f"[TrendAnalyzer] {stock_code}: {error}")
        return {
            'stock_code': stock_code,
            'error': error,
            'trend_summary': {'direction': 'unknown', 'strength': 'unknown',
                              'regime': 'unknown', 'score': 0},
            'turning_points': [],
            'prediction': self.predictor._default_prediction(error),
            'signals': {'composite': 'neutral', 'details': []},
            'chart_path': '',
        }
