import pandas as pd
import numpy as np
import logging
from typing import Dict, Optional, Any
from config import TechnicalConfig
from logger_config import logger

class TechnicalAnalyzer:
    """
    技术分析器 - 量化评分系统
    
    6维度综合评分：
    1. 趋势强度 (Trend Strength) - 权重 30%
    2. 动量指标 (Momentum) - 权重 20%
    3. 波动率状态 (Volatility Regime) - 权重 15%
    4. 量价配合 (Volume-Price Confirmation) - 权重 15%
    5. 支撑阻力 (Support/Resistance) - 权重 10%
    6. 相对强度 (Relative Strength) - 权重 10%
    """
    
    def analyze(self, stock_data: Dict[str, Any]) -> Dict[str, Any]:
        try:
            historical_data = stock_data.get('historical')
            
            if historical_data is None or historical_data.empty:
                logger.warning("没有历史数据，无法进行技术分析")
                return self._get_empty_result()
            
            if 'Close' not in historical_data.columns:
                logger.warning("历史数据缺少 Close 列")
                return self._get_empty_result()
            
            indicators = {
                'ma': self._calculate_moving_averages(historical_data),
                'rsi': self._calculate_rsi(historical_data),
                'bollinger': self._calculate_bollinger_bands(historical_data),
                'macd': self._calculate_macd(historical_data),
                'adx': self._calculate_adx(historical_data),
                'atr': self._calculate_atr(historical_data),
                'stochastic': self._calculate_stochastic(historical_data),
                'kdj': self._calculate_kdj(historical_data),
                'obv': self._calculate_obv(historical_data),
                'roc': self._calculate_roc(historical_data)
            }

            signals = self._generate_signals(indicators, historical_data)

            macd_analysis = self._generate_macd_analysis(indicators)
            kdj_analysis = self._generate_kdj_analysis(indicators)
            rsi_analysis = self._generate_rsi_analysis(indicators, historical_data)
            bollinger_analysis = self._generate_bollinger_analysis(indicators, historical_data)
            detailed_descriptions = self._generate_detailed_descriptions(indicators, signals, historical_data)

            composite_analysis = self._calculate_composite_score(indicators, historical_data)

            analysis_result = {
                'indicators': indicators,
                'signals': signals,
                'latest_signals': self._get_latest_signals(signals),
                'composite_score': composite_analysis['composite_score'],
                'dimension_scores': composite_analysis['dimension_scores'],
                'signal_strength': composite_analysis['signal_strength'],
                'market_regime': composite_analysis['market_regime'],
                'support_resistance': composite_analysis.get('support_resistance', {}),
                'macd_analysis': macd_analysis,
                'kdj_analysis': kdj_analysis,
                'rsi_analysis': rsi_analysis,
                'bollinger_analysis': bollinger_analysis,
                'detailed_descriptions': detailed_descriptions
            }
            
            logger.info(f"技术分析完成 - 综合评分:{composite_analysis['composite_score']}, 市场状态:{composite_analysis['market_regime']}")
            return analysis_result
            
        except Exception as e:
            logger.error(f"技术分析失败: {e}")
            return self._get_empty_result()
    
    def _calculate_moving_averages(self, data: pd.DataFrame) -> Dict[str, pd.Series]:
        ma = {}
        for period in TechnicalConfig.MA_PERIODS:
            if len(data) >= period:
                ma[f'ma{period}'] = data['Close'].rolling(window=period).mean()
        return ma
    
    def _calculate_rsi(self, data: pd.DataFrame, period: int = TechnicalConfig.RSI_PERIOD) -> pd.Series:
        delta = data['Close'].diff()
        gain = (delta.where(delta > 0, 0)).rolling(window=period).mean()
        loss = (-delta.where(delta < 0, 0)).rolling(window=period).mean()
        
        rs = gain / loss.replace(0, float('nan'))
        rsi = 100 - (100 / (1 + rs))
        
        return rsi
    
    def _calculate_bollinger_bands(self, data: pd.DataFrame, period: int = TechnicalConfig.BOLLINGER_PERIOD, num_std: float = TechnicalConfig.BOLLINGER_STD) -> Dict[str, pd.Series]:
        sma = data['Close'].rolling(window=period).mean()
        std = data['Close'].rolling(window=period).std()
        
        upper_band = sma + (std * num_std)
        lower_band = sma - (std * num_std)
        
        return {
            'sma': sma,
            'upper_band': upper_band,
            'lower_band': lower_band,
            'width': (upper_band - lower_band) / sma.replace(0, float('nan')) * 100
        }
    
    def _calculate_macd(self, data: pd.DataFrame, fast_period: int = TechnicalConfig.MACD_FAST, slow_period: int = TechnicalConfig.MACD_SLOW, signal_period: int = TechnicalConfig.MACD_SIGNAL) -> Dict[str, pd.Series]:
        fast_ema = data['Close'].ewm(span=fast_period, adjust=False).mean()
        slow_ema = data['Close'].ewm(span=slow_period, adjust=False).mean()
        
        macd_line = fast_ema - slow_ema
        signal_line = macd_line.ewm(span=signal_period, adjust=False).mean()
        histogram = macd_line - signal_line
        
        return {
            'macd_line': macd_line,
            'signal_line': signal_line,
            'histogram': histogram
        }
    
    def _calculate_adx(self, data: pd.DataFrame, period: int = TechnicalConfig.ADX_PERIOD) -> pd.Series:
        if 'High' not in data.columns or 'Low' not in data.columns:
            return pd.Series(0, index=data.index)
        
        high = data['High']
        low = data['Low']
        close = data['Close']
        
        plus_dm = high.diff()
        minus_dm = low.diff()
        plus_dm[plus_dm < 0] = 0
        minus_dm[minus_dm > 0] = 0
        
        tr1 = high - low
        tr2 = abs(high - close.shift(1))
        tr3 = abs(low - close.shift(1))
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        
        atr = tr.rolling(window=period).mean()
        plus_di = 100 * (plus_dm.rolling(window=period).mean() / atr)
        minus_di = 100 * (minus_dm.rolling(window=period).mean() / atr)
        
        dx = 100 * abs(plus_di - minus_di) / (plus_di + minus_di).replace(0, float('nan'))
        adx = dx.rolling(window=period).mean()
        
        return adx.fillna(0)
    
    def _calculate_atr(self, data: pd.DataFrame, period: int = TechnicalConfig.ATR_PERIOD) -> pd.Series:
        if 'High' not in data.columns or 'Low' not in data.columns:
            return pd.Series(0, index=data.index)
        
        high = data['High']
        low = data['Low']
        close = data['Close']
        
        tr1 = high - low
        tr2 = abs(high - close.shift(1))
        tr3 = abs(low - close.shift(1))
        tr = pd.concat([tr1, tr2, tr3], axis=1).max(axis=1)
        
        return tr.rolling(window=period).mean()
    
    def _calculate_stochastic(self, data: pd.DataFrame, period: int = TechnicalConfig.STOCHASTIC_PERIOD, signal_period: int = TechnicalConfig.STOCHASTIC_SIGNAL) -> Dict[str, pd.Series]:
        if 'High' not in data.columns or 'Low' not in data.columns:
            return {'k': pd.Series(0, index=data.index), 'd': pd.Series(0, index=data.index)}
        
        high = data['High']
        low = data['Low']
        close = data['Close']
        
        lowest_low = low.rolling(window=period).min()
        highest_high = high.rolling(window=period).max()
        
        k = 100 * (close - lowest_low) / (highest_high - lowest_low).replace(0, float('nan'))
        d = k.rolling(window=signal_period).mean()
        
        return {'k': k.fillna(0), 'd': d.fillna(0)}
    
    def _calculate_kdj(self, data: pd.DataFrame, period: int = TechnicalConfig.KDJ_PERIOD, signal_period: int = TechnicalConfig.KDJ_SIGNAL_PERIOD) -> Dict[str, pd.Series]:
        if 'High' not in data.columns or 'Low' not in data.columns:
            return {'k': pd.Series(0, index=data.index), 'd': pd.Series(0, index=data.index), 'j': pd.Series(0, index=data.index)}

        low = data['Low']
        high = data['High']
        close = data['Close']

        lowest_low = low.rolling(window=period).min()
        highest_high = high.rolling(window=period).max()

        rsv = 100 * (close - lowest_low) / (highest_high - lowest_low).replace(0, float('nan'))

        k = rsv.ewm(com=2, adjust=False).mean()
        d = k.ewm(com=2, adjust=False).mean()
        j = 3 * k - 2 * d

        return {'k': k.fillna(0), 'd': d.fillna(0), 'j': j.fillna(0)}

    def _calculate_obv(self, data: pd.DataFrame) -> pd.Series:
        if 'Volume' not in data.columns:
            return pd.Series(0, index=data.index)
        close_diff = data['Close'].diff()
        direction = np.sign(close_diff)
        direction.iloc[0] = 1
        return (data['Volume'] * direction).cumsum()
    
    def _calculate_roc(self, data: pd.DataFrame) -> Dict[str, pd.Series]:
        roc = {}
        for period in TechnicalConfig.ROC_PERIODS:
            if len(data) >= period:
                roc[f'roc{period}'] = data['Close'].pct_change(period) * 100
        return roc
    
    def _generate_signals(self, indicators: Dict[str, Any], data: pd.DataFrame) -> Dict[str, pd.Series]:
        signals = {}
        
        if 'ma5' in indicators['ma'] and 'ma20' in indicators['ma']:
            ma5 = indicators['ma']['ma5']
            ma20 = indicators['ma']['ma20']
            ma_signal = pd.Series(0, index=data.index)
            ma_signal[(ma5 > ma20) & (ma5.shift(1) <= ma20.shift(1))] = 1
            ma_signal[(ma5 < ma20) & (ma5.shift(1) >= ma20.shift(1))] = -1
            signals['ma_signal'] = ma_signal
        else:
            signals['ma_signal'] = pd.Series(0, index=data.index)
        
        rsi = indicators['rsi']
        rsi_signal = pd.Series(0, index=data.index)
        rsi_signal[rsi < TechnicalConfig.RSI_OVERSOLD] = 1
        rsi_signal[rsi > TechnicalConfig.RSI_OVERBOUGHT] = -1
        signals['rsi_signal'] = rsi_signal
        
        bollinger = indicators['bollinger']
        bb_signal = pd.Series(0, index=data.index)
        close = data['Close']
        bb_signal[close > bollinger['upper_band']] = -1
        bb_signal[close < bollinger['lower_band']] = 1
        signals['bb_signal'] = bb_signal
        
        macd = indicators['macd']
        macd_signal = pd.Series(0, index=data.index)
        macd_signal[(macd['macd_line'] > macd['signal_line']) & (macd['macd_line'].shift(1) <= macd['signal_line'].shift(1))] = 1
        macd_signal[(macd['macd_line'] < macd['signal_line']) & (macd['macd_line'].shift(1) >= macd['signal_line'].shift(1))] = -1
        signals['macd_signal'] = macd_signal

        kdj = indicators.get('kdj', {})
        if 'k' in kdj and 'd' in kdj and not kdj['k'].empty:
            kdj_signal = pd.Series(0, index=data.index)
            kdj_signal[(kdj['k'] > kdj['d']) & (kdj['k'].shift(1) <= kdj['d'].shift(1))] = 1
            kdj_signal[(kdj['k'] < kdj['d']) & (kdj['k'].shift(1) >= kdj['d'].shift(1))] = -1
            signals['kdj_signal'] = kdj_signal
        else:
            signals['kdj_signal'] = pd.Series(0, index=data.index)

        composite = signals['ma_signal'] + signals['rsi_signal'] + signals['bb_signal'] + signals['macd_signal'] + signals['kdj_signal']
        signals['composite_signal'] = composite.apply(lambda x: 1 if x > 1 else (-1 if x < -1 else 0))

        return signals

    def _generate_macd_analysis(self, indicators: Dict[str, Any]) -> Dict[str, Any]:
        """生成详细的MACD分析，包括金叉/死叉、零轴位置、柱状图趋势、背离检测"""
        macd = indicators['macd']
        if 'macd_line' not in macd or macd['macd_line'].empty:
            return {'description': 'MACD数据不足', 'bullish': False, 'bearish': False}

        macd_line = macd['macd_line'].iloc[-1]
        signal_line = macd['signal_line'].iloc[-1]
        histogram = macd['histogram']
        hist_current = histogram.iloc[-1]
        hist_prev = histogram.iloc[-2] if len(histogram) >= 2 else 0
        hist_prev2 = histogram.iloc[-3] if len(histogram) >= 3 else 0

        macd_above_signal = macd_line > signal_line
        macd_above_zero = macd_line > 0

        analysis = {'macd_value': round(macd_line, 3), 'signal_value': round(signal_line, 3)}

        # --- 金叉/死叉状态 ---
        if macd_above_signal:
            analysis['cross_status'] = 'golden'
            analysis['cross_text'] = 'MACD金叉'
        else:
            analysis['cross_status'] = 'death'
            analysis['cross_text'] = 'MACD死叉'

        # --- 零轴位置 ---
        if macd_above_zero:
            analysis['zero_status'] = 'above'
            analysis['zero_text'] = '零轴上方'
        else:
            analysis['zero_status'] = 'below'
            analysis['zero_text'] = '零轴下方'

        # --- 柱状图趋势 ---
        if hist_current > 0 and hist_current > hist_prev:
            analysis['histogram_status'] = 'bullish_expanding'
            analysis['histogram_text'] = '多头放量'
        elif hist_current > 0 and hist_current <= hist_prev:
            analysis['histogram_status'] = 'bullish_contracting'
            analysis['histogram_text'] = '多头缩量'
        elif hist_current <= 0 and hist_current > hist_prev:
            analysis['histogram_status'] = 'bearish_contracting'
            analysis['histogram_text'] = '空头缩量'
        else:
            analysis['histogram_status'] = 'bearish_expanding'
            analysis['histogram_text'] = '空头放量'

        # --- 柱状图连续变化趋势 ---
        if len(histogram) >= 3:
            if hist_current > hist_prev > hist_prev2:
                analysis['histogram_trend'] = '连续增长'
            elif hist_current < hist_prev < hist_prev2:
                analysis['histogram_trend'] = '连续衰减'
            else:
                analysis['histogram_trend'] = '震荡变化'

        # --- MACD背离检测 (简化版) ---
        analysis['divergence'] = 'none'
        analysis['divergence_text'] = '无明显背离'
        if len(histogram) >= 30:
            if hist_current > 0 and macd_line > 0:
                peak_pos = histogram.iloc[-20:-5].idxmax() if len(histogram) >= 20 else None
                if peak_pos is not None:
                    peak_val = histogram.loc[peak_pos]
                    current_val = histogram.iloc[-1]
                    if current_val < peak_val * 0.8 and current_val > 0:
                        analysis['divergence'] = 'bearish'
                        analysis['divergence_text'] = '⚠️ 顶背离预警：价格可能仍在走高，但MACD柱状图已明显回落，上涨动能减弱'

            elif hist_current < 0 and macd_line < 0:
                trough_pos = histogram.iloc[-20:-5].idxmin() if len(histogram) >= 20 else None
                if trough_pos is not None:
                    trough_val = histogram.loc[trough_pos]
                    current_val = histogram.iloc[-1]
                    if current_val > trough_val * 0.8 and current_val < 0:
                        analysis['divergence'] = 'bullish'
                        analysis['divergence_text'] = '✅ 底背离预警：价格可能仍在走低，但MACD柱状图已明显回升，下跌动能减弱'

        # --- 综合描述 ---
        parts = []
        parts.append(f"MACD快线{analysis.get('macd_value'):.3f}")
        parts.append(f"慢线{analysis.get('signal_value'):.3f}")
        if macd_above_signal:
            parts.append('快线在慢线之上')
        else:
            parts.append('快线在慢线之下')
        parts.append(f"位于{analysis['zero_text']}")
        parts.append(f"柱状图{analysis['histogram_text']}")
        parts.append(analysis['divergence_text'])

        bullish = (macd_above_signal and macd_above_zero and analysis['histogram_status'] == 'bullish_expanding')
        bearish = (not macd_above_signal and not macd_above_zero and analysis['histogram_status'] == 'bearish_expanding')

        analysis['description'] = '，'.join(parts)
        analysis['bullish'] = bullish
        analysis['bearish'] = bearish
        return analysis

    def _generate_kdj_analysis(self, indicators: Dict[str, Any]) -> Dict[str, Any]:
        """生成详细的KDJ分析，包括金叉/死叉、超买超卖、高位/低位钝化"""
        kdj = indicators.get('kdj', {})
        if not kdj or 'k' not in kdj or kdj['k'].empty:
            return {'description': 'KDJ数据不足', 'bullish': False, 'bearish': False}

        k_val = kdj['k'].iloc[-1]
        d_val = kdj['d'].iloc[-1]
        j_val = kdj['j'].iloc[-1]
        k_prev = kdj['k'].iloc[-2] if len(kdj['k']) >= 2 else k_val
        d_prev = kdj['d'].iloc[-2] if len(kdj['d']) >= 2 else d_val

        analysis = {'k': round(k_val, 2), 'd': round(d_val, 2), 'j': round(j_val, 2)}

        # --- 金叉/死叉检测 ---
        if k_val > d_val and k_prev <= d_prev:
            analysis['cross'] = 'golden'
            analysis['cross_text'] = '🟢 KDJ金叉 — K线上穿D线，短期看多'
        elif k_val < d_val and k_prev >= d_prev:
            analysis['cross'] = 'death'
            analysis['cross_text'] = '🔴 KDJ死叉 — K线下穿D线，短期看空'
        else:
            analysis['cross'] = 'none'
            if k_val > d_val:
                analysis['cross_text'] = 'K线在D线上方运行，偏多'
            else:
                analysis['cross_text'] = 'K线在D线下方运行，偏空'

        # --- KDJ位置判断 ---
        if j_val > 100:
            analysis['j_position'] = '超买'
            analysis['j_text'] = f'J值{j_val:.1f}进入超买区（>100），注意回调风险'
        elif j_val < 0:
            analysis['j_position'] = '超卖'
            analysis['j_text'] = f'J值{j_val:.1f}进入超卖区（<0），可能存在反弹机会'
        elif k_val > 80:
            analysis['j_position'] = '高位'
            analysis['j_text'] = f'K值{k_val:.1f}处于高位（>80），短期可能超买'
        elif k_val < 20:
            analysis['j_position'] = '低位'
            analysis['j_text'] = f'K值{k_val:.1f}处于低位（<20），短期可能超卖'
        else:
            analysis['j_position'] = '中位'
            analysis['j_text'] = f'K值{k_val:.1f}处于中位区间，方向待明确'

        # --- KDJ综合判断 ---
        if k_val > 80 and d_val > 80 and j_val > 100:
            analysis['overall'] = '高位钝化'
            analysis['overall_text'] = '⚠️ KDJ高位钝化，强势但需警惕回调'
        elif k_val < 20 and d_val < 20 and j_val < 0:
            analysis['overall'] = '低位钝化'
            analysis['overall_text'] = '💡 KDJ低位钝化，弱势但可能出现反弹'
        elif k_val > d_val and k_val > 50:
            analysis['overall'] = '偏多'
            analysis['overall_text'] = 'KDJ多头格局'
        elif k_val < d_val and k_val < 50:
            analysis['overall'] = '偏空'
            analysis['overall_text'] = 'KDJ空头格局'
        else:
            analysis['overall'] = '中性'
            analysis['overall_text'] = 'KDJ方向不明，观望为主'

        bullish = (k_val > d_val and k_val > 50 and j_val < 100)
        bearish = (k_val < d_val and k_val < 50 and j_val > 0)

        analysis['description'] = f"K值{k_val:.1f}、D值{d_val:.1f}、J值{j_val:.1f}。" + analysis['cross_text'] + "。" + analysis['j_text'] + "。" + analysis['overall_text']
        analysis['bullish'] = bullish
        analysis['bearish'] = bearish
        return analysis

    def _generate_rsi_analysis(self, indicators: Dict[str, Any], data: pd.DataFrame) -> Dict[str, Any]:
        """生成详细的RSI分析，包括区间定位、趋势方向、中轴穿越、背离检测"""
        rsi = indicators['rsi']
        if rsi.empty:
            return {'description': 'RSI数据不足', 'bullish': False, 'bearish': False}

        rsi_val = round(rsi.iloc[-1], 1)
        rsi_arr = rsi.dropna().values
        analysis = {'rsi_value': rsi_val}

        # --- 区间定位 ---
        if rsi_val >= TechnicalConfig.RSI_OVERBOUGHT:
            analysis['zone'] = 'overbought'
            analysis['zone_text'] = f'超买区（≥{TechnicalConfig.RSI_OVERBOUGHT}），短期可能出现回调'
        elif rsi_val >= TechnicalConfig.RSI_CENTERLINE:
            analysis['zone'] = 'strong'
            analysis['zone_text'] = f'偏强区（{TechnicalConfig.RSI_CENTERLINE}-{TechnicalConfig.RSI_OVERBOUGHT}），多头占优'
        elif rsi_val >= TechnicalConfig.RSI_OVERSOLD:
            analysis['zone'] = 'weak'
            analysis['zone_text'] = f'偏弱区（{TechnicalConfig.RSI_OVERSOLD}-{TechnicalConfig.RSI_CENTERLINE}），空头占优'
        else:
            analysis['zone'] = 'oversold'
            analysis['zone_text'] = f'超卖区（≤{TechnicalConfig.RSI_OVERSOLD}），可能存在反弹机会'

        # --- 趋势方向（最近5期）---
        if len(rsi_arr) >= 5:
            recent5 = rsi_arr[-5:]
            if all(recent5[i] < recent5[i + 1] for i in range(4)):
                analysis['trend'] = 'rising'
                analysis['trend_text'] = 'RSI持续上行，多头动能增强'
            elif all(recent5[i] > recent5[i + 1] for i in range(4)):
                analysis['trend'] = 'falling'
                analysis['trend_text'] = 'RSI持续下行，空头动能增强'
            else:
                analysis['trend'] = 'oscillating'
                analysis['trend_text'] = 'RSI震荡运行，方向待明确'
        elif len(rsi_arr) >= 2:
            if rsi_arr[-1] > rsi_arr[-2]:
                analysis['trend'] = 'rising'
                analysis['trend_text'] = 'RSI拐头向上'
            else:
                analysis['trend'] = 'falling'
                analysis['trend_text'] = 'RSI拐头向下'
        else:
            analysis['trend'] = 'oscillating'
            analysis['trend_text'] = 'RSI数据不足，无法判断趋势'

        # --- 50中轴穿越 ---
        rsi_prev = rsi.iloc[-2] if len(rsi) >= 2 else rsi_val
        if rsi_prev < TechnicalConfig.RSI_CENTERLINE <= rsi_val:
            analysis['centerline_cross'] = 'bullish'
            analysis['centerline_cross_text'] = '🟢 RSI上穿50中轴，由弱转强'
        elif rsi_prev >= TechnicalConfig.RSI_CENTERLINE > rsi_val:
            analysis['centerline_cross'] = 'bearish'
            analysis['centerline_cross_text'] = '🔴 RSI下破50中轴，由强转弱'
        else:
            analysis['centerline_cross'] = 'none'
            analysis['centerline_cross_text'] = '无中轴穿越信号'

        # --- RSI背离检测（简化版）---
        analysis['divergence'] = 'none'
        analysis['divergence_text'] = '无明显背离信号'
        close = data['Close']
        if len(rsi) >= 30 and len(data) >= 30:
            close_window = close.iloc[-20:].values
            rsi_window = rsi.dropna().iloc[-20:].values
            if len(rsi_window) >= 15:
                close_high_idx = close_window.argmax()
                close_low_idx = close_window.argmin()
                rsi_high_idx = rsi_window.argmax()
                rsi_low_idx = rsi_window.argmin()

                close_high = close_window[close_high_idx]
                close_low = close_window[close_low_idx]
                rsi_high = rsi_window[rsi_high_idx]
                rsi_low = rsi_window[rsi_low_idx]
                last_close = close_window[-1]
                last_rsi = rsi_window[-1]

                # 经典顶背离：价格创新高但RSI未创新高
                if last_close >= close_high * 0.99 and last_rsi < rsi_high * 0.95:
                    analysis['divergence'] = 'bearish'
                    analysis['divergence_text'] = '⚠️ 顶背离预警：价格创近20期新高，但RSI未能同步创高，上涨动能减弱'
                # 经典底背离：价格创新低但RSI未创新低
                elif last_close <= close_low * 1.01 and last_rsi > rsi_low * 1.05:
                    analysis['divergence'] = 'bullish'
                    analysis['divergence_text'] = '✅ 底背离预警：价格创近20期新低，但RSI未能同步创低，下跌动能减弱'
                # 隐藏顶背离：回撤低点抬高但RSI低点降低（上升趋势减弱信号）
                elif close_window[-1] > close_window[-10] and rsi_window[-10:-5].min() > rsi_window[-5:].min():
                    analysis['divergence'] = 'hidden_bearish'
                    analysis['divergence_text'] = '⚠️ 隐藏顶背离：价格回撤低点抬高，但RSI低点降低，上升趋势内在动能减弱'
                # 隐藏底背离：反弹高点降低但RSI高点抬高（下降趋势减弱信号）
                elif close_window[-1] < close_window[-10] and rsi_window[-10:-5].max() < rsi_window[-5:].max():
                    analysis['divergence'] = 'hidden_bullish'
                    analysis['divergence_text'] = '✅ 隐藏底背离：价格反弹高点降低，但RSI高点抬高，下降趋势内在动能减弱'

        # --- 综合描述 ---
        parts = [f"RSI(14)={rsi_val}"]
        parts.append(analysis['zone_text'])
        parts.append(analysis['trend_text'])
        parts.append(analysis['centerline_cross_text'])
        if analysis['divergence'] != 'none':
            parts.append(analysis['divergence_text'])

        bullish = (analysis['zone'] in ('strong',) and analysis['trend'] == 'rising') or (analysis['divergence'] in ('bullish', 'hidden_bullish'))
        bearish = (analysis['zone'] in ('weak', 'oversold') and analysis['trend'] == 'falling') or (analysis['divergence'] in ('bearish', 'hidden_bearish'))

        analysis['description'] = '，'.join(parts)
        analysis['bullish'] = bullish
        analysis['bearish'] = bearish
        return analysis

    def _generate_bollinger_analysis(self, indicators: Dict[str, Any], data: pd.DataFrame) -> Dict[str, Any]:
        """生成详细的布林带分析，包括%B、带宽、挤压检测、轨道行走、反转信号、中轨倾斜"""
        bb = indicators['bollinger']
        if 'upper_band' not in bb or not bb['upper_band'].iloc[-1]:
            return {'description': '布林带数据不足', 'bullish': False, 'bearish': False}

        close = data['Close']
        upper = bb['upper_band'].iloc[-1]
        mid = bb['sma'].iloc[-1]
        lower = bb['lower_band'].iloc[-1]
        current_price = close.iloc[-1]
        width_series = bb.get('width')

        analysis = {'upper_band': round(upper, 2), 'middle_band': round(mid, 2), 'lower_band': round(lower, 2)}

        # --- %B指标 ---
        if upper > lower:
            percent_b = (current_price - lower) / (upper - lower)
            analysis['percent_b'] = round(percent_b, 3)
        else:
            analysis['percent_b'] = 0.5

        # --- 带宽 ---
        if mid > 0:
            bandwidth = (upper - lower) / mid * 100
            analysis['bandwidth'] = round(bandwidth, 2)
        else:
            analysis['bandwidth'] = 0

        # --- 价格位置 ---
        if current_price > upper:
            analysis['price_position'] = 'above_upper'
            analysis['price_position_text'] = f'价格突破上轨（%B={analysis["percent_b"]:.2f}），超买状态'
        elif current_price >= mid:
            analysis['price_position'] = 'upper_to_mid'
            analysis['price_position_text'] = f'价格在上轨与中轨之间运行（%B={analysis["percent_b"]:.2f}），偏强'
        elif current_price >= lower:
            analysis['price_position'] = 'mid_to_lower'
            analysis['price_position_text'] = f'价格在中轨与下轨之间运行（%B={analysis["percent_b"]:.2f}），偏弱'
        else:
            analysis['price_position'] = 'below_lower'
            analysis['price_position_text'] = f'价格跌破下轨（%B={analysis["percent_b"]:.2f}），超卖状态'

        # --- 挤压检测 ---
        analysis['squeeze'] = 'normal'
        analysis['squeeze_text'] = '布林带正常开口'
        if width_series is not None and not width_series.empty and len(width_series) >= TechnicalConfig.BOLLINGER_SQUEEZE_PERIOD:
            recent_widths = width_series.iloc[-TechnicalConfig.BOLLINGER_SQUEEZE_PERIOD:]
            min_width = recent_widths.min()
            current_width = recent_widths.iloc[-1]
            if min_width > 0:
                width_ratio = current_width / min_width
                if width_ratio < 1.10:
                    analysis['squeeze'] = 'squeeze'
                    analysis['squeeze_text'] = '⚠️ 布林带极度收窄（历史低位），变盘信号强烈'
                elif width_ratio < 1.25:
                    analysis['squeeze'] = 'tightening'
                    analysis['squeeze_text'] = '布林带收窄中，面临方向选择'
                elif width_ratio > 1.5:
                    analysis['squeeze'] = 'expanding'
                    analysis['squeeze_text'] = '布林带扩张，趋势延续中'
                else:
                    analysis['squeeze_text'] = '布林带正常运行'

        # --- 轨道行走检测（最近5期）---
        analysis['walking_band'] = 'none'
        analysis['walking_text'] = '无轨道行走现象'
        if len(close) >= 5:
            recent_close5 = close.iloc[-5:]
            touch_upper = sum(1 for p in recent_close5 if p >= upper * 0.98)
            touch_lower = sum(1 for p in recent_close5 if p <= lower * 1.02)
            if touch_upper >= 4:
                analysis['walking_band'] = 'walking_upper'
                analysis['walking_text'] = f'⚠️ 价格持续沿上轨运行（5期触轨{touch_upper}次），强势极端'
            elif touch_lower >= 4:
                analysis['walking_band'] = 'walking_lower'
                analysis['walking_text'] = f'💡 价格持续沿下轨运行（5期触轨{touch_lower}次），弱势极端'

        # --- 反转信号检测（近10期）---
        analysis['reversal'] = 'none'
        analysis['reversal_text'] = '无明显反转信号'
        if len(close) >= 10:
            first5 = close.iloc[-10:-5]
            last5 = close.iloc[-5:]
            first5_min = first5.min()
            first5_max = first5.max()
            last5_avg = last5.mean()
            # 看涨反转：前半段触下轨，当前在中轨以上
            if first5_min <= lower * 1.02 and last5_avg >= mid:
                analysis['reversal'] = 'bullish'
                analysis['reversal_text'] = '✅ 多头反转：前5期曾触下轨，现已回升至中轨上方'
            # 看跌反转：前半段触上轨，当前在中轨以下
            elif first5_max >= upper * 0.98 and last5_avg <= mid:
                analysis['reversal'] = 'bearish'
                analysis['reversal_text'] = '🔴 空头反转：前5期曾触上轨，现已回落至中轨下方'

        # --- 中轨倾斜方向 ---
        analysis['band_tilt'] = 'flat'
        analysis['band_tilt_text'] = '中轨走平，方向待定'
        if len(close) >= 5:
            mid_current = mid
            mid_5ago = bb['sma'].iloc[-5]
            if mid_current > mid_5ago * 1.005:
                analysis['band_tilt'] = 'upward'
                analysis['band_tilt_text'] = '中轨向上倾斜，趋势偏多'
            elif mid_current < mid_5ago * 0.995:
                analysis['band_tilt'] = 'downward'
                analysis['band_tilt_text'] = '中轨向下倾斜，趋势偏空'

        # --- 综合描述 ---
        parts = [
            f"布林上轨={upper:.2f}",
            f"中轨={mid:.2f}",
            f"下轨={lower:.2f}",
            f"%B={analysis['percent_b']:.2f}",
            f"带宽={analysis['bandwidth']:.1f}%",
            analysis['price_position_text'],
            analysis['squeeze_text'],
            analysis['band_tilt_text']
        ]
        if analysis['walking_band'] != 'none':
            parts.append(analysis['walking_text'])
        if analysis['reversal'] != 'none':
            parts.append(analysis['reversal_text'])

        bullish = (analysis['price_position'] in ('upper_to_mid',) and analysis['band_tilt'] == 'upward') or analysis['reversal'] == 'bullish'
        bearish = (analysis['price_position'] in ('mid_to_lower',) and analysis['band_tilt'] == 'downward') or analysis['reversal'] == 'bearish'

        analysis['description'] = '，'.join(parts)
        analysis['bullish'] = bullish
        analysis['bearish'] = bearish
        return analysis

    def _generate_detailed_descriptions(self, indicators: Dict[str, Any], signals: Dict[str, Any], data: pd.DataFrame) -> Dict[str, str]:
        """生成所有指标的详细中文描述"""
        desc = {}
        current_price = data['Close'].iloc[-1]

        # --- 均线描述 ---
        if 'ma5' in indicators['ma'] and 'ma20' in indicators['ma']:
            ma5 = indicators['ma']['ma5'].iloc[-1]
            ma20 = indicators['ma']['ma20'].iloc[-1]
            ma50 = indicators['ma'].get('ma50', pd.Series([None])).iloc[-1]
            ma200 = indicators['ma'].get('ma200', pd.Series([None])).iloc[-1]

            parts = [f"MA5={ma5:.2f}", f"MA20={ma20:.2f}"]
            if ma50 is not None and not pd.isna(ma50):
                parts.append(f"MA50={ma50:.2f}")
            if ma200 is not None and not pd.isna(ma200):
                parts.append(f"MA200={ma200:.2f}")
            parts.append(f"当前价{current_price:.2f}")

            ma_signal = signals.get('ma_signal', pd.Series([0])).iloc[-1]
            if ma5 > ma20:
                parts.append('短期均线多头排列，MA5在MA20之上')
                if ma50 is not None and not pd.isna(ma50) and ma5 > ma50:
                    parts.append('中期趋势偏多')
                else:
                    parts.append('中期趋势待确认')
            else:
                parts.append('短期均线空头排列，MA5在MA20之下')
                if ma50 is not None and not pd.isna(ma50) and ma5 < ma50:
                    parts.append('中期趋势偏空')
                else:
                    parts.append('中期趋势待确认')

            if ma_signal == 1:
                parts.append('🟢 MA5金叉MA20，短期趋势转多')
            elif ma_signal == -1:
                parts.append('🔴 MA5死叉MA20，短期趋势转空')

            alignment = self._check_ma_alignment(indicators, data)
            alignment_map = {
                'strong_bullish': '多头完全排列（MA5>MA20>MA50>MA200），强势上涨格局',
                'bullish': '短期均线在中期均线上方，趋势偏多',
                'strong_bearish': '空头完全排列（MA5<MA20<MA50<MA200），弱势下跌格局',
                'bearish': '短期均线在中期均线下方，趋势偏空',
                'neutral': '均线交织，趋势不明'
            }
            parts.append(alignment_map.get(alignment, ''))

            desc['ma'] = '，'.join([p for p in parts if p])
        else:
            desc['ma'] = '均线数据不足'

        # --- RSI描述 ---
        if not indicators['rsi'].empty:
            rsi_analysis = self._generate_rsi_analysis(indicators, data)
            desc['rsi'] = rsi_analysis['description']
        else:
            desc['rsi'] = 'RSI数据不足'

        # --- MACD描述 ---
        if 'macd_line' in indicators['macd'] and not indicators['macd']['macd_line'].empty:
            macd_analysis = self._generate_macd_analysis(indicators)
            desc['macd'] = macd_analysis['description']
        else:
            desc['macd'] = 'MACD数据不足'

        # --- KDJ描述 ---
        kdj = indicators.get('kdj', {})
        if 'k' in kdj and not kdj['k'].empty:
            kdj_analysis = self._generate_kdj_analysis(indicators)
            desc['kdj'] = kdj_analysis['description']
        else:
            desc['kdj'] = 'KDJ数据不足'

        # --- 布林带描述 ---
        bb = indicators['bollinger']
        if 'upper_band' in bb and bb['upper_band'].iloc[-1]:
            bollinger_analysis = self._generate_bollinger_analysis(indicators, data)
            desc['bollinger'] = bollinger_analysis['description']
        else:
            desc['bollinger'] = '布林带数据不足'

        return desc

    def _calculate_composite_score(self, indicators: Dict[str, Any], data: pd.DataFrame) -> Dict[str, Any]:
        trend_score = self._calculate_trend_score(indicators, data)
        momentum_score = self._calculate_momentum_score(indicators, data)
        volatility_score = self._calculate_volatility_score(indicators, data)
        volume_score = self._calculate_volume_score(indicators, data)
        sr_score = self._calculate_support_resistance_score(indicators, data)
        relative_strength = self._calculate_relative_strength_score(data)
        
        scores = {
            'trend': trend_score,
            'momentum': momentum_score,
            'volatility': volatility_score,
            'volume': volume_score,
            'support_resistance': sr_score,
            'relative_strength': relative_strength
        }
        
        weights = {
            'trend': TechnicalConfig.TREND_WEIGHT,
            'momentum': TechnicalConfig.MOMENTUM_WEIGHT,
            'volatility': TechnicalConfig.VOLATILITY_WEIGHT,
            'volume': TechnicalConfig.VOLUME_WEIGHT,
            'support_resistance': TechnicalConfig.SUPPORT_RESISTANCE_WEIGHT,
            'relative_strength': TechnicalConfig.RELATIVE_STRENGTH_WEIGHT
        }
        
        total_score = sum(scores[k] * weights[k] for k in scores)
        total_score = round(min(max(total_score, 0), 100), 1)
        
        market_regime = self._identify_market_regime(scores)
        signal_strength = self._classify_signal_strength(total_score)
        
        support_resistance = self._calculate_support_resistance_levels(data)
        
        return {
            'composite_score': total_score,
            'dimension_scores': scores,
            'signal_strength': signal_strength,
            'market_regime': market_regime,
            'support_resistance': support_resistance
        }
    
    def _calculate_trend_score(self, indicators, data) -> float:
        score = 0
        
        adx = indicators['adx'].iloc[-1] if not indicators['adx'].empty else 0
        if adx >= TechnicalConfig.ADX_VERY_STRONG:
            score += 40
        elif adx >= TechnicalConfig.ADX_STRONG_TREND:
            score += 30
        elif adx >= 20:
            score += 20
        else:
            score += 10
        
        ma_alignment = self._check_ma_alignment(indicators, data)
        if ma_alignment == 'strong_bullish':
            score += 40
        elif ma_alignment == 'bullish':
            score += 30
        elif ma_alignment == 'neutral':
            score += 20
        elif ma_alignment == 'bearish':
            score += 10
        else:
            score += 0
        
        if 'ma200' in indicators['ma']:
            current_price = data['Close'].iloc[-1]
            ma200 = indicators['ma']['ma200'].iloc[-1]
            if current_price > ma200 * 1.1:
                score += 20
            elif current_price > ma200:
                score += 15
            elif current_price > ma200 * 0.9:
                score += 10
            else:
                score += 0
        
        return min(score, 100)
    
    def _check_ma_alignment(self, indicators, data):
        ma = indicators['ma']
        
        if all(k in ma for k in ['ma5', 'ma20', 'ma50', 'ma200']):
            latest = {k: ma[k].iloc[-1] for k in ['ma5', 'ma20', 'ma50', 'ma200']}
            if latest['ma5'] > latest['ma20'] > latest['ma50'] > latest['ma200']:
                return 'strong_bullish'
            elif latest['ma5'] > latest['ma20'] > latest['ma50']:
                return 'bullish'
            elif latest['ma5'] < latest['ma20'] < latest['ma50'] < latest['ma200']:
                return 'strong_bearish'
            elif latest['ma5'] < latest['ma20'] < latest['ma50']:
                return 'bearish'
        
        return 'neutral'
    
    def _calculate_momentum_score(self, indicators, data) -> float:
        score = 0
        
        rsi = indicators['rsi'].iloc[-1] if not indicators['rsi'].empty else 50
        if 50 <= rsi <= 65:
            score += 30
        elif 40 <= rsi < 50:
            score += 20
        elif 65 < rsi <= 70:
            score += 15
        elif rsi < 30:
            score += 25
        else:
            score += 5
        
        macd_hist = indicators['macd']['histogram']
        if not macd_hist.empty and len(macd_hist) >= 2:
            current = macd_hist.iloc[-1]
            previous = macd_hist.iloc[-2]
            if current > 0 and current > previous:
                score += 35
            elif current > 0:
                score += 25
            elif current < 0 and current > previous:
                score += 20
            else:
                score += 5
        else:
            score += 15
        
        roc = indicators.get('roc', {})
        if 'roc5' in roc and 'roc20' in roc:
            roc5 = roc['roc5'].iloc[-1]
            roc20 = roc['roc20'].iloc[-1]
            if roc5 > 0 and roc20 > 0:
                score += 35
            elif roc5 > 0:
                score += 25
            elif roc20 > 0:
                score += 15
            else:
                score += 5
        
        stochastic = indicators.get('stochastic', {})
        if 'k' in stochastic and 'd' in stochastic:
            k = stochastic['k'].iloc[-1]
            d = stochastic['d'].iloc[-1]
            if k < 20 and k > d:
                score = min(score + 10, 100)
            elif k > 80 and k < d:
                score = max(score - 5, 0)
        
        return min(score, 100)
    
    def _calculate_volatility_score(self, indicators, data) -> float:
        score = 50
        
        atr = indicators['atr']
        if not atr.empty:
            current_atr = atr.iloc[-1]
            avg_atr = atr.rolling(window=20).mean().iloc[-1]
            
            if avg_atr > 0:
                atr_ratio = current_atr / avg_atr
                if 0.8 <= atr_ratio <= 1.2:
                    score += 30
                elif 0.5 <= atr_ratio <= 1.5:
                    score += 20
                elif atr_ratio > 2.0:
                    score -= 30
                else:
                    score += 10
        
        bb = indicators['bollinger']
        if 'width' in bb and not bb['width'].empty:
            bb_width = bb['width'].iloc[-1]
            if bb_width and bb_width < 10:
                score += 20
            elif bb_width and bb_width > 20:
                score -= 10
        
        return min(max(score, 0), 100)
    
    def _calculate_volume_score(self, indicators, data) -> float:
        score = 0
        
        if 'Volume' not in data.columns:
            return 50
        
        returns = data['Close'].pct_change()
        volumes = data['Volume']
        
        up_volume = volumes[returns > 0].rolling(window=20).mean()
        down_volume = volumes[returns < 0].rolling(window=20).mean()
        
        if not up_volume.empty and not down_volume.empty:
            up_vol = up_volume.iloc[-1]
            down_vol = down_volume.iloc[-1]
            volume_ratio = up_vol / down_vol if down_vol and down_vol > 0 else 1
            
            if volume_ratio > 1.5:
                score += 40
            elif volume_ratio > 1.2:
                score += 30
            elif volume_ratio > 1.0:
                score += 20
            else:
                score += 10
        
        obv = indicators['obv']
        if not obv.empty and len(obv) >= 20:
            current_obv = obv.iloc[-1]
            avg_obv = obv.rolling(window=20).mean().iloc[-1]
            if current_obv > avg_obv:
                score += 30
            elif current_obv > obv.rolling(window=50).mean().iloc[-1] if len(obv) >= 50 else current_obv:
                score += 20
            else:
                score += 10
        
        if len(volumes) >= TechnicalConfig.VOLUME_MA_PERIOD:
            avg_volume_5 = volumes.rolling(window=5).mean().iloc[-1]
            avg_volume_20 = volumes.rolling(window=TechnicalConfig.VOLUME_MA_PERIOD).mean().iloc[-1]
            activity_ratio = avg_volume_5 / avg_volume_20 if avg_volume_20 > 0 else 1
            
            if 1.2 <= activity_ratio <= 2.0:
                score += 30
            elif activity_ratio > 2.0:
                score += 15
            elif activity_ratio > 0.8:
                score += 25
            else:
                score += 10
        
        return min(score, 100)
    
    def _calculate_support_resistance_score(self, indicators, data) -> float:
        score = 50
        
        close = data['Close']
        current_price = close.iloc[-1]
        
        bb = indicators['bollinger']
        if 'lower_band' in bb and 'upper_band' in bb:
            lower = bb['lower_band'].iloc[-1]
            upper = bb['upper_band'].iloc[-1]
            
            if lower and upper:
                position = (current_price - lower) / (upper - lower) if upper > lower else 0.5
                
                if position < 0.2:
                    score += 30
                elif position < 0.4:
                    score += 15
                elif position > 0.8:
                    score -= 20
                elif position > 0.6:
                    score -= 10
        
        ma = indicators['ma']
        if 'ma20' in ma and 'ma50' in ma:
            ma20 = ma['ma20'].iloc[-1]
            ma50 = ma['ma50'].iloc[-1]
            
            if abs(current_price - ma20) / current_price < 0.02:
                score += 15
            elif abs(current_price - ma50) / current_price < 0.02:
                score += 10
        
        return min(max(score, 0), 100)
    
    def _calculate_relative_strength_score(self, data) -> float:
        score = 50
        
        if len(data) < 20:
            return score
        
        close = data['Close']
        
        roc_20 = close.pct_change(20).iloc[-1] * 100
        
        if roc_20 > 10:
            score += 30
        elif roc_20 > 5:
            score += 20
        elif roc_20 > 0:
            score += 10
        elif roc_20 < -10:
            score -= 20
        elif roc_20 < -5:
            score -= 10
        
        if len(data) >= 60:
            roc_60 = close.pct_change(60).iloc[-1] * 100
            if roc_60 > 0:
                score += 20
            else:
                score -= 10
        
        return min(max(score, 0), 100)
    
    def _calculate_support_resistance_levels(self, data) -> Dict[str, Any]:
        close = data['Close']
        current_price = close.iloc[-1]
        
        high_20 = close.rolling(window=20).max().iloc[-1]
        low_20 = close.rolling(window=20).min().iloc[-1]
        
        high_60 = close.rolling(window=60).max().iloc[-1] if len(close) >= 60 else high_20
        low_60 = close.rolling(window=60).min().iloc[-1] if len(close) >= 60 else low_20
        
        pivot = (high_20 + low_20 + current_price) / 3
        
        return {
            'resistance_2': round(high_60, 2),
            'resistance_1': round(high_20, 2),
            'pivot': round(pivot, 2),
            'support_1': round(low_20, 2),
            'support_2': round(low_60, 2),
            'current_price': round(current_price, 2)
        }
    
    def _identify_market_regime(self, scores) -> str:
        trend = scores['trend']
        momentum = scores['momentum']
        volatility = scores['volatility']
        
        if trend >= 60 and momentum >= 60:
            return 'strong_uptrend'
        elif trend >= 40 and momentum >= 40:
            return 'moderate_uptrend'
        elif trend < 30 and momentum < 30:
            return 'strong_downtrend'
        elif volatility >= 70:
            return 'high_volatility_choppy'
        else:
            return 'range_bound'
    
    def _classify_signal_strength(self, score) -> str:
        if score >= 80:
            return '强烈买入'
        elif score >= 65:
            return '建议买入'
        elif score >= 55:
            return '谨慎买入'
        elif score >= 45:
            return '持有观望'
        elif score >= 35:
            return '谨慎卖出'
        elif score >= 20:
            return '建议卖出'
        else:
            return '强烈卖出'
    
    def _get_latest_signals(self, signals: Dict[str, pd.Series]) -> Dict[str, int]:
        latest = {}
        for key, signal_series in signals.items():
            latest[key] = int(signal_series.iloc[-1]) if not signal_series.empty else 0
        return latest
    
    def _get_empty_result(self) -> Dict[str, Any]:
        return {
            'indicators': {
                'ma': {},
                'rsi': pd.Series(dtype=float),
                'bollinger': {},
                'macd': {},
                'adx': pd.Series(dtype=float),
                'atr': pd.Series(dtype=float),
                'stochastic': {},
                'kdj': {},
                'obv': pd.Series(dtype=float),
                'roc': {}
            },
            'signals': {},
            'latest_signals': {
                'ma_signal': 0,
                'rsi_signal': 0,
                'bb_signal': 0,
                'macd_signal': 0,
                'kdj_signal': 0,
                'composite_signal': 0
            },
            'composite_score': 0,
            'dimension_scores': {
                'trend': 0,
                'momentum': 0,
                'volatility': 0,
                'volume': 0,
                'support_resistance': 0,
                'relative_strength': 0
            },
            'signal_strength': '无法评估',
            'market_regime': 'unknown',
            'support_resistance': {},
            'macd_analysis': {'description': '无数据', 'bullish': False, 'bearish': False},
            'kdj_analysis': {'description': '无数据', 'bullish': False, 'bearish': False},
            'rsi_analysis': {'description': '无数据', 'bullish': False, 'bearish': False},
            'bollinger_analysis': {'description': '无数据', 'bullish': False, 'bearish': False},
            'detailed_descriptions': {}
        }