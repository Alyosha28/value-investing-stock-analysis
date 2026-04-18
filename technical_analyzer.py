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
                'obv': self._calculate_obv(historical_data),
                'roc': self._calculate_roc(historical_data)
            }
            
            signals = self._generate_signals(indicators, historical_data)
            
            composite_analysis = self._calculate_composite_score(indicators, historical_data)
            
            analysis_result = {
                'indicators': indicators,
                'signals': signals,
                'latest_signals': self._get_latest_signals(signals),
                'composite_score': composite_analysis['composite_score'],
                'dimension_scores': composite_analysis['dimension_scores'],
                'signal_strength': composite_analysis['signal_strength'],
                'market_regime': composite_analysis['market_regime'],
                'support_resistance': composite_analysis.get('support_resistance', {})
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
        
        composite = signals['ma_signal'] + signals['rsi_signal'] + signals['bb_signal'] + signals['macd_signal']
        signals['composite_signal'] = composite.apply(lambda x: 1 if x > 1 else (-1 if x < -1 else 0))
        
        return signals
    
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
                'obv': pd.Series(dtype=float),
                'roc': {}
            },
            'signals': {},
            'latest_signals': {
                'ma_signal': 0,
                'rsi_signal': 0,
                'bb_signal': 0,
                'macd_signal': 0,
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
            'support_resistance': {}
        }