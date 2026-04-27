#!/usr/bin/env python3
"""
Agent Bridge — 多Agent按需调用桥接器

用法:
    python agent_bridge.py 600338 data          # 数据分析师
    python agent_bridge.py 600338 value         # 价值分析师
    python agent_bridge.py 600338 technical     # 技术分析师
    python agent_bridge.py 600338 macro         # 宏观分析师
    python agent_bridge.py 600338 risk          # 风控官
    python agent_bridge.py 600338 full          # 完整个股分析（多Agent协作）
    python agent_bridge.py 600338 decision      # 投资决策（并行+风控）

选项:
    --format json|text    输出格式 (默认: json)
    --no-cache            禁用缓存
    --cache-ttl SECONDS   缓存过期时间 (默认: 300)
"""

import argparse
import json
import os
import sys
import io
import hashlib
import time
import threading
import traceback
from datetime import datetime
from pathlib import Path
from typing import Dict, Optional, Any, List
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

if sys.platform == 'win32':
    try:
        if hasattr(sys.stdout, 'buffer') and sys.stdout.buffer is not None:
            sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
        if hasattr(sys.stderr, 'buffer') and sys.stderr.buffer is not None:
            sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
    except (ValueError, AttributeError, IOError):
        pass

CACHE_DIR = Path(__file__).parent / 'data' / '.agent_cache'

AGENT_MAP = {
    'data': '数据分析师',
    'value': '价值分析师',
    'technical': '技术分析师',
    'macro': '宏观分析师',
    'risk': '风控官',
    'industry': '行业分析师',
    'financial_report': '财报解读专家',
    'full': '完整个股分析',
    'decision': '投资决策',
    'position': '持仓分析师',
}

CONFIDENCE_LEVELS = {0.8: '高', 0.5: '中', 0.0: '低'}


def _confidence(score: float) -> str:
    for threshold, label in CONFIDENCE_LEVELS.items():
        if score >= threshold:
            return label
    return '低'


class AgentCache:
    def __init__(self, ttl: int = 300, enabled: bool = True):
        self.ttl = ttl
        self.enabled = enabled
        if self.enabled:
            CACHE_DIR.mkdir(parents=True, exist_ok=True)

    def _key(self, stock_code: str, agent_type: str) -> str:
        raw = f"{stock_code}:{agent_type}:{datetime.now().strftime('%Y%m%d')}"
        return hashlib.md5(raw.encode()).hexdigest()

    def get(self, stock_code: str, agent_type: str) -> Optional[Dict]:
        if not self.enabled:
            return None
        path = CACHE_DIR / f"{self._key(stock_code, agent_type)}.json"
        if not path.exists():
            return None
        try:
            with open(path, 'r', encoding='utf-8') as f:
                cached = json.load(f)
            if time.time() - cached.get('timestamp', 0) > self.ttl:
                path.unlink(missing_ok=True)
                return None
            return cached['data']
        except Exception:
            return None

    def set(self, stock_code: str, agent_type: str, data: Dict):
        if not self.enabled:
            return
        path = CACHE_DIR / f"{self._key(stock_code, agent_type)}.json"
        try:
            with open(path, 'w', encoding='utf-8') as f:
                json.dump({'timestamp': time.time(), 'data': data}, f, ensure_ascii=False, default=str)
        except Exception:
            pass


class SharedDataContext:
    _instance = None
    _lock = Lock()

    def __init__(self):
        self._stock_data = {}
        self._agent_results = {}
        self._data_lock = Lock()
        self._result_lock = Lock()

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = SharedDataContext()
        return cls._instance

    @classmethod
    def reset(cls):
        cls._instance = None

    def get_stock_data(self, stock_code: str) -> Optional[Dict]:
        with self._data_lock:
            return self._stock_data.get(stock_code)

    def set_stock_data(self, stock_code: str, data: Dict):
        with self._data_lock:
            self._stock_data[stock_code] = data

    def get_agent_result(self, stock_code: str, agent_type: str) -> Optional[Dict]:
        with self._result_lock:
            return self._agent_results.get(f"{stock_code}:{agent_type}")

    def set_agent_result(self, stock_code: str, agent_type: str, result: Dict):
        with self._result_lock:
            self._agent_results[f"{stock_code}:{agent_type}"] = result


def _get_data_hub():
    from data_hub import DataHub
    return DataHub.get_instance()


def _fetch_stock_data(stock_code: str) -> Optional[Dict]:
    hub = _get_data_hub()
    return hub.get_stock_data(stock_code)


def _safe_serialize(obj: Any) -> Any:
    import pandas as pd
    import numpy as np
    if isinstance(obj, (pd.DataFrame, pd.Series)):
        return obj.to_dict(orient='records') if isinstance(obj, pd.DataFrame) else obj.tolist()
    if isinstance(obj, np.integer):
        return int(obj)
    if isinstance(obj, np.floating):
        return float(obj)
    if isinstance(obj, np.ndarray):
        return obj.tolist()
    if isinstance(obj, dict):
        return {k: _safe_serialize(v) for k, v in obj.items()}
    if isinstance(obj, (list, tuple)):
        return [_safe_serialize(i) for i in obj]
    if isinstance(obj, (datetime,)):
        return obj.isoformat()
    if isinstance(obj, float) and (obj != obj):
        return None
    return obj


def _wrap_result(agent_name: str, stock_code: str, result: Dict, confidence_score: float = 0.5) -> Dict:
    return {
        'agent': agent_name,
        'stock_code': stock_code,
        'timestamp': datetime.now().isoformat(),
        'confidence': _confidence(confidence_score),
        'confidence_score': round(confidence_score, 2),
        'result': _safe_serialize(result),
    }


def _data_quality_score(stock_data: Dict) -> float:
    if not stock_data:
        return 0.0
    financial = stock_data.get('financial', {})
    info = stock_data.get('info', {})
    critical_fields = ['pe', 'pb', 'roe', 'total_mv', 'eps']
    present = sum(1 for f in critical_fields if financial.get(f) is not None)
    base = present / len(critical_fields)
    has_historical = stock_data.get('historical') is not None
    has_info = bool(info.get('stock_name'))
    bonus = 0.1 * int(has_historical) + 0.1 * int(has_info)
    return min(base + bonus, 1.0)


def agent_data(stock_code: str, cache: AgentCache) -> Dict:
    ctx = SharedDataContext.get_instance()
    ctx_result = ctx.get_agent_result(stock_code, 'data')
    if ctx_result:
        ctx_result['cached'] = True
        return ctx_result

    cached = cache.get(stock_code, 'data')
    if cached:
        cached['cached'] = True
        ctx.set_agent_result(stock_code, 'data', cached)
        return cached

    stock_data = _fetch_stock_data(stock_code)
    if not stock_data:
        return _wrap_result('数据分析师', stock_code, {'error': f'无法获取 {stock_code} 的数据'}, 0.0)

    quality = _data_quality_score(stock_data)
    financial = stock_data.get('financial', {})
    info = stock_data.get('info', {})

    result = {
        'data_source': stock_data.get('source', 'unknown'),
        'stock_name': info.get('stock_name', '未知'),
        'quality_score': round(quality * 100, 1),
        'quality_level': _confidence(quality),
        'key_metrics': {
            'PE': financial.get('pe'),
            'PB': financial.get('pb'),
            'ROE': financial.get('roe'),
            'EPS': financial.get('eps'),
            '总市值': financial.get('total_mv'),
            '流通市值': financial.get('float_mv'),
            '股息率': financial.get('dividend_yield'),
            '毛利率': financial.get('gross_margin'),
            '净利率': financial.get('net_margin'),
        },
        'has_historical_data': stock_data.get('historical') is not None,
        'industry': info.get('industry'),
        'issues': [],
    }

    missing = [k for k, v in result['key_metrics'].items() if v is None]
    if missing:
        result['issues'].append(f'缺失指标: {", ".join(missing)}')

    wrapped = _wrap_result('数据分析师', stock_code, result, quality)
    cache.set(stock_code, 'data', wrapped)
    ctx.set_agent_result(stock_code, 'data', wrapped)
    return wrapped


def agent_value(stock_code: str, cache: AgentCache) -> Dict:
    ctx = SharedDataContext.get_instance()
    ctx_result = ctx.get_agent_result(stock_code, 'value')
    if ctx_result:
        ctx_result['cached'] = True
        return ctx_result

    cached = cache.get(stock_code, 'value')
    if cached:
        cached['cached'] = True
        ctx.set_agent_result(stock_code, 'value', cached)
        return cached

    stock_data = _fetch_stock_data(stock_code)
    if not stock_data:
        return _wrap_result('价值分析师', stock_code, {'error': f'无法获取 {stock_code} 的数据'}, 0.0)

    from graham_analyzer import GrahamAnalyzer
    from buffett_analyzer import BuffettAnalyzer
    from lynch_analyzer import LynchAnalyzer
    from munger_analyzer import MungerAnalyzer
    from dalio_analyzer import DalioAnalyzer

    graham = GrahamAnalyzer().analyze(stock_data)
    buffett = BuffettAnalyzer().analyze(stock_data)
    lynch = LynchAnalyzer().analyze(stock_data)
    munger = MungerAnalyzer().analyze(stock_data)
    dalio = DalioAnalyzer().analyze(stock_data)

    scores = {
        'graham': graham.get('graham_score', 0),
        'buffett': buffett.get('buffett_score', 0),
        'lynch': lynch.get('lynch_score', 0),
        'munger': munger.get('munger_score', 0),
        'dalio': dalio.get('dalio_score', 0),
    }

    valid_scores = [s for s in scores.values() if s and s > 0]
    avg_score = sum(valid_scores) / len(valid_scores) if valid_scores else 0

    if avg_score >= 70:
        valuation = '低估'
    elif avg_score >= 50:
        valuation = '合理'
    elif avg_score >= 30:
        valuation = '偏高'
    else:
        valuation = '高估'

    result = {
        'valuation': valuation,
        'average_score': round(avg_score, 1),
        'scores': scores,
        'graham_detail': {
            'score': graham.get('graham_score', 0),
            'safety_margin': graham.get('safety_margin'),
            'intrinsic_value': graham.get('intrinsic_value'),
            'verdict': graham.get('verdict', 'N/A'),
        },
        'buffett_detail': {
            'score': buffett.get('buffett_score', 0),
            'moat': buffett.get('moat_width', 'N/A'),
            'dcf_value': buffett.get('dcf_intrinsic_value'),
            'verdict': buffett.get('verdict', 'N/A'),
        },
        'lynch_detail': {
            'score': lynch.get('lynch_score', 0),
            'peg': lynch.get('peg_ratio'),
            'category': lynch.get('company_category', 'N/A'),
            'verdict': lynch.get('verdict', 'N/A'),
        },
        'munger_detail': {
            'score': munger.get('munger_score', 0),
            'roic': munger.get('roic'),
            'quality_rating': munger.get('quality_rating', 'N/A'),
            'verdict': munger.get('verdict', 'N/A'),
        },
        'dalio_detail': {
            'score': dalio.get('dalio_score', 0),
            'quadrant': dalio.get('all_weather_quadrant', {}).get('quadrant', 'N/A'),
            'debt_rating': dalio.get('debt_cycle_analysis', {}).get('rating', 'N/A'),
            'real_return': dalio.get('real_return_estimate', {}).get('estimated_real_return'),
            'verdict': dalio.get('verdict', 'N/A'),
        },
        'key_risks': [],
        'buy_price_range': None,
    }

    risks = []
    if graham.get('safety_margin') is not None and graham['safety_margin'] < 20:
        risks.append('安全边际不足20%')
    if buffett.get('moat_width') in ['narrow', '无']:
        risks.append('护城河较窄')
    if lynch.get('peg_ratio') is not None and lynch['peg_ratio'] > 2:
        risks.append('PEG偏高，成长性不足')
    if munger.get('roic') is not None and munger['roic'] < 10:
        risks.append('ROIC低于10%，资本效率不佳')
    dalio_real_return = dalio.get('real_return_estimate', {}).get('estimated_real_return')
    if dalio_real_return is not None and dalio_real_return < 0:
        risks.append('真实回报为负（通胀调整后亏损）')
    if dalio.get('debt_cycle_analysis', {}).get('rating') in ['差', '较差']:
        risks.append('债务周期健康度不足')
    result['key_risks'] = risks if risks else ['暂未识别重大基本面风险']

    graham_iv = graham.get('intrinsic_value')
    buffett_iv = buffett.get('intrinsic_value')
    # DCF返回的是企业总价值，需转换为每股价值才能与Graham内在价值比较
    total_share = stock_data.get('financial', {}).get('total_share')
    if buffett_iv and total_share and total_share > 0:
        buffett_per_share = buffett_iv / total_share
    else:
        buffett_per_share = None

    buy_prices = [p for p in [graham_iv, buffett_per_share] if p and p > 0]
    if buy_prices:
        low = min(buy_prices) * 0.8
        high = max(buy_prices)
        result['buy_price_range'] = {'low': round(low, 2), 'high': round(high, 2)}

    confidence = min(avg_score / 100, 1.0) if avg_score > 0 else 0.1
    wrapped = _wrap_result('价值分析师', stock_code, result, confidence)
    cache.set(stock_code, 'value', wrapped)
    ctx.set_agent_result(stock_code, 'value', wrapped)
    return wrapped


def agent_technical(stock_code: str, cache: AgentCache) -> Dict:
    ctx = SharedDataContext.get_instance()
    ctx_result = ctx.get_agent_result(stock_code, 'technical')
    if ctx_result:
        ctx_result['cached'] = True
        return ctx_result

    cached = cache.get(stock_code, 'technical')
    if cached:
        cached['cached'] = True
        ctx.set_agent_result(stock_code, 'technical', cached)
        return cached

    stock_data = _fetch_stock_data(stock_code)
    if not stock_data:
        return _wrap_result('技术分析师', stock_code, {'error': f'无法获取 {stock_code} 的数据'}, 0.0)

    from technical_analyzer import TechnicalAnalyzer
    tech = TechnicalAnalyzer().analyze(stock_data)

    composite = tech.get('composite_score', 0)
    sr = tech.get('support_resistance', {})
    dim = tech.get('dimension_scores', {})

    if composite >= 65:
        signal = '买入'
    elif composite >= 50:
        signal = '持有'
    elif composite >= 35:
        signal = '观望'
    else:
        signal = '卖出'

    result = {
        'composite_score': composite,
        'signal_strength': tech.get('signal_strength', 'N/A'),
        'market_regime': tech.get('market_regime', 'unknown'),
        'signal': signal,
        'dimension_scores': dim,
        'support_resistance': sr,
        'latest_signals': tech.get('latest_signals', {}),
        'key_levels': {
            'current_price': sr.get('current_price'),
            'support_1': sr.get('support_1'),
            'support_2': sr.get('support_2'),
            'resistance_1': sr.get('resistance_1'),
            'resistance_2': sr.get('resistance_2'),
        },
        'risk_warnings': [],
    }

    warnings = []
    rsi_val = None
    latest = tech.get('latest_signals', {})
    if latest.get('rsi_signal') == -1:
        warnings.append('RSI超买区域，注意回调风险')
    elif latest.get('rsi_signal') == 1:
        warnings.append('RSI超卖区域，可能存在反弹机会')
    if latest.get('macd_signal') == -1:
        warnings.append('MACD死叉，短期趋势偏空')
    elif latest.get('macd_signal') == 1:
        warnings.append('MACD金叉，短期趋势偏多')
    if tech.get('market_regime') == 'high_volatility_choppy':
        warnings.append('高波动震荡行情，建议降低仓位')
    result['risk_warnings'] = warnings if warnings else ['暂无特别技术风险信号']

    confidence = min(composite / 100, 1.0) if composite > 0 else 0.1
    wrapped = _wrap_result('技术分析师', stock_code, result, confidence)
    cache.set(stock_code, 'technical', wrapped)
    ctx.set_agent_result(stock_code, 'technical', wrapped)
    return wrapped


def agent_macro(stock_code: str = None, cache: AgentCache = None) -> Dict:
    ctx = SharedDataContext.get_instance()
    cache_key = stock_code or 'market'
    ctx_result = ctx.get_agent_result(cache_key, 'macro')
    if ctx_result:
        ctx_result['cached'] = True
        return ctx_result

    cached = cache.get(cache_key, 'macro')
    if cached:
        cached['cached'] = True
        ctx.set_agent_result(cache_key, 'macro', cached)
        return cached

    from market_regime import MarketRegimeAnalyzer
    regime = MarketRegimeAnalyzer().analyze()

    result = {
        'composite_regime': regime.get('composite_regime', '未知'),
        'trend_strength': regime.get('trend_strength', 0),
        'volatility_regime': regime.get('volatility_regime', '未知'),
        'recommend_position': regime.get('recommend_position', 'N/A'),
        'analysis_date': regime.get('analysis_date', datetime.now().strftime('%Y-%m-%d')),
        'index_regimes': regime.get('index_regimes', {}),
        'details': regime.get('details', []),
        'index_snapshots': regime.get('index_snapshots', {}),
        'breadth_ratio': regime.get('breadth_ratio', 0.5),
        'bullish_count': regime.get('bullish_count', 0),
        'total_index_count': regime.get('total_index_count', 0),
    }

    position = regime.get('recommend_position', 50)
    br = regime.get('breadth_ratio', 0.5)
    if isinstance(position, (int, float)):
        if position >= 70:
            env = '强牛'
        elif position >= 50:
            if br >= 0.6:
                env = '偏多震荡/结构性'
            else:
                env = '弱牛/结构性'
        elif position >= 30:
            env = '震荡'
        elif position >= 10:
            env = '弱熊'
        else:
            env = '强熊'
    else:
        env = '未知'

    result['environment'] = env

    confidence = 0.6
    if isinstance(regime.get('trend_strength'), (int, float)):
        ts = regime['trend_strength']
        confidence = min(ts / 100, 1.0) if ts > 50 else 0.4

    wrapped = _wrap_result('宏观分析师', cache_key, result, confidence)
    cache.set(cache_key, 'macro', wrapped)
    ctx.set_agent_result(cache_key, 'macro', wrapped)
    return wrapped


def agent_risk(stock_code: str, cache: AgentCache,
               value_result: Dict = None, tech_result: Dict = None, macro_result: Dict = None) -> Dict:
    ctx = SharedDataContext.get_instance()
    if not value_result and not tech_result:
        ctx_result = ctx.get_agent_result(stock_code, 'risk')
        if ctx_result:
            ctx_result['cached'] = True
            return ctx_result

    cached = cache.get(stock_code, 'risk')
    if cached and not value_result and not tech_result:
        cached['cached'] = True
        ctx.set_agent_result(stock_code, 'risk', cached)
        return cached

    if not value_result:
        value_result = agent_value(stock_code, cache)
    if not tech_result:
        tech_result = agent_technical(stock_code, cache)
    if not macro_result:
        macro_result = agent_macro(stock_code, cache)

    value_data = value_result.get('result', {})
    tech_data = tech_result.get('result', {})
    macro_data = macro_result.get('result', {})

    value_score = value_data.get('average_score', 0)
    tech_score = tech_data.get('composite_score', 0)
    position = macro_data.get('recommend_position', 50)
    if isinstance(position, str):
        try:
            position = int(position.replace('%', ''))
        except ValueError:
            position = 50

    risk_score = 0
    risk_points = []

    if value_score < 30:
        risk_score += 30
        risk_points.append(f'基本面评分偏低({value_score}分)，估值风险较高')
    elif value_score < 50:
        risk_score += 15
        risk_points.append(f'基本面评分一般({value_score}分)，安全边际不足')

    if tech_score < 35:
        risk_score += 25
        risk_points.append(f'技术面偏空({tech_score}分)，趋势向下')
    elif tech_score < 50:
        risk_score += 10
        risk_points.append(f'技术面中性偏弱({tech_score}分)')

    if position < 30:
        risk_score += 25
        risk_points.append(f'宏观环境偏弱，建议仓位仅{position}%')
    elif position < 50:
        risk_score += 10
        risk_points.append(f'宏观环境一般，建议仓位{position}%')

    value_risks = value_data.get('key_risks', [])
    tech_warnings = tech_data.get('risk_warnings', [])
    risk_points.extend(value_risks[:2])
    risk_points.extend(tech_warnings[:2])

    if risk_score >= 60:
        risk_level = '极高风险'
        max_position = 5
    elif risk_score >= 40:
        risk_level = '高风险'
        max_position = 10
    elif risk_score >= 25:
        risk_level = '中风险'
        max_position = 15
    elif risk_score >= 10:
        risk_level = '低风险'
        max_position = 20
    else:
        risk_level = '极低风险'
        max_position = 20

    current_price = tech_data.get('key_levels', {}).get('current_price')
    stop_loss = None
    if current_price:
        stop_loss = round(current_price * 0.92, 2)

    result = {
        'risk_level': risk_level,
        'risk_score': risk_score,
        'max_position_pct': max_position,
        'key_risk_points': risk_points[:6],
        'stop_loss_price': stop_loss,
        'stop_loss_pct': 8,
        'pressure_test': {
            'scenario_1_limit_down': f'单日跌停损失约10%',
            'scenario_3_limit_down': '连续3日跌停损失约27%',
            'scenario_delist': '极端情况：本金全部损失',
        },
        'value_score': value_score,
        'tech_score': tech_score,
        'macro_position': position,
    }

    confidence = 0.7 if risk_score > 0 else 0.3
    wrapped = _wrap_result('风控官', stock_code, result, confidence)
    cache.set(stock_code, 'risk', wrapped)
    ctx.set_agent_result(stock_code, 'risk', wrapped)
    return wrapped


def agent_full(stock_code: str, cache: AgentCache) -> Dict:
    data_result = agent_data(stock_code, cache)
    if 'error' in data_result.get('result', {}):
        return data_result

    value_result = agent_value(stock_code, cache)
    tech_result = agent_technical(stock_code, cache)
    macro_result = agent_macro(stock_code, cache)
    risk_result = agent_risk(stock_code, cache, value_result, tech_result, macro_result)

    value_data = value_result.get('result', {})
    tech_data = tech_result.get('result', {})
    risk_data = risk_result.get('result', {})

    value_score = value_data.get('average_score', 0)
    tech_score = tech_data.get('composite_score', 0)
    risk_level = risk_data.get('risk_level', '未知')

    if value_score >= 60 and tech_score >= 55:
        conclusion = '基本面与技术面共振，可考虑建仓'
    elif value_score >= 60 and tech_score < 45:
        conclusion = '基本面良好但时机不佳，建议等待回调'
    elif value_score < 40 and tech_score >= 55:
        conclusion = '技术面偏强但基本面偏弱，仅适合短线投机'
    else:
        conclusion = '基本面与技术面均偏弱，建议回避'

    result = {
        'conclusion': conclusion,
        'value_analysis': value_data,
        'technical_analysis': tech_data,
        'macro_analysis': macro_result.get('result', {}),
        'risk_assessment': risk_data,
        'data_quality': data_result.get('result', {}).get('quality_score', 0),
    }

    confidence = min((value_score + tech_score) / 200, 1.0)
    return _wrap_result('完整个股分析', stock_code, result, confidence)


def agent_decision(stock_code: str, cache: AgentCache) -> Dict:
    data_result = agent_data(stock_code, cache)
    if 'error' in data_result.get('result', {}):
        return data_result

    results = {}
    errors = []
    lock = Lock()

    def _run_agent(name, func, *args):
        try:
            r = func(*args)
            with lock:
                results[name] = r
        except Exception as e:
            with lock:
                errors.append(f'{name}: {str(e)}')

    with ThreadPoolExecutor(max_workers=3) as executor:
        f_value = executor.submit(agent_value, stock_code, cache)
        f_tech = executor.submit(agent_technical, stock_code, cache)
        f_macro = executor.submit(agent_macro, stock_code, cache)

        value_result = f_value.result()
        tech_result = f_tech.result()
        macro_result = f_macro.result()

    risk_result = agent_risk(stock_code, cache, value_result, tech_result, macro_result)

    value_data = value_result.get('result', {})
    tech_data = tech_result.get('result', {})
    macro_data = macro_result.get('result', {})
    risk_data = risk_result.get('result', {})

    votes = []
    value_score = value_data.get('average_score', 0)
    tech_score = tech_data.get('composite_score', 0)

    if value_score >= 50:
        votes.append('价值分析师: 偏多')
    else:
        votes.append('价值分析师: 偏空')

    if tech_score >= 50:
        votes.append('技术分析师: 偏多')
    else:
        votes.append('技术分析师: 偏空')

    position = macro_data.get('recommend_position', 50)
    if isinstance(position, (int, float)) and position >= 50:
        votes.append('宏观分析师: 偏多')
    else:
        votes.append('宏观分析师: 偏空')

    bull_count = sum(1 for v in votes if '偏多' in v)
    if bull_count >= 2:
        consensus = '多数看多'
        action = '可以考虑买入'
    elif bull_count == 1:
        consensus = '分歧较大'
        action = '建议观望'
    else:
        consensus = '多数看空'
        action = '建议回避'

    result = {
        'consensus': consensus,
        'action': action,
        'votes': votes,
        'risk_constraint': f"风控约束：单股仓位不超过{risk_data.get('max_position_pct', 20)}%，止损{risk_data.get('stop_loss_pct', 8)}%",
        'value_analysis': value_data,
        'technical_analysis': tech_data,
        'macro_analysis': macro_data,
        'risk_assessment': risk_data,
    }

    confidence = 0.5 + 0.2 * (bull_count - 1)
    confidence = min(max(confidence, 0.2), 0.9)
    return _wrap_result('投资决策', stock_code, result, confidence)


def agent_position(stock_code: str, cost_price: float, cache: AgentCache) -> Dict:
    ctx = SharedDataContext.get_instance()

    data_result = agent_data(stock_code, cache)
    if 'error' in data_result.get('result', {}):
        return _wrap_result('持仓分析师', stock_code, {'error': '数据获取失败，无法进行持仓分析'}, 0.0)

    value_result = agent_value(stock_code, cache)
    tech_result = agent_technical(stock_code, cache)
    macro_result = agent_macro(stock_code, cache)
    risk_result = agent_risk(stock_code, cache)

    value_data = value_result.get('result', {})
    tech_data = tech_result.get('result', {})
    macro_data = macro_result.get('result', {})
    risk_data = risk_result.get('result', {})

    stock_data = _fetch_stock_data(stock_code)

    from report_generator import ReportGenerator
    report_gen = ReportGenerator()
    report_path = report_gen.generate_position_report(
        stock_data=stock_data,
        cost_price=cost_price,
        graham_result=value_data.get('graham_detail', {}),
        buffett_result=value_data.get('buffett_detail', {}),
        lynch_result=value_data.get('lynch_detail', {}),
        munger_result=value_data.get('munger_detail', {}),
        dalio_result=value_data.get('dalio_detail', {}),
        technical_result=tech_data,
        market_regime=macro_data,
        risk_result=risk_data,
    )

    avg_fund = value_data.get('avg_score', 0)
    tech_score = tech_data.get('composite_score', 0)

    result = {
        'stock_code': stock_code,
        'cost_price': cost_price,
        'current_price': stock_data.get('financial', {}).get('current_price'),
        'pnl_pct': ((stock_data.get('financial', {}).get('current_price', 0) - cost_price) / cost_price * 100) if cost_price > 0 else 0,
        'value_analysis': value_data,
        'technical_analysis': tech_data,
        'macro_analysis': macro_data,
        'risk_assessment': risk_data,
        'report_path': report_path,
        'conclusion': (
            '基本面与技术面共振，持仓可继续持有' if (avg_fund >= 40 and tech_score >= 55)
            else '基本面偏弱但技术面偏强，建议轻仓持有' if (avg_fund < 40 and tech_score >= 55)
            else '技术面偏弱但基本面尚可，建议等待技术面改善' if (avg_fund >= 40 and tech_score < 55)
            else '基本面与技术面均偏弱，建议考虑减仓或止损'
        ),
    }
    confidence = 0.5 + (avg_fund + tech_score) / 400
    confidence = min(max(confidence, 0.2), 0.9)
    return _wrap_result('持仓分析师', stock_code, result, confidence)


def agent_industry(stock_code: str, cache: AgentCache) -> Dict:
    ctx = SharedDataContext.get_instance()
    ctx_result = ctx.get_agent_result(stock_code, 'industry')
    if ctx_result:
        ctx_result['cached'] = True
        return ctx_result

    cached = cache.get(stock_code, 'industry')
    if cached:
        cached['cached'] = True
        ctx.set_agent_result(stock_code, 'industry', cached)
        return cached

    stock_data = _fetch_stock_data(stock_code)
    if not stock_data:
        return _wrap_result('行业分析师', stock_code, {'error': f'无法获取 {stock_code} 的数据'}, 0.0)

    financial = stock_data.get('financial', {})
    info = stock_data.get('info', {})
    industry_name = info.get('industry', '未知')

    policy_score = 50
    strategic_keywords = ['半导体', '芯片', '新能源', '光伏', '军工', '航天', 'AI', '5G',
                          '稀土', '锂', '医药', '创新药', '高端制造', '国产替代']
    for kw in strategic_keywords:
        if kw in industry_name:
            policy_score = 80
            break

    supply_demand_score = 50
    revenue = financial.get('revenue')
    net_profit = financial.get('net_profit')
    if revenue and net_profit and revenue > 0:
        net_margin = net_profit / revenue * 100
        if net_margin > 20:
            supply_demand_score = 75
        elif net_margin > 10:
            supply_demand_score = 60
        elif net_margin > 5:
            supply_demand_score = 45
        else:
            supply_demand_score = 30

    profitability_score = 50
    roe = financial.get('roe')
    gross_margin = financial.get('gross_margin')
    if roe and gross_margin:
        if roe > 15 and gross_margin > 40:
            profitability_score = 80
        elif roe > 10 and gross_margin > 25:
            profitability_score = 65
        elif roe > 5:
            profitability_score = 45
        else:
            profitability_score = 25

    capital_score = 50
    total_mv = financial.get('total_mv')
    if total_mv:
        if total_mv > 1e11:
            capital_score = 70
        elif total_mv > 5e10:
            capital_score = 55

    prosperity_score = (
        policy_score * 0.30 +
        supply_demand_score * 0.30 +
        profitability_score * 0.25 +
        capital_score * 0.15
    )

    if prosperity_score >= 80:
        prosperity_level = '高景气'
    elif prosperity_score >= 60:
        prosperity_level = '中高景气'
    elif prosperity_score >= 40:
        prosperity_level = '中性'
    elif prosperity_score >= 20:
        prosperity_level = '低景气'
    else:
        prosperity_level = '极低景气'

    lifecycle = '成熟期'
    growth_keywords = ['新能源', 'AI', '芯片', '光伏', '储能', '创新药']
    decline_keywords = ['煤炭', '钢铁', '纺织', '传统制造']
    for kw in growth_keywords:
        if kw in industry_name:
            lifecycle = '成长期'
            break
    for kw in decline_keywords:
        if kw in industry_name:
            lifecycle = '成熟期'
            break

    competition = '竞争分散'
    total_mv_val = financial.get('total_mv', 0) or 0
    if total_mv_val > 5e11:
        competition = '寡头垄断'
    elif total_mv_val > 1e11:
        competition = '寡头竞争'

    if total_mv_val > 2e11:
        position = '龙头'
    elif total_mv_val > 5e10:
        position = '二线'
    else:
        position = '尾部'

    if prosperity_score >= 70:
        allocation = '超配'
    elif prosperity_score >= 50:
        allocation = '标配'
    else:
        allocation = '低配'

    risks = []
    if policy_score < 40:
        risks.append('政策环境不确定，行业监管风险较高')
    if supply_demand_score < 40:
        risks.append('供需关系恶化，产能过剩风险')
    if profitability_score < 40:
        risks.append('行业盈利能力偏弱，利润空间压缩')
    if not risks:
        risks.append('暂未识别重大行业风险')

    result = {
        'industry_name': industry_name,
        'industry_score': round(prosperity_score, 1),
        'prosperity_level': prosperity_level,
        'lifecycle': lifecycle,
        'competition': competition,
        'company_position': position,
        'allocation': allocation,
        'sub_scores': {
            'policy': policy_score,
            'supply_demand': supply_demand_score,
            'profitability': profitability_score,
            'capital_flow': capital_score,
        },
        'industry_risks': risks,
    }

    confidence = min(prosperity_score / 100, 0.9)
    wrapped = _wrap_result('行业分析师', stock_code, result, confidence)
    cache.set(stock_code, 'industry', wrapped)
    ctx.set_agent_result(stock_code, 'industry', wrapped)
    return wrapped


def agent_financial_report(stock_code: str, cache: AgentCache) -> Dict:
    ctx = SharedDataContext.get_instance()
    ctx_result = ctx.get_agent_result(stock_code, 'financial_report')
    if ctx_result:
        ctx_result['cached'] = True
        return ctx_result

    cached = cache.get(stock_code, 'financial_report')
    if cached:
        cached['cached'] = True
        ctx.set_agent_result(stock_code, 'financial_report', cached)
        return cached

    stock_data = _fetch_stock_data(stock_code)
    if not stock_data:
        return _wrap_result('财报解读专家', stock_code, {'error': f'无法获取 {stock_code} 的数据'}, 0.0)

    financial = stock_data.get('financial', {})
    red_flags = []

    earnings_quality_score = 50
    operating_cf = financial.get('operating_cashflow')
    net_profit = financial.get('net_profit')
    cf_np_ratio = None
    if operating_cf and net_profit and net_profit > 0:
        cf_np_ratio = operating_cf / net_profit
        if cf_np_ratio >= 1.0:
            earnings_quality_score = 85
        elif cf_np_ratio >= 0.7:
            earnings_quality_score = 65
        elif cf_np_ratio >= 0.5:
            earnings_quality_score = 40
            red_flags.append(f'经营现金流/净利润={cf_np_ratio:.2f}，盈利质量存疑')
        else:
            earnings_quality_score = 20
            red_flags.append(f'经营现金流/净利润={cf_np_ratio:.2f}，严重不匹配，利润可能虚增')

    revenue = financial.get('revenue')
    if revenue and net_profit:
        net_margin = net_profit / revenue * 100
        if net_margin > 30 and (not operating_cf or operating_cf < net_profit * 0.5):
            red_flags.append(f'净利率{net_margin:.1f}%偏高但现金流不支撑，需关注收入确认方式')

    manipulation_score = 80
    current_ratio = financial.get('current_ratio')
    debt_to_equity = financial.get('debt_to_equity')
    if debt_to_equity and debt_to_equity > 3:
        manipulation_score -= 20
        red_flags.append(f'资产负债率偏高(D/E={debt_to_equity:.2f})，关注债务风险')
    if current_ratio and current_ratio < 1:
        manipulation_score -= 15
        red_flags.append(f'流动比率仅{current_ratio:.2f}，短期偿债压力较大')

    gross_margin = financial.get('gross_margin')
    if gross_margin and gross_margin > 60 and (not operating_cf or operating_cf < net_profit * 0.6 if net_profit else False):
        manipulation_score -= 10
        red_flags.append(f'毛利率{gross_margin:.1f}%异常偏高且现金流不匹配，需验证收入真实性')

    cross_validation = '通过'
    cross_score = 80
    if operating_cf and net_profit:
        if operating_cf < net_profit * 0.5:
            cross_validation = '不通过'
            cross_score = 30
        elif operating_cf < net_profit * 0.7:
            cross_validation = '存疑'
            cross_score = 55

    sustainability_score = 50
    roe = financial.get('roe')
    roe_history = financial.get('roe_history', [])
    if roe:
        if roe > 15:
            sustainability_score += 20
        elif roe > 10:
            sustainability_score += 10
        elif roe < 5:
            sustainability_score -= 15

    if len(roe_history) >= 3:
        if all(r > 10 for r in roe_history[:3]):
            sustainability_score += 15
        elif any(r < 0 for r in roe_history[:3]):
            sustainability_score -= 20
            red_flags.append('ROE历史出现负值，盈利可持续性存疑')

    revenue_history = financial.get('revenue_history', [])
    if len(revenue_history) >= 2:
        growth_rates = []
        for i in range(min(len(revenue_history) - 1, 4)):
            if revenue_history[i + 1] and revenue_history[i] and revenue_history[i] > 0:
                growth_rates.append((revenue_history[i] - revenue_history[i + 1]) / revenue_history[i + 1] * 100)
        if growth_rates:
            avg_growth = sum(growth_rates) / len(growth_rates)
            if avg_growth > 30:
                sustainability_score += 10
            elif avg_growth < 0:
                sustainability_score -= 15
                red_flags.append(f'营收平均增速{avg_growth:.1f}%为负，增长不可持续')

    sustainability_score = max(0, min(100, sustainability_score))

    overall_quality = (
        earnings_quality_score * 0.35 +
        manipulation_score * 0.25 +
        cross_score * 0.25 +
        sustainability_score * 0.15
    )

    if overall_quality >= 80:
        quality_level = '优秀'
    elif overall_quality >= 60:
        quality_level = '良好'
    elif overall_quality >= 40:
        quality_level = '一般'
    else:
        quality_level = '较差'

    if sustainability_score >= 70:
        sustainability_level = '高'
    elif sustainability_score >= 45:
        sustainability_level = '中'
    else:
        sustainability_level = '低'

    key_findings = []
    if cf_np_ratio is not None:
        if cf_np_ratio >= 1.0:
            key_findings.append('经营现金流充沛，利润含金量高')
        else:
            key_findings.append(f'经营现金流/净利润={cf_np_ratio:.2f}，利润含金量不足')
    if roe and roe > 15:
        key_findings.append(f'ROE={roe:.1f}%，资本回报率优秀')
    elif roe and roe < 5:
        key_findings.append(f'ROE={roe:.1f}%，资本回报率偏低')
    if not key_findings:
        key_findings.append('数据有限，无法得出明确结论')

    if not red_flags:
        red_flags.append('暂未识别重大财务红旗信号')

    result = {
        'overall_quality_score': round(overall_quality, 1),
        'quality_level': quality_level,
        'sub_scores': {
            'earnings_quality': earnings_quality_score,
            'manipulation_risk': manipulation_score,
            'cross_validation': cross_score,
            'sustainability': sustainability_score,
        },
        'red_flags': red_flags,
        'cross_validation': cross_validation,
        'sustainability': sustainability_level,
        'key_findings': key_findings,
        'key_metrics': {
            'cf_to_np_ratio': round(cf_np_ratio, 2) if cf_np_ratio else None,
            'roe': roe,
            'gross_margin': gross_margin,
            'net_margin': round(net_profit / revenue * 100, 2) if revenue and net_profit and revenue > 0 else None,
            'current_ratio': current_ratio,
            'debt_to_equity': debt_to_equity,
        },
    }

    confidence = min(overall_quality / 100, 0.9)
    wrapped = _wrap_result('财报解读专家', stock_code, result, confidence)
    cache.set(stock_code, 'financial_report', wrapped)
    ctx.set_agent_result(stock_code, 'financial_report', wrapped)
    return wrapped


AGENT_FUNCTIONS = {
    'data': agent_data,
    'value': agent_value,
    'technical': agent_technical,
    'macro': agent_macro,
    'risk': agent_risk,
    'industry': agent_industry,
    'financial_report': agent_financial_report,
    'full': agent_full,
    'decision': agent_decision,
    'position': agent_position,
}


def format_text(wrapped: Dict) -> str:
    agent = wrapped.get('agent', '未知')
    stock_code = wrapped.get('stock_code', '')
    confidence = wrapped.get('confidence', '低')
    ts = wrapped.get('timestamp', '')
    result = wrapped.get('result', {})

    lines = [
        '=' * 60,
        f'Agent: {agent}',
        f'股票代码: {stock_code}',
        f'置信度: {confidence}',
        f'分析时间: {ts}',
        '=' * 60,
    ]

    if 'error' in result:
        lines.append(f'错误: {result["error"]}')
        return '\n'.join(lines)

    if agent == '数据分析师':
        lines.append(f'数据来源: {result.get("data_source", "N/A")}')
        lines.append(f'股票名称: {result.get("stock_name", "N/A")}')
        lines.append(f'数据质量: {result.get("quality_score", 0)} ({result.get("quality_level", "N/A")})')
        lines.append('')
        lines.append('关键指标:')
        for k, v in result.get('key_metrics', {}).items():
            lines.append(f'  {k}: {v if v is not None else "缺失"}')
        if result.get('issues'):
            lines.append(f'数据问题: {"; ".join(result["issues"])}')

    elif agent == '价值分析师':
        lines.append(f'估值判断: {result.get("valuation", "N/A")}')
        lines.append(f'综合评分: {result.get("average_score", 0)}')
        lines.append('')
        lines.append('五维度评分:')
        for k, v in result.get('scores', {}).items():
            lines.append(f'  {k}: {v}')
        lines.append('')
        lines.append('关键风险:')
        for r in result.get('key_risks', []):
            lines.append(f'  - {r}')
        if result.get('buy_price_range'):
            bpr = result['buy_price_range']
            lines.append(f'买入参考价: {bpr["low"]} ~ {bpr["high"]}')

    elif agent == '技术分析师':
        lines.append(f'综合评分: {result.get("composite_score", 0)}')
        lines.append(f'信号强度: {result.get("signal_strength", "N/A")}')
        lines.append(f'市场状态: {result.get("market_regime", "N/A")}')
        lines.append(f'操作建议: {result.get("signal", "N/A")}')
        lines.append('')
        kl = result.get('key_levels', {})
        lines.append('关键价位:')
        lines.append(f'  当前价: {kl.get("current_price", "N/A")}')
        lines.append(f'  支撑1: {kl.get("support_1", "N/A")}')
        lines.append(f'  支撑2: {kl.get("support_2", "N/A")}')
        lines.append(f'  阻力1: {kl.get("resistance_1", "N/A")}')
        lines.append(f'  阻力2: {kl.get("resistance_2", "N/A")}')
        lines.append('')
        lines.append('风险提醒:')
        for w in result.get('risk_warnings', []):
            lines.append(f'  - {w}')

    elif agent == '宏观分析师':
        lines.append(f'市场环境: {result.get("environment", "N/A")}')
        lines.append(f'综合判断: {result.get("composite_regime", "N/A")}')
        lines.append(f'趋势强度: {result.get("trend_strength", 0)}/100')
        lines.append(f'波动率: {result.get("volatility_regime", "N/A")}')
        lines.append(f'建议仓位: {result.get("recommend_position", "N/A")}%')
        bc = result.get('bullish_count', 0)
        tc = result.get('total_index_count', 0)
        if tc:
            br_pct = (result.get('breadth_ratio', 0) or 0) * 100
            lines.append(f'市场宽度: {bc}/{tc} 偏多（广度比率 {br_pct:.1f}%）')

    elif agent == '风控官':
        lines.append(f'风险等级: {result.get("risk_level", "N/A")}')
        lines.append(f'风险评分: {result.get("risk_score", 0)}')
        lines.append(f'最大仓位: {result.get("max_position_pct", 20)}%')
        lines.append(f'止损价: {result.get("stop_loss_price", "N/A")} (跌幅{result.get("stop_loss_pct", 8)}%)')
        lines.append('')
        lines.append('关键风险点:')
        for r in result.get('key_risk_points', []):
            lines.append(f'  - {r}')
        lines.append('')
        lines.append('压力测试:')
        for k, v in result.get('pressure_test', {}).items():
            lines.append(f'  {k}: {v}')

    elif agent == '行业分析师':
        lines.append(f'行业: {result.get("industry_name", "N/A")}')
        lines.append(f'景气度评分: {result.get("industry_score", 0)} ({result.get("prosperity_level", "N/A")})')
        lines.append(f'生命周期: {result.get("lifecycle", "N/A")}')
        lines.append(f'竞争格局: {result.get("competition", "N/A")}')
        lines.append(f'公司位置: {result.get("company_position", "N/A")}')
        lines.append(f'配置建议: {result.get("allocation", "N/A")}')
        lines.append('')
        lines.append('子维度评分:')
        for k, v in result.get('sub_scores', {}).items():
            lines.append(f'  {k}: {v}')
        lines.append('')
        lines.append('行业风险:')
        for r in result.get('industry_risks', []):
            lines.append(f'  - {r}')

    elif agent == '财报解读专家':
        lines.append(f'盈利质量评分: {result.get("overall_quality_score", 0)} ({result.get("quality_level", "N/A")})')
        lines.append(f'三表验证: {result.get("cross_validation", "N/A")}')
        lines.append(f'业绩可持续性: {result.get("sustainability", "N/A")}')
        lines.append('')
        lines.append('子维度评分:')
        for k, v in result.get('sub_scores', {}).items():
            lines.append(f'  {k}: {v}')
        lines.append('')
        lines.append('关键发现:')
        for f in result.get('key_findings', []):
            lines.append(f'  - {f}')
        lines.append('')
        lines.append('红旗信号:')
        for r in result.get('red_flags', []):
            lines.append(f'  ⚠ {r}')

    elif agent in ('完整个股分析', '投资决策'):
        if agent == '投资决策':
            lines.append(f'共识: {result.get("consensus", "N/A")}')
            lines.append(f'行动建议: {result.get("action", "N/A")}')
            lines.append(f'风控约束: {result.get("risk_constraint", "N/A")}')
            lines.append('')
            lines.append('投票:')
            for v in result.get('votes', []):
                lines.append(f'  {v}')
        else:
            lines.append(f'综合结论: {result.get("conclusion", "N/A")}')

        ra = result.get('risk_assessment', {})
        lines.append('')
        lines.append(f'风控: {ra.get("risk_level", "N/A")} | 最大仓位{ra.get("max_position_pct", 20)}%')

    return '\n'.join(lines)


def main():
    parser = argparse.ArgumentParser(
        description='Agent Bridge — 多Agent按需调用桥接器',
        formatter_class=argparse.RawDescriptionHelpFormatter,
        epilog="""
示例:
  python agent_bridge.py 600338 data              数据分析师
  python agent_bridge.py 600338 value             价值分析师
  python agent_bridge.py 600338 technical         技术分析师
  python agent_bridge.py 600338 macro             宏观分析师
  python agent_bridge.py 600338 risk              风控官
  python agent_bridge.py 600338 industry          行业分析师
  python agent_bridge.py 600338 financial_report  财报解读专家
  python agent_bridge.py 600338 full              完整个股分析
  python agent_bridge.py 600338 decision          投资决策
  python agent_bridge.py 600338 position --cost-price 21.298  持仓分析
        """
    )
    parser.add_argument('stock_code', help='股票代码 (如 600338)')
    parser.add_argument('agent_type', choices=list(AGENT_MAP.keys()), help='Agent类型')
    parser.add_argument('--format', '-f', choices=['json', 'text'], default='json', help='输出格式')
    parser.add_argument('--no-cache', action='store_true', help='禁用缓存')
    parser.add_argument('--cache-ttl', type=int, default=300, help='缓存过期秒数 (默认300)')
    parser.add_argument('--cost-price', type=float, default=None, help='持仓成本价 (position agent 必填)')

    args = parser.parse_args()

    cache = AgentCache(ttl=args.cache_ttl, enabled=not args.no_cache)
    agent_func = AGENT_FUNCTIONS[args.agent_type]

    try:
        if args.agent_type == 'position':
            if args.cost_price is None:
                print(json.dumps({'error': 'position agent 需要 --cost-price 参数 (持仓成本价)'}, ensure_ascii=False))
                return
            result = agent_func(stock_code=args.stock_code, cost_price=args.cost_price, cache=cache)
        elif args.agent_type == 'macro':
            result = agent_func(stock_code=args.stock_code, cache=cache)
        elif args.agent_type == 'risk':
            result = agent_func(stock_code=args.stock_code, cache=cache)
        else:
            result = agent_func(stock_code=args.stock_code, cache=cache)

        if args.format == 'json':
            print(json.dumps(_safe_serialize(result), ensure_ascii=False, indent=2))
        else:
            print(format_text(result))

    except Exception as e:
        error_result = {
            'agent': AGENT_MAP.get(args.agent_type, args.agent_type),
            'stock_code': args.stock_code,
            'timestamp': datetime.now().isoformat(),
            'confidence': '低',
            'result': {'error': str(e), 'traceback': traceback.format_exc()},
        }
        print(json.dumps(_safe_serialize(error_result), ensure_ascii=False, indent=2), file=sys.stderr)
        sys.exit(1)


if __name__ == '__main__':
    main()
