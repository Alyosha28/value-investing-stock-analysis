#!/usr/bin/env python3
import json
import re
import logging
import time
import threading
import hashlib
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Optional, Any, Tuple
from threading import Lock
from concurrent.futures import ThreadPoolExecutor, as_completed

from logger_config import logger


class CollaborationLog:
    _instance = None
    _lock = Lock()

    def __init__(self, log_dir: str = None):
        if log_dir is None:
            log_dir = str(Path(__file__).parent / 'data' / '.collaboration_logs')
        self._log_dir = Path(log_dir)
        self._log_dir.mkdir(parents=True, exist_ok=True)
        self._buffer = []
        self._buffer_lock = Lock()
        self._flush_interval = 10
        self._last_flush = time.time()

    @classmethod
    def get_instance(cls, log_dir: str = None):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = CollaborationLog(log_dir)
        return cls._instance

    @classmethod
    def reset(cls):
        cls._instance = None

    def log(self, from_agent: str, to_agent: str, action: str,
            data_summary: str = None, stock_code: str = None,
            metadata: Dict = None):
        entry = {
            'timestamp': datetime.now().isoformat(),
            'from_agent': from_agent,
            'to_agent': to_agent,
            'action': action,
            'stock_code': stock_code,
            'data_summary': data_summary,
            'metadata': metadata or {},
        }
        with self._buffer_lock:
            self._buffer.append(entry)
            if len(self._buffer) >= 50 or (time.time() - self._last_flush) > self._flush_interval:
                self._flush()

    def _flush(self):
        if not self._buffer:
            return
        date_str = datetime.now().strftime('%Y%m%d')
        log_file = self._log_dir / f'collab_{date_str}.jsonl'
        try:
            with open(log_file, 'a', encoding='utf-8') as f:
                for entry in self._buffer:
                    f.write(json.dumps(entry, ensure_ascii=False, default=str) + '\n')
            self._buffer.clear()
            self._last_flush = time.time()
        except Exception as e:
            logger.warning(f"协作日志写入失败: {e}")

    def get_recent_logs(self, limit: int = 100, stock_code: str = None) -> List[Dict]:
        self._flush()
        logs = []
        log_files = sorted(self._log_dir.glob('collab_*.jsonl'), reverse=True)
        for log_file in log_files[:3]:
            try:
                with open(log_file, 'r', encoding='utf-8') as f:
                    for line in f:
                        try:
                            entry = json.loads(line.strip())
                            if stock_code and entry.get('stock_code') != stock_code:
                                continue
                            logs.append(entry)
                        except json.JSONDecodeError:
                            continue
            except Exception:
                continue
        logs.sort(key=lambda x: x.get('timestamp', ''), reverse=True)
        return logs[:limit]


class DataExchangeFormat:
    AGENT_RESULT_SCHEMA = {
        'agent': str,
        'stock_code': str,
        'timestamp': str,
        'confidence': str,
        'confidence_score': float,
        'result': dict,
        'data_dependencies': list,
        'cached': bool,
    }

    @staticmethod
    def create_transfer_packet(from_agent: str, to_agent: str,
                                data: Dict, stock_code: str = None,
                                priority: int = 5) -> Dict:
        return {
            'packet_id': hashlib.md5(
                f"{from_agent}:{to_agent}:{time.time()}".encode()
            ).hexdigest()[:12],
            'from_agent': from_agent,
            'to_agent': to_agent,
            'stock_code': stock_code,
            'timestamp': datetime.now().isoformat(),
            'priority': priority,
            'data': data,
            'checksum': DataExchangeFormat._compute_checksum(data),
        }

    @staticmethod
    def validate_packet(packet: Dict) -> Tuple[bool, str]:
        required_fields = ['packet_id', 'from_agent', 'to_agent', 'timestamp', 'data', 'checksum']
        for field in required_fields:
            if field not in packet:
                return False, f"缺少必要字段: {field}"
        expected_checksum = DataExchangeFormat._compute_checksum(packet['data'])
        if packet.get('checksum') != expected_checksum:
            return False, "数据校验失败: checksum不匹配"
        return True, "校验通过"

    @staticmethod
    def _compute_checksum(data: Any) -> str:
        data_str = json.dumps(data, sort_keys=True, ensure_ascii=False, default=str)
        return hashlib.md5(data_str.encode()).hexdigest()[:8]


class AgentCollaborationBus:
    _instance = None
    _lock = Lock()
    DEFAULT_CACHE_TTL = 300
    CLEANUP_INTERVAL = 60
    MAX_CACHE_SIZE = 500

    def __init__(self):
        self._channels = {}
        self._subscriptions = {}
        self._results_cache = {}
        self._cache_lock = Lock()
        self._channel_lock = Lock()
        self._collab_log = CollaborationLog.get_instance()
        self._conflict_resolvers = {}
        self._resolver_lock = Lock()
        self._last_cleanup = time.time()
        self._cleanup_thread = None
        self._stop_cleanup = threading.Event()
        self._start_cleanup_thread()

    def _start_cleanup_thread(self):
        def _cleanup_loop():
            while not self._stop_cleanup.is_set():
                self._stop_cleanup.wait(self.CLEANUP_INTERVAL)
                if self._stop_cleanup.is_set():
                    break
                try:
                    self._cleanup_expired_cache()
                except Exception as e:
                    logger.warning(f"缓存清理异常: {e}")
        self._cleanup_thread = threading.Thread(target=_cleanup_loop, daemon=True)
        self._cleanup_thread.start()
        logger.debug("协作总线缓存清理线程已启动")

    def _cleanup_expired_cache(self):
        now = time.time()
        expired_keys = []
        with self._cache_lock:
            for key, entry in list(self._results_cache.items()):
                if now - entry.get('timestamp', 0) > self.DEFAULT_CACHE_TTL:
                    expired_keys.append(key)
            for key in expired_keys:
                del self._results_cache[key]

            if len(self._results_cache) > self.MAX_CACHE_SIZE:
                sorted_keys = sorted(
                    self._results_cache.keys(),
                    key=lambda k: self._results_cache[k].get('timestamp', 0)
                )
                evict_count = len(self._results_cache) - self.MAX_CACHE_SIZE + 50
                for key in sorted_keys[:evict_count]:
                    del self._results_cache[key]

        if expired_keys:
            logger.debug(f"清理过期缓存: {len(expired_keys)} 条")

    def shutdown(self):
        self._stop_cleanup.set()
        if self._cleanup_thread and self._cleanup_thread.is_alive():
            self._cleanup_thread.join(timeout=5)

    @classmethod
    def get_instance(cls):
        if cls._instance is None:
            with cls._lock:
                if cls._instance is None:
                    cls._instance = AgentCollaborationBus()
        return cls._instance

    @classmethod
    def reset(cls):
        if cls._instance is not None:
            cls._instance.shutdown()
        cls._instance = None

    def publish_result(self, agent_name: str, stock_code: str,
                       result: Dict, dependencies: List[str] = None):
        with self._channel_lock:
            if stock_code not in self._channels:
                self._channels[stock_code] = {}
            self._channels[stock_code][agent_name] = result

        with self._cache_lock:
            cache_key = f"{stock_code}:{agent_name}"
            self._results_cache[cache_key] = {
                'result': result,
                'timestamp': time.time(),
                'dependencies': dependencies or [],
            }

        self._collab_log.log(
            from_agent=agent_name,
            to_agent='bus',
            action='publish',
            stock_code=stock_code,
            data_summary=f"发布 {agent_name} 分析结果",
            metadata={'dependencies': dependencies or []}
        )

        self._notify_subscribers(agent_name, stock_code, result)

    def request_data(self, requesting_agent: str, stock_code: str,
                     target_agent: str, timeout: float = 30.0) -> Optional[Dict]:
        cache_key = f"{stock_code}:{target_agent}"
        with self._cache_lock:
            cached = self._results_cache.get(cache_key)
            if cached and (time.time() - cached['timestamp']) < 300:
                self._collab_log.log(
                    from_agent=requesting_agent,
                    to_agent=target_agent,
                    action='request_cached',
                    stock_code=stock_code,
                    data_summary=f"从缓存获取 {target_agent} 数据"
                )
                return cached['result']

        start_time = time.time()
        while time.time() - start_time < timeout:
            with self._channel_lock:
                channel = self._channels.get(stock_code, {})
                if target_agent in channel:
                    result = channel[target_agent]
                    self._collab_log.log(
                        from_agent=requesting_agent,
                        to_agent=target_agent,
                        action='request_fulfilled',
                        stock_code=stock_code,
                        data_summary=f"获取 {target_agent} 数据成功"
                    )
                    return result
            time.sleep(0.5)

        self._collab_log.log(
            from_agent=requesting_agent,
            to_agent=target_agent,
            action='request_timeout',
            stock_code=stock_code,
            data_summary=f"获取 {target_agent} 数据超时"
        )
        return None

    def subscribe(self, agent_name: str, target_agent: str, callback):
        key = f"{target_agent}"
        with self._channel_lock:
            if key not in self._subscriptions:
                self._subscriptions[key] = []
            self._subscriptions[key].append((agent_name, callback))

    def _notify_subscribers(self, publisher_agent: str, stock_code: str, result: Dict):
        key = publisher_agent
        with self._channel_lock:
            subscribers = self._subscriptions.get(key, [])
        for subscriber_name, callback in subscribers:
            try:
                callback(subscriber_name, publisher_agent, stock_code, result)
                self._collab_log.log(
                    from_agent=publisher_agent,
                    to_agent=subscriber_name,
                    action='notify',
                    stock_code=stock_code,
                    data_summary=f"通知 {subscriber_name} 数据已就绪"
                )
            except Exception as e:
                logger.warning(f"通知订阅者 {subscriber_name} 失败: {e}")

    def resolve_conflict(self, stock_code: str, agent_results: Dict[str, Dict]) -> Dict:
        if not agent_results:
            return {}

        if len(agent_results) <= 1:
            return list(agent_results.values())[0] if agent_results else {}

        scores = {}
        for agent_name, result in agent_results.items():
            confidence = result.get('confidence_score', 0.5)
            has_error = 'error' in result.get('result', {})
            priority_boost = 0
            if agent_name == '风控官':
                priority_boost = 0.2
            elif agent_name == '数据分析师':
                priority_boost = 0.1
            scores[agent_name] = confidence + priority_boost - (0.5 if has_error else 0)

        best_agent = max(scores, key=scores.get)

        self._collab_log.log(
            from_agent='conflict_resolver',
            to_agent=best_agent,
            action='conflict_resolved',
            stock_code=stock_code,
            data_summary=f"冲突解决: 选择 {best_agent} (分数: {scores[best_agent]:.2f})",
            metadata={'all_scores': scores}
        )

        return agent_results[best_agent]

    def get_collaboration_status(self, stock_code: str = None) -> Dict:
        with self._channel_lock:
            if stock_code:
                return {
                    'stock_code': stock_code,
                    'completed_agents': list(self._channels.get(stock_code, {}).keys()),
                    'pending_agents': [],
                }
            return {
                'all_stocks': list(self._channels.keys()),
                'channels': {
                    k: list(v.keys()) for k, v in self._channels.items()
                },
            }


class RouteResult:
    def __init__(self, agent_type: str, agent_name: str, confidence: float = 1.0,
                 matched_keywords: List[str] = None, matched_pattern: str = None,
                 is_multi_agent: bool = False, scenario: str = None):
        self.agent_type = agent_type
        self.agent_name = agent_name
        self.confidence = confidence
        self.matched_keywords = matched_keywords or []
        self.matched_pattern = matched_pattern
        self.is_multi_agent = is_multi_agent
        self.scenario = scenario

    def to_dict(self) -> Dict:
        return {
            'agent_type': self.agent_type,
            'agent_name': self.agent_name,
            'confidence': self.confidence,
            'matched_keywords': self.matched_keywords,
            'matched_pattern': self.matched_pattern,
            'is_multi_agent': self.is_multi_agent,
            'scenario': self.scenario,
        }


class AgentRouter:
    DEFAULT_CONFIG_PATH = Path(__file__).parent / '.trae' / 'router.json'

    def __init__(self, config_path: str = None):
        self._config_path = Path(config_path) if config_path else self.DEFAULT_CONFIG_PATH
        self._config = self._load_config()
        self._agents = {}
        self._multi_agent_scenarios = {}
        self._fallback_chain = []
        self._default_agent = 'value'
        self._parse_config()
        self._collab_bus = AgentCollaborationBus.get_instance()
        logger.info(f"AgentRouter 初始化完成，已注册 {len(self._agents)} 个Agent")

    def _load_config(self) -> Dict:
        try:
            if self._config_path.exists():
                with open(self._config_path, 'r', encoding='utf-8') as f:
                    return json.load(f)
        except Exception as e:
            logger.warning(f"加载路由配置失败: {e}，使用默认配置")
        return self._get_default_config()

    def _get_default_config(self) -> Dict:
        return {
            'default_agent': '价值分析师',
            'routing_rules': [
                {'priority': 1, 'agent': '数据分析师',
                 'trigger_keywords': ['数据', '抓取', '获取', '数据源', '财务数据'],
                 'trigger_patterns': [r'^获取.*数据$', r'.*数据.*质量']},
                {'priority': 2, 'agent': '价值分析师',
                 'trigger_keywords': ['估值', '价值', '基本面', '分析', '股票'],
                 'trigger_patterns': [r'^\d{6}$', r'.*估值.*']},
                {'priority': 3, 'agent': '技术分析师',
                 'trigger_keywords': ['技术面', 'K线', '趋势', '均线', 'MACD'],
                 'trigger_patterns': [r'.*技术面.*', r'.*K线.*']},
                {'priority': 4, 'agent': '宏观分析师',
                 'trigger_keywords': ['大盘', '行情', '仓位', '宏观'],
                 'trigger_patterns': [r'.*大盘.*', r'.*行情.*']},
                {'priority': 5, 'agent': '行业分析师',
                 'trigger_keywords': ['行业', '板块', '赛道', '景气度'],
                 'trigger_patterns': [r'.*行业.*', r'.*板块.*']},
                {'priority': 6, 'agent': '财报解读专家',
                 'trigger_keywords': ['财报', '年报', '季报', '报表', '盈利质量'],
                 'trigger_patterns': [r'.*财报.*', r'.*年报.*']},
                {'priority': 7, 'agent': '风控官',
                 'trigger_keywords': ['风险', '止损', '回撤'],
                 'trigger_patterns': [r'.*风险.*', r'.*止损.*'],
                 'co_trigger': True},
                {'priority': 8, 'agent': '趋势分析师',
                 'trigger_keywords': ['趋势', 'K线', '拐点', '转折', '技术图表', '走势'],
                 'trigger_patterns': [r'.*趋势分.*', r'.*K线.*', r'.*走势分.*']},
            ],
            'multi_agent_scenarios': [
                {'name': '完整个股分析', 'agents': ['数据分析师', '价值分析师', '技术分析师', '风控官'],
                 'execution_order': 'sequential'},
                {'name': '投资决策', 'agents': ['价值分析师', '技术分析师', '宏观分析师', '风控官'],
                 'execution_order': 'parallel_then_merge'},
            ],
            'fallback_chain': ['价值分析师', '宏观分析师', '数据分析师'],
        }

    def _parse_config(self):
        agent_name_to_type = {
            '数据分析师': 'data',
            '价值分析师': 'value',
            '技术分析师': 'technical',
            '宏观分析师': 'macro',
            '行业分析师': 'industry',
            '财报解读专家': 'financial_report',
            '风控官': 'risk',
            '趋势分析师': 'trend',
        }

        self._default_agent = agent_name_to_type.get(
            self._config.get('default_agent', '价值分析师'), 'value'
        )

        for rule in self._config.get('routing_rules', []):
            agent_name = rule.get('agent', '')
            agent_type = agent_name_to_type.get(agent_name, '')
            if not agent_type:
                continue
            self._agents[agent_type] = {
                'name': agent_name,
                'type': agent_type,
                'priority': rule.get('priority', 99),
                'keywords': rule.get('trigger_keywords', []),
                'patterns': [re.compile(p) for p in rule.get('trigger_patterns', [])],
                'co_trigger': rule.get('co_trigger', False),
            }

        for scenario in self._config.get('multi_agent_scenarios', []):
            self._multi_agent_scenarios[scenario['name']] = {
                'agents': [agent_name_to_type.get(a, a) for a in scenario.get('agents', [])],
                'execution_order': scenario.get('execution_order', 'sequential'),
                'merge_strategy': scenario.get('merge_strategy', 'consensus'),
            }

        self._fallback_chain = [
            agent_name_to_type.get(a, a)
            for a in self._config.get('fallback_chain', ['价值分析师'])
        ]

    def route(self, user_input: str) -> RouteResult:
        if not user_input or not user_input.strip():
            return RouteResult(
                agent_type=self._default_agent,
                agent_name=self._get_agent_name(self._default_agent),
                confidence=0.3,
            )

        user_input = user_input.strip()

        stock_code = self._extract_stock_code(user_input)
        if stock_code and len(user_input.strip()) == 6 and user_input.strip().isdigit():
            return RouteResult(
                agent_type='value',
                agent_name='价值分析师',
                confidence=1.0,
                matched_keywords=[stock_code],
                matched_pattern='^\\d{6}$',
            )

        multi_scenario = self._detect_multi_agent_scenario(user_input)
        if multi_scenario:
            return RouteResult(
                agent_type='multi',
                agent_name=multi_scenario,
                confidence=0.9,
                is_multi_agent=True,
                scenario=multi_scenario,
            )

        candidates = []
        for agent_type, agent_info in self._agents.items():
            score = 0
            matched_kw = []
            for kw in agent_info['keywords']:
                if kw in user_input:
                    score += 2
                    matched_kw.append(kw)

            matched_pattern = None
            for pattern in agent_info['patterns']:
                if pattern.search(user_input):
                    score += 3
                    matched_pattern = pattern.pattern

            if score > 0:
                candidates.append((
                    agent_type, agent_info['name'], score,
                    matched_kw, matched_pattern, agent_info.get('co_trigger', False)
                ))

        if not candidates:
            return RouteResult(
                agent_type=self._default_agent,
                agent_name=self._get_agent_name(self._default_agent),
                confidence=0.3,
            )

        candidates.sort(key=lambda x: (-x[2], self._agents.get(x[0], {}).get('priority', 99)))

        best = candidates[0]
        confidence = min(best[2] / 10.0, 1.0)

        co_trigger_agents = [c for c in candidates if c[5]]
        result = RouteResult(
            agent_type=best[0],
            agent_name=best[1],
            confidence=confidence,
            matched_keywords=best[3],
            matched_pattern=best[4],
        )

        if co_trigger_agents:
            result.co_trigger_agents = [
                {'type': c[0], 'name': c[1]} for c in co_trigger_agents
            ]

        return result

    def route_multi(self, user_input: str) -> List[RouteResult]:
        route_result = self.route(user_input)

        if route_result.is_multi_agent and route_result.scenario:
            scenario = self._multi_agent_scenarios.get(route_result.scenario, {})
            agents = scenario.get('agents', [])
            return [
                RouteResult(
                    agent_type=a,
                    agent_name=self._get_agent_name(a),
                    confidence=0.8,
                    is_multi_agent=True,
                    scenario=route_result.scenario,
                )
                for a in agents
            ]

        results = [route_result]
        if hasattr(route_result, 'co_trigger_agents'):
            for co_agent in route_result.co_trigger_agents:
                results.append(RouteResult(
                    agent_type=co_agent['type'],
                    agent_name=co_agent['name'],
                    confidence=0.7,
                ))

        return results

    def get_scenario_config(self, scenario_name: str) -> Optional[Dict]:
        return self._multi_agent_scenarios.get(scenario_name)

    def get_all_agents(self) -> Dict:
        return {
            agent_type: {
                'name': info['name'],
                'type': info['type'],
                'priority': info['priority'],
                'keywords': info['keywords'],
                'co_trigger': info.get('co_trigger', False),
            }
            for agent_type, info in self._agents.items()
        }

    def _extract_stock_code(self, text: str) -> Optional[str]:
        match = re.search(r'\b(\d{6})\b', text)
        if match:
            code = match.group(1)
            if code.startswith(('0', '3', '6')):
                return code
        return None

    def _detect_multi_agent_scenario(self, user_input: str) -> Optional[str]:
        full_analysis_keywords = ['全面分析', '深度分析', '完整分析', '综合分析', '详细分析']
        decision_keywords = ['能不能买', '值得投资', '买入决策', '投资决策', '该不该买']
        data_check_keywords = ['数据异常', '数据矛盾', '数据排查']

        for kw in full_analysis_keywords:
            if kw in user_input:
                return '完整个股分析'
        for kw in decision_keywords:
            if kw in user_input:
                return '投资决策'
        for kw in data_check_keywords:
            if kw in user_input:
                return '数据质量排查'
        return None

    def _get_agent_name(self, agent_type: str) -> str:
        return self._agents.get(agent_type, {}).get('name', agent_type)


class AgentOrchestrator:
    def __init__(self, router: AgentRouter = None):
        self._router = router or AgentRouter()
        self._collab_bus = AgentCollaborationBus.get_instance()
        self._collab_log = CollaborationLog.get_instance()

    def analyze(self, stock_code: str, user_input: str = None,
                agent_type: str = None, cache_ttl: int = 300,
                no_cache: bool = False) -> Dict:
        from agent_bridge import (
            AgentCache, agent_data, agent_value, agent_technical,
            agent_macro, agent_risk, agent_full, agent_decision,
            agent_industry, agent_financial_report,
        )

        cache = AgentCache(ttl=cache_ttl, enabled=not no_cache)

        if agent_type:
            return self._execute_single_agent(
                stock_code, agent_type, cache
            )

        if not user_input:
            user_input = stock_code

        route_result = self._router.route(user_input)

        if route_result.is_multi_agent and route_result.scenario:
            return self._execute_multi_agent_scenario(
                stock_code, route_result.scenario, cache
            )

        result = self._execute_single_agent(
            stock_code, route_result.agent_type, cache
        )

        if hasattr(route_result, 'co_trigger_agents'):
            for co_agent in route_result.co_trigger_agents:
                co_result = self._execute_single_agent(
                    stock_code, co_agent['type'], cache
                )
                result['co_analysis'] = result.get('co_analysis', [])
                result['co_analysis'].append(co_result)

        return result

    def _execute_single_agent(self, stock_code: str, agent_type: str,
                               cache) -> Dict:
        from agent_bridge import (
            AgentCache, agent_data, agent_value, agent_technical,
            agent_macro, agent_risk, agent_industry, agent_financial_report,
        )

        agent_map = {
            'data': agent_data,
            'value': agent_value,
            'technical': agent_technical,
            'macro': agent_macro,
            'risk': agent_risk,
            'industry': agent_industry,
            'financial_report': agent_financial_report,
        }

        agent_func = agent_map.get(agent_type)
        if not agent_func:
            logger.error(f"未知Agent类型: {agent_type}")
            return {
                'agent': agent_type,
                'stock_code': stock_code,
                'timestamp': datetime.now().isoformat(),
                'confidence': '低',
                'confidence_score': 0.0,
                'result': {'error': f'未知Agent类型: {agent_type}'},
            }

        self._collab_log.log(
            from_agent='orchestrator',
            to_agent=agent_type,
            action='dispatch',
            stock_code=stock_code,
            data_summary=f"分发任务到 {agent_type}"
        )

        try:
            if agent_type == 'risk':
                value_result = self._collab_bus.request_data(
                    'risk', stock_code, 'value', timeout=0.5
                )
                tech_result = self._collab_bus.request_data(
                    'risk', stock_code, 'technical', timeout=0.5
                )
                macro_result = self._collab_bus.request_data(
                    'risk', stock_code, 'macro', timeout=0.5
                )
                result = agent_func(
                    stock_code=stock_code, cache=cache,
                    value_result=value_result,
                    tech_result=tech_result,
                    macro_result=macro_result,
                )
            else:
                result = agent_func(stock_code=stock_code, cache=cache)

            self._collab_bus.publish_result(
                agent_name=agent_type,
                stock_code=stock_code,
                result=result,
            )

            return result
        except Exception as e:
            logger.error(f"Agent {agent_type} 执行失败: {e}")
            return {
                'agent': agent_type,
                'stock_code': stock_code,
                'timestamp': datetime.now().isoformat(),
                'confidence': '低',
                'confidence_score': 0.0,
                'result': {'error': str(e)},
            }

    def _execute_multi_agent_scenario(self, stock_code: str,
                                       scenario_name: str, cache) -> Dict:
        scenario = self._router.get_scenario_config(scenario_name)
        if not scenario:
            logger.error(f"未知协作场景: {scenario_name}")
            return {'error': f'未知协作场景: {scenario_name}'}

        agents = scenario.get('agents', [])
        execution_order = scenario.get('execution_order', 'sequential')

        self._collab_log.log(
            from_agent='orchestrator',
            to_agent='multi',
            action='scenario_start',
            stock_code=stock_code,
            data_summary=f"启动协作场景: {scenario_name}, 模式: {execution_order}",
            metadata={'agents': agents}
        )

        if execution_order == 'sequential':
            return self._execute_sequential(stock_code, agents, cache, scenario_name)
        elif execution_order == 'parallel_then_merge':
            return self._execute_parallel_merge(stock_code, agents, cache, scenario_name)
        else:
            return self._execute_sequential(stock_code, agents, cache, scenario_name)

    def _execute_sequential(self, stock_code: str, agents: List[str],
                             cache, scenario_name: str) -> Dict:
        results = {}
        for agent_type in agents:
            result = self._execute_single_agent(stock_code, agent_type, cache)
            results[agent_type] = result

        return self._merge_results(stock_code, results, scenario_name)

    def _execute_parallel_merge(self, stock_code: str, agents: List[str],
                                 cache, scenario_name: str) -> Dict:
        results = {}
        errors = []
        lock = Lock()

        merge_agents = [a for a in agents if a != 'risk']
        has_risk = 'risk' in agents

        def _run(agent_type):
            try:
                r = self._execute_single_agent(stock_code, agent_type, cache)
                with lock:
                    results[agent_type] = r
            except Exception as e:
                with lock:
                    errors.append(f'{agent_type}: {str(e)}')

        with ThreadPoolExecutor(max_workers=min(len(merge_agents), 4)) as executor:
            futures = [executor.submit(_run, a) for a in merge_agents]
            for f in as_completed(futures):
                try:
                    f.result()
                except Exception as e:
                    logger.error(f"并行执行异常: {e}")

        if has_risk:
            from agent_bridge import agent_risk
            value_result = results.get('value')
            tech_result = results.get('technical')
            macro_result = results.get('macro')
            risk_result = agent_risk(
                stock_code, cache,
                value_result=value_result,
                tech_result=tech_result,
                macro_result=macro_result,
            )
            results['risk'] = risk_result

        return self._merge_results(stock_code, results, scenario_name)

    def _merge_results(self, stock_code: str, results: Dict[str, Dict],
                        scenario_name: str) -> Dict:
        merged = {
            'scenario': scenario_name,
            'stock_code': stock_code,
            'timestamp': datetime.now().isoformat(),
            'agents_executed': list(results.keys()),
            'agent_results': results,
        }

        if scenario_name == '投资决策':
            merged.update(self._merge_decision(results))
        elif scenario_name == '完整个股分析':
            merged.update(self._merge_full_analysis(results))
        else:
            merged.update(self._merge_generic(results))

        return merged

    def _merge_decision(self, results: Dict) -> Dict:
        votes = []
        value_data = results.get('value', {}).get('result', {})
        tech_data = results.get('technical', {}).get('result', {})
        macro_data = results.get('macro', {}).get('result', {})
        risk_data = results.get('risk', {}).get('result', {})

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

        industry_data = results.get('industry', {}).get('result', {})
        if industry_data.get('industry_score', 0) >= 60:
            votes.append('行业分析师: 偏多')
        else:
            votes.append('行业分析师: 偏空')

        bull_count = sum(1 for v in votes if '偏多' in v)
        if bull_count >= 3:
            consensus = '多数看多'
            action = '可以考虑买入'
        elif bull_count >= 2:
            consensus = '谨慎偏多'
            action = '可小仓位试探'
        elif bull_count == 1:
            consensus = '分歧较大'
            action = '建议观望'
        else:
            consensus = '多数看空'
            action = '建议回避'

        return {
            'consensus': consensus,
            'action': action,
            'votes': votes,
            'risk_constraint': f"风控约束：单股仓位不超过{risk_data.get('max_position_pct', 20)}%，"
                               f"止损{risk_data.get('stop_loss_pct', 8)}%",
        }

    def _merge_full_analysis(self, results: Dict) -> Dict:
        value_data = results.get('value', {}).get('result', {})
        tech_data = results.get('technical', {}).get('result', {})
        risk_data = results.get('risk', {}).get('result', {})

        value_score = value_data.get('average_score', 0)
        tech_score = tech_data.get('composite_score', 0)

        if value_score >= 60 and tech_score >= 55:
            conclusion = '基本面与技术面共振，可考虑建仓'
        elif value_score >= 60 and tech_score < 45:
            conclusion = '基本面良好但时机不佳，建议等待回调'
        elif value_score < 40 and tech_score >= 55:
            conclusion = '技术面偏强但基本面偏弱，仅适合短线投机'
        else:
            conclusion = '基本面与技术面均偏弱，建议回避'

        return {
            'conclusion': conclusion,
            'risk_level': risk_data.get('risk_level', '未知'),
        }

    def _merge_generic(self, results: Dict) -> Dict:
        return {
            'summary': f"共执行 {len(results)} 个Agent分析",
        }
