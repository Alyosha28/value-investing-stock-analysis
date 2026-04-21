import json
import re
import requests
import logging
from typing import Dict, Optional, Any
from config import APIConfig
from logger_config import logger

class AIAnalyzer:
    def __init__(self):
        self.api_key = APIConfig.DEEPSEEK_API_KEY
        self.api_url = APIConfig.DEEPSEEK_API_URL
        
        if not self.api_key:
            logger.warning("未设置 DEEPSEEK_API_KEY，AI 分析将不可用")
    
    def analyze(self, stock_data: Dict[str, Any], graham_result: Dict[str, Any], buffett_result: Dict[str, Any],
                lynch_result: Dict[str, Any] = None, munger_result: Dict[str, Any] = None,
                dalio_result: Dict[str, Any] = None, technical_result: Dict[str, Any] = None) -> Dict[str, Any]:
        try:
            prompt = self._build_prompt(stock_data, graham_result, buffett_result, lynch_result, munger_result, dalio_result, technical_result)
            
            response = self._call_deepseek_api(prompt)
            
            if not response:
                logger.warning("AI 分析失败，返回默认结果")
                return self._get_default_result()
            
            analysis_result = self._parse_response(response)
            
            logger.info("AI 分析完成")
            return analysis_result
            
        except Exception as e:
            logger.error(f"AI 分析失败: {e}")
            return self._get_default_result()
    
    def _build_prompt(self, stock_data: Dict[str, Any], graham_result: Dict[str, Any], buffett_result: Dict[str, Any],
                      lynch_result: Dict[str, Any] = None, munger_result: Dict[str, Any] = None,
                      dalio_result: Dict[str, Any] = None, technical_result: Dict[str, Any] = None) -> str:
        stock_info = stock_data.get('info', {})
        stock_name = stock_info.get('stock_name', '未知股票')
        stock_code = stock_info.get('stock_code', '未知代码')
        
        prompt_parts = []
        prompt_parts.append(f"请作为专业价值投资分析师，对 {stock_name}（{stock_code}）进行分析。")
        prompt_parts.append("请严格按照以下JSON格式返回分析结果：")
        prompt_parts.append("""
{
    "investment_opportunity": "投资机会描述",
    "risk_factors": "风险提示",
    "recommendation": "明确的投资建议（买入/持有/卖出）",
    "target_price_low": 目标价下限（数字）,
    "target_price_high": 目标价上限（数字）,
    "time_horizon": "建议持有时间周期",
    "confidence_level": "置信度（高/中/低）",
    "key_reasons": ["关键理由1", "关键理由2", "关键理由3"]
}
""")
        
        if graham_result:
            prompt_parts.append("\n【格雷厄姆分析】")
            prompt_parts.append(f"PE: {graham_result.get('pe', 'N/A')}, PB: {graham_result.get('pb', 'N/A')}, ROE: {graham_result.get('roe', 'N/A')}%")
            prompt_parts.append(f"评分: {graham_result.get('graham_score', 0)}/100, 安全边际: {graham_result.get('margin_of_safety', 0)}%")
            prompt_parts.append(f"内在价值: {graham_result.get('intrinsic_value', 'N/A')}")
            prompt_parts.append(f"符合标准: {'是' if graham_result.get('meets_graham_standards', False) else '否'}")
            prompt_parts.append(f"建议: {graham_result.get('suggestion', '无')}")
        
        if buffett_result:
            prompt_parts.append("\n【巴菲特分析】")
            prompt_parts.append(f"ROE: {buffett_result.get('roe', 'N/A')}%, 股息率: {buffett_result.get('dividend_yield', 0)}%")
            prompt_parts.append(f"自由现金流: {buffett_result.get('free_cashflow', 'N/A')}")
            prompt_parts.append(f"FCF增长率: {buffett_result.get('fcf_growth_rate', 'N/A')}%")
            prompt_parts.append(f"护城河: {buffett_result.get('moat_rating', '无')}, 评分: {buffett_result.get('buffett_score', 0)}/100")
            prompt_parts.append(f"内在价值(DCF): {buffett_result.get('intrinsic_value', 'N/A')}")
            prompt_parts.append(f"安全边际: {buffett_result.get('margin_of_safety', 0)}%")
            prompt_parts.append(f"债务风险: {buffett_result.get('debt_analysis', {}).get('risk_level', '未知')}")
            prompt_parts.append(f"建议: {buffett_result.get('suggestion', '无')}")

        if lynch_result:
            prompt_parts.append("\n【彼得·林奇分析】")
            prompt_parts.append(f"PE: {lynch_result.get('pe', 'N/A')}, PEG: {lynch_result.get('peg', 'N/A')}, 增长率: {lynch_result.get('growth_rate', 'N/A')}%")
            prompt_parts.append(f"公司分类: {lynch_result.get('category', '未知')}")
            prompt_parts.append(f"消费品属性: {'是' if lynch_result.get('consumer_facing') else '否'}")
            prompt_parts.append(f"评分: {lynch_result.get('lynch_score', 0)}/100")
            prompt_parts.append(f"建议: {lynch_result.get('suggestion', '无')}")

        if munger_result:
            prompt_parts.append("\n【查理·芒格分析】")
            prompt_parts.append(f"ROE: {munger_result.get('roe', 'N/A')}%, ROIC: {munger_result.get('roic', 'N/A')}%")
            prompt_parts.append(f"企业质量: {munger_result.get('quality_analysis', {}).get('rating', '未知')}")
            prompt_parts.append(f"Lollapalooza效应数: {munger_result.get('lollapalooza_score', 0)}")
            prompt_parts.append(f"评分: {munger_result.get('munger_score', 0)}/100")
            prompt_parts.append(f"建议: {munger_result.get('suggestion', '无')}")

        if dalio_result:
            prompt_parts.append("\n【瑞·达里奥分析】")
            prompt_parts.append(f"债务周期健康度: {dalio_result.get('debt_cycle_analysis', {}).get('rating', '未知')}")
            prompt_parts.append(f"盈利稳定性: {dalio_result.get('earnings_stability', {}).get('rating', '未知')}")
            prompt_parts.append(f"通胀对冲: {dalio_result.get('inflation_hedge', {}).get('rating', '弱')}")
            prompt_parts.append(f"全天候象限: {dalio_result.get('all_weather_quadrant', {}).get('quadrant', '未知')}")
            prompt_parts.append(f"评分: {dalio_result.get('dalio_score', 0)}/100")
            prompt_parts.append(f"建议: {dalio_result.get('suggestion', '无')}")

        if technical_result:
            latest_signals = technical_result.get('latest_signals', {})
            composite_score = technical_result.get('composite_score', 0)
            dimension_scores = technical_result.get('dimension_scores', {})
            market_regime = technical_result.get('market_regime', 'unknown')
            signal_strength = technical_result.get('signal_strength', '无法评估')
            sr_levels = technical_result.get('support_resistance', {})
            
            prompt_parts.append("\n【技术分析】")
            prompt_parts.append(f"综合评分: {composite_score}/100, 信号强度: {signal_strength}")
            prompt_parts.append(f"市场状态: {market_regime}")
            prompt_parts.append(f"趋势得分: {dimension_scores.get('trend', 0)}, 动量得分: {dimension_scores.get('momentum', 0)}")
            prompt_parts.append(f"波动率得分: {dimension_scores.get('volatility', 0)}, 量价得分: {dimension_scores.get('volume', 0)}")
            signal_map = {1: '买入', -1: '卖出', 0: '持有'}
            for key, name in [('ma_signal', '均线'), ('rsi_signal', 'RSI'), ('bb_signal', '布林带'), ('macd_signal', 'MACD'), ('composite_signal', '综合')]:
                signal_val = latest_signals.get(key, 0)
                prompt_parts.append(f"{name}: {signal_map.get(signal_val, '持有')}")
            
            if sr_levels:
                prompt_parts.append(f"支撑位: {sr_levels.get('support_1', 'N/A')}, 阻力位: {sr_levels.get('resistance_1', 'N/A')}")
        
        prompt_parts.append("\n请仅返回JSON格式的分析结果，不要包含其他内容。")
        
        return '\n'.join(prompt_parts)
    
    def _call_deepseek_api(self, prompt: str) -> Optional[Dict[str, Any]]:
        if not self.api_key:
            logger.error("缺少 DEEPSEEK_API_KEY")
            return None
        
        headers = {
            'Content-Type': 'application/json',
            'Authorization': f'Bearer {self.api_key}'
        }
        
        payload = {
            'model': APIConfig.DEEPSEEK_MODEL,
            'messages': [{'role': 'user', 'content': prompt}],
            'temperature': APIConfig.DEEPSEEK_TEMPERATURE,
            'max_tokens': APIConfig.DEEPSEEK_MAX_TOKENS
        }
        
        try:
            response = requests.post(self.api_url, headers=headers, json=payload, timeout=APIConfig.DEEPSEEK_TIMEOUT)
            response.raise_for_status()
            return response.json()
        except Exception as e:
            logger.error(f"调用 DeepSeek API 失败: {e}")
            return None
    
    def _parse_response(self, response: Dict[str, Any]) -> Dict[str, Any]:
        try:
            content = response.get('choices', [{}])[0].get('message', {}).get('content', '')
            
            json_match = re.search(r'\{[\s\S]*\}', content)
            if json_match:
                json_str = json_match.group()
                parsed = json.loads(json_str)
                
                return {
                    'full_analysis': content,
                    'investment_opportunity': parsed.get('investment_opportunity', '无法分析'),
                    'risk_factors': parsed.get('risk_factors', '无法分析'),
                    'recommendation': parsed.get('recommendation', '无法分析'),
                    'target_price_low': parsed.get('target_price_low'),
                    'target_price_high': parsed.get('target_price_high'),
                    'time_horizon': parsed.get('time_horizon', '无法分析'),
                    'confidence_level': parsed.get('confidence_level', '中'),
                    'key_reasons': parsed.get('key_reasons', []),
                    'is_structured': True
                }
            
            return {
                'full_analysis': content,
                'investment_opportunity': self._extract_section(content, '投资机会'),
                'risk_factors': self._extract_section(content, '风险提示'),
                'recommendation': self._extract_section(content, '投资建议'),
                'target_price_low': None,
                'target_price_high': None,
                'time_horizon': self._extract_section(content, '时间周期'),
                'confidence_level': '中',
                'key_reasons': [],
                'is_structured': False
            }
        except Exception as e:
            logger.error(f"解析 API 响应失败: {e}")
            return self._get_default_result()
    
    def _extract_section(self, content: str, section_name: str) -> str:
        lines = content.split('\n')
        section_content = []
        capture = False
        
        for line in lines:
            if section_name in line:
                capture = True
                continue
            elif capture and (line.strip().startswith('##') or line.strip().startswith('#')):
                break
            elif capture:
                section_content.append(line)
        
        return '\n'.join(section_content).strip()
    
    def _get_default_result(self) -> Dict[str, Any]:
        return {
            'full_analysis': 'AI 分析失败，无法提供详细分析',
            'investment_opportunity': '无法分析',
            'risk_factors': '无法分析',
            'recommendation': '无法分析',
            'target_price_low': None,
            'target_price_high': None,
            'time_horizon': '无法分析',
            'confidence_level': '低',
            'key_reasons': [],
            'is_structured': False
        }