import logging
from typing import Dict, Optional, Any, List
from logger_config import logger
from config import GrahamThresholds, calculate_cagr

class GrahamAnalyzer:
    """
    格雷厄姆价值投资分析器
    
    核心依据：《聪明的投资者》第14版 - 防御型投资者标准
    
    格雷厄姆的核心思想：
    1. 安全边际是价值投资的核心
    2. 买入价格必须显著低于内在价值
    3. 关注企业的基本面而非市场波动
    4. 分散投资以降低风险
    
    内在价值公式：V = EPS × (8.5 + 2g) × (4.4 / Y)
    8.5 = 零增长企业的合理PE
    2g = 增长率调整
    4.4 = 1962年AAA级企业债利率(基准)
    Y = 当前AAA级企业债利率
    """
    
    def analyze(self, stock_data: Dict[str, Any]) -> Dict[str, Any]:
        try:
            financial_data = stock_data.get('financial', {})
            
            if not financial_data:
                logger.warning("没有财务数据，无法进行格雷厄姆分析")
                return self._get_empty_result()
            
            pe = financial_data.get('pe')
            pb = financial_data.get('pb')
            roe = financial_data.get('roe')
            total_mv = financial_data.get('total_mv')
            gross_margin = financial_data.get('gross_margin')
            net_margin = financial_data.get('net_margin')
            eps = financial_data.get('eps')
            earnings_history = financial_data.get('earnings_history', [])
            dividend_history = financial_data.get('dividend_history', [])
            current_ratio = financial_data.get('current_ratio')
            debt_to_equity = financial_data.get('debt_to_equity')
            
            criteria_results = self._check_defensive_criteria(
                pe, pb, roe, total_mv, gross_margin, net_margin,
                current_ratio, debt_to_equity, earnings_history, dividend_history
            )
            
            intrinsic_value = self._calculate_intrinsic_value(
                eps, earnings_history
            )
            
            current_price = financial_data.get('current_price')
            margin_of_safety = self._calculate_margin_of_safety(
                intrinsic_value, current_price
            )
            
            score = self._calculate_score(criteria_results, margin_of_safety)
            
            suggestion = self._get_suggestion(score, margin_of_safety, criteria_results)
            
            analysis_result = {
                'pe': pe,
                'pb': pb,
                'roe': roe,
                'total_mv': total_mv,
                'eps': eps,
                'current_price': current_price,
                'current_ratio': current_ratio,
                'debt_to_equity': debt_to_equity,
                'graham_score': score,
                'meets_graham_standards': criteria_results['passed_count'] >= 5,
                'margin_of_safety': margin_of_safety,
                'intrinsic_value': intrinsic_value,
                'criteria_details': criteria_results,
                'suggestion': suggestion
            }
            
            logger.info(f"格雷厄姆分析完成 - PE:{pe}, PB:{pb}, 内在价值:{intrinsic_value}, 评分:{score}")
            return analysis_result
            
        except Exception as e:
            logger.error(f"格雷厄姆分析失败: {e}")
            return self._get_empty_result()
    
    def _check_defensive_criteria(self, pe, pb, roe, total_mv, gross_margin, net_margin,
                                   current_ratio, debt_to_equity, earnings_history, dividend_history):
        criteria = {
            'passed_count': 0,
            'total_criteria': 9,
            'details': []
        }
        
        if total_mv and total_mv >= 1e9:
            criteria['passed_count'] += 1
            criteria['details'].append(f"✓ 企业规模: 市值{total_mv/1e8:.0f}亿 ≥ 10亿")
        else:
            criteria['details'].append(f"✗ 企业规模: 市值不足10亿")
        
        if pe and pe > 0:
            if pe <= GrahamThresholds.PE_MAX:
                criteria['passed_count'] += 1
                criteria['details'].append(f"✓ 市盈率: PE={pe:.2f} ≤ {GrahamThresholds.PE_MAX}")
            else:
                criteria['details'].append(f"✗ 市盈率: PE={pe:.2f} > {GrahamThresholds.PE_MAX}")
        else:
            criteria['details'].append(f"✗ 市盈率: 数据缺失或为负")
        
        if pb and pb > 0:
            if pb <= GrahamThresholds.PB_MAX:
                criteria['passed_count'] += 1
                criteria['details'].append(f"✓ 市净率: PB={pb:.2f} ≤ {GrahamThresholds.PB_MAX}")
            else:
                criteria['details'].append(f"✗ 市净率: PB={pb:.2f} > {GrahamThresholds.PB_MAX}")
        else:
            criteria['details'].append(f"✗ 市净率: 数据缺失")
        
        if pe and pb and pe > 0 and pb > 0:
            product = pe * pb
            if product <= GrahamThresholds.GRAHAM_NUMBER_MULTIPLIER:
                criteria['passed_count'] += 1
                criteria['details'].append(f"✓ PE×PB: {product:.2f} ≤ {GrahamThresholds.GRAHAM_NUMBER_MULTIPLIER}")
            else:
                criteria['details'].append(f"✗ PE×PB: {product:.2f} > {GrahamThresholds.GRAHAM_NUMBER_MULTIPLIER}")
        else:
            criteria['details'].append(f"✗ PE×PB: 数据缺失")
        
        if roe and roe >= 12:
            criteria['passed_count'] += 1
            criteria['details'].append(f"✓ 盈利能力: ROE={roe:.2f}% ≥ 12%")
        else:
            criteria['details'].append(f"✗ 盈利能力: ROE={roe}% < 12%")
        
        if current_ratio and current_ratio >= GrahamThresholds.CURRENT_RATIO_MIN:
            criteria['passed_count'] += 1
            criteria['details'].append(f"✓ 流动比率: {current_ratio:.2f} ≥ {GrahamThresholds.CURRENT_RATIO_MIN}")
        elif current_ratio:
            criteria['details'].append(f"✗ 流动比率: {current_ratio:.2f} < {GrahamThresholds.CURRENT_RATIO_MIN}")
        else:
            criteria['details'].append(f"○ 流动比率: 数据缺失")
        
        if debt_to_equity and debt_to_equity <= GrahamThresholds.DEBT_TO_EQUITY_MAX:
            criteria['passed_count'] += 1
            criteria['details'].append(f"✓ 债务权益比: {debt_to_equity:.2f} ≤ {GrahamThresholds.DEBT_TO_EQUITY_MAX}")
        elif debt_to_equity:
            criteria['details'].append(f"✗ 债务权益比: {debt_to_equity:.2f} > {GrahamThresholds.DEBT_TO_EQUITY_MAX}")
        else:
            criteria['details'].append(f"○ 债务权益比: 数据缺失")
        
        if earnings_history and len(earnings_history) >= GrahamThresholds.EARNINGS_YEARS_REQUIRED:
            earnings_stable = all(e > 0 for e in earnings_history[-GrahamThresholds.EARNINGS_YEARS_REQUIRED:])
            if earnings_stable:
                criteria['passed_count'] += 1
                criteria['details'].append(f"✓ 盈利稳定性: 近{GrahamThresholds.EARNINGS_YEARS_REQUIRED}年盈利均为正")
            else:
                criteria['details'].append(f"✗ 盈利稳定性: 近{GrahamThresholds.EARNINGS_YEARS_REQUIRED}年存在亏损年份")
        else:
            criteria['details'].append(f"○ 盈利稳定性: 历史数据不足{GrahamThresholds.EARNINGS_YEARS_REQUIRED}年")
        
        if dividend_history and len(dividend_history) >= 5:
            dividends_paid = sum(1 for d in dividend_history[-5:] if d and d > 0)
            if dividends_paid >= 5:
                criteria['passed_count'] += 1
                criteria['details'].append(f"✓ 分红记录: 近5年连续分红")
            else:
                criteria['details'].append(f"✗ 分红记录: 近5年分红{dividends_paid}/5年")
        else:
            criteria['details'].append(f"○ 分红记录: 历史数据不足")
        
        return criteria
    
    def _calculate_intrinsic_value(self, eps, earnings_history):
        """
        格雷厄姆内在价值计算公式
        V = EPS × (8.5 + 2g) × (4.4 / Y)
        """
        if not eps or eps <= 0:
            return None
        
        growth_rate = self._calculate_earnings_growth(earnings_history)
        
        if growth_rate is None:
            growth_rate = 5
        
        growth_rate = min(max(growth_rate, 0), 15)
        
        base_value = eps * (8.5 + 2 * growth_rate)
        
        aaa_bond_yield = 4.4
        interest_adjustment = GrahamThresholds.AAA_BOND_YIELD_DEFAULT / aaa_bond_yield
        
        intrinsic_value = base_value * interest_adjustment
        
        logger.info(f"内在价值计算 - EPS:{eps}, 增长率:{growth_rate:.1f}%, 内在价值:{intrinsic_value:.2f}")
        
        return round(intrinsic_value, 2)
    
    def _calculate_earnings_growth(self, earnings_history):
        return calculate_cagr(earnings_history)
    
    def _calculate_margin_of_safety(self, intrinsic_value, current_price):
        """
        安全边际 = (内在价值 - 当前价格) / 内在价值 × 100%
        格雷厄姆要求安全边际至少33%
        """
        if not intrinsic_value or not current_price or current_price <= 0:
            return 0.0
        
        margin = (intrinsic_value - current_price) / intrinsic_value * 100
        
        return round(max(0, margin), 2)
    
    def _calculate_score(self, criteria, margin_of_safety):
        base_score = (criteria['passed_count'] / criteria['total_criteria']) * 70
        
        bonus = 0
        if margin_of_safety >= GrahamThresholds.MARGIN_SAFETY_HIGH:
            bonus = 30
        elif margin_of_safety >= GrahamThresholds.MARGIN_SAFETY_MEDIUM:
            bonus = 25
        elif margin_of_safety >= GrahamThresholds.MARGIN_SAFETY_MINIMUM:
            bonus = 15
        elif margin_of_safety >= 10:
            bonus = 5
        
        return min(int(base_score + bonus), 100)
    
    def _get_suggestion(self, score, margin_of_safety, criteria):
        passed = criteria['passed_count']
        total = criteria['total_criteria']
        
        if score >= 80 and margin_of_safety >= GrahamThresholds.MARGIN_SAFETY_MEDIUM:
            return f"强烈建议买入 - 符合{passed}/{total}项标准，安全边际{margin_of_safety:.0f}%"
        elif score >= 65 and margin_of_safety >= GrahamThresholds.MARGIN_SAFETY_MINIMUM:
            return f"建议买入 - 符合{passed}/{total}项标准，有安全边际"
        elif score >= 50:
            return f"考虑买入 - 符合{passed}/{total}项标准"
        elif score >= 35:
            return f"关注 - 符合{passed}/{total}项标准"
        else:
            return f"不建议买入 - 仅符合{passed}/{total}项标准"
    
    def _get_empty_result(self):
        return {
            'pe': None,
            'pb': None,
            'roe': None,
            'total_mv': None,
            'eps': None,
            'current_price': None,
            'current_ratio': None,
            'debt_to_equity': None,
            'graham_score': 0,
            'meets_graham_standards': False,
            'margin_of_safety': 0.0,
            'intrinsic_value': None,
            'criteria_details': {'passed_count': 0, 'total_criteria': 9, 'details': []},
            'suggestion': "无法分析 - 数据不足"
        }