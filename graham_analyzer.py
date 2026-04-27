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
                'meets_graham_standards': criteria_results.get('criteria_score', 0) >= 60,
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
        """
        防御型投资者检查——连续评分而非二元通过/失败

        每个指标映射到 0-100 分（与基准值对齐），最终平均得到 criteria_score。
        """
        scores = []
        details = []

        def _norm(val, good, bad):
            """0-100连续评分：val==good→100，val==bad→0，中间线性插值"""
            if val is None:
                return None
            score = (val - bad) / (good - bad) * 100
            return max(0, min(100, int(score)))

        # 1) 市值（越大越好）
        if total_mv and total_mv > 0:
            s = _norm(total_mv, 1e10, 1e9)  # 100亿→100, 10亿→0
            mv_str = f"市值 {total_mv/1e8:.0f}亿 → {s}/100"
        else:
            s = None
            mv_str = "数据缺失"
        if s is not None:
            scores.append(s)
        details.append(f"{'✓' if s and s >= 50 else '○' if s is None else '✗'} 企业规模: {mv_str}")

        # 2) PE（越低越好，15为中性）
        if pe and pe > 0:
            s = _norm(pe, 0, 30)  # PE=0→100, PE=30→0
            pe_str = f"PE={pe:.2f} → {s}/100"
        else:
            s = None
            pe_str = "数据缺失或为负"
        if s is not None:
            scores.append(s)
        details.append(f"{'✓' if s and s >= 50 else '○' if s is None else '✗'} 市盈率: {pe_str}")

        # 3) PB（越低越好，1.5为中性）
        if pb and pb > 0:
            s = _norm(pb, 0, 3)  # PB=0→100, PB=3→0
            pb_str = f"PB={pb:.2f} → {s}/100"
        else:
            s = None
            pb_str = "数据缺失"
        if s is not None:
            scores.append(s)
        details.append(f"{'✓' if s and s >= 50 else '○' if s is None else '✗'} 市净率: {pb_str}")

        # 4) PE×PB（越低越好，22.5为中性）
        if pe and pb and pe > 0 and pb > 0:
            product = pe * pb
            s = _norm(product, 0, 45)  # 0→100, 45→0
            gs_str = f"PE×PB={product:.1f} → {s}/100"
        else:
            s = None
            gs_str = "数据缺失"
        if s is not None:
            scores.append(s)
        details.append(f"{'✓' if s and s >= 50 else '○' if s is None else '✗'} {gs_str}")

        # 5) ROE（越高越好，12%为中性）
        if roe is not None and roe > 0:
            s = _norm(roe, 25, 0)  # ROE=25%→100, ROE=0%→0
            roe_str = f"ROE={roe:.2f}% → {s}/100"
        else:
            s = None
            roe_str = "数据缺失"
        if s is not None:
            scores.append(s)
        details.append(f"{'✓' if s and s >= 50 else '○' if s is None else '✗'} 盈利能力: {roe_str}")

        # 6) 流动比率（适中为佳，2.0为中性）
        if current_ratio and current_ratio > 0:
            if current_ratio >= 2.0:
                s = _norm(current_ratio, 3.0, 2.0)  # 3.0→100, 2.0→50
            else:
                s = _norm(current_ratio, 2.0, 0.5)  # 2.0→50, 0.5→0
            cr_str = f"流动比率={current_ratio:.2f} → {s}/100"
        else:
            s = None
            cr_str = "数据缺失"
        if s is not None:
            scores.append(s)
        details.append(f"{'✓' if s and s >= 50 else '○' if s is None else '✗'} {cr_str}")

        # 7) 债务权益比（越低越好，1.0为中性）
        if debt_to_equity is not None and debt_to_equity >= 0:
            s = _norm(max(0, 1.0 - debt_to_equity), 1.0, 0)  # D/E=0→100, D/E=1→50, D/E=3→0
            de_str = f"D/E={debt_to_equity:.2f} → {s}/100"
        else:
            s = None
            de_str = "数据缺失"
        if s is not None:
            scores.append(s)
        details.append(f"{'✓' if s and s >= 50 else '○' if s is None else '✗'} 债务权益比: {de_str}")

        # 8) 盈利稳定性（连续盈利年数占比）
        if earnings_history and len(earnings_history) >= 5:
            positive_years = sum(1 for e in earnings_history if e and e > 0)
            ratio = positive_years / len(earnings_history)
            s = _norm(ratio, 1.0, 0.5)  # 100%→100, 50%→0
            eh_str = f"近{len(earnings_history)}年盈利年数:{positive_years}/{len(earnings_history)} → {s}/100"
        else:
            s = None
            eh_str = f"历史数据不足"
        if s is not None:
            scores.append(s)
        details.append(f"{'✓' if s and s >= 50 else '○' if s is None else '✗'} 盈利稳定性: {eh_str}")

        # 9) 分红记录（分红年数比例）
        if dividend_history and len(dividend_history) >= 3:
            paid_years = sum(1 for d in dividend_history if d and d > 0)
            ratio = paid_years / len(dividend_history)
            s = _norm(ratio, 1.0, 0.5)  # 100%→100, 50%→0
            dh_str = f"近{len(dividend_history)}年分红年数:{paid_years}/{len(dividend_history)} → {s}/100"
        else:
            s = None
            dh_str = "历史数据不足"
        if s is not None:
            scores.append(s)
        details.append(f"{'✓' if s and s >= 50 else '○' if s is None else '✗'} 分红记录: {dh_str}")

        criteria_score = int(sum(scores) / len(scores)) if scores else 0

        return {
            'passed_count': criteria_score,
            'total_criteria': 100,
            'details': details,
            'criteria_score': criteria_score,
        }
    
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
            return None

        margin = (intrinsic_value - current_price) / intrinsic_value * 100

        return round(margin, 2)
    
    def _calculate_score(self, criteria, margin_of_safety):
        # criteria['passed_count'] 已变为连续值 0-100
        base_score = criteria.get('criteria_score', criteria['passed_count']) * 0.60

        bonus = 0
        if margin_of_safety is not None and margin_of_safety > 0:
            bonus = min(margin_of_safety / 50 * 40, 40)

        return min(int(base_score + bonus), 100)
    
    def _get_suggestion(self, score, margin_of_safety, criteria):
        cs = criteria.get('criteria_score', criteria['passed_count'])

        if score >= 80 and margin_of_safety is not None and margin_of_safety >= GrahamThresholds.MARGIN_SAFETY_MEDIUM:
            return f"强烈建议买入 - 综合评分{cs:.0f}分，安全边际{margin_of_safety:.0f}%"
        elif score >= 65 and margin_of_safety is not None and margin_of_safety >= GrahamThresholds.MARGIN_SAFETY_MINIMUM:
            return f"建议买入 - 综合评分{cs:.0f}分，有安全边际"
        elif score >= 50:
            return f"考虑买入 - 综合评分{cs:.0f}分"
        elif score >= 35:
            return f"关注 - 综合评分{cs:.0f}分"
        else:
            return f"不建议买入 - 仅{cs:.0f}分"
    
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