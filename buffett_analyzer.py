import logging
from typing import Dict, Optional, Any, List
from logger_config import logger
from config import BuffettThresholds, DCFConfig, calculate_cagr, MoatConfig

class BuffettAnalyzer:
    """
    巴菲特价值投资分析器
    
    核心依据：巴菲特致股东信、1979-2020年历年演讲
    
    巴菲特的核心投资原则（按优先级）：
    1. 护城河（竞争优势）- 最重要
    2. 优秀的管理层（资本配置能力）
    3. 合理的价格（安全边际）
    4. 能力圈内的企业
    
    估值方法：自由现金流折现（DCF）
    Owner Earnings = 净利润 + 折旧摊销 - 资本支出 - 营运资本增加
    """
    
    def analyze(self, stock_data: Dict[str, Any]) -> Dict[str, Any]:
        try:
            financial_data = stock_data.get('financial', {})
            info = stock_data.get('info', {})
            
            if not financial_data:
                logger.warning("没有财务数据，无法进行巴菲特分析")
                return self._get_empty_result()
            
            roe = financial_data.get('roe')
            pe = financial_data.get('pe')
            pb = financial_data.get('pb')
            total_mv = financial_data.get('total_mv')
            net_profit = financial_data.get('net_profit')
            gross_margin = financial_data.get('gross_margin')
            net_margin = financial_data.get('net_margin')
            dividend_yield = financial_data.get('dividend_yield')
            free_cashflow = financial_data.get('free_cashflow')
            fcf_history = financial_data.get('fcf_history', [])
            roe_history = financial_data.get('roe_history', [])
            debt_to_ebitda = financial_data.get('debt_to_ebitda')
            capex_to_depreciation = financial_data.get('capex_to_depreciation')
            retained_earnings_efficiency = financial_data.get('retained_earnings_efficiency')
            
            industry = info.get('industry', '')
            
            moat_analysis = self._analyze_moat(roe, gross_margin, net_margin, total_mv, roe_history, industry)
            
            management_score = self._evaluate_management(roe, net_margin, retained_earnings_efficiency)
            
            intrinsic_value = self._estimate_intrinsic_value_dcf(
                free_cashflow, fcf_history, net_profit
            )
            
            dcf_scenarios = self._estimate_dcf_scenarios(
                free_cashflow, fcf_history, net_profit
            )
            
            margin_of_safety = self._calculate_margin_of_safety(intrinsic_value, total_mv)
            
            capital_allocation = self._assess_capital_allocation(
                roe, net_profit, pe, debt_to_ebitda, capex_to_depreciation
            )
            
            debt_analysis = self._analyze_debt(debt_to_ebitda, financial_data)
            
            score = self._calculate_score(
                moat_analysis, management_score, margin_of_safety, 
                capital_allocation, debt_analysis
            )
            
            suggestion = self._get_suggestion(score, moat_analysis, margin_of_safety)
            
            analysis_result = {
                'roe': roe,
                'pe': pe,
                'pb': pb,
                'dividend_yield': dividend_yield,
                'free_cashflow': free_cashflow,
                'fcf_growth_rate': self._calculate_fcf_growth(fcf_history),
                'moat_rating': moat_analysis['rating'],
                'moat_details': moat_analysis['details'],
                'resource_moat': moat_analysis.get('resource_moat', {}),
                'policy_moat': moat_analysis.get('policy_moat', {}),
                'management_score': management_score,
                'margin_of_safety': margin_of_safety,
                'intrinsic_value': intrinsic_value,
                'dcf_scenarios': dcf_scenarios,
                'capital_allocation': capital_allocation,
                'debt_analysis': debt_analysis,
                'retained_earnings_efficiency': retained_earnings_efficiency,
                'buffett_score': score,
                'suggestion': suggestion
            }
            
            logger.info(f"巴菲特分析完成 - ROE:{roe}%, 护城河:{moat_analysis['rating']}, 评分:{score}")
            return analysis_result
            
        except Exception as e:
            logger.error(f"巴菲特分析失败: {e}")
            return self._get_empty_result()
    
    def _analyze_moat(self, roe, gross_margin, net_margin, total_mv, roe_history=None, industry=''):
        """
        分析经济护城河
        
        巴菲特的护城河类型：
        1. 品牌溢价（毛利率高）
        2. 转换成本（客户粘性强）
        3. 网络效应（用户越多价值越大）
        4. 成本优势（规模经济、独特资源）
        5. 法规壁垒（牌照、专利）
        6. 资源壁垒（新增：稀缺资源储量/行业龙头地位）
        7. 政策壁垒（新增：国家战略保护/出口管制/关键矿产目录）
        """
        moat_score = 0
        details = []
        
        if roe:
            if roe >= 20:
                moat_score += 4
                details.append(f"✓ ROE={roe:.1f}% ≥ 20%（强盈利能力）")
            elif roe >= 15:
                moat_score += 3
                details.append(f"✓ ROE={roe:.1f}% ≥ 15%（良好盈利能力）")
            elif roe >= 10:
                moat_score += 2
                details.append(f"○ ROE={roe:.1f}% ≥ 10%（一般盈利能力）")
            else:
                details.append(f"✗ ROE={roe:.1f}% < 10%（盈利能力弱）")
        
        if roe_history and len(roe_history) >= BuffettThresholds.ROE_STABILITY_YEARS:
            recent_roe = roe_history[-BuffettThresholds.ROE_STABILITY_YEARS:]
            avg_roe = sum(recent_roe) / len(recent_roe)
            roe_std = (sum((r - avg_roe) ** 2 for r in recent_roe) / len(recent_roe)) ** 0.5
            
            if avg_roe >= 15 and roe_std < 3:
                moat_score += 3
                details.append(f"✓ ROE稳定性: 近{BuffettThresholds.ROE_STABILITY_YEARS}年平均{avg_roe:.1f}%（标准差{roe_std:.1f}）")
            elif avg_roe >= 12 and roe_std < 5:
                moat_score += 2
                details.append(f"○ ROE稳定性: 近{BuffettThresholds.ROE_STABILITY_YEARS}年平均{avg_roe:.1f}%（标准差{roe_std:.1f}）")
            else:
                details.append(f"✗ ROE不稳定: 标准差{roe_std:.1f}%")
        
        if gross_margin:
            if gross_margin >= 40:
                moat_score += 3
                details.append(f"✓ 毛利率={gross_margin:.1f}% ≥ 40%（强品牌/定价权）")
            elif gross_margin >= 25:
                moat_score += 2
                details.append(f"✓ 毛利率={gross_margin:.1f}% ≥ 25%（一定定价权）")
            elif gross_margin >= 15:
                moat_score += 1
                details.append(f"○ 毛利率={gross_margin:.1f}% ≥ 15%（定价权一般）")
            else:
                details.append(f"✗ 毛利率={gross_margin:.1f}% < 15%（无定价权）")
        
        if net_margin:
            if net_margin >= 20:
                moat_score += 2
                details.append(f"✓ 净利率={net_margin:.1f}% ≥ 20%（成本控制优秀）")
            elif net_margin >= 10:
                moat_score += 1
                details.append(f"○ 净利率={net_margin:.1f}% ≥ 10%（成本控制一般）")
        
        if total_mv and total_mv >= 1e11:
            moat_score += 1
            details.append(f"✓ 市值={total_mv/1e8:.0f}亿（规模优势）")
        
        resource_moat = self._analyze_resource_moat(industry, total_mv)
        if resource_moat['score'] > 0:
            moat_score += resource_moat['score']
            details.extend(resource_moat['details'])
        
        policy_moat = self._analyze_policy_moat(industry)
        if policy_moat['score'] > 0:
            moat_score += policy_moat['score']
            details.extend(policy_moat['details'])
        
        if moat_score >= 14:
            rating = "宽护城河"
        elif moat_score >= 10:
            rating = "中等护城河"
        elif moat_score >= 6:
            rating = "窄护城河"
        else:
            rating = "无护城河"
        
        return {
            'rating': rating,
            'score': moat_score,
            'details': "; ".join(details),
            'resource_moat': resource_moat,
            'policy_moat': policy_moat
        }
    
    def _analyze_resource_moat(self, industry, total_mv):
        """
        分析资源壁垒护城河
        
        评估企业是否拥有稀缺资源储量或行业龙头地位
        这对于有色金属、矿产等战略资源企业尤其重要
        """
        score = 0
        details = []
        
        if not industry:
            return {'score': score, 'details': details, 'has_resource_moat': False}
        
        resource_keywords = MoatConfig.STRATEGIC_RESOURCE_KEYWORDS
        has_resource = any(kw in industry for kw in resource_keywords)
        
        if has_resource:
            score += 3
            details.append(f"✓ 战略资源行业: {industry}（稀缺资源壁垒）")
        
        if total_mv and total_mv >= 2e11:
            score += 2
            details.append(f"✓ 行业龙头: 市值={total_mv/1e8:.0f}亿（规模领先优势）")
        elif total_mv and total_mv >= 1e11:
            score += 1
            details.append(f"○ 规模尚可: 市值={total_mv/1e8:.0f}亿（中等规模优势）")
        
        return {
            'score': score,
            'details': details,
            'has_resource_moat': has_resource
        }
    
    def _analyze_policy_moat(self, industry):
        """
        分析政策壁垒护城河
        
        评估企业是否属于国家战略保护行业
        包括关键矿产目录、出口管制、产业扶持等政策
        """
        score = 0
        details = []
        
        if not industry:
            return {'score': score, 'details': details, 'has_policy_moat': False}
        
        policy_keywords = MoatConfig.STRATEGIC_INDUSTRY_KEYWORDS + MoatConfig.STRATEGIC_RESOURCE_KEYWORDS
        has_policy_protection = any(kw in industry for kw in policy_keywords)
        
        if has_policy_protection:
            score += 4
            details.append(f"✓ 国家战略行业: {industry}（政策保护壁垒）")
            details.append("✓ 关键矿产/战略材料（出口管制+产业扶持）")
        
        return {
            'score': score,
            'details': details,
            'has_policy_moat': has_policy_protection
        }
    
    def _evaluate_management(self, roe, net_margin, retained_earnings_efficiency=None):
        """
        评估管理层质量
        
        巴菲特看重的管理层特质：
        1. 资本配置能力（ROE高说明会用钱）
        2. 诚实透明
        3. 股东利益导向
        4. 留存收益再投资效率
        """
        score = 0
        
        if roe and roe >= 20:
            score += 40
        elif roe and roe >= 15:
            score += 35
        elif roe and roe >= 10:
            score += 25
        elif roe and roe >= 5:
            score += 10
        
        if net_margin and net_margin >= 20:
            score += 30
        elif net_margin and net_margin >= 15:
            score += 25
        elif net_margin and net_margin >= 8:
            score += 20
        elif net_margin and net_margin >= 3:
            score += 10
        
        if retained_earnings_efficiency is not None:
            if retained_earnings_efficiency >= 1.2:
                score += 30
                logger.info(f"留存收益效率优秀: 每留存1元创造{retained_earnings_efficiency:.2f}元市值")
            elif retained_earnings_efficiency >= 1.0:
                score += 20
            elif retained_earnings_efficiency >= 0.8:
                score += 10
            else:
                score += 0
                logger.warning(f"留存收益效率低: 每留存1元仅创造{retained_earnings_efficiency:.2f}元市值")
        else:
            score += 15
        
        return min(score, 100)
    
    def _estimate_intrinsic_value_dcf(self, free_cashflow, fcf_history=None, net_profit=None):
        """
        巴菲特方法：自由现金流折现（DCF）
        
        Owner Earnings = 净利润 + 折旧摊销 - 资本支出 - 营运资本增加
        简化为使用自由现金流
        """
        fcf = free_cashflow
        
        if not fcf or fcf <= 0:
            if net_profit and net_profit > 0:
                fcf = net_profit * 0.8
                logger.info(f"使用净利润近似自由现金流: {fcf/1e8:.2f}亿")
            else:
                logger.warning("无自由现金流和净利润数据，无法DCF估值")
                return None
        
        growth_rate = self._calculate_fcf_growth(fcf_history)
        
        if growth_rate is None:
            growth_rate = 5.0
        
        growth_rate = min(max(growth_rate, 0), 15)
        
        discount_rate = DCFConfig.DISCOUNT_RATE
        terminal_growth = DCFConfig.TERMINAL_GROWTH_RATE
        projection_years = DCFConfig.PROJECTION_YEARS
        
        total_pv = 0
        future_fcf = fcf
        
        for year in range(1, projection_years + 1):
            future_fcf *= (1 + growth_rate / 100)
            pv = future_fcf / (1 + discount_rate) ** year
            total_pv += pv
        
        if discount_rate <= terminal_growth:
            logger.warning(f"折现率({discount_rate}) <= 永续增长率({terminal_growth})，调整永续增长率")
            terminal_growth = discount_rate - 0.005

        terminal_value = (future_fcf * (1 + terminal_growth)) / (discount_rate - terminal_growth)
        terminal_pv = terminal_value / (1 + discount_rate) ** projection_years

        intrinsic_value = total_pv + terminal_pv

        logger.info(f"DCF估值 - FCF:{fcf/1e8:.2f}亿, 增长率:{growth_rate:.1f}%, 内在价值:{intrinsic_value/1e8:.2f}亿")

        return round(intrinsic_value, 2)
    
    def _estimate_dcf_scenarios(self, free_cashflow, fcf_history=None, net_profit=None):
        """
        多情景DCF估值分析
        
        返回保守、中性、乐观三种情景下的估值
        """
        fcf = free_cashflow
        
        if not fcf or fcf <= 0:
            if net_profit and net_profit > 0:
                fcf = net_profit * 0.8
            else:
                return None
        
        base_growth = self._calculate_fcf_growth(fcf_history)
        if base_growth is None:
            base_growth = 5.0

        # 统一边界处理，防止极端值
        base_growth = min(max(base_growth, -30), 30)

        scenarios = {
            '保守': {
                'growth_rate': max(0, base_growth * 0.6),  # 负基线时取0，正基线时打6折
                'discount_rate': DCFConfig.DISCOUNT_RATE + 0.02,
                'terminal_growth': DCFConfig.TERMINAL_GROWTH_RATE * 0.5,
                'description': '保守假设（行业增速放缓、竞争加剧）'
            },
            '中性': {
                'growth_rate': min(max(base_growth, 0), 15),  # 统一clamp到[0,15]，与单情景一致
                'discount_rate': DCFConfig.DISCOUNT_RATE,
                'terminal_growth': DCFConfig.TERMINAL_GROWTH_RATE,
                'description': '中性假设（行业发展稳定）'
            },
            '乐观': {
                'growth_rate': min(max(base_growth * 1.3, 5), 25),  # 至少5%增长，上限25%
                'discount_rate': DCFConfig.DISCOUNT_RATE - 0.01,
                'terminal_growth': min(DCFConfig.TERMINAL_GROWTH_RATE * 1.5, 0.05),
                'description': '乐观假设（行业高景气、需求爆发）'
            }
        }
        
        results = {}
        
        for scenario_name, params in scenarios.items():
            growth_rate = params['growth_rate']
            discount_rate = params['discount_rate']
            terminal_growth = params['terminal_growth']
            projection_years = DCFConfig.PROJECTION_YEARS
            
            total_pv = 0
            future_fcf = fcf
            
            for year in range(1, projection_years + 1):
                future_fcf *= (1 + growth_rate / 100)
                pv = future_fcf / (1 + discount_rate) ** year
                total_pv += pv
            
            if discount_rate <= terminal_growth:
                logger.warning(f"情景({scenario_name})折现率({discount_rate}) <= 永续增长率({terminal_growth})，调整永续增长率")
                terminal_growth = discount_rate - 0.005

            terminal_value = (future_fcf * (1 + terminal_growth)) / (discount_rate - terminal_growth)
            terminal_pv = terminal_value / (1 + discount_rate) ** projection_years
            
            intrinsic_value = total_pv + terminal_pv
            
            results[scenario_name] = {
                'intrinsic_value': round(intrinsic_value, 2),
                'growth_rate': round(growth_rate, 1),
                'description': params['description']
            }
            
            logger.info(f"DCF情景({scenario_name}) - FCF:{fcf/1e8:.2f}亿, 增长率:{growth_rate:.1f}%, 内在价值:{intrinsic_value/1e8:.2f}亿")
        
        return results
    
    def _calculate_fcf_growth(self, fcf_history):
        return calculate_cagr(fcf_history)
    
    def _calculate_margin_of_safety(self, intrinsic_value, current_mv):
        """
        安全边际 = (内在价值 - 当前市值) / 内在价值 × 100%
        """
        if not intrinsic_value or not current_mv or current_mv <= 0:
            return None

        margin = (intrinsic_value - current_mv) / intrinsic_value * 100

        return round(margin, 2)
    
    def _assess_capital_allocation(self, roe, net_profit, pe, debt_to_ebitda=None, capex_to_depreciation=None):
        """
        评估资本配置能力
        
        巴菲特最看重ROE和留存收益的使用效率
        """
        score = 0
        
        if roe and roe >= 20:
            score += 40
        elif roe and roe >= 15:
            score += 35
        elif roe and roe >= 10:
            score += 25
        elif roe and roe >= 5:
            score += 10
        
        if net_profit and net_profit > 0:
            score += 20
        
        if debt_to_ebitda is not None:
            if debt_to_ebitda <= 1.5:
                score += 20
            elif debt_to_ebitda <= BuffettThresholds.DEBT_TO_EBITDA_MAX:
                score += 15
            elif debt_to_ebitda <= 5.0:
                score += 10
            else:
                score += 0
                logger.warning(f"债务/EBITDA过高: {debt_to_ebitda:.2f}")
        else:
            score += 10
        
        if capex_to_depreciation is not None:
            if capex_to_depreciation <= 1.0:
                score += 20
            elif capex_to_depreciation <= BuffettThresholds.CAPEX_TO_DEPRECIATION_MAX:
                score += 15
            else:
                score += 5
        else:
            score += 10
        
        return min(score, 100)
    
    def _analyze_debt(self, debt_to_ebitda, financial_data):
        """
        债务分析
        
        巴菲特偏好低负债或无负债企业
        """
        analysis = {
            'debt_level': '未知',
            'debt_to_ebitda': debt_to_ebitda,
            'risk_level': '未知',
            'details': []
        }
        
        if debt_to_ebitda is not None:
            analysis['debt_level'] = f"{debt_to_ebitda:.2f}"
            
            if debt_to_ebitda <= 1.0:
                analysis['risk_level'] = '低'
                analysis['details'].append("✓ 债务水平极低")
            elif debt_to_ebitda <= BuffettThresholds.DEBT_TO_EBITDA_MAX:
                analysis['risk_level'] = '中低'
                analysis['details'].append("✓ 债务水平合理")
            elif debt_to_ebitda <= 5.0:
                analysis['risk_level'] = '中等'
                analysis['details'].append("○ 债务水平偏高，需关注")
            else:
                analysis['risk_level'] = '高'
                analysis['details'].append("✗ 债务水平过高")
        
        total_debt = financial_data.get('total_debt')
        cash = financial_data.get('cash_and_equivalents')
        
        if total_debt and cash:
            net_debt = total_debt - cash
            if net_debt <= 0:
                analysis['details'].append(f"✓ 净现金状态（现金{cash/1e8:.0f}亿 > 债务{total_debt/1e8:.0f}亿）")
            else:
                analysis['details'].append(f"○ 净债务: {net_debt/1e8:.0f}亿")
        
        return analysis
    
    def _calculate_score(self, moat, management, margin_of_safety, capital_allocation, debt_analysis):
        score = 0

        # 护城河连续评分：moat_score 范围约 0-22 → 映射到 0-35
        raw_moat = moat.get('score', 0)
        moat_continuous = min(int(raw_moat / 22 * 35), 35)
        score += moat_continuous

        score += management * 0.2

        # 安全边际连续贡献：MoS 0%→0分, 25%→15分, 50%→25分（非线性映射）
        if margin_of_safety is not None and margin_of_safety > 0:
            if margin_of_safety >= 50:
                mos_score = 25
            else:
                mos_score = margin_of_safety / 50 * 25
            score += int(mos_score)

        score += capital_allocation * 0.15

        # 债务风险连续评分
        debt_risk = debt_analysis.get('risk_level', '未知')
        debt_score = {'低': 10, '中低': 8, '中等': 5, '高': 0, '未知': 5}
        score += debt_score.get(debt_risk, 5)

        return min(int(score), 100)
    
    def _get_suggestion(self, score, moat, margin_of_safety):
        rating = moat['rating']
        
        if score >= 80 and rating in ['宽护城河', '中等护城河'] and margin_of_safety is not None and margin_of_safety >= 25:
            return "强烈建议买入 - 优质企业+安全价格"
        elif score >= 70 and rating in ['宽护城河', '中等护城河']:
            return "建议买入 - 好企业，价格合理"
        elif score >= 55:
            return "考虑买入 - 基本面尚可，需观察"
        elif score >= 40:
            return "关注 - 部分指标符合巴菲特标准"
        else:
            return "不建议买入 - 不符合巴菲特标准"
    
    def _get_empty_result(self):
        return {
            'roe': None,
            'pe': None,
            'pb': None,
            'dividend_yield': None,
            'free_cashflow': None,
            'fcf_growth_rate': None,
            'moat_rating': "无法评估",
            'moat_details': "数据不足",
            'management_score': 0,
            'margin_of_safety': 0.0,
            'intrinsic_value': None,
            'capital_allocation': 0,
            'debt_analysis': {'debt_level': '未知', 'risk_level': '未知', 'details': []},
            'retained_earnings_efficiency': None,
            'buffett_score': 0,
            'suggestion': "无法分析 - 数据不足"
        }