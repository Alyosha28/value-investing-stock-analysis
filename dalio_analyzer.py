import numpy as np
from typing import Dict, Optional, Any, List
from logger_config import logger
from config import calculate_cagr


class DalioThresholds:
    DEBT_TO_EBITDA_MAX = 3.0
    INTEREST_COVERAGE_MIN = 3.0
    DEBT_SERVICE_RATIO_MAX = 0.4
    FCF_TO_DEBT_MIN = 0.15
    EARNINGS_VOLATILITY_MAX = 30
    INFLATION_HEDGE_MIN_DIV_YIELD = 2.5
    BETA_MAX_FOR_DEFENSIVE = 0.8
    BETA_MIN_FOR_GROWTH = 1.0
    DIVERSIFICATION_CORR_MAX = 0.7
    LIQUIDITY_CURRENT_RATIO_MIN = 1.5
    ASSET_TURNOVER_MIN = 0.5
    REVENUE_GROWTH_MIN = 5


class DalioAnalyzer:
    """
    瑞·达里奥（Ray Dalio）宏观周期与全天候投资分析器

    核心依据：《原则》、《债务危机》、全天候策略（All Weather）、纯阿尔法策略（Pure Alpha）

    达里奥的核心投资原则：
    1. 理解经济这台机器 - 债务周期驱动一切
    2. 分散化是投资的圣杯 - 15个无相关性的回报流可将风险降低80%
    3. 风险平价 - 配置风险而非配置资金
    4. 四大经济情景（增长/通胀四象限）
       - 增长上升 + 通胀上升 → 股票、商品、信用债
       - 增长上升 + 通胀下降 → 股票、信用债
       - 增长下降 + 通胀上升 → 黄金、通胀挂钩债券、商品
       - 增长下降 + 通胀下降 → 国债、高等级债券
    5. 宏观先行指标 - 债务增速、产能利用率、失业率、通胀预期
    6. Alpha与Beta分离 - 不赌市场方向，只赌相对表现

    对个股的分析视角（Bridgewater 股票研究框架）：
    - 现金流的可预测性（穿越周期的稳定性）
    - 债务承受能力（利息覆盖、债务/EBITDA、债务偿还期限结构）
    - 通胀对冲属性（定价权、资源属性、股息率）
    - 分散化价值（与大盘/债券的相关性）
    - 真实回报（名义回报 - 通胀预期）
    - 系统重要性（大而不倒/国家依赖度）
    """

    def analyze(self, stock_data: Dict[str, Any]) -> Dict[str, Any]:
        try:
            financial_data = stock_data.get('financial', {})
            info = stock_data.get('info', {})
            historical_data = stock_data.get('historical')

            if not financial_data:
                logger.warning("没有财务数据，无法进行达里奥分析")
                return self._get_empty_result()

            pe = financial_data.get('pe')
            pb = financial_data.get('pb')
            roe = financial_data.get('roe')
            net_profit = financial_data.get('net_profit')
            revenue = financial_data.get('revenue')
            total_mv = financial_data.get('total_mv')
            free_cashflow = financial_data.get('free_cashflow')
            total_debt = financial_data.get('total_debt')
            ebitda = financial_data.get('ebitda')
            interest_expense = financial_data.get('interest_expense')
            debt_to_equity = financial_data.get('debt_to_equity')
            current_ratio = financial_data.get('current_ratio')
            dividend_yield = financial_data.get('dividend_yield')
            earnings_history = financial_data.get('earnings_history', [])
            revenue_history = financial_data.get('revenue_history', [])
            total_assets = financial_data.get('total_assets')
            industry = info.get('industry', '')

            debt_cycle_analysis = self._analyze_debt_cycle_position(
                total_debt, ebitda, interest_expense, debt_to_equity,
                free_cashflow, current_ratio, net_profit
            )
            earnings_stability = self._analyze_earnings_stability(
                earnings_history, revenue_history
            )
            inflation_hedge = self._analyze_inflation_hedge(
                dividend_yield, industry, revenue_history
            )
            diversification = self._analyze_diversification_value(
                historical_data, total_mv, industry
            )
            real_return = self._estimate_real_return(pe, dividend_yield, earnings_history)
            system_importance = self._assess_system_importance(total_mv, industry)

            score = self._calculate_score(
                debt_cycle_analysis, earnings_stability, inflation_hedge,
                diversification, real_return, system_importance
            )
            suggestion = self._get_suggestion(score, debt_cycle_analysis, earnings_stability)
            quadrant_fit = self._determine_quadrant_fit(
                earnings_stability, inflation_hedge, debt_cycle_analysis
            )

            result = {
                'debt_cycle_analysis': debt_cycle_analysis,
                'earnings_stability': earnings_stability,
                'inflation_hedge': inflation_hedge,
                'diversification_value': diversification,
                'real_return_estimate': real_return,
                'system_importance': system_importance,
                'all_weather_quadrant': quadrant_fit,
                'dalio_score': score,
                'suggestion': suggestion,
            }

            logger.info(
                f"达里奥分析完成 - 债务周期:{debt_cycle_analysis['rating']}, "
                f"盈利稳定性:{earnings_stability['rating']}, 评分:{score}"
            )
            return result

        except Exception as e:
            logger.error(f"达里奥分析失败: {e}")
            return self._get_empty_result()

    def _analyze_debt_cycle_position(
        self, total_debt, ebitda, interest_expense, debt_to_equity,
        free_cashflow, current_ratio, net_profit
    ) -> Dict[str, Any]:
        analysis = {'details': [], 'score_contrib': 0, 'rating': '未知'}
        score = 0

        if debt_to_equity is not None:
            if debt_to_equity <= 0.3:
                analysis['details'].append(f"✓ 低杠杆: D/E={debt_to_equity:.2f}（抗衰退能力强）")
                score += 15
            elif debt_to_equity <= DalioThresholds.DEBT_TO_EQUITY_MAX:
                analysis['details'].append(f"○ 中等杠杆: D/E={debt_to_equity:.2f}")
                score += 8
            else:
                analysis['details'].append(f"✗ 高杠杆: D/E={debt_to_equity:.2f}（债务周期脆弱）")
        else:
            analysis['details'].append("○ 负债率数据缺失")
            score += 5

        if total_debt and ebitda and ebitda > 0:
            debt_to_ebitda = total_debt / ebitda
            if debt_to_ebitda <= 1.5:
                analysis['details'].append(f"✓ 债务/EBITDA={debt_to_ebitda:.1f}（极低偿债压力）")
                score += 15
            elif debt_to_ebitda <= DalioThresholds.DEBT_TO_EBITDA_MAX:
                analysis['details'].append(f"○ 债务/EBITDA={debt_to_ebitda:.1f}")
                score += 10
            else:
                analysis['details'].append(f"✗ 债务/EBITDA={debt_to_ebitda:.1f}（偿债压力大）")
        else:
            analysis['details'].append("○ 债务/EBITDA数据缺失")
            score += 5

        if interest_expense and ebitda and ebitda > 0 and interest_expense > 0:
            interest_coverage = ebitda / interest_expense
            if interest_coverage >= DalioThresholds.INTEREST_COVERAGE_MIN:
                analysis['details'].append(
                    f"✓ 利息覆盖={interest_coverage:.1f}x（安全垫充足）"
                )
                score += 10
            elif interest_coverage >= 1.5:
                analysis['details'].append(
                    f"○ 利息覆盖={interest_coverage:.1f}x"
                )
                score += 5
            else:
                analysis['details'].append(
                    f"✗ 利息覆盖={interest_coverage:.1f}x（偿债风险高）"
                )
        else:
            analysis['details'].append("○ 利息覆盖数据缺失")
            score += 3

        if free_cashflow and total_debt and total_debt > 0:
            fcf_to_debt = free_cashflow / total_debt
            if fcf_to_debt >= DalioThresholds.FCF_TO_DEBT_MIN:
                analysis['details'].append(
                    f"✓ FCF/债务={fcf_to_debt:.1%}（快速去杠杆能力）"
                )
                score += 10
            elif fcf_to_debt > 0:
                analysis['details'].append(
                    f"○ FCF/债务={fcf_to_debt:.1%}"
                )
                score += 5
            else:
                analysis['details'].append(
                    f"✗ FCF为负，无法覆盖债务"
                )
        else:
            analysis['details'].append("○ FCF/债务数据缺失")
            score += 3

        if current_ratio is not None:
            if current_ratio >= DalioThresholds.LIQUIDITY_CURRENT_RATIO_MIN:
                analysis['details'].append(f"✓ 流动比率={current_ratio:.1f}（流动性充裕）")
                score += 5
            elif current_ratio >= 1.0:
                analysis['details'].append(f"○ 流动比率={current_ratio:.1f}")
                score += 3
            else:
                analysis['details'].append(f"✗ 流动比率={current_ratio:.1f}（流动性紧张）")
        else:
            analysis['details'].append("○ 流动比率数据缺失")
            score += 2

        analysis['score_contrib'] = score
        if score >= 45:
            analysis['rating'] = '健康'
        elif score >= 30:
            analysis['rating'] = '一般'
        else:
            analysis['rating'] = '脆弱'

        return analysis

    def _analyze_earnings_stability(
        self, earnings_history: List[float], revenue_history: List[float]
    ) -> Dict[str, Any]:
        analysis = {'details': [], 'score_contrib': 0, 'rating': '未知'}
        score = 0

        if earnings_history and len(earnings_history) >= 5:
            valid = [e for e in earnings_history if e is not None and e > 0]
            if len(valid) >= 5:
                if len(valid) >= 2:
                    cagr = calculate_cagr(valid)
                    if cagr is not None and cagr >= DalioThresholds.REVENUE_GROWTH_MIN:
                        analysis['details'].append(f"✓ 盈利CAGR={cagr:.1f}%（持续增长）")
                        score += 15
                    elif cagr is not None:
                        analysis['details'].append(f"○ 盈利CAGR={cagr:.1f}%")
                        score += 8
                    else:
                        analysis['details'].append("○ 盈利增长数据不足")
                        score += 5
                else:
                    score += 5

                avg = sum(valid[-5:]) / 5
                std = (sum((e - avg) ** 2 for e in valid[-5:]) / 5) ** 0.5
                cv = (std / abs(avg) * 100) if avg != 0 else 999
                if cv <= 20:
                    analysis['details'].append(f"✓ 盈利波动极低: CV={cv:.1f}%（可预测性强）")
                    score += 20
                elif cv <= DalioThresholds.EARNINGS_VOLATILITY_MAX:
                    analysis['details'].append(f"○ 盈利波动适中: CV={cv:.1f}%")
                    score += 10
                else:
                    analysis['details'].append(f"✗ 盈利波动高: CV={cv:.1f}%（不可预测）")
            else:
                analysis['details'].append("✗ 近5年存在亏损年份")
        else:
            analysis['details'].append("○ 盈利历史不足5年")
            score += 5

        if revenue_history and len(revenue_history) >= 5:
            valid_rev = [r for r in revenue_history if r is not None and r > 0]
            if len(valid_rev) >= 5:
                rev_cagr = calculate_cagr(valid_rev)
                if rev_cagr is not None and rev_cagr >= DalioThresholds.REVENUE_GROWTH_MIN:
                    analysis['details'].append(f"✓ 收入CAGR={rev_cagr:.1f}%（需求稳定）")
                    score += 10
                elif rev_cagr is not None:
                    analysis['details'].append(f"○ 收入CAGR={rev_cagr:.1f}%")
                    score += 5
        else:
            analysis['details'].append("○ 收入历史不足")
            score += 3

        analysis['score_contrib'] = score
        if score >= 35:
            analysis['rating'] = '稳定'
        elif score >= 20:
            analysis['rating'] = '一般'
        else:
            analysis['rating'] = '波动'

        return analysis

    def _analyze_inflation_hedge(
        self, dividend_yield, industry: str, revenue_history: List[float]
    ) -> Dict[str, Any]:
        analysis = {'details': [], 'score_contrib': 0, 'rating': '弱'}
        score = 0

        inflation_sensitive = [
            '黄金', '有色', '石油', '天然气', '煤炭', '矿产', '稀土',
            '锂', '铜', '铝', '农产品', '粮食', '林业',
            '地产', '港口', '机场', '电力', '水务', '燃气', '高速',
        ]
        has_inflation_exposure = any(kw in industry for kw in inflation_sensitive)

        if has_inflation_exposure:
            analysis['details'].append(f"✓ 通胀敏感型行业: {industry}（实物资产/定价权转移）")
            score += 20
        else:
            analysis['details'].append(f"○ 行业通胀敏感度一般: {industry}")

        if dividend_yield and dividend_yield >= DalioThresholds.INFLATION_HEDGE_MIN_DIV_YIELD:
            analysis['details'].append(
                f"✓ 股息率={dividend_yield:.1f}%（现金流回报抗通胀）"
            )
            score += 10
        elif dividend_yield and dividend_yield >= 2:
            analysis['details'].append(
                f"○ 股息率={dividend_yield:.1f}%"
            )
            score += 5
        else:
            analysis['details'].append(
                f"○ 股息率较低或无"
            )

        if revenue_history and len(revenue_history) >= 3:
            growth_rates = []
            for i in range(1, min(len(revenue_history), 6)):
                prev = revenue_history[-(i+1)]
                curr = revenue_history[-i]
                if prev and curr and prev > 0:
                    growth_rates.append((curr - prev) / prev * 100)
            if growth_rates:
                avg_growth = sum(growth_rates) / len(growth_rates)
                if avg_growth >= 10:
                    analysis['details'].append(
                        f"✓ 收入增长持续跑赢名义GDP（{avg_growth:.1f}%）"
                    )
                    score += 10
                elif avg_growth >= 5:
                    analysis['details'].append(
                        f"○ 收入增长与名义GDP持平（{avg_growth:.1f}%）"
                    )
                    score += 5
                else:
                    analysis['details'].append(
                        f"✗ 收入增长落后（{avg_growth:.1f}%）"
                    )

        analysis['score_contrib'] = score
        if score >= 25:
            analysis['rating'] = '强'
        elif score >= 12:
            analysis['rating'] = '中'
        else:
            analysis['rating'] = '弱'

        return analysis

    def _analyze_diversification_value(
        self, historical_data, total_mv, industry: str
    ) -> Dict[str, Any]:
        analysis = {'details': [], 'score_contrib': 0, 'rating': '一般'}
        score = 0

        defensive_industries = [
            '食品', '饮料', '白酒', '医药', '医疗', '公用', '电力',
            '水务', '燃气', '高速', '港口', '银行', '保险',
        ]
        is_defensive = any(kw in industry for kw in defensive_industries)

        if is_defensive:
            analysis['details'].append(f"✓ 防御性行业: {industry}（衰退期分散价值高）")
            score += 15
        else:
            analysis['details'].append(f"○ 周期性/进攻性行业: {industry}")
            score += 5

        if total_mv and total_mv >= 1e11:
            analysis['details'].append(f"✓ 大盘股: 市值{total_mv/1e8:.0f}亿（流动性好，组合配置价值高）")
            score += 10
        elif total_mv and total_mv >= 5e9:
            analysis['details'].append(f"○ 中盘股: 市值{total_mv/1e8:.0f}亿")
            score += 5
        else:
            analysis['details'].append(f"○ 小盘股: 市值{total_mv/1e8:.0f}亿（流动性差）")
            score += 2

        if historical_data is not None and not historical_data.empty and 'Close' in historical_data.columns:
            try:
                returns = historical_data['Close'].pct_change().dropna()
                if len(returns) >= 60:
                    vol = returns.std() * np.sqrt(252) * 100
                    if vol <= 25:
                        analysis['details'].append(f"✓ 波动率={vol:.1f}%（低波动分散价值高）")
                        score += 10
                    elif vol <= 40:
                        analysis['details'].append(f"○ 波动率={vol:.1f}%")
                        score += 5
                    else:
                        analysis['details'].append(f"✗ 波动率={vol:.1f}%（高波动，分散价值低）")
                else:
                    analysis['details'].append("○ 历史数据不足计算波动率")
                    score += 3
            except Exception:
                analysis['details'].append("○ 波动率计算失败")
                score += 3
        else:
            analysis['details'].append("○ 无历史价格数据")
            score += 3

        analysis['score_contrib'] = score
        if score >= 25:
            analysis['rating'] = '高'
        elif score >= 15:
            analysis['rating'] = '中'
        else:
            analysis['rating'] = '低'

        return analysis

    def _estimate_real_return(
        self, pe: Optional[float], dividend_yield: Optional[float],
        earnings_history: List[float]
    ) -> Dict[str, Any]:
        analysis = {'details': [], 'estimated_real_return': None, 'score_contrib': 0}

        earnings_growth = calculate_cagr(earnings_history) or 5.0
        dividend = dividend_yield or 0.0

        inflation_estimate = 2.5

        if pe and pe > 0:
            earnings_yield = 100 / pe
            nominal_return = earnings_yield + dividend
            real_return = nominal_return - inflation_estimate
            analysis['estimated_real_return'] = round(real_return, 2)
            analysis['details'].append(
                f"盈利收益率={earnings_yield:.1f}% + 股息={dividend:.1f}% - 通胀={inflation_estimate:.1f}% = "
                f"真实回报≈{real_return:.1f}%"
            )
            if real_return >= 8:
                analysis['score_contrib'] = 15
                analysis['details'].append("✓ 真实回报远超无风险利率")
            elif real_return >= 5:
                analysis['score_contrib'] = 10
                analysis['details'].append("○ 真实回报合理")
            elif real_return >= 2:
                analysis['score_contrib'] = 5
                analysis['details'].append("○ 真实回报偏低")
            else:
                analysis['score_contrib'] = 0
                analysis['details'].append("✗ 真实回报可能为负")
        else:
            analysis['details'].append("○ PE缺失，无法估算真实回报")
            analysis['score_contrib'] = 3

        return analysis

    def _assess_system_importance(
        self, total_mv, industry: str
    ) -> Dict[str, Any]:
        analysis = {'details': [], 'score_contrib': 0, 'rating': '低'}
        score = 0

        system_critical = [
            '银行', '保险', '证券', '电力', '水务', '燃气', '电信',
            '石油', '煤炭', '铁路', '航空', '军工', '航天',
        ]
        is_critical = any(kw in industry for kw in system_critical)

        if is_critical:
            analysis['details'].append(f"✓ 系统重要性行业: {industry}")
            score += 10
            if total_mv and total_mv >= 5e11:
                analysis['details'].append(f"✓ 超大市值: {total_mv/1e8:.0f}亿（大而不倒）")
                score += 5
        else:
            analysis['details'].append(f"○ 非系统重要性行业: {industry}")
            score += 2

        analysis['score_contrib'] = score
        if score >= 12:
            analysis['rating'] = '高'
        elif score >= 5:
            analysis['rating'] = '中'
        else:
            analysis['rating'] = '低'

        return analysis

    def _determine_quadrant_fit(
        self, earnings_stability: Dict, inflation_hedge: Dict, debt_cycle: Dict
    ) -> Dict[str, str]:
        es = earnings_stability.get('rating', '一般')
        ih = inflation_hedge.get('rating', '弱')
        dc = debt_cycle.get('rating', '一般')

        quadrant = '未知'
        if es in ('稳定',) and ih in ('强', '中'):
            quadrant = '增长上升 + 通胀上升（股票/商品偏好）'
        elif es in ('稳定',) and ih == '弱':
            quadrant = '增长上升 + 通胀下降（股票/信用债偏好）'
        elif es in ('波动', '一般') and ih in ('强', '中'):
            quadrant = '增长下降 + 通胀上升（黄金/商品/通胀债偏好）'
        elif es in ('波动', '一般') and ih == '弱':
            quadrant = '增长下降 + 通胀下降（国债/高等级债偏好）'

        risk_parity_role = '中性配置'
        if dc == '健康' and es == '稳定' and ih in ('强', '中'):
            risk_parity_role = '核心持仓（全天候组合中的增长/通胀暴露）'
        elif dc == '脆弱':
            risk_parity_role = '降低权重或剔除（债务周期脆弱）'
        elif es == '波动' and ih == '弱':
            risk_parity_role = '低配（衰退期表现差且无通胀保护）'

        return {
            'quadrant': quadrant,
            'risk_parity_role': risk_parity_role,
        }

    def _calculate_score(
        self, debt_cycle, earnings_stability, inflation_hedge,
        diversification, real_return, system_importance
    ) -> int:
        score = 0
        score += debt_cycle.get('score_contrib', 0)
        score += earnings_stability.get('score_contrib', 0)
        score += inflation_hedge.get('score_contrib', 0)
        score += diversification.get('score_contrib', 0)
        score += real_return.get('score_contrib', 0)
        score += system_importance.get('score_contrib', 0)
        return min(int(score), 100)

    def _get_suggestion(self, score: int, debt_cycle: Dict, earnings_stability: Dict) -> str:
        dc_rating = debt_cycle.get('rating', '一般')
        es_rating = earnings_stability.get('rating', '一般')

        if score >= 75 and dc_rating == '健康' and es_rating == '稳定':
            return "强烈建议纳入全天候组合 - 债务健康、盈利稳定、分散价值高"
        elif score >= 60 and dc_rating != '脆弱':
            return "建议配置 - 适合作为组合中的风险回报流"
        elif score >= 45:
            return "可考虑小仓位 - 部分属性符合宏观配置要求"
        elif dc_rating == '脆弱':
            return "回避 - 债务周期位置不利，去杠杆风险高"
        else:
            return "不建议 - 不符合达里奥的宏观风险框架"

    def _get_empty_result(self) -> Dict[str, Any]:
        return {
            'debt_cycle_analysis': {
                'details': [], 'score_contrib': 0, 'rating': '未知',
            },
            'earnings_stability': {
                'details': [], 'score_contrib': 0, 'rating': '未知',
            },
            'inflation_hedge': {
                'details': [], 'score_contrib': 0, 'rating': '弱',
            },
            'diversification_value': {
                'details': [], 'score_contrib': 0, 'rating': '一般',
            },
            'real_return_estimate': {
                'details': [], 'estimated_real_return': None, 'score_contrib': 0,
            },
            'system_importance': {
                'details': [], 'score_contrib': 0, 'rating': '低',
            },
            'all_weather_quadrant': {'quadrant': '未知', 'risk_parity_role': '中性配置'},
            'dalio_score': 0,
            'suggestion': "无法分析 - 数据不足",
        }
