from typing import Dict, Optional, Any, List
from logger_config import logger
from config import calculate_cagr


class MungerThresholds:
    ROIC_MIN = 15
    ROE_MIN = 15
    GROSS_MARGIN_MIN = 30
    NET_MARGIN_MIN = 10
    DEBT_TO_EQUITY_MAX = 0.5
    FCF_TO_NET_PROFIT_MIN = 0.8
    EARNINGS_STABILITY_YEARS = 5
    EARNINGS_STABILITY_STD_MAX = 15
    REINVESTMENT_RATE_MIN = 0.3
    CAPITAL_INTENSITY_MAX = 0.3


class MungerAnalyzer:
    """
    查理·芒格价值投资分析器

    核心依据：芒格的投资原则与多元思维模型

    芒格的核心投资原则（按优先级）：
    1. 以合理价格买入伟大的公司（Quality at a Fair Price）
    2. Invert, always invert（反过来想，总是反过来想）
    3. 能力圈原则 - 只投自己能懂的生意
    4. 检查清单思维 - 排除失败因素
    5. 长期复利 - 寻找能长期 compound 的企业
    6. Lollapalooza 效应 - 多个正向因素叠加
    7. 理性资本配置 - 管理层是否明智
    8. 避免与糟糕的人/企业打交道

    芒格最看重的指标：
    - ROIC（投入资本回报率）> 15%
    - 高毛利率（定价权）
    - 低资本开支需求（轻资产）
    - 长期盈利稳定性
    - 自由现金流 / 净利润 > 80%
    - 再投资回报率
    """

    def analyze(self, stock_data: Dict[str, Any]) -> Dict[str, Any]:
        try:
            financial_data = stock_data.get('financial', {})
            info = stock_data.get('info', {})

            if not financial_data:
                logger.warning("没有财务数据，无法进行芒格分析")
                return self._get_empty_result()

            roe = financial_data.get('roe')
            roic = financial_data.get('roic')
            pe = financial_data.get('pe')
            pb = financial_data.get('pb')
            gross_margin = financial_data.get('gross_margin')
            net_margin = financial_data.get('net_margin')
            free_cashflow = financial_data.get('free_cashflow')
            net_profit = financial_data.get('net_profit')
            revenue = financial_data.get('revenue')
            earnings_history = financial_data.get('earnings_history', [])
            roe_history = financial_data.get('roe_history', [])
            debt_to_equity = financial_data.get('debt_to_equity')
            capex = financial_data.get('capex')
            depreciation = financial_data.get('depreciation')
            total_assets = financial_data.get('total_assets')
            current_price = financial_data.get('current_price')
            industry = info.get('industry', '')

            quality_analysis = self._analyze_business_quality(
                roe, roic, gross_margin, net_margin, earnings_history, roe_history
            )
            capital_efficiency = self._analyze_capital_efficiency(
                free_cashflow, net_profit, revenue, capex, depreciation, total_assets
            )
            invert_checklist = self._invert_checklist(
                debt_to_equity, earnings_history, roe_history, industry, financial_data
            )
            lollapalooza = self._count_lollapalooza_effects(
                quality_analysis, capital_efficiency, invert_checklist
            )

            score = self._calculate_score(
                quality_analysis, capital_efficiency, invert_checklist, lollapalooza
            )
            suggestion = self._get_suggestion(score, quality_analysis, invert_checklist)

            result = {
                'roe': roe,
                'roic': roic,
                'pe': pe,
                'pb': pb,
                'quality_analysis': quality_analysis,
                'capital_efficiency': capital_efficiency,
                'invert_checklist': invert_checklist,
                'lollapalooza_score': lollapalooza,
                'munger_score': score,
                'suggestion': suggestion,
            }

            logger.info(
                f"芒格分析完成 - ROE:{roe}, ROIC:{roic}, 质量:{quality_analysis['rating']}, "
                f"评分:{score}"
            )
            return result

        except Exception as e:
            logger.error(f"芒格分析失败: {e}")
            return self._get_empty_result()

    def _analyze_business_quality(
        self, roe, roic, gross_margin, net_margin,
        earnings_history: List[float], roe_history: List[float]
    ) -> Dict[str, Any]:
        analysis = {
            'details': [],
            'score_contrib': 0,
            'rating': '未知',
            'earnings_stability': None,
        }
        score = 0

        def _norm(val, good, bad):
            if val is None:
                return None
            s = (val - bad) / (good - bad) * 100
            return max(0, min(100, int(s)))

        # ROIC 连续评分 0-25
        if roic is not None:
            rs = _norm(roic, 20, 0)
            roic_score = rs * 0.25
            score += int(roic_score)
            if roic >= MungerThresholds.ROIC_MIN:
                analysis['details'].append(f"✓ ROIC={roic:.1f}%（资本回报优秀） +{int(roic_score)}")
            elif roic >= 10:
                analysis['details'].append(f"○ ROIC={roic:.1f}%（尚可） +{int(roic_score)}")
            else:
                analysis['details'].append(f"✗ ROIC={roic:.1f}%（不足） +{int(roic_score)}")
        else:
            analysis['details'].append("○ ROIC数据缺失")
            if roe is not None and roe >= MungerThresholds.ROE_MIN:
                score += 15

        # ROE 连续评分 0-15
        if roe is not None:
            rs = _norm(roe, 25, 0)
            roe_score = rs * 0.15
            score += int(roe_score)
            if roe >= 20:
                analysis['details'].append(f"✓ ROE={roe:.1f}%（卓越） +{int(roe_score)}")
            elif roe >= MungerThresholds.ROE_MIN:
                analysis['details'].append(f"✓ ROE={roe:.1f}%（良好） +{int(roe_score)}")
            elif roe >= 10:
                analysis['details'].append(f"○ ROE={roe:.1f}%（一般） +{int(roe_score)}")
            else:
                analysis['details'].append(f"✗ ROE={roe:.1f}%（差） +{int(roe_score)}")

        # 毛利率 连续评分 0-15
        if gross_margin is not None:
            gs = _norm(gross_margin, 50, 0)
            gm_score = gs * 0.15
            score += int(gm_score)
            if gross_margin >= 40:
                analysis['details'].append(f"✓ 毛利率={gross_margin:.1f}%（强定价权） +{int(gm_score)}")
            elif gross_margin >= MungerThresholds.GROSS_MARGIN_MIN:
                analysis['details'].append(f"✓ 毛利率={gross_margin:.1f}%（合理） +{int(gm_score)}")
            elif gross_margin >= 15:
                analysis['details'].append(f"○ 毛利率={gross_margin:.1f}%（一般） +{int(gm_score)}")
            else:
                analysis['details'].append(f"✗ 毛利率={gross_margin:.1f}%（商品型企业） +{int(gm_score)}")

        # 净利率 连续评分 0-10
        if net_margin is not None:
            ns = _norm(net_margin, 20, 0)
            nm_score = ns * 0.10
            score += int(nm_score)
            if net_margin >= MungerThresholds.NET_MARGIN_MIN:
                analysis['details'].append(f"✓ 净利率={net_margin:.1f}%（强） +{int(nm_score)}")
            elif net_margin >= 5:
                analysis['details'].append(f"○ 净利率={net_margin:.1f}% +{int(nm_score)}")
            else:
                analysis['details'].append(f"✗ 净利率={net_margin:.1f}%（弱） +{int(nm_score)}")

        stability_score, stability_desc = self._evaluate_earnings_stability(
            earnings_history, roe_history
        )
        analysis['earnings_stability'] = stability_desc
        analysis['details'].append(stability_desc)
        score += stability_score

        analysis['score_contrib'] = score
        if score >= 60:
            analysis['rating'] = '卓越企业'
        elif score >= 40:
            analysis['rating'] = '良好企业'
        elif score >= 20:
            analysis['rating'] = '一般企业'
        else:
            analysis['rating'] = '差劲企业'

        return analysis

    def _evaluate_earnings_stability(
        self, earnings_history: List[float], roe_history: List[float]
    ):
        years_needed = MungerThresholds.EARNINGS_STABILITY_YEARS

        if earnings_history and len(earnings_history) >= years_needed:
            recent = earnings_history[-years_needed:]
            if all(e and e > 0 for e in recent):
                if len(recent) >= 2:
                    avg = sum(recent) / len(recent)
                    std = (sum((e - avg) ** 2 for e in recent) / len(recent)) ** 0.5
                    cv = (std / avg * 100) if avg != 0 else 999
                    if cv <= MungerThresholds.EARNINGS_STABILITY_STD_MAX:
                        return 15, f"✓ 盈利稳定: 近{years_needed}年CV={cv:.1f}%"
                    else:
                        return 8, f"○ 盈利波动: 近{years_needed}年CV={cv:.1f}%"
                else:
                    return 10, f"✓ 近{years_needed}年盈利均为正"
            else:
                return 0, f"✗ 近{years_needed}年存在亏损"
        else:
            return 5, "○ 盈利历史数据不足"

    def _analyze_capital_efficiency(
        self, free_cashflow, net_profit, revenue,
        capex, depreciation, total_assets
    ) -> Dict[str, Any]:
        analysis = {'details': [], 'score_contrib': 0}
        score = 0

        if free_cashflow is not None and net_profit is not None and net_profit > 0:
            fcf_ratio = free_cashflow / net_profit
            if fcf_ratio >= MungerThresholds.FCF_TO_NET_PROFIT_MIN:
                analysis['details'].append(
                    f"✓ FCF/净利润={fcf_ratio:.1%}（利润含金量高）"
                )
                score += 15
            elif fcf_ratio >= 0.5:
                analysis['details'].append(
                    f"○ FCF/净利润={fcf_ratio:.1%}"
                )
                score += 8
            else:
                analysis['details'].append(
                    f"✗ FCF/净利润={fcf_ratio:.1%}（利润含金量低）"
                )
        else:
            analysis['details'].append("○ FCF或净利润数据缺失")
            score += 5

        if capex is not None and depreciation is not None and depreciation > 0:
            capex_ratio = capex / depreciation
            if capex_ratio <= 1.0:
                analysis['details'].append(
                    f"✓ 资本开支低: Capex/折旧={capex_ratio:.1f}（轻资产）"
                )
                score += 15
            elif capex_ratio <= 1.5:
                analysis['details'].append(
                    f"○ 资本开支适中: Capex/折旧={capex_ratio:.1f}"
                )
                score += 10
            else:
                analysis['details'].append(
                    f"✗ 资本开支高: Capex/折旧={capex_ratio:.1f}（重资产）"
                )
        else:
            analysis['details'].append("○ 资本开支数据缺失")
            score += 5

        if revenue and total_assets and total_assets > 0:
            asset_turnover = revenue / total_assets
            if asset_turnover >= 0.8:
                analysis['details'].append(
                    f"✓ 资产周转率={asset_turnover:.2f}（资产运用高效）"
                )
                score += 5
            else:
                analysis['details'].append(
                    f"○ 资产周转率={asset_turnover:.2f}"
                )
                score += 3
        else:
            analysis['details'].append("○ 资产周转数据缺失")
            score += 3

        analysis['score_contrib'] = score
        return analysis

    def _invert_checklist(
        self, debt_to_equity, earnings_history: List[float],
        roe_history: List[float], industry: str, financial_data: Dict
    ) -> Dict[str, Any]:
        checklist = {'failures': [], 'passes': [], 'score_contrib': 40}

        if debt_to_equity is not None:
            if debt_to_equity > MungerThresholds.DEBT_TO_EQUITY_MAX:
                deduct = min(int(debt_to_equity / 1.5 * 10), 10)
                checklist['failures'].append(f"✗ 负债过高: D/E={debt_to_equity:.2f} -{deduct}")
                checklist['score_contrib'] -= deduct
            else:
                checklist['passes'].append("✓ 负债水平可控")

        if earnings_history and len(earnings_history) >= 3:
            negative_years = sum(1 for e in earnings_history[-3:] if e is None or e <= 0)
            if negative_years > 0:
                deduct = negative_years * 5
                checklist['failures'].append(f"✗ 近3年有{negative_years}年亏损 -{deduct}")
                checklist['score_contrib'] -= deduct
            else:
                checklist['passes'].append("✓ 近三年持续盈利")

        if roe_history and len(roe_history) >= 3:
            low_roe = sum(1 for r in roe_history[-3:] if r is None or r < 8)
            if low_roe > 0:
                deduct = low_roe * 4
                checklist['failures'].append(f"✗ 近3年有{low_roe}年ROE低于8% -{deduct}")
                checklist['score_contrib'] -= deduct
            else:
                checklist['passes'].append("✓ 近三年ROE稳定")

        commodity_keywords = [
            '钢铁', '煤炭', '石油', '化工', '有色', '水泥', '造纸',
            '航运', '航空', '建材', '玻璃', '纺织', '农业',
        ]
        if any(kw in industry for kw in commodity_keywords):
            checklist['failures'].append(f"○ 大宗商品/强周期行业: {industry}")
            checklist['score_contrib'] -= 5
        else:
            checklist['passes'].append("✓ 非大宗商品型企业")

        total_debt = financial_data.get('total_debt')
        cash = financial_data.get('cash_and_equivalents')
        if total_debt and cash and total_debt > cash * 3:
            checklist['failures'].append("✗ 债务远超现金储备")
            checklist['score_contrib'] -= 5
        elif total_debt and cash:
            checklist['passes'].append("✓ 现金与债务相对平衡")

        return checklist

    def _count_lollapalooza_effects(
        self, quality: Dict, capital: Dict, invert: Dict
    ) -> int:
        count = 0
        if quality.get('rating') in ('卓越企业', '良好企业'):
            count += 1
        if capital.get('score_contrib', 0) >= 20:
            count += 1
        if not invert.get('failures'):
            count += 1
        if quality.get('earnings_stability', '').startswith('✓'):
            count += 1
        return count

    def _calculate_score(
        self, quality_analysis, capital_efficiency, invert_checklist, lollapalooza
    ) -> int:
        score = quality_analysis.get('score_contrib', 0)
        score += capital_efficiency.get('score_contrib', 0)
        score += invert_checklist.get('score_contrib', 0)
        score += lollapalooza * 5
        return min(int(score), 100)

    def _get_suggestion(self, score: int, quality_analysis: Dict, invert_checklist: Dict) -> str:
        rating = quality_analysis.get('rating', '未知')
        failures = invert_checklist.get('failures', [])

        if score >= 80 and rating == '卓越企业' and not failures:
            return "强烈建议买入 - 伟大企业，值得长期持有"
        elif score >= 70 and rating in ('卓越企业', '良好企业'):
            return "建议买入 - 优质企业，基本面扎实"
        elif score >= 55:
            return "可考虑 - 企业质量尚可，需关注负面因素"
        elif score >= 40:
            return "观望 - 存在明显不足，等待改善"
        else:
            return "不建议 - 不符合芒格的质量标准"

    def _get_empty_result(self) -> Dict[str, Any]:
        return {
            'roe': None,
            'roic': None,
            'pe': None,
            'pb': None,
            'quality_analysis': {
                'details': [], 'score_contrib': 0, 'rating': '未知', 'earnings_stability': None,
            },
            'capital_efficiency': {'details': [], 'score_contrib': 0},
            'invert_checklist': {'failures': [], 'passes': [], 'score_contrib': 0},
            'lollapalooza_score': 0,
            'munger_score': 0,
            'suggestion': "无法分析 - 数据不足",
        }
