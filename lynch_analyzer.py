from typing import Dict, Optional, Any, List
from logger_config import logger
from config import calculate_cagr


class LynchThresholds:
    PEG_BUY = 1.0
    PEG_CONSIDER = 2.0
    DEBT_TO_EQUITY_MAX = 0.5
    CASH_TO_MARKETCAP_MIN = 0.1
    INVENTORY_GROWTH_MAX_VS_SALES = 1.5
    INSTITUTIONAL_MAX_PCT = 60
    FAST_GROWTH_MIN = 25
    STALWART_GROWTH_MIN = 10
    SLOW_GROWTH_MIN = 0
    DIVIDEND_SLOW_GROWER_MIN = 3


class LynchAnalyzer:
    """
    彼得·林奇价值投资分析器

    核心依据：《彼得·林奇的成功投资》

    林奇的核心投资原则：
    1. PEG 指标 - 市盈率相对盈利增长比率（最核心）
    2. 投资于你了解的生意（生活经验选股）
    3. 公司分类：缓慢增长型、稳定增长型、快速增长型、周期型、隐蔽资产型
    4. 避开热门行业中最热门的股票
    5. 机构持股不宜过高（有主力尚未发现的空间）
    6. 现金流与债务 - 现金充裕、负债低
    7. 存货增长不应大幅超过销售增长

    PEG = PE / (盈利增长率 × 100)
    PEG < 1：极具投资价值
    PEG 1-2：合理估值
    PEG > 2：估值偏高
    """

    def analyze(self, stock_data: Dict[str, Any]) -> Dict[str, Any]:
        try:
            financial_data = stock_data.get('financial', {})
            info = stock_data.get('info', {})

            if not financial_data:
                logger.warning("没有财务数据，无法进行林奇分析")
                return self._get_empty_result()

            pe = financial_data.get('pe')
            pb = financial_data.get('pb')
            eps = financial_data.get('eps')
            earnings_history = financial_data.get('earnings_history', [])
            revenue_history = financial_data.get('revenue_history', [])
            net_profit = financial_data.get('net_profit')
            debt_to_equity = financial_data.get('debt_to_equity')
            cash = financial_data.get('cash_and_equivalents')
            total_mv = financial_data.get('total_mv')
            inventory_history = financial_data.get('inventory_history', [])
            institutional_pct = financial_data.get('institutional_ownership_pct')
            dividend_yield = financial_data.get('dividend_yield')
            current_price = financial_data.get('current_price')
            industry = info.get('industry', '')
            stock_name = info.get('stock_name', '')

            growth_rate = self._calculate_earnings_growth(earnings_history)
            peg = self._calculate_peg(pe, growth_rate)
            category = self._classify_company(growth_rate, dividend_yield, industry, stock_name)

            debt_analysis = self._analyze_debt(debt_to_equity, cash, total_mv)
            inventory_analysis = self._analyze_inventory(inventory_history, revenue_history)
            institutional_analysis = self._analyze_institutional(institutional_pct)
            consumer_facing = self._is_consumer_facing(industry, stock_name)

            valuation = self._assess_valuation(peg, pe, pb, growth_rate, category)
            score = self._calculate_score(
                peg, growth_rate, debt_analysis, inventory_analysis,
                institutional_analysis, consumer_facing, valuation, category
            )
            suggestion = self._get_suggestion(score, peg, category, valuation)

            result = {
                'pe': pe,
                'peg': peg,
                'growth_rate': growth_rate,
                'category': category,
                'debt_analysis': debt_analysis,
                'inventory_analysis': inventory_analysis,
                'institutional_analysis': institutional_analysis,
                'consumer_facing': consumer_facing,
                'valuation': valuation,
                'lynch_score': score,
                'suggestion': suggestion,
            }

            logger.info(
                f"林奇分析完成 - PE:{pe}, PEG:{peg}, 分类:{category}, 评分:{score}"
            )
            return result

        except Exception as e:
            logger.error(f"林奇分析失败: {e}")
            return self._get_empty_result()

    def _calculate_earnings_growth(self, earnings_history: List[float]) -> Optional[float]:
        cagr = calculate_cagr(earnings_history)
        if cagr is not None:
            return cagr
        return None

    def _calculate_peg(self, pe, growth_rate) -> Optional[float]:
        if pe is None or pe <= 0:
            return None
        if growth_rate is None or growth_rate <= 0:
            return None
        peg = pe / growth_rate
        return round(peg, 2)

    def _classify_company(
        self, growth_rate: Optional[float], dividend_yield: Optional[float],
        industry: str, stock_name: str
    ) -> str:
        cyclical_keywords = [
            '钢铁', '煤炭', '石油', '化工', '有色', '铝', '铜', '锌',
            '汽车', '航运', '航空', '建材', '水泥', '玻璃', '造纸',
            '半导体', '芯片', '面板', '存储', '光伏', '锂',
        ]
        asset_keywords = [
            '地产', '银行', '保险', '信托', '证券', '港口', '机场',
            '高速公路', '电力', '水务', '燃气',
        ]

        if any(kw in industry for kw in cyclical_keywords):
            return '周期型'
        if any(kw in industry for kw in asset_keywords):
            return '隐蔽资产型'
        if growth_rate is None:
            return '未知类型'
        if growth_rate >= LynchThresholds.FAST_GROWTH_MIN:
            return '快速增长型'
        if growth_rate >= LynchThresholds.STALWART_GROWTH_MIN:
            return '稳定增长型'
        if growth_rate >= LynchThresholds.SLOW_GROWTH_MIN:
            if dividend_yield and dividend_yield >= LynchThresholds.DIVIDEND_SLOW_GROWER_MIN:
                return '缓慢增长型（高股息）'
            return '缓慢增长型'
        return '缓慢增长型（衰退风险）'

    def _analyze_debt(self, debt_to_equity, cash, total_mv) -> Dict[str, Any]:
        analysis = {'debt_to_equity': debt_to_equity, 'details': [], 'score_contrib': 0}

        if debt_to_equity is not None:
            if debt_to_equity <= LynchThresholds.DEBT_TO_EQUITY_MAX:
                analysis['details'].append(
                    f"✓ 负债率健康: D/E={debt_to_equity:.2f} ≤ {LynchThresholds.DEBT_TO_EQUITY_MAX}"
                )
                analysis['score_contrib'] = 15
            elif debt_to_equity <= 1.0:
                analysis['details'].append(
                    f"○ 负债率尚可: D/E={debt_to_equity:.2f}"
                )
                analysis['score_contrib'] = 8
            else:
                analysis['details'].append(
                    f"✗ 负债率偏高: D/E={debt_to_equity:.2f}"
                )
                analysis['score_contrib'] = 0
        else:
            analysis['details'].append("○ 负债数据缺失")
            analysis['score_contrib'] = 5

        if cash and total_mv and total_mv > 0:
            cash_ratio = cash / total_mv
            if cash_ratio >= LynchThresholds.CASH_TO_MARKETCAP_MIN:
                analysis['details'].append(
                    f"✓ 现金充裕: 现金/市值={cash_ratio:.1%}"
                )
                analysis['score_contrib'] += 10
            else:
                analysis['details'].append(
                    f"○ 现金/市值={cash_ratio:.1%}"
                )

        return analysis

    def _analyze_inventory(
        self, inventory_history: List[float], revenue_history: List[float]
    ) -> Dict[str, Any]:
        analysis = {'details': [], 'score_contrib': 10}

        if len(inventory_history) >= 2 and len(revenue_history) >= 2:
            try:
                inv_growth = (
                    (inventory_history[-1] - inventory_history[0]) / abs(inventory_history[0])
                    if inventory_history[0] != 0 else 0
                )
                rev_growth = (
                    (revenue_history[-1] - revenue_history[0]) / abs(revenue_history[0])
                    if revenue_history[0] != 0 else 0
                )
                if rev_growth > 0 and inv_growth / rev_growth > LynchThresholds.INVENTORY_GROWTH_MAX_VS_SALES:
                    analysis['details'].append(
                        f"✗ 存货增长({inv_growth:.1%})远超销售增长({rev_growth:.1%})"
                    )
                    analysis['score_contrib'] = 0
                else:
                    analysis['details'].append(
                        f"✓ 存货与销售增长匹配"
                    )
            except Exception:
                analysis['details'].append("○ 存货数据无法计算")
        else:
            analysis['details'].append("○ 存货历史数据不足")

        return analysis

    def _analyze_institutional(self, institutional_pct: Optional[float]) -> Dict[str, Any]:
        analysis = {'institutional_pct': institutional_pct, 'details': [], 'score_contrib': 10}

        if institutional_pct is None:
            analysis['details'].append("○ 机构持股数据缺失")
            analysis['score_contrib'] = 5
        elif institutional_pct > LynchThresholds.INSTITUTIONAL_MAX_PCT:
            analysis['details'].append(
                f"✗ 机构持股过高({institutional_pct:.0f}%)，上涨空间有限"
            )
            analysis['score_contrib'] = 0
        elif institutional_pct > 40:
            analysis['details'].append(
                f"○ 机构持股{institutional_pct:.0f}%（适中）"
            )
            analysis['score_contrib'] = 5
        else:
            analysis['details'].append(
                f"✓ 机构持股较低({institutional_pct:.0f}%)，存在发现空间"
            )
            analysis['score_contrib'] = 15

        return analysis

    def _is_consumer_facing(self, industry: str, stock_name: str) -> bool:
        consumer_keywords = [
            '食品', '饮料', '白酒', '医药', '医疗', '家电', '服装',
            '零售', '百货', '超市', '美容', '日化', '家居', '文具',
            '玩具', '宠物', '餐饮', '酒店', '旅游', '传媒', '游戏',
        ]
        return any(kw in industry or kw in stock_name for kw in consumer_keywords)

    def _assess_valuation(
        self, peg: Optional[float], pe: Optional[float],
        pb: Optional[float], growth_rate: Optional[float], category: str
    ) -> Dict[str, Any]:
        analysis = {'peg': peg, 'details': [], 'score_contrib': 0}

        if peg is not None:
            if peg <= LynchThresholds.PEG_BUY:
                analysis['details'].append(f"✓ PEG={peg} ≤ 1（显著低估）")
                analysis['score_contrib'] = 35
            elif peg <= 1.5:
                analysis['details'].append(f"✓ PEG={peg}（合理偏低）")
                analysis['score_contrib'] = 25
            elif peg <= LynchThresholds.PEG_CONSIDER:
                analysis['details'].append(f"○ PEG={peg}（合理范围）")
                analysis['score_contrib'] = 15
            else:
                analysis['details'].append(f"✗ PEG={peg} > 2（高估）")
                analysis['score_contrib'] = 0
        else:
            if pe is not None and growth_rate is not None:
                analysis['details'].append("○ PEG无法计算（增长率或PE缺失）")
            else:
                analysis['details'].append("○ 估值数据不足")
            analysis['score_contrib'] = 5

        if category == '快速增长型' and pe is not None:
            if pe <= growth_rate if growth_rate else False:
                analysis['details'].append("✓ PE低于增长率（林奇经典买点）")
                analysis['score_contrib'] += 10

        return analysis

    def _calculate_score(
        self, peg, growth_rate, debt_analysis, inventory_analysis,
        institutional_analysis, consumer_facing, valuation, category
    ) -> int:
        score = 0
        score += debt_analysis.get('score_contrib', 0)
        score += inventory_analysis.get('score_contrib', 0)
        score += institutional_analysis.get('score_contrib', 0)
        score += valuation.get('score_contrib', 0)

        if consumer_facing:
            score += 10

        category_bonus = {
            '快速增长型': 10,
            '稳定增长型': 5,
            '缓慢增长型（高股息）': 5,
            '缓慢增长型': 0,
            '周期型': 0,
            '隐蔽资产型': 5,
        }
        score += category_bonus.get(category, 0)

        if growth_rate is not None:
            if growth_rate >= 30:
                score += 10
            elif growth_rate >= 20:
                score += 5

        return min(int(score), 100)

    def _get_suggestion(self, score: int, peg: Optional[float], category: str, valuation: Dict) -> str:
        if peg is not None and peg <= LynchThresholds.PEG_BUY and score >= 70:
            return f"强烈建议买入 - {category}，PEG={peg}，显著低估"
        elif peg is not None and peg <= 1.5 and score >= 55:
            return f"建议买入 - {category}，PEG合理"
        elif score >= 50:
            return f"可考虑 - {category}，部分指标达标"
        elif score >= 35:
            return f"观望 - {category}，估值或基本面存在疑虑"
        else:
            return f"不建议 - {category}，不符合林奇标准"

    def _get_empty_result(self) -> Dict[str, Any]:
        return {
            'pe': None,
            'peg': None,
            'growth_rate': None,
            'category': '未知类型',
            'debt_analysis': {'details': [], 'score_contrib': 0},
            'inventory_analysis': {'details': [], 'score_contrib': 0},
            'institutional_analysis': {'details': [], 'score_contrib': 0},
            'consumer_facing': False,
            'valuation': {'details': [], 'score_contrib': 0},
            'lynch_score': 0,
            'suggestion': "无法分析 - 数据不足",
        }
