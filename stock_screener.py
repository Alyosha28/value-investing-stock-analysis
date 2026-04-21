import pandas as pd
import time
from typing import Dict, List, Optional, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from logger_config import logger
from config import GrahamThresholds, BuffettThresholds, SystemConfig

class StockScreener:
    """
    A股全市场批量筛选器

    基于 akshare 全市场实时数据，应用格雷厄姆/巴菲特/自定义阈值，
    批量发现符合价值投资标准的标的，并输出评分排名。
    """

    def __init__(self):
        self._ak = None
        try:
            import akshare as ak
            self._ak = ak
        except Exception as e:
            logger.warning(f"akshare 未安装，筛选器将不可用: {e}")

    def screen(self,
               strategy: str = 'comprehensive',
               top_n: int = 50,
               min_market_cap: float = 5e9,
               max_stocks: int = 3000) -> Dict[str, Any]:
        """
        执行全市场筛选

        Args:
            strategy: 筛选策略 - 'graham'(格雷厄姆) / 'buffett'(巴菲特) / 'comprehensive'(综合)
            top_n: 返回前N名
            min_market_cap: 最小市值(元)，默认50亿
            max_stocks: 最大扫描股票数，防止限流
        """
        try:
            logger.info(f"开始全市场筛选，策略={strategy}...")
            raw_data = self._fetch_market_data(max_stocks)
            if raw_data is None or raw_data.empty:
                return {'success': False, 'error': '无法获取市场数据', 'stocks': []}

            filtered = self._apply_basic_filters(raw_data, min_market_cap)
            scored = self._score_stocks(filtered, strategy)
            ranked = scored.sort_values('total_score', ascending=False).head(top_n)

            results = []
            for _, row in ranked.iterrows():
                results.append({
                    'rank': len(results) + 1,
                    'stock_code': row.get('stock_code', ''),
                    'stock_name': row.get('stock_name', ''),
                    'industry': row.get('industry', ''),
                    'total_score': round(row.get('total_score', 0), 1),
                    'graham_score': round(row.get('graham_score', 0), 1),
                    'buffett_score': round(row.get('buffett_score', 0), 1),
                    'pe': self._safe_round(row.get('pe')),
                    'pb': self._safe_round(row.get('pb')),
                    'roe': self._safe_round(row.get('roe')),
                    'dividend_yield': self._safe_round(row.get('dividend_yield')),
                    'market_cap_yi': round(row.get('market_cap', 0) / 1e8, 1) if row.get('market_cap') else None,
                    'graham_pass': row.get('graham_pass', 0),
                    'graham_total': row.get('graham_total', 9),
                    'moat_rating': row.get('moat_rating', '无'),
                    'suggestion': row.get('suggestion', '')
                })

            summary = self._generate_summary(results, strategy)

            return {
                'success': True,
                'strategy': strategy,
                'scan_date': datetime.now().strftime('%Y-%m-%d %H:%M'),
                'total_scanned': len(raw_data),
                'after_basic_filter': len(filtered),
                'summary': summary,
                'stocks': results
            }

        except Exception as e:
            logger.error(f"全市场筛选失败: {e}")
            return {'success': False, 'error': str(e), 'stocks': []}

    def _fetch_market_data(self, max_stocks: int) -> Optional[pd.DataFrame]:
        """获取全市场实时数据"""
        if not self._ak:
            return None

        try:
            df = self._ak.stock_zh_a_spot_em()
            if df is None or df.empty:
                return None

            column_map = {
                '代码': 'stock_code',
                '名称': 'stock_name',
                '市盈率': 'pe',
                '市净率': 'pb',
                '总市值': 'market_cap',
                '流通市值': 'float_cap',
                'ROE': 'roe',
                '股息率': 'dividend_yield',
                '所属行业': 'industry',
                '最新价': 'current_price',
                '涨跌幅': 'change_pct',
                '换手率': 'turnover',
                '振幅': 'amplitude',
                '量比': 'volume_ratio',
                '60日涨跌幅': 'change_60d',
                '年初至今涨跌幅': 'change_ytd',
                '每股未分配利润': 'retained_eps',
                '市销率': 'ps',
                '总股本': 'total_share',
            }

            for old_col, new_col in column_map.items():
                if old_col in df.columns:
                    df[new_col] = df[old_col]

            numeric_cols = ['pe', 'pb', 'roe', 'dividend_yield', 'market_cap', 'current_price',
                           'change_pct', 'turnover', 'ps']
            for col in numeric_cols:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')

            df = df[df['market_cap'].notna() & (df['market_cap'] > 0)]
            df = df.head(max_stocks)

            logger.info(f"获取全市场数据成功，共 {len(df)} 只股票")
            return df

        except Exception as e:
            logger.error(f"获取全市场数据失败: {e}")
            return None

    def _apply_basic_filters(self, df: pd.DataFrame, min_market_cap: float) -> pd.DataFrame:
        """基础过滤：排除明显不合格的股票"""
        before = len(df)

        df = df[df['market_cap'] >= min_market_cap].copy()

        df = df[df['pe'].notna() & (df['pe'] > 0)]

        df = df[df['pb'].notna() & (df['pb'] > 0)]

        after = len(df)
        logger.info(f"基础过滤: {before} -> {after} 只 (排除小市值/负PE/负PB)")
        return df

    def _score_stocks(self, df: pd.DataFrame, strategy: str) -> pd.DataFrame:
        """对股票进行多维度评分"""
        df = df.copy()

        df['graham_score'] = df.apply(self._calc_graham_score, axis=1)
        df['buffett_score'] = df.apply(self._calc_buffett_score, axis=1)
        df['graham_pass'] = df.apply(self._calc_graham_pass_count, axis=1)
        df['graham_total'] = 9
        df['moat_rating'] = df.apply(self._calc_moat_rating, axis=1)

        if strategy == 'graham':
            df['total_score'] = df['graham_score']
        elif strategy == 'buffett':
            df['total_score'] = df['buffett_score']
        else:
            graham_weight = 0.4
            buffett_weight = 0.6
            df['total_score'] = df['graham_score'] * graham_weight + df['buffett_score'] * buffett_weight

        df['suggestion'] = df.apply(lambda row: self._get_suggestion(row, strategy), axis=1)
        return df

    def _calc_graham_score(self, row) -> float:
        """格雷厄姆评分（简化版）"""
        score = 0
        pe = row.get('pe')
        pb = row.get('pb')
        roe = row.get('roe')
        market_cap = row.get('market_cap', 0)

        if pe and pe <= 15:
            score += 20
        elif pe and pe <= 20:
            score += 10

        if pb and pb <= 1.5:
            score += 20
        elif pb and pb <= 2.0:
            score += 10

        if pe and pb and pe * pb <= 22.5:
            score += 15

        if roe and roe >= 15:
            score += 20
        elif roe and roe >= 10:
            score += 10

        if market_cap >= 5e10:
            score += 10
        elif market_cap >= 1e10:
            score += 5

        dividend = row.get('dividend_yield')
        if dividend and dividend >= 2:
            score += 15
        elif dividend and dividend >= 1:
            score += 8

        return min(score, 100)

    def _calc_graham_pass_count(self, row) -> int:
        """格雷厄姆通过项计数"""
        passed = 0
        pe = row.get('pe')
        pb = row.get('pb')
        roe = row.get('roe')
        market_cap = row.get('market_cap', 0)
        dividend = row.get('dividend_yield')

        if market_cap >= 1e9: passed += 1
        if pe and 0 < pe <= 15: passed += 1
        if pb and 0 < pb <= 1.5: passed += 1
        if pe and pb and pe * pb <= 22.5: passed += 1
        if roe and roe >= 12: passed += 1
        if dividend and dividend > 0: passed += 1
        return passed

    def _calc_buffett_score(self, row) -> float:
        """巴菲特评分（简化版）"""
        score = 0
        roe = row.get('roe')
        pe = row.get('pe')
        pb = row.get('pb')
        dividend = row.get('dividend_yield')
        market_cap = row.get('market_cap', 0)

        if roe and roe >= 20:
            score += 35
        elif roe and roe >= 15:
            score += 25
        elif roe and roe >= 10:
            score += 15

        if pe and 0 < pe <= 15:
            score += 20
        elif pe and pe <= 25:
            score += 10

        if pb and 0 < pb <= 1.5:
            score += 10

        if dividend and dividend >= 3:
            score += 20
        elif dividend and dividend >= 1:
            score += 10

        if market_cap >= 1e11:
            score += 10
        elif market_cap >= 5e10:
            score += 5

        ps = row.get('ps')
        if ps and ps <= 3:
            score += 5

        return min(score, 100)

    def _calc_moat_rating(self, row) -> str:
        """护城河评级（简化版，仅基于公开指标）"""
        roe = row.get('roe')
        market_cap = row.get('market_cap', 0)
        score = 0

        if roe and roe >= 20: score += 4
        elif roe and roe >= 15: score += 3
        elif roe and roe >= 10: score += 2

        if market_cap >= 1e11: score += 2
        elif market_cap >= 5e10: score += 1

        if score >= 5: return '宽护城河'
        elif score >= 3: return '中等护城河'
        elif score >= 1: return '窄护城河'
        return '无护城河'

    def _get_suggestion(self, row, strategy: str) -> str:
        """生成投资建议"""
        total = row.get('total_score', 0)
        graham = row.get('graham_score', 0)
        buffett = row.get('buffett_score', 0)
        pe = row.get('pe')
        pb = row.get('pb')

        if strategy == 'graham':
            if graham >= 70 and pe and pe <= 15 and pb and pb <= 1.5:
                return '强烈建议关注 - 深度价值'
            elif graham >= 50:
                return '建议关注 - 符合格雷厄姆标准'
        elif strategy == 'buffett':
            if buffett >= 75:
                return '强烈建议关注 - 优质企业'
            elif buffett >= 60:
                return '建议关注 - 基本面良好'
        else:
            if total >= 70:
                return '强烈建议关注 - 综合评分优秀'
            elif total >= 55:
                return '建议关注 - 综合评分良好'
            elif total >= 40:
                return '可考虑 - 部分指标达标'

        return '观望 - 暂不符合核心标准'

    def _generate_summary(self, results: List[Dict], strategy: str) -> str:
        """生成筛选结果摘要"""
        if not results:
            return '未找到符合条件的股票'

        avg_score = sum(s['total_score'] for s in results) / len(results)
        avg_pe = sum(s['pe'] for s in results if s['pe']) / max(1, sum(1 for s in results if s['pe']))
        avg_pb = sum(s['pb'] for s in results if s['pb']) / max(1, sum(1 for s in results if s['pb']))

        industries = {}
        for s in results:
            ind = s.get('industry', '未知')
            industries[ind] = industries.get(ind, 0) + 1
        top_industries = sorted(industries.items(), key=lambda x: x[1], reverse=True)[:3]

        summary = f"""筛选策略: {strategy}
Top {len(results)} 平均评分: {avg_score:.1f}
平均 PE: {avg_pe:.1f} | 平均 PB: {avg_pb:.2f}
主要行业分布: {', '.join(f'{k}({v}只)' for k, v in top_industries)}
"""
        return summary.strip()

    def export_to_csv(self, result: Dict[str, Any], filepath: str):
        """导出筛选结果到 CSV"""
        stocks = result.get('stocks', [])
        if not stocks:
            logger.warning("无数据可导出")
            return

        df = pd.DataFrame(stocks)
        df.to_csv(filepath, index=False, encoding='utf-8-sig')
        logger.info(f"筛选结果已导出: {filepath}")

    def _safe_round(self, val, digits: int = 2):
        if val is None or pd.isna(val):
            return None
        try:
            return round(float(val), digits)
        except:
            return None


if __name__ == '__main__':
    screener = StockScreener()
    result = screener.screen(strategy='comprehensive', top_n=20)
    if result['success']:
        print(f"扫描 {result['total_scanned']} 只，过滤后 {result['after_basic_filter']} 只")
        print(result['summary'])
        print("\nTop 10:")
        for s in result['stocks'][:10]:
            print(f"{s['rank']}. {s['stock_name']}({s['stock_code']}) 评分:{s['total_score']} PE:{s['pe']} PB:{s['pb']} ROE:{s['roe']}% {s['suggestion']}")
    else:
        print(f"筛选失败: {result['error']}")
