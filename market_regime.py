import pandas as pd
import numpy as np
from typing import Dict, Optional, Any
from datetime import datetime
from logger_config import logger

class MarketRegimeAnalyzer:
    """
    市场阶段判断器

    基于大盘指数（上证指数 + 沪深300）的均线系统与 MACD，
    判断当前 A 股市场处于何种宏观阶段，并给出建议仓位。

    借鉴思路：AI-Value-Investor 的 market stage 判断，
    但使用更稳定的 akshare 数据源，并增加趋势强度量化。
    """

    INDEX_CODES = {
        '上证指数': '000001',
        '沪深300': '000300'
    }

    def __init__(self, data_fetcher=None):
        self._data_fetcher = data_fetcher
        self._ak = None
        try:
            import akshare as ak
            self._ak = ak
        except Exception as e:
            logger.warning(f"akshare 未安装，市场阶段分析将受限: {e}")

    def analyze(self) -> Dict[str, Any]:
        """分析当前市场阶段"""
        try:
            index_data = self._get_index_data()
            if not index_data:
                return self._get_default_result()

            regimes = {}
            for name, data in index_data.items():
                regimes[name] = self._analyze_single_index(data)

            composite = self._composite_regime(regimes)

            return {
                'index_regimes': regimes,
                'composite_regime': composite['regime'],
                'trend_strength': composite['trend_strength'],
                'volatility_regime': composite['volatility'],
                'recommend_position': composite['position'],
                'analysis_date': datetime.now().strftime('%Y-%m-%d'),
                'details': composite['details']
            }

        except Exception as e:
            logger.error(f"市场阶段分析失败: {e}")
            return self._get_default_result()

    def _get_index_data(self) -> Dict[str, pd.DataFrame]:
        """获取指数历史数据（通过DataHub实现去重共享）"""
        from data_hub import DataHub
        hub = DataHub.get_instance()

        result = {}
        for name, code in self.INDEX_CODES.items():
            try:
                df = hub.get_index_history(code, name)
                if df is not None and not df.empty:
                    result[name] = df
                    logger.info(f"[MarketRegime] 从DataHub获取 {name} 数据成功，共 {len(df)} 条")
            except Exception as e:
                logger.warning(f"获取 {name} 数据失败: {e}")

        if not result:
            if self._ak:
                logger.info("[MarketRegime] DataHub无缓存，尝试直接获取...")
                for name, code in self.INDEX_CODES.items():
                    try:
                        df = self._ak.index_zh_a_hist(symbol=code, period="daily",
                                                       start_date="20230101")
                        if df is not None and not df.empty:
                            df = df.rename(columns={
                                '日期': 'Date',
                                '开盘': 'Open',
                                '收盘': 'Close',
                                '最高': 'High',
                                '最低': 'Low',
                                '成交量': 'Volume'
                            })
                            df['Date'] = pd.to_datetime(df['Date'])
                            df = df.set_index('Date').sort_index()
                            result[name] = df
                            logger.info(f"获取 {name} 数据成功，共 {len(df)} 条")
                    except Exception as e:
                        logger.warning(f"获取 {name} 数据失败: {e}")

        return result

    def _analyze_single_index(self, data: pd.DataFrame) -> Dict[str, Any]:
        """分析单个指数的市场阶段"""
        if len(data) < 200:
            return {'stage': '数据不足', 'trend_score': 50}

        close = data['Close']

        ma20 = close.rolling(20).mean()
        ma50 = close.rolling(50).mean()
        ma200 = close.rolling(200).mean()

        ema12 = close.ewm(span=12, adjust=False).mean()
        ema26 = close.ewm(span=26, adjust=False).mean()
        macd_line = ema12 - ema26
        signal_line = macd_line.ewm(span=9, adjust=False).mean()

        latest = close.iloc[-1]
        prev = close.iloc[-2]
        ma20_val = ma20.iloc[-1]
        ma50_val = ma50.iloc[-1]
        ma200_val = ma200.iloc[-1]
        prev_ma20 = ma20.iloc[-2]

        macd_val = macd_line.iloc[-1]
        signal_val = signal_line.iloc[-1]
        prev_macd = macd_line.iloc[-2]

        returns = close.pct_change().dropna()
        volatility = returns.rolling(20).std().iloc[-1] * np.sqrt(252) * 100
        avg_vol = (returns.rolling(100).std().iloc[-1] * np.sqrt(252) * 100) if len(returns) >= 100 else volatility
        high_vol = volatility > avg_vol * 1.5 if avg_vol > 0 else False

        ma20_gt_ma50 = ma20_val > ma50_val
        ma50_gt_ma200 = ma50_val > ma200_val
        ma20_rising = ma20_val > prev_ma20
        macd_bull = macd_val > signal_val
        macd_rising = macd_val > prev_macd
        price_gt_ma200 = latest > ma200_val

        bullish_signals = sum([ma20_gt_ma50, ma50_gt_ma200, ma20_rising, macd_bull, macd_rising, price_gt_ma200])
        bearish_signals = sum([not ma20_gt_ma50, not ma50_gt_ma200, not ma20_rising, not macd_bull, not macd_rising, not price_gt_ma200])

        if bullish_signals >= 5:
            if high_vol:
                stage = '牛市初期 - 波动较大'
            else:
                stage = '牛市中期 - 稳定上涨'
        elif bearish_signals >= 5:
            if high_vol:
                stage = '熊市初期 - 波动较大'
            else:
                stage = '熊市中期 - 持续下跌'
        elif ma20_gt_ma50 and not ma50_gt_ma200 and ma20_rising:
            stage = '震荡上行 - 短期走强'
        elif not ma20_gt_ma50 and ma50_gt_ma200 and not ma20_rising:
            stage = '震荡下行 - 短期走弱'
        else:
            stage = '震荡整理 - 方向不明'

        trend_score = int((bullish_signals / 6) * 100)

        return {
            'stage': stage,
            'trend_score': trend_score,
            'latest_close': round(latest, 2),
            'ma20': round(ma20_val, 2),
            'ma50': round(ma50_val, 2),
            'ma200': round(ma200_val, 2),
            'macd': round(macd_val, 4),
            'macd_signal': round(signal_val, 4),
            'volatility_pct': round(volatility, 2)
        }

    def _composite_regime(self, regimes: Dict[str, Any]) -> Dict[str, Any]:
        """综合多个指数判断整体市场阶段"""
        if not regimes:
            return {'regime': '未知', 'trend_strength': 0, 'volatility': '未知', 'position': 50, 'details': []}

        stages = [r['stage'] for r in regimes.values()]
        scores = [r['trend_score'] for r in regimes.values()]

        avg_trend = int(sum(scores) / len(scores))

        stage_counts = {}
        for s in stages:
            stage_counts[s] = stage_counts.get(s, 0) + 1
        most_common = max(stage_counts, key=stage_counts.get)

        volatilities = [r.get('volatility_pct', 15) for r in regimes.values()]
        avg_vol = sum(volatilities) / len(volatilities)
        if avg_vol > 25:
            vol_regime = '高波动'
        elif avg_vol > 15:
            vol_regime = '中等波动'
        else:
            vol_regime = '低波动'

        position_map = {
            '牛市中期 - 稳定上涨': 90,
            '牛市初期 - 波动较大': 70,
            '震荡上行 - 短期走强': 60,
            '震荡整理 - 方向不明': 50,
            '震荡下行 - 短期走弱': 30,
            '熊市初期 - 波动较大': 20,
            '熊市中期 - 持续下跌': 10,
            '数据不足': 50,
            '未知': 50
        }
        position = position_map.get(most_common, 50)

        if avg_trend >= 70 and vol_regime == '高波动':
            position = min(position + 10, 100)
        elif avg_trend <= 30 and vol_regime == '高波动':
            position = max(position - 10, 0)

        details = []
        for name, r in regimes.items():
            details.append(f"{name}: {r['stage']} (趋势分{r['trend_score']}, 波动{r['volatility_pct']}%)")

        return {
            'regime': most_common,
            'trend_strength': avg_trend,
            'volatility': vol_regime,
            'position': position,
            'details': details
        }

    def get_position_advice(self, regime_result: Dict[str, Any]) -> str:
        """根据市场阶段给出仓位建议文字"""
        regime = regime_result.get('composite_regime', '未知')
        position = regime_result.get('recommend_position', 50)

        advice_map = {
            '牛市中期 - 稳定上涨': '市场处于强势上涨期，可重仓持有优质股票，关注行业龙头。',
            '牛市初期 - 波动较大': '市场趋势向好但波动剧烈，建议逐步加仓，保留部分现金应对回调。',
            '震荡上行 - 短期走强': '短期趋势改善，可适度加仓，优选基本面扎实的标的。',
            '震荡整理 - 方向不明': '市场方向不明，建议保持中性仓位，观望为主，避免追涨杀跌。',
            '震荡下行 - 短期走弱': '短期趋势偏弱，建议降低仓位，增配防御性板块。',
            '熊市初期 - 波动较大': '市场风险释放中，建议轻仓或空仓，现金为王，等待企稳信号。',
            '熊市中期 - 持续下跌': '市场处于下跌通道，严格控制仓位，仅保留核心底仓。'
        }
        base = advice_map.get(regime, '市场阶段不明，建议保持谨慎。')
        return f"建议仓位：{position}%。{base}"

    def _get_default_result(self) -> Dict[str, Any]:
        return {
            'index_regimes': {},
            'composite_regime': '未知',
            'trend_strength': 0,
            'volatility_regime': '未知',
            'recommend_position': 50,
            'analysis_date': datetime.now().strftime('%Y-%m-%d'),
            'details': ['数据获取失败']
        }


if __name__ == '__main__':
    analyzer = MarketRegimeAnalyzer()
    result = analyzer.analyze()
    print(f"市场阶段: {result['composite_regime']}")
    print(f"趋势强度: {result['trend_strength']}/100")
    print(f"建议仓位: {result['recommend_position']}%")
    print(analyzer.get_position_advice(result))
    for d in result['details']:
        print(d)
