import pandas as pd
import numpy as np
from typing import Dict, Optional, Any
from datetime import datetime
from logger_config import logger

class MarketRegimeAnalyzer:
    """
    市场阶段判断器

    基于 A 股七大指数（上证/深证/创业板/科创50/上证50/沪深300/中证500）的
    均线系统与 MACD，结合实时行情快照，判断市场阶段、宽度与建议仓位。
    """

    INDEX_CODES = {
        '上证指数': '000001',
        '深证成指': '399001',
        '创业板指': '399006',
        '科创50':   '000688',
        '上证50':   '000016',
        '沪深300':  '000300',
        '中证500':  '000905',
    }

    def __init__(self, data_fetcher=None):
        self._data_fetcher = data_fetcher
        self._ak = None
        self._cached_snapshot_df = None
        self._cached_snapshot_ts = 0
        self._snapshot_cache_ttl = 60
        try:
            import akshare as ak
            self._ak = ak
        except Exception as e:
            logger.warning(f"akshare 未安装，市场阶段分析将受限: {e}")

    def analyze(self) -> Dict[str, Any]:
        """分析当前市场阶段（含实时行情快照）"""
        try:
            index_data = self._get_index_data()
            snapshots = self._fetch_index_snapshot()

            if not index_data:
                if not snapshots:
                    return self._get_default_result()
                return {
                    'index_regimes': {},
                    'composite_regime': '数据不足（仅快照可用）',
                    'trend_strength': 0,
                    'volatility_regime': '未知',
                    'recommend_position': 50,
                    'analysis_date': datetime.now().strftime('%Y-%m-%d'),
                    'details': ['历史数据获取失败，仅可查看实时行情'],
                    'index_snapshots': snapshots,
                    'breadth_ratio': 0.5,
                    'bullish_count': 0,
                    'total_index_count': len(self.INDEX_CODES),
                }

            regimes = {}
            for name, data in index_data.items():
                regimes[name] = self._analyze_single_index(data)

            for name, snap in snapshots.items():
                if name in regimes:
                    regimes[name]['latest_close'] = snap['latest_price']
                    regimes[name]['change_pct'] = snap['change_pct']
                    regimes[name]['change_amount'] = snap['change_amount']
                    regimes[name]['amplitude'] = snap['amplitude']
                    regimes[name]['open'] = snap['open']
                    regimes[name]['high'] = snap['high']
                    regimes[name]['low'] = snap['low']
                    regimes[name]['prev_close'] = snap['prev_close']
                    regimes[name]['volume'] = snap['volume']
                    regimes[name]['amount'] = snap['amount']

            composite = self._composite_regime(regimes)

            return {
                'index_regimes': regimes,
                'composite_regime': composite['regime'],
                'trend_strength': composite['trend_strength'],
                'volatility_regime': composite['volatility'],
                'recommend_position': composite['position'],
                'analysis_date': datetime.now().strftime('%Y-%m-%d'),
                'details': composite['details'],
                'index_snapshots': snapshots,
                'breadth_ratio': composite.get('breadth_ratio', 0.5),
                'bullish_count': composite.get('bullish_count', 0),
                'total_index_count': len(regimes),
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
            logger.info("[MarketRegime] DataHub无缓存，尝试直接获取...")

            # ── 第2层：BackupDataFetcher（东方财富 HTTP 直连，带重试）──
            try:
                from backup_data_fetcher import BackupDataFetcher
                bf = BackupDataFetcher()
                for name, code in self.INDEX_CODES.items():
                    try:
                        df = bf.get_index_kline_from_eastmoney(code)
                        if df is not None and not df.empty:
                            result[name] = df
                            logger.info(f"[BackupDataFetcher] 获取 {name} 成功，共 {len(df)} 条")
                    except Exception:
                        pass
            except Exception as e:
                logger.warning(f"[BackupDataFetcher] 指数历史获取失败: {e}")

            # ── 第3层：akshare 回退 ──
            if not result and self._ak:
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
                            logger.info(f"[akshare] 获取 {name} 成功，共 {len(df)} 条")
                    except Exception as e:
                        logger.warning(f"[akshare] 获取 {name} 失败: {e}")

        return result

    def _fetch_index_snapshot(self) -> Dict[str, Dict[str, Any]]:
        """获取7个指数的实时行情快照。优先级：DataHub(EastMoney直连)→akshare回退。缓存60秒。"""
        import time
        now = time.time()

        # ── 第1层：DataHub（东方财富直连 + 新浪回退）──
        from data_hub import DataHub
        hub = DataHub.get_instance()
        snap = hub.get_index_snapshot(list(self.INDEX_CODES.values()))
        if snap:
            self._cached_snapshot_ts = now
            return snap

        # ── 第2层：akshare 回退（缓存 DataFrame 以复用）──
        if self._cached_snapshot_df is not None and (now - self._cached_snapshot_ts) < self._snapshot_cache_ttl:
            pass
        else:
            if self._ak is None:
                logger.warning("[MarketRegime] 所有快照数据源均不可用")
                return {}
            try:
                df = self._ak.stock_zh_index_spot_em()
                if df is not None and not df.empty:
                    self._cached_snapshot_df = df
                    self._cached_snapshot_ts = now
                    logger.info(f"[MarketRegime] akshare快照获取成功，共 {len(df)} 条")
                else:
                    logger.warning("[MarketRegime] akshare快照返回空数据")
                    return {}
            except Exception as e:
                logger.warning(f"[MarketRegime] akshare快照获取失败: {e}")
                return {}

        df = self._cached_snapshot_df
        result = {}
        for name, code in self.INDEX_CODES.items():
            try:
                row = df[df['代码'] == code]
                if row.empty:
                    continue
                row = row.iloc[0]
                result[name] = {
                    'latest_price': float(row.get('最新价', 0) or 0),
                    'change_pct': float(row.get('涨跌幅', 0) or 0),
                    'change_amount': float(row.get('涨跌额', 0) or 0),
                    'volume': float(row.get('成交量', 0) or 0),
                    'amount': float(row.get('成交额', 0) or 0),
                    'amplitude': float(row.get('振幅', 0) or 0),
                    'high': float(row.get('最高', 0) or 0),
                    'low': float(row.get('最低', 0) or 0),
                    'open': float(row.get('今开', 0) or 0),
                    'prev_close': float(row.get('昨收', 0) or 0),
                }
            except Exception as e:
                logger.warning(f"[MarketRegime] 处理 {name}({code}) 快照数据失败: {e}")
        return result

    def _analyze_single_index(self, data: pd.DataFrame) -> Dict[str, Any]:
        """分析单个指数的市场阶段（连续评分，0-100 充分差异化）"""
        if len(data) < 200:
            return {'stage': '数据不足', 'trend_score': 50,
                    'latest_close': 0, 'ma20': 0, 'ma50': 0, 'ma200': 0,
                    'macd': 0, 'macd_signal': 0, 'volatility_pct': 0}

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
        high_vol = avg_vol > 0 and volatility > avg_vol * 1.5

        # ── 连续评分系统（7维度，0-100连续而非二元开关）──
        def _norm(val, center, half_range):
            """将 val 映射到 0-100 分。val==center→50，val==center+half_range→100"""
            score = 50 + (val - center) / half_range * 50
            return max(0, min(100, int(score)))

        # 1) MA20-MA50 差距（20%权重）
        ma20_50_pct = ((ma20_val - ma50_val) / ma50_val * 100) if ma50_val > 0 else 0
        s1 = _norm(ma20_50_pct, center=0, half_range=2.0)

        # 2) MA50-MA200 差距（20%权重）
        ma50_200_pct = ((ma50_val - ma200_val) / ma200_val * 100) if ma200_val > 0 else 0
        s2 = _norm(ma50_200_pct, center=0, half_range=3.0)

        # 3) MA20 斜率（10%权重）：趋势增强还是减弱
        ma20_slope_pct = ((ma20_val - prev_ma20) / prev_ma20 * 100) if prev_ma20 > 0 else 0
        s3 = _norm(ma20_slope_pct, center=0, half_range=0.3)

        # 4) 价格相对 MA200 位置（15%权重）
        price_vs_ma200_pct = ((latest - ma200_val) / ma200_val * 100) if ma200_val > 0 else 0
        s4 = _norm(price_vs_ma200_pct, center=0, half_range=8.0)

        # 5) 近期相对动量（15%权重）：短周期涨幅 vs 长周期
        ret_5 = close.pct_change(5).iloc[-1] * 100 if len(close) >= 5 else 0
        ret_60 = close.pct_change(60).iloc[-1] * 100 if len(close) >= 60 else -999
        momentum_spread = ret_5 - ret_60 if ret_60 > -998 else 0
        s5 = _norm(momentum_spread, center=0, half_range=15.0)

        # 6) MACD 柱高度（10%权重）：多空力量对比
        macd_norm = max(abs(close.tail(100).mean() * 0.001), 0.01)
        macd_hist_ratio = (macd_val - signal_val) / macd_norm
        s6 = _norm(macd_hist_ratio, center=0, half_range=1.5)

        # 7) MACD 方向变化（10%权重）
        macd_delta_ratio = (macd_val - prev_macd) / macd_norm
        s7 = _norm(macd_delta_ratio, center=0, half_range=1.0)

        # ── 加权综合 ──
        trend_score = int(s1 * 0.20 + s2 * 0.20 + s3 * 0.10 + s4 * 0.15 + s5 * 0.15 + s6 * 0.10 + s7 * 0.10)
        trend_score = max(0, min(100, trend_score))

        # ── 基于连续评分的阶段判断（粒度更细）──
        if trend_score >= 70:
            stage = '牛市初期 - 波动较大' if high_vol else '牛市中期 - 稳定上涨'
        elif trend_score >= 55:
            stage = '震荡上行 - 短期走强'
        elif trend_score >= 40:
            stage = '震荡整理 - 方向不明'
        elif trend_score >= 25:
            stage = '震荡下行 - 短期走弱'
        else:
            stage = '熊市初期 - 波动较大' if high_vol else '熊市中期 - 持续下跌'

        return {'stage': stage, 'trend_score': trend_score,
                'latest_close': round(latest, 2),
                'ma20': round(ma20_val, 2),
                'ma50': round(ma50_val, 2),
                'ma200': round(ma200_val, 2),
                'macd': round(macd_val, 4),
                'macd_signal': round(signal_val, 4),
                'volatility_pct': round(volatility, 2)}

    def _composite_regime(self, regimes: Dict[str, Any]) -> Dict[str, Any]:
        """综合多个指数判断整体市场阶段（含市场宽度）"""
        if not regimes:
            return {'regime': '未知', 'trend_strength': 0, 'volatility': '未知',
                    'position': 50, 'details': [], 'breadth_ratio': 0.5, 'bullish_count': 0}

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

        bullish_count = sum(1 for s in stages if '牛市' in s or '震荡上行' in s)
        total = len(stages) if stages else 1
        breadth_ratio = round(bullish_count / total, 3)

        details = []
        for name, r in regimes.items():
            chg = r.get('change_pct')
            chg_str = f", {chg:+.2f}%" if chg is not None else ''
            details.append(f"{name}: {r['stage']} (趋势分{r['trend_score']}, 波动{r['volatility_pct']}%{chg_str})")

        return {
            'regime': most_common,
            'trend_strength': avg_trend,
            'volatility': vol_regime,
            'position': position,
            'details': details,
            'breadth_ratio': breadth_ratio,
            'bullish_count': bullish_count,
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
            'details': ['数据获取失败'],
            'index_snapshots': {},
            'breadth_ratio': 0.5,
            'bullish_count': 0,
            'total_index_count': len(self.INDEX_CODES),
        }


if __name__ == '__main__':
    analyzer = MarketRegimeAnalyzer()
    result = analyzer.analyze()
    print("=" * 80)
    print("A股市场大盘综合分析")
    print("=" * 80)
    print(f"分析日期: {result.get('analysis_date', 'N/A')}")
    print(f"综合判断: {result['composite_regime']}")
    print(f"趋势强度: {result['trend_strength']}/100")
    print(f"波动率状态: {result['volatility_regime']}")
    print(f"建议仓位: {result['recommend_position']}%")
    print(f"市场宽度: {result.get('bullish_count', '?')}/{result.get('total_index_count', '?')} 偏多 "
          f"(广度比率: {result.get('breadth_ratio', 0) * 100:.1f}%)")
    print()
    print(analyzer.get_position_advice(result))
    print("-" * 80)
    print(f"{'指数名称':<10} {'代码':<8} {'最新价':>8} {'涨跌幅':>8} {'趋势阶段':<22} {'趋势分':>6}")
    print("-" * 80)
    for name, r in result.get('index_regimes', {}).items():
        code = analyzer.INDEX_CODES.get(name, 'N/A')
        price = r.get('latest_close', 'N/A')
        chg = r.get('change_pct')
        chg_str = f"{chg:+.2f}%" if chg is not None else 'N/A'
        stage = r.get('stage', '未知')
        score = r.get('trend_score', 'N/A')
        print(f"{name:<8} {code:<8} {str(price):>8} {chg_str:>8} {stage:<22} {score:>6}")
    print("-" * 80)
