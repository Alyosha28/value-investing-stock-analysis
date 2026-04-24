import os
import re
import pickle
import requests
import pandas as pd
import time
from typing import Dict, List, Optional, Any
from concurrent.futures import ThreadPoolExecutor, as_completed
from datetime import datetime
from logger_config import logger
from config import GrahamThresholds, BuffettThresholds, SystemConfig, to_tushare_code

class StockScreener:
    """
    A股全市场批量筛选器

    基于多源实时数据（akshare/efinance/tushare），应用格雷厄姆/巴菲特/自定义阈值，
    批量发现符合价值投资标准的标的，并输出评分排名。
    具备自动重试、多源故障转移、本地缓存能力，解决单源不稳定问题。
    """

    CACHE_DIR = 'data'
    CACHE_FILE = os.path.join(CACHE_DIR, 'screener_cache.pkl')
    CACHE_MAX_AGE_HOURS = 24
    ROE_CACHE_FILE = os.path.join(CACHE_DIR, 'roe_cache.pkl')
    ROE_CACHE_MAX_AGE_DAYS = 7

    def __init__(self):
        self._ak = None
        self._ef = None
        self._ts = None
        self._ts_token = os.getenv('TUSHARE_TOKEN', '')
        self._roe_data = {}
        self._industry_data = {}

        try:
            import akshare as ak
            self._ak = ak
        except Exception as e:
            logger.warning(f"akshare 未安装: {e}")

        try:
            import efinance as ef
            self._ef = ef
        except Exception as e:
            logger.warning(f"efinance 未安装: {e}")

        try:
            import tushare as ts
            if self._ts_token:
                self._ts = ts.pro_api(self._ts_token)
                proxy_url = os.getenv('TUSHARE_PROXY_URL', 'http://118.89.66.41:8010/')
                self._ts._DataApi__http_url = proxy_url
                logger.info(f"Tushare Pro 初始化成功，代理地址: {proxy_url}")
            else:
                logger.warning("未设置 TUSHARE_TOKEN，Tushare 数据源不可用")
        except Exception as e:
            logger.warning(f"tushare 未安装或初始化失败: {e}")

        os.makedirs(self.CACHE_DIR, exist_ok=True)
        self._load_roe_cache()

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

    # ------------------------------------------------------------------
    # 数据获取：多源故障转移 + 重试 + 本地缓存
    # ------------------------------------------------------------------

    def _fetch_market_data(self, max_stocks: int) -> Optional[pd.DataFrame]:
        """
        获取全市场实时数据，具备多源故障转移能力。
        全市场扫描优先级：腾讯财经 -> akshare(东财) -> akshare(新浪) -> efinance -> tushare -> 本地缓存
        与全局单股分析优先级（新华财经 -> 腾讯财经 -> Tushare Pro -> 其他）保持一致，
        腾讯财经 HTTP 接口无需第三方库，最稳定快速，优先尝试。
        """
        df = None
        sources_attempted = []

        # 1. 腾讯财经（HTTP 接口，无需库依赖，最稳定快速）
        df = self._try_fetch_with_retry(self._fetch_tencent, max_stocks, retries=2, delay=1.0)
        if df is not None:
            logger.info(f"腾讯财经 获取全市场数据成功，共 {len(df)} 只")
            self._save_cache(df)
            return df
        sources_attempted.append("腾讯财经")

        # 2. akshare 东方财富（字段最全，但连接不稳定）— 仅重试 1 次，快速失败
        if self._ak:
            df = self._try_fetch_with_retry(self._fetch_akshare_em, max_stocks, retries=1, delay=1.0)
            if df is not None:
                logger.info(f"akshare(东财) 获取全市场数据成功，共 {len(df)} 只")
                self._save_cache(df)
                return df
            sources_attempted.append("akshare(东财)")

        # 3. akshare 新浪财经（备用接口）— 仅重试 1 次
        if self._ak:
            df = self._try_fetch_with_retry(self._fetch_akshare_sina, max_stocks, retries=1, delay=1.0)
            if df is not None:
                logger.info(f"akshare(新浪) 获取全市场数据成功，共 {len(df)} 只")
                self._save_cache(df)
                return df
            sources_attempted.append("akshare(新浪)")

        # 4. efinance（东财实时行情，速度快但易被封）
        if self._ef:
            df = self._try_fetch_with_retry(self._fetch_efinance, max_stocks, retries=1)
            if df is not None:
                logger.info(f"efinance 获取全市场数据成功，共 {len(df)} 只")
                self._save_cache(df)
                return df
            sources_attempted.append("efinance")

        # 5. tushare daily_basic（需要 token，有积分限制）
        if self._ts:
            df = self._try_fetch_with_retry(self._fetch_tushare, max_stocks, retries=1)
            if df is not None:
                logger.info(f"tushare 获取全市场数据成功，共 {len(df)} 只")
                self._save_cache(df)
                return df
            sources_attempted.append("tushare")

        # 6. 本地缓存兜底
        df = self._load_cache()
        if df is not None:
            logger.warning(f"所有实时数据源均失败（已尝试: {', '.join(sources_attempted)}），使用本地缓存数据")
            return df

        logger.error(f"全市场数据获取完全失败，已尝试: {', '.join(sources_attempted)}")
        return None

    def _try_fetch_with_retry(self, fetch_func, max_stocks: int, retries: int = 3, delay: float = 2.0) -> Optional[pd.DataFrame]:
        """带指数退避重试的数据获取"""
        for attempt in range(retries):
            try:
                df = fetch_func(max_stocks)
                if df is not None and not df.empty:
                    return df
            except Exception as e:
                logger.warning(f"{fetch_func.__name__} 第 {attempt + 1} 次尝试失败: {e}")
            if attempt < retries - 1:
                sleep_time = delay * (2 ** attempt)
                logger.info(f"{sleep_time:.1f} 秒后重试...")
                time.sleep(sleep_time)
        return None

    def _fetch_akshare_em(self, max_stocks: int) -> Optional[pd.DataFrame]:
        """akshare 东方财富全市场行情"""
        df = self._ak.stock_zh_a_spot_em()
        if df is None or df.empty:
            return None
        return self._normalize_df(df, source='akshare_em', max_stocks=max_stocks)

    def _fetch_akshare_sina(self, max_stocks: int) -> Optional[pd.DataFrame]:
        """akshare 新浪财经全市场行情"""
        df = self._ak.stock_zh_a_spot()
        if df is None or df.empty:
            return None
        return self._normalize_df(df, source='akshare_sina', max_stocks=max_stocks)

    def _fetch_efinance(self, max_stocks: int) -> Optional[pd.DataFrame]:
        """efinance 实时行情"""
        df = self._ef.stock.get_realtime_quotes()
        if df is None or df.empty:
            return None
        return self._normalize_df(df, source='efinance', max_stocks=max_stocks)

    def _fetch_tushare(self, max_stocks: int) -> Optional[pd.DataFrame]:
        """tushare 每日指标"""
        trade_date = datetime.now().strftime('%Y%m%d')
        # 尝试当天，若失败则尝试前一天（非交易日）
        for offset in [0, -1, -2]:
            try:
                check_date = (datetime.now() + pd.Timedelta(days=offset)).strftime('%Y%m%d')
                df = self._ts.daily_basic(trade_date=check_date)
                if df is not None and not df.empty:
                    return self._normalize_df(df, source='tushare', max_stocks=max_stocks)
            except Exception:
                continue
        return None

    def _fetch_tencent(self, max_stocks: int) -> Optional[pd.DataFrame]:
        """
        腾讯财经实时行情接口（HTTP，无需第三方库）。
        先获取全市场代码列表，再分批请求行情数据。
        """
        try:
            codes = self._get_all_stock_codes()
            if not codes:
                logger.warning("无法获取股票代码列表，腾讯财经接口跳过")
                return None

            batch_size = 800
            all_rows = []
            for i in range(0, len(codes), batch_size):
                batch = codes[i:i + batch_size]
                url = f"http://qt.gtimg.cn/q={','.join(batch)}"
                resp = requests.get(url, timeout=10, proxies={'http': None, 'https': None})
                resp.encoding = 'gbk'
                rows = self._parse_tencent_response(resp.text)
                all_rows.extend(rows)
                if len(all_rows) >= max_stocks:
                    break
                time.sleep(0.3)  # 批次间短暂休眠，避免触发限流

            if not all_rows:
                return None

            df = pd.DataFrame(all_rows)
            return self._normalize_df(df, source='tencent', max_stocks=max_stocks)

        except Exception as e:
            logger.warning(f"腾讯财经获取失败: {e}")
            return None

    def _get_all_stock_codes(self) -> List[str]:
        """获取全市场 A 股代码，转换为腾讯格式（sh600519 / sz000001）"""
        # 不依赖 akshare，直接返回硬编码列表，避免单点阻塞
        # 覆盖 5000+ 只常见沪深代码（沪市主板/科创板 + 深市主板/中小板/创业板）
        codes = []
        for prefix in ['600', '601', '603', '605', '688']:
            for i in range(1000):
                codes.append(f"sh{prefix}{i:03d}")
        for prefix in ['000', '001', '002', '003', '300']:
            for i in range(1000):
                codes.append(f"sz{prefix}{i:03d}")
        return codes

    @staticmethod
    def _to_tencent_code(code: str) -> str:
        """统一股票代码转换为腾讯格式"""
        code = str(code).strip().replace('.SH', '').replace('.SZ', '')
        if code.startswith('6') or code.startswith('688'):
            return f"sh{code}"
        return f"sz{code}"

    def _parse_tencent_response(self, text: str) -> List[Dict[str, Any]]:
        """解析腾讯财经返回的文本数据"""
        rows = []
        # 格式: v_sh600519="1~贵州茅台~600519~...";
        pattern = re.compile(r'v_(sh|sz)(\d+)="([^"]*)"')
        for match in pattern.finditer(text):
            _, code, data = match.groups()
            fields = data.split('~')
            if len(fields) < 40:
                continue

            try:
                # 腾讯接口字段索引（以 ~ 分隔）
                # 1:名称, 2:代码, 3:现价, 32:涨跌幅%, 38:换手率, 39:市盈率, 44:流通市值(万), 45:总市值(万), 46:市净率, 49:量比
                row = {
                    'stock_code': code,
                    'stock_name': fields[1] if len(fields) > 1 else code,
                    'current_price': self._safe_float(fields[3]),
                    'change_pct': self._safe_float(fields[32]),
                    'turnover': self._safe_float(fields[38]),
                    'pe': self._safe_float(fields[39]),
                    'pb': self._safe_float(fields[46]),
                    'volume_ratio': self._safe_float(fields[49]),
                }
                # 市值字段单位为"亿元"，转换为"元"
                if len(fields) > 45:
                    row['market_cap'] = self._safe_float(fields[45])
                    if row['market_cap']:
                        row['market_cap'] = row['market_cap'] * 1e8
                if len(fields) > 44:
                    row['float_cap'] = self._safe_float(fields[44])
                    if row['float_cap']:
                        row['float_cap'] = row['float_cap'] * 1e8

                # 过滤无效数据
                if row['current_price'] and row['current_price'] > 0:
                    rows.append(row)
            except (IndexError, ValueError, TypeError, KeyError):
                continue
        return rows

    @staticmethod
    def _safe_float(val) -> Optional[float]:
        if val is None or str(val) in ['-', '', 'None', 'nan', 'NaN']:
            return None
        try:
            v = float(val)
            return v if v != float('inf') and v != float('-inf') and not pd.isna(v) else None
        except (ValueError, TypeError):
            return None

    def _normalize_df(self, df: pd.DataFrame, source: str, max_stocks: int) -> pd.DataFrame:
        """统一各数据源的列名和数据类型"""
        column_maps = {
            'akshare_em': {
                '代码': 'stock_code', '名称': 'stock_name', '市盈率': 'pe', '市净率': 'pb',
                '总市值': 'market_cap', '流通市值': 'float_cap', 'ROE': 'roe',
                '股息率': 'dividend_yield', '所属行业': 'industry', '最新价': 'current_price',
                '涨跌幅': 'change_pct', '换手率': 'turnover', '振幅': 'amplitude',
                '量比': 'volume_ratio', '60日涨跌幅': 'change_60d',
                '年初至今涨跌幅': 'change_ytd', '每股未分配利润': 'retained_eps',
                '市销率': 'ps', '总股本': 'total_share',
            },
            'akshare_sina': {
                'symbol': 'stock_code', 'name': 'stock_name', 'trade': 'current_price',
                'pricechange': 'change_pct', 'changepercent': 'change_pct',
                'mktcap': 'market_cap', 'nmc': 'float_cap', 'pb': 'pb', 'turnoverratio': 'turnover',
            },
            'efinance': {
                '股票代码': 'stock_code', '股票名称': 'stock_name', '最新价': 'current_price',
                '涨跌幅': 'change_pct', '总市值': 'market_cap', '流通市值': 'float_cap',
                '市盈率': 'pe', '市净率': 'pb', '股息率': 'dividend_yield',
                '所属行业': 'industry', '换手率': 'turnover', '振幅': 'amplitude',
                '量比': 'volume_ratio', '60日涨跌幅': 'change_60d',
                '年初至今涨跌幅': 'change_ytd', '市销率': 'ps', '总股本': 'total_share',
            },
            'tushare': {
                'ts_code': 'stock_code', 'close': 'current_price',
                'pe': 'pe', 'pe_ttm': 'pe_ttm', 'pb': 'pb', 'ps': 'ps', 'ps_ttm': 'ps_ttm',
                'dv_ratio': 'dividend_yield', 'total_mv': 'market_cap', 'circ_mv': 'float_cap',
                'total_share': 'total_share', 'float_share': 'float_share',
                'turnover_rate': 'turnover', 'volume_ratio': 'volume_ratio',
            },
        }

        col_map = column_maps.get(source, {})
        for old_col, new_col in col_map.items():
            if old_col in df.columns:
                df[new_col] = df[old_col]

        # tushare 的 stock_code 带后缀 .SH/.SZ，需要去除
        if source == 'tushare' and 'stock_code' in df.columns:
            df['stock_code'] = df['stock_code'].str.replace('.SH', '', regex=False).str.replace('.SZ', '', regex=False)

        # 统一数值类型
        numeric_cols = ['pe', 'pb', 'roe', 'dividend_yield', 'market_cap', 'current_price',
                        'change_pct', 'turnover', 'ps']
        for col in numeric_cols:
            if col in df.columns:
                df[col] = pd.to_numeric(df[col], errors='coerce')

        # tushare 的 turnover 是百分比小数（0.05），其他源是百分比数值（5），统一为数值形式
        if source == 'tushare' and 'turnover' in df.columns:
            df['turnover'] = df['turnover'] * 100

        # 过滤无效数据
        if 'market_cap' in df.columns:
            df = df[df['market_cap'].notna() & (df['market_cap'] > 0)]

        df = df.head(max_stocks)
        return df

    # ------------------------------------------------------------------
    # 本地缓存
    # ------------------------------------------------------------------

    def _save_cache(self, df: pd.DataFrame):
        """将数据保存到本地缓存"""
        try:
            df.to_pickle(self.CACHE_FILE)
            logger.info(f"全市场数据已缓存到 {self.CACHE_FILE}")
        except Exception as e:
            logger.warning(f"缓存保存失败: {e}")

    def _load_cache(self) -> Optional[pd.DataFrame]:
        """读取本地缓存，检查有效期"""
        try:
            if not os.path.exists(self.CACHE_FILE):
                return None
            mtime = os.path.getmtime(self.CACHE_FILE)
            age_hours = (time.time() - mtime) / 3600
            if age_hours > self.CACHE_MAX_AGE_HOURS:
                logger.warning(f"本地缓存已过期 ({age_hours:.1f} 小时)，放弃使用")
                return None
            df = pd.read_pickle(self.CACHE_FILE)
            logger.info(f"加载本地缓存成功，共 {len(df)} 只，缓存年龄 {age_hours:.1f} 小时")
            return df
        except Exception as e:
            logger.warning(f"缓存加载失败: {e}")
            return None

    # ------------------------------------------------------------------
    # ROE 缓存管理
    # ------------------------------------------------------------------

    def _load_roe_cache(self):
        try:
            if os.path.exists(self.ROE_CACHE_FILE):
                mtime = os.path.getmtime(self.ROE_CACHE_FILE)
                age_days = (time.time() - mtime) / 86400
                if age_days < self.ROE_CACHE_MAX_AGE_DAYS:
                    with open(self.ROE_CACHE_FILE, 'rb') as f:
                        cache = pickle.load(f)
                        self._roe_data = cache.get('roe_data', {})
                        self._industry_data = cache.get('industry_data', {})
                    logger.info(f"ROE缓存加载成功，共 {len(self._roe_data)} 只股票，缓存年龄 {age_days:.1f} 天")
                    return
                logger.warning(f"ROE缓存已过期 ({age_days:.1f} 天)，将重新获取")
        except Exception as e:
            logger.warning(f"ROE缓存加载失败: {e}")

    def _save_roe_cache(self):
        try:
            with open(self.ROE_CACHE_FILE, 'wb') as f:
                pickle.dump({
                    'roe_data': self._roe_data,
                    'industry_data': self._industry_data,
                    'update_time': datetime.now().isoformat()
                }, f)
            logger.info(f"ROE缓存已保存，共 {len(self._roe_data)} 只股票")
        except Exception as e:
            logger.warning(f"ROE缓存保存失败: {e}")

    def _fetch_roe_batch_tushare(self, stock_codes: List[str]) -> Dict[str, Dict]:
        if not self._ts:
            return {}

        results = {}
        ts_codes = [to_tushare_code(code) for code in stock_codes]

        try:
            df = self._ts.fina_indicator(
                ts_code=",".join(ts_codes[:100]),
                fields='ts_code,ann_date,roe,net_profit_ratio,gross_profit_margin,debt_to_assets'
            )
            if df is not None and not df.empty:
                for code in stock_codes:
                    ts_code = to_tushare_code(code)
                    stock_df = df[df['ts_code'] == ts_code]
                    if not stock_df.empty:
                        roe_values = stock_df['roe'].dropna().head(5).tolist()
                        net_margin = stock_df['net_profit_ratio'].dropna().head(1).iloc[0] if 'net_profit_ratio' in stock_df else None
                        gross_margin = stock_df['gross_profit_margin'].dropna().head(1).iloc[0] if 'gross_profit_margin' in stock_df else None
                        results[code] = {
                            'roe': round(sum(roe_values) / len(roe_values), 2) if roe_values else None,
                            'roe_history': roe_values,
                            'net_margin': net_margin,
                            'gross_margin': gross_margin,
                        }
        except Exception as e:
            logger.warning(f"tushare ROE批量获取失败: {e}")

        return results

    def _fetch_industry_batch_tushare(self, stock_codes: List[str]) -> Dict[str, str]:
        if not self._ts:
            return {}

        results = {}
        try:
            df = self._ts.stock_basic(
                fields='symbol,industry,name'
            )
            if df is not None and not df.empty:
                for code in stock_codes:
                    stock_df = df[df['symbol'] == code]
                    if not stock_df.empty:
                        ind = stock_df.iloc[0].get('industry')
                        if ind and str(ind) != 'nan':
                            results[code] = str(ind)
        except Exception as e:
            logger.warning(f"tushare行业批量获取失败: {e}")

        return results

    def _get_roe_for_stocks(self, stock_codes: List[str]):
        missing_codes = [c for c in stock_codes if c not in self._roe_data]
        if not missing_codes:
            return

        if self._ts:
            batch_size = 100
            for i in range(0, len(missing_codes), batch_size):
                batch = missing_codes[i:i + batch_size]
                batch_results = self._fetch_roe_batch_tushare(batch)
                self._roe_data.update(batch_results)
                time.sleep(0.3)

        industry_missing = [c for c in stock_codes if c not in self._industry_data]
        if industry_missing and self._ts:
            industry_results = self._fetch_industry_batch_tushare(industry_missing)
            self._industry_data.update(industry_results)

    def _apply_basic_filters(self, df: pd.DataFrame, min_market_cap: float) -> pd.DataFrame:
        """基础过滤：排除明显不合格的股票"""
        before = len(df)

        stock_codes = df['stock_code'].tolist()
        self._get_roe_for_stocks(stock_codes)

        df = df[df['market_cap'] >= min_market_cap].copy()

        df = df[df['pe'].notna() & (df['pe'] > 0)]

        df = df[df['pb'].notna() & (df['pb'] > 0)]

        if 'industry' not in df.columns:
            df['industry'] = None

        for idx, row in df.iterrows():
            code = row.get('stock_code')
            if code in self._roe_data:
                df.at[idx, 'roe'] = self._roe_data[code].get('roe')
                if code in self._industry_data:
                    current_industry = df.at[idx, 'industry']
                    if current_industry is None or pd.isna(current_industry):
                        df.at[idx, 'industry'] = self._industry_data[code]

        self._save_roe_cache()

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

    def save_screening_report(self, result: Dict[str, Any], filepath: str = None):
        """将筛选报告保存到 output 目录"""
        if not result.get('success'):
            logger.warning("筛选失败，跳过报告保存")
            return

        if filepath is None:
            strategy = result.get('strategy', 'comprehensive')
            timestamp = datetime.now().strftime('%Y%m%d_%H%M%S')
            filepath = os.path.join('output', f'screening_{strategy}_{timestamp}.txt')

        try:
            os.makedirs('output', exist_ok=True)
            with open(filepath, 'w', encoding='utf-8') as f:
                f.write("=" * 80 + "\n")
                f.write(f"全市场筛选结果 (策略: {result.get('strategy')})\n")
                f.write("=" * 80 + "\n\n")

                f.write(f"扫描日期: {result.get('scan_date', 'N/A')}\n")
                f.write(f"扫描总数: {result.get('total_scanned', 0)} 只\n")
                f.write(f"基础过滤后: {result.get('after_basic_filter', 0)} 只\n")
                f.write(f"返回数量: Top {len(result.get('stocks', []))}\n\n")

                f.write("-" * 40 + "\n")
                f.write(result.get('summary', '') + "\n")
                f.write("-" * 40 + "\n\n")

                f.write("【Top 50 筛选结果】\n\n")
                for s in result.get('stocks', []):
                    f.write(f"{s['rank']:>3}. {s['stock_name']}({s['stock_code']}) | "
                            f"评分:{s['total_score']} | PE:{s['pe']} | PB:{s['pb']} | "
                            f"ROE:{s['roe']}% | 股息率:{s['dividend_yield']}% | "
                            f"市值:{s.get('market_cap_yi', 'N/A')}亿\n")
                    f.write(f"      行业:{s.get('industry', '未知')} | 护城河:{s.get('moat_rating', '无')} | {s.get('suggestion', '')}\n")
                    f.write("\n")

                top_good = [s for s in result.get('stocks', []) if '建议关注' in s.get('suggestion', '')]
                if top_good:
                    f.write("\n" + "=" * 40 + "\n")
                    f.write("【建议关注标的】\n")
                    f.write("=" * 40 + "\n")
                    for s in top_good:
                        f.write(f"• {s['stock_name']}({s['stock_code']}) - {s['suggestion']}\n")
                        f.write(f"  评分:{s['total_score']} ROE:{s['roe']}% PE:{s['pe']} PB:{s['pb']}\n\n")

            logger.info(f"筛选报告已保存: {filepath}")
            print(f"\n✅ 报告已保存到: {filepath}")

        except Exception as e:
            logger.error(f"报告保存失败: {e}")

    def _safe_round(self, val, digits: int = 2):
        if val is None or pd.isna(val):
            return None
        try:
            return round(float(val), digits)
        except (ValueError, TypeError):
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
