#!/usr/bin/env python3
"""
DataHub — 全局数据中心（单例模式）

解决了多Agent协作中的数据重复爬取问题：

核心机制：
1. RequestTracker — 请求级去重：同一 (stock_code, source, method) 在去重窗口内只发起一次
2. SourceCache — 数据源缓存：_enrich_missing_fields 不再重新调用 get_stock_data()
3. 统一入口 — 所有 Agent 通过 DataHub 获取数据，而非直接调用 UltimateDataFetcher

使用方式：
    from data_hub import DataHub
    hub = DataHub.get_instance()
    data = hub.get_stock_data("600338")
"""

import time
import logging
from typing import Dict, Optional, Any, Tuple
from threading import Lock

logger = logging.getLogger(__name__)

DUPLICATE_WINDOW_SECONDS = 60


class RequestTracker:
    """请求级去重器：同一参数组合在窗口期内只允许一次请求。"""

    def __init__(self, window_seconds: int = DUPLICATE_WINDOW_SECONDS):
        self._window = window_seconds
        self._records: Dict[Tuple, float] = {}
        self._lock = Lock()

    def should_proceed(self, stock_code: str, source: str, method: str) -> bool:
        key = (stock_code, source, method)
        now = time.time()
        with self._lock:
            last = self._records.get(key)
            if last is None or (now - last) > self._window:
                self._records[key] = now
                return True
            logger.info(f"[去重拦截] ({stock_code}, {source}, {method}) — "
                        f"距上次请求仅 {now - last:.1f}s，跳过")
            return False

    def clear_expired(self):
        now = time.time()
        with self._lock:
            expired = [k for k, v in self._records.items() if (now - v) > self._window]
            for k in expired:
                del self._records[k]


class SourceCache:
    """数据源级缓存：存储每个 (stock_code, source) 的原始返回数据。"""

    def __init__(self, ttl: int = 300):
        self._ttl = ttl
        self._data: Dict[Tuple, Tuple[float, Any]] = {}
        self._lock = Lock()

    def _key(self, stock_code: str, source: str) -> tuple:
        return (stock_code, source)

    def get(self, stock_code: str, source: str) -> Optional[Any]:
        key = self._key(stock_code, source)
        with self._lock:
            entry = self._data.get(key)
            if entry is None:
                return None
            ts, data = entry
            if time.time() - ts > self._ttl:
                del self._data[key]
                return None
            return data

    def set(self, stock_code: str, source: str, data: Any):
        key = self._key(stock_code, source)
        with self._lock:
            self._data[key] = (time.time(), data)

    def clear(self):
        with self._lock:
            self._data.clear()


class DataHub:
    """
    全局数据中心

    所有数据获取必须通过本中心进行，确保：
    - 同一 stock_code 在同一次会话中只被爬取一次
    - 缺失字段补充时复用已有数据源缓存
    - 自动追踪和拦截重复请求
    """

    _instance = None
    _instance_lock = Lock()

    def __init__(self):
        self._fetcher = None
        self._tracker = RequestTracker()
        self._source_cache = SourceCache(ttl=300)
        self._final_data: Dict[str, Dict] = {}
        self._data_lock = Lock()
        self._request_count: Dict[str, int] = {}
        self._stats_lock = Lock()

    @classmethod
    def get_instance(cls) -> "DataHub":
        if cls._instance is None:
            with cls._instance_lock:
                if cls._instance is None:
                    cls._instance = DataHub()
        return cls._instance

    @classmethod
    def reset(cls):
        with cls._instance_lock:
            if cls._instance is not None:
                cls._instance._source_cache.clear()
                cls._instance._final_data.clear()
            cls._instance = None

    def _get_fetcher(self):
        if self._fetcher is None:
            from ultimate_data_fetcher import UltimateDataFetcher
            self._fetcher = UltimateDataFetcher()
        return self._fetcher

    def _record_request(self, key: str):
        with self._stats_lock:
            self._request_count[key] = self._request_count.get(key, 0) + 1

    def get_request_stats(self) -> Dict[str, int]:
        with self._stats_lock:
            return dict(self._request_count)

    def get_stock_data(self, stock_code: str) -> Optional[Dict[str, Any]]:
        """
        获取股票完整数据（带去重和缓存）

        逻辑：
        1. 先查 _final_data 缓存（会话级最终结果缓存）
        2. 首次调用时从数据源获取，并将原始源数据存入 SourceCache
        3. 缺失字段补充直接使用 SourceCache 中的数据，不再重复爬取
        """
        with self._data_lock:
            if stock_code in self._final_data:
                logger.info(f"[DataHub] 命中最终数据缓存: {stock_code}")
                return self._final_data[stock_code]

        fetcher = self._get_fetcher()
        self._record_request(stock_code)

        data = self._fetch_with_dedup(stock_code, fetcher)
        if data:
            with self._data_lock:
                self._final_data[stock_code] = data
        return data

    def _fetch_with_dedup(self, stock_code: str, fetcher) -> Optional[Dict]:
        """带去重的数据获取：优先使用数据源级别的缓存。"""
        from config import DataSourceConfig
        sources = DataSourceConfig.get_source_priority()

        primary_data = None
        primary_source = None

        for source_name in sources:
            if source_name not in fetcher._source_map:
                continue

            if not self._tracker.should_proceed(stock_code, source_name, 'get_stock_data'):
                cached = self._source_cache.get(stock_code, source_name)
                if cached is not None:
                    if primary_data is None:
                        primary_data = cached
                        primary_source = source_name
                    continue

            fetcher_inst = fetcher._source_map[source_name]
            try:
                logger.info(f"[DataHub] 从 {source_name} 获取 {stock_code} 数据...")
                data = fetcher_inst.get_stock_data(stock_code)
                if data and fetcher.validator.validate(data):
                    data['source'] = source_name
                    data = fetcher._normalize_stock_data(data, stock_code)
                    self._source_cache.set(stock_code, source_name, data)
                    primary_data = data
                    primary_source = source_name
                    logger.info(f"[DataHub] {source_name} 主数据获取成功")
                    break
                logger.warning(f"[DataHub] {source_name} 数据无效")
            except Exception as e:
                logger.error(f"[DataHub] {source_name} 获取失败: {e}")

        if not primary_data:
            logger.error(f"[DataHub] 所有数据源都无法获取 {stock_code} 的数据")
            return None

        enriched = self._enrich_from_cache(stock_code, primary_data, primary_source, sources, fetcher)
        return enriched

    def _enrich_from_cache(self, stock_code: str, primary_data: Dict,
                           primary_source: str, sources: list, fetcher) -> Dict:
        """
        从 SourceCache 补充缺失字段（不再重复爬取！）

        相比原始 _enrich_missing_fields 的关键改进：
        - 不再对每个补充源调用 get_stock_data()（避免重复网络请求）
        - 直接使用 SourceCache 中已缓存的数据
        - 仅当缓存中也没有时才考虑网络获取
        """
        result = dict(primary_data)
        financial = result.setdefault('financial', {})
        info = result.setdefault('info', {})

        info_fields = {'industry', 'list_date', 'stock_name'}
        financial_fields = {
            'pb', 'roe', 'total_mv', 'float_mv', 'dividend_yield',
            'gross_margin', 'net_margin', 'revenue', 'net_profit',
            'current_price', 'eps', 'total_share', 'float_share',
            'free_cashflow', 'current_ratio', 'debt_to_equity',
            'cash_and_equivalents', 'total_debt', 'pe',
        }
        history_fields = {
            'fcf_history', 'dividend_history', 'earnings_history',
            'revenue_history', 'roe_history'
        }
        all_critical = financial_fields | info_fields | history_fields

        def _get_value(data_dict, field):
            if data_dict is None:
                return None
            val = data_dict.get(field)
            if val is not None:
                return val
            val = data_dict.get('financial', {}).get(field)
            if val is not None:
                return val
            val = data_dict.get('info', {}).get(field)
            if val is not None:
                return val
            return None

        def _has_value(data_dict, field):
            val = _get_value(data_dict, field)
            if val is None:
                return False
            if isinstance(val, list) and len(val) == 0:
                return False
            return True

        missing = [f for f in all_critical if not _has_value(result, f)]
        hist_val = result.get('historical')
        try:
            import pandas as pd
            need_historical = hist_val is None or (isinstance(hist_val, pd.DataFrame) and hist_val.empty)
        except ImportError:
            need_historical = hist_val is None or getattr(hist_val, 'empty', False)

        if not missing and not need_historical:
            return result

        logger.info(f"[DataHub] 主数据源 {primary_source} 缺 {len(missing)} 个字段: {missing[:10]}..."
                    if len(missing) > 10 else
                    f"[DataHub] 主数据源 {primary_source} 缺字段: {missing}")

        for source_name in sources:
            if source_name == primary_source or source_name not in fetcher._source_map:
                continue

            cache_data = self._source_cache.get(stock_code, source_name)
            if cache_data is None and not missing and not need_historical:
                continue

            if cache_data is None:
                if not self._tracker.should_proceed(stock_code, source_name, 'enrich'):
                    continue
                try:
                    logger.info(f"[DataHub] 缓存未命中，从 {source_name} 补充获取...")
                    fetcher_inst = fetcher._source_map[source_name]
                    cache_data = fetcher_inst.get_stock_data(stock_code)
                    if cache_data and fetcher.validator.validate(cache_data):
                        self._source_cache.set(stock_code, source_name, cache_data)
                    else:
                        continue
                except Exception as e:
                    logger.warning(f"[DataHub] {source_name} 补充获取失败: {e}")
                    continue

            enriched_count = 0
            for field in list(missing):
                val = _get_value(cache_data, field)
                if val is not None and not (isinstance(val, list) and len(val) == 0):
                    if field in info_fields:
                        info[field] = val
                    if field in financial_fields or field in history_fields:
                        financial[field] = val
                    result[field] = val
                    missing.remove(field)
                    enriched_count += 1

            if enriched_count > 0:
                logger.info(f"[DataHub] 从 {source_name} 缓存补充了 {enriched_count} 个字段")

            if need_historical and 'historical' in (cache_data or {}):
                hist = cache_data['historical']
                if hist is not None and not getattr(hist, 'empty', False):
                    result['historical'] = hist
                    need_historical = False
                    logger.info(f"[DataHub] 从 {source_name} 补充了历史数据")

            if not missing and not need_historical:
                logger.info("[DataHub] 所有缺失字段补充完毕")
                break

        if missing:
            logger.warning(f"[DataHub] 以下字段仍缺失: {missing}")

        result['financial'] = financial
        result['info'] = info
        return result

    def get_market_data(self) -> Optional[Dict]:
        """[已弃用] 获取市场指数数据（带去重）
        注意：各数据源均未实现 get_market_data() 方法，调用会失败。
        请使用 get_index_history()（历史数据）或 MarketRegimeAnalyzer._fetch_index_snapshot()（实时行情）。
        """
        if not self._tracker.should_proceed('market', 'primary', 'get_market_data'):
            for source in ['xinhua', 'backup', 'tushare', 'akshare']:
                cached = self._source_cache.get('market', source)
                if cached:
                    return cached
            return None

        fetcher = self._get_fetcher()
        self._record_request('market')
        from config import DataSourceConfig
        sources = DataSourceConfig.get_source_priority()
        for source_name in sources:
            if source_name not in fetcher._source_map:
                continue
            try:
                fetcher_inst = fetcher._source_map[source_name]
                data = fetcher_inst.get_market_data()
                if data and fetcher.validator.validate(data):
                    data['source'] = source_name
                    self._source_cache.set('market', source_name, data)
                    return data
            except Exception as e:
                logger.error(f"[DataHub] {source_name} 市场数据获取失败: {e}")
        return None

    def get_index_history(self, index_code: str, index_name: str = "") -> Optional[Any]:
        """
        获取指数历史数据（MarketRegimeAnalyzer 用）。
        优先级：东方财富直连（BackupDataFetcher）→ akshare 回退。
        自带去重缓存。
        """
        cache_key = f"index_{index_code}"
        if not self._tracker.should_proceed(cache_key, 'eastmoney', 'index_history'):
            cached = self._source_cache.get(cache_key, 'eastmoney')
            if cached is not None:
                return cached
            cached = self._source_cache.get(cache_key, 'akshare')
            if cached is not None:
                return cached
            return None

        self._record_request(cache_key)

        # ── 第1层：东方财富直连 HTTP（带重试，快且可靠）──
        try:
            from backup_data_fetcher import BackupDataFetcher
            bf = BackupDataFetcher()
            df = bf.get_index_kline_from_eastmoney(index_code)
            if df is not None and not df.empty:
                self._source_cache.set(cache_key, 'eastmoney', df)
                logger.info(f"[DataHub] 东方财富指数 {index_name or index_code} 数据获取成功，{len(df)} 条")
                return df
        except Exception as e:
            logger.warning(f"[DataHub] 东方财富指数 {index_code} 获取失败，尝试 akshare: {e}")

        # ── 第2层：akshare 回退 ──
        try:
            import akshare as ak
            df = ak.index_zh_a_hist(symbol=index_code, period="daily", start_date="20230101")
            if df is not None and not df.empty:
                import pandas as pd
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
                self._source_cache.set(cache_key, 'akshare', df)
                logger.info(f"[DataHub] akshare指数 {index_name or index_code} 数据获取成功，{len(df)} 条")
                return df
        except Exception as e:
            logger.warning(f"[DataHub] akshare指数 {index_code} 数据获取失败: {e}")
        return None

    def get_index_snapshot(self, index_codes: list = None) -> Dict[str, Dict]:
        """
        获取指数实时快照（一次调用获取全部指数）。
        优先级：东方财富 ulist API → 新浪财经 → 空字典。
        带60秒去重缓存。
        """
        cache_key = "index_snapshot_all"
        if not self._tracker.should_proceed(cache_key, 'eastmoney', 'index_snapshot'):
            cached = self._source_cache.get(cache_key, 'eastmoney')
            if cached is not None:
                return cached
            cached = self._source_cache.get(cache_key, 'sina')
            if cached is not None:
                return cached

        self._record_request(cache_key)
        try:
            from backup_data_fetcher import BackupDataFetcher
            bf = BackupDataFetcher()
            snap = (bf.get_index_snapshot_from_eastmoney(index_codes)
                    or bf.get_index_snapshot_from_sina(index_codes) or {})
            if snap:
                self._source_cache.set(cache_key, 'eastmoney', snap)
            return snap
        except Exception as e:
            logger.warning(f"[DataHub] 指数快照获取失败: {e}")
        return {}

    def get_stock_kline(self, stock_code: str, period: str = 'daily',
                         start_date: str = None, end_date: str = None,
                         limit: int = 1000):
        """
        获取个股OHLCV K线数据，支持日/周/月周期。四级回退。

        period: 'daily'(klt=101) | 'weekly'(klt=102) | 'monthly'(klt=103)
        limit: 最大K线数量（默认1000）
        """
        import pandas as pd
        klt_map = {'daily': 101, 'weekly': 102, 'monthly': 103}
        klt = klt_map.get(period, 101)

        cache_key = f"kline_{stock_code}_{period}"
        if not self._tracker.should_proceed(cache_key, 'eastmoney', 'get_kline'):
            cached = self._source_cache.get(cache_key, 'eastmoney')
            if cached is not None:
                return self._apply_date_filter(cached, start_date, end_date)

        from backup_data_fetcher import BackupDataFetcher
        bf = BackupDataFetcher()

        # 第1层：东方财富原生周期数据
        df = bf.get_kline_from_eastmoney(stock_code, klt=klt, limit=limit)
        if df is not None and not df.empty:
            self._source_cache.set(cache_key, 'eastmoney', df)
            return self._apply_date_filter(df, start_date, end_date)

        # 第2层：东方财富日K + 聚合回退
        if period != 'daily':
            daily_df = bf.get_kline_from_eastmoney(stock_code, klt=101, limit=limit * 5)
            if daily_df is not None and not daily_df.empty:
                try:
                    from trend_analyzer import TimeFrameAggregator
                    agg = TimeFrameAggregator()
                    df = agg.aggregate(daily_df, period)
                    if df is not None:
                        self._source_cache.set(cache_key, 'eastmoney', df)
                        return self._apply_date_filter(df, start_date, end_date)
                except Exception:
                    pass

        # 第3层：Tushare Pro
        if df is None:
            try:
                from tushare_pro_data_fetcher import TushareProDataFetcher
                tsf = TushareProDataFetcher()
                ts_data = tsf.get_stock_data(stock_code)
                ts_hist = ts_data.get('historical') if ts_data else None
                if ts_hist is not None and not ts_hist.empty:
                    df = ts_hist if period == 'daily' else TimeFrameAggregator().aggregate(ts_hist, period)
                    if df is not None:
                        return self._apply_date_filter(df, start_date, end_date)
            except Exception:
                pass

        # 第4层：Akshare
        if df is None:
            try:
                from akshare_data_fetcher import AkshareDataFetcher
                akf = AkshareDataFetcher()
                ak_data = akf.get_stock_data(stock_code)
                ak_hist = ak_data.get('historical') if ak_data else None
                if ak_hist is not None and not ak_hist.empty:
                    df = ak_hist if period == 'daily' else TimeFrameAggregator().aggregate(ak_hist, period)
                    if df is not None:
                        return self._apply_date_filter(df, start_date, end_date)
            except Exception:
                pass

        return None

    def _apply_date_filter(self, df, start_date: str = None,
                           end_date: str = None):
        """对K线DataFrame应用时间过滤。"""
        if df is None or df.empty:
            return df
        try:
            import pandas as pd
            if start_date:
                df = df[df.index >= pd.Timestamp(start_date)]
            if end_date:
                df = df[df.index <= pd.Timestamp(end_date)]
        except Exception:
            pass
        return df

    def close(self):
        if self._fetcher:
            self._fetcher.close()
        self._source_cache.clear()
        self._final_data.clear()
        self._fetcher = None
