import logging
import time
import traceback
from typing import Dict, Optional, Any
from concurrent.futures import ThreadPoolExecutor, as_completed

from xinhua_data_fetcher import XinhuaFinanceDataFetcher
from akshare_data_fetcher import AkshareDataFetcher
from backup_data_fetcher import BackupDataFetcher
from tushare_pro_data_fetcher import TushareProDataFetcher
from datasource_validator import DataSourceValidator
from config import DataSourceConfig, SystemConfig

logger = logging.getLogger(__name__)


class UltimateDataFetcher:
    """统一数据获取器，支持多数据源优先级获取和并发验证。"""

    def __init__(self):
        self.xinhua_fetcher = XinhuaFinanceDataFetcher()
        self.akshare_fetcher = AkshareDataFetcher()
        self.backup_fetcher = BackupDataFetcher()
        self.tushare_fetcher = TushareProDataFetcher()
        self.validator = DataSourceValidator()
        self._source_map = {
            'xinhua': self.xinhua_fetcher,
            'akshare': self.akshare_fetcher,
            'backup': self.backup_fetcher,
            'tushare': self.tushare_fetcher,
        }
        logger.info("终极数据获取器初始化完成")

    def get_stock_data(self, stock_code: str) -> Optional[Dict[str, Any]]:
        """按优先级从多个数据源获取股票数据，支持多源互补。"""
        sources = DataSourceConfig.get_source_priority()
        logger.info(f"开始获取股票 {stock_code} 的数据，数据源优先级: {sources}")

        primary_data = None
        primary_source = None

        for source_name in sources:
            if source_name not in self._source_map:
                continue
            fetcher = self._source_map[source_name]
            try:
                logger.info(f"尝试从 {source_name} 获取数据...")
                data = fetcher.get_stock_data(stock_code)
                if data and self.validator.validate(data):
                    data['source'] = source_name
                    data = self._normalize_stock_data(data, stock_code)
                    primary_data = data
                    primary_source = source_name
                    logger.info(f"从 {source_name} 成功获取主数据")
                    break
                logger.warning(f"{source_name} 数据无效或验证失败")
            except Exception as e:
                logger.error(f"从 {source_name} 获取数据失败: {e}")
                if logger.isEnabledFor(logging.DEBUG):
                    logger.debug(traceback.format_exc())

        if not primary_data:
            logger.error(f"所有数据源都无法获取股票 {stock_code} 的数据")
            return None

        # 若主数据存在缺失字段，尝试从其他数据源补充
        enriched = self._enrich_missing_fields(stock_code, primary_data, primary_source, sources)
        return enriched

    def _enrich_missing_fields(self, stock_code: str, primary_data: Dict[str, Any],
                               primary_source: str, sources: list) -> Dict[str, Any]:
        """当主数据源缺少关键字段时，从其他数据源补充。"""
        result = dict(primary_data)
        financial = result.setdefault('financial', {})
        info = result.setdefault('info', {})

        # 需要补充的关键字段，按所属子字典分组
        info_fields = {'industry', 'list_date'}
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
            """从扁平或嵌套结构中提取字段值"""
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
            """检查字段是否已在结果中存在且非空"""
            val = _get_value(data_dict, field)
            if val is None:
                return False
            if isinstance(val, list) and len(val) == 0:
                return False
            return True

        missing = [f for f in all_critical if not _has_value(result, f)]

        logger.info(f"主数据源 {primary_source} 缺少字段: {missing}，尝试从其他数据源补充...")

        # 同时检查是否需要补充历史数据（技术分析用）
        need_historical = not result.get('historical') or getattr(result.get('historical'), 'empty', False)

        for source_name in sources:
            if source_name == primary_source or source_name not in self._source_map:
                continue
            fetcher = self._source_map[source_name]
            try:
                data = fetcher.get_stock_data(stock_code)
                if not data or not self.validator.validate(data):
                    continue

                # 从补充数据源提取缺失字段
                enriched_count = 0
                for field in list(missing):
                    val = _get_value(data, field)
                    if val is not None:
                        if isinstance(val, list) and len(val) == 0:
                            continue
                        if field in info_fields:
                            info[field] = val
                        if field in financial_fields or field in history_fields:
                            financial[field] = val
                        result[field] = val
                        missing.remove(field)
                        enriched_count += 1

                if enriched_count > 0:
                    logger.info(f"从 {source_name} 补充了 {enriched_count} 个字段")

                # 补充历史数据
                if need_historical and 'historical' in data:
                    hist = data['historical']
                    if hist is not None and not getattr(hist, 'empty', False):
                        result['historical'] = hist
                        need_historical = False
                        logger.info(f"从 {source_name} 补充了历史数据")

                if not missing and not need_historical:
                    logger.info("所有缺失字段及历史数据已补充完成")
                    break
            except Exception as e:
                logger.warning(f"从 {source_name} 补充数据失败: {e}")

        if missing:
            logger.warning(f"以下字段仍缺失: {missing}")

        result['financial'] = financial
        result['info'] = info
        return result

    @staticmethod
    def _normalize_stock_data(data: Dict[str, Any], stock_code: str) -> Dict[str, Any]:
        """将扁平数据结构转换为分析器/报告生成器期望的嵌套结构"""
        if not data:
            return data
        # 若已包含嵌套结构则跳过
        if 'financial' in data or 'info' in data:
            return data

        result = dict(data)

        # info 字段（股票基本信息）
        info_fields = ['stock_name', 'stock_code', 'industry', 'list_date',
                       'name', 'area', 'market', 'exchange']
        info = {}
        for field in info_fields:
            if field in data and data[field] is not None:
                info[field] = data[field]
        if info:
            result['info'] = info

        # financial 字段（财务/估值数据）
        financial_fields = [
            'pe', 'pb', 'roe', 'eps', 'total_mv', 'float_mv',
            'current_price', 'dividend_yield', 'gross_margin',
            'net_margin', 'revenue', 'net_profit', 'operating_cashflow',
            'free_cashflow', 'total_assets', 'total_liabilities',
            'current_assets', 'current_liabilities', 'cash_and_equivalents',
            'total_equity', 'total_debt', 'current_ratio', 'debt_to_equity',
            'asset_liability_ratio', 'short_term_debt', 'long_term_debt',
            'total_share', 'float_share', 'turnover_rate',
            'earnings_history', 'roe_history', 'fcf_history',
            'dividend_history', 'revenue_history', 'volume', 'amount',
            'open', 'high', 'low', 'close', 'pct_chg', 'amplitude',
        ]
        financial = {}
        for field in financial_fields:
            if field in data and data[field] is not None:
                financial[field] = data[field]
        if financial:
            result['financial'] = financial

        return result

    def get_market_data(self) -> Optional[Dict[str, Any]]:
        """获取市场环境数据。"""
        sources = DataSourceConfig.get_source_priority()
        for source_name in sources:
            if source_name not in self._source_map:
                continue
            fetcher = self._source_map[source_name]
            try:
                data = fetcher.get_market_data()
                if data and self.validator.validate(data):
                    data['source'] = source_name
                    return data
            except Exception as e:
                logger.error(f"从 {source_name} 获取市场数据失败: {e}")
        return None

    def get_stock_list(self) -> Optional[list]:
        """获取股票列表。"""
        sources = DataSourceConfig.get_source_priority()
        for source_name in sources:
            if source_name not in self._source_map:
                continue
            fetcher = self._source_map[source_name]
            try:
                data = fetcher.get_stock_list()
                if data:
                    return data
            except Exception as e:
                logger.error(f"从 {source_name} 获取股票列表失败: {e}")
        return None

    def get_stock_financials(self, stock_code: str) -> Optional[Dict[str, Any]]:
        """获取股票财务数据。"""
        sources = DataSourceConfig.get_source_priority()
        for source_name in sources:
            if source_name not in self._source_map:
                continue
            fetcher = self._source_map[source_name]
            try:
                data = fetcher.get_stock_financials(stock_code)
                if data:
                    return data
            except Exception as e:
                logger.error(f"从 {source_name} 获取财务数据失败: {e}")
        return None

    def get_stock_price(self, stock_code: str) -> Optional[Dict[str, Any]]:
        """获取股票价格数据。"""
        sources = DataSourceConfig.get_source_priority()
        for source_name in sources:
            if source_name not in self._source_map:
                continue
            fetcher = self._source_map[source_name]
            try:
                data = fetcher.get_stock_price(stock_code)
                if data:
                    return data
            except Exception as e:
                logger.error(f"从 {source_name} 获取价格数据失败: {e}")
        return None

    def get_stock_indicators(self, stock_code: str) -> Optional[Dict[str, Any]]:
        """获取股票技术指标数据。"""
        sources = DataSourceConfig.get_source_priority()
        for source_name in sources:
            if source_name not in self._source_map:
                continue
            fetcher = self._source_map[source_name]
            try:
                data = fetcher.get_stock_indicators(stock_code)
                if data:
                    return data
            except Exception as e:
                logger.error(f"从 {source_name} 获取指标数据失败: {e}")
        return None

    def get_stock_industry(self, stock_code: str) -> Optional[Dict[str, Any]]:
        """获取股票行业数据。"""
        sources = DataSourceConfig.get_source_priority()
        for source_name in sources:
            if source_name not in self._source_map:
                continue
            fetcher = self._source_map[source_name]
            try:
                data = fetcher.get_stock_industry(stock_code)
                if data:
                    return data
            except Exception as e:
                logger.error(f"从 {source_name} 获取行业数据失败: {e}")
        return None

    def get_stock_news(self, stock_code: str) -> Optional[list]:
        """获取股票新闻数据。"""
        sources = DataSourceConfig.get_source_priority()
        for source_name in sources:
            if source_name not in self._source_map:
                continue
            fetcher = self._source_map[source_name]
            try:
                data = fetcher.get_stock_news(stock_code)
                if data:
                    return data
            except Exception as e:
                logger.error(f"从 {source_name} 获取新闻数据失败: {e}")
        return None

    def get_stock_fundamentals(self, stock_code: str) -> Optional[Dict[str, Any]]:
        """获取股票基本面数据。"""
        sources = DataSourceConfig.get_source_priority()
        for source_name in sources:
            if source_name not in self._source_map:
                continue
            fetcher = self._source_map[source_name]
            try:
                data = fetcher.get_stock_fundamentals(stock_code)
                if data:
                    return data
            except Exception as e:
                logger.error(f"从 {source_name} 获取基本面数据失败: {e}")
        return None

    def get_stock_dividends(self, stock_code: str) -> Optional[list]:
        """获取股票分红数据。"""
        sources = DataSourceConfig.get_source_priority()
        for source_name in sources:
            if source_name not in self._source_map:
                continue
            fetcher = self._source_map[source_name]
            try:
                data = fetcher.get_stock_dividends(stock_code)
                if data:
                    return data
            except Exception as e:
                logger.error(f"从 {source_name} 获取分红数据失败: {e}")
        return None

    def get_stock_shareholders(self, stock_code: str) -> Optional[Dict[str, Any]]:
        """获取股票股东数据。"""
        sources = DataSourceConfig.get_source_priority()
        for source_name in sources:
            if source_name not in self._source_map:
                continue
            fetcher = self._source_map[source_name]
            try:
                data = fetcher.get_stock_shareholders(stock_code)
                if data:
                    return data
            except Exception as e:
                logger.error(f"从 {source_name} 获取股东数据失败: {e}")
        return None

    def get_stock_institutional(self, stock_code: str) -> Optional[Dict[str, Any]]:
        """获取机构持仓数据。"""
        sources = DataSourceConfig.get_source_priority()
        for source_name in sources:
            if source_name not in self._source_map:
                continue
            fetcher = self._source_map[source_name]
            try:
                data = fetcher.get_stock_institutional(stock_code)
                if data:
                    return data
            except Exception as e:
                logger.error(f"从 {source_name} 获取机构持仓数据失败: {e}")
        return None

    def get_stock_concept(self, stock_code: str) -> Optional[list]:
        """获取股票概念数据。"""
        sources = DataSourceConfig.get_source_priority()
        for source_name in sources:
            if source_name not in self._source_map:
                continue
            fetcher = self._source_map[source_name]
            try:
                data = fetcher.get_stock_concept(stock_code)
                if data:
                    return data
            except Exception as e:
                logger.error(f"从 {source_name} 获取概念数据失败: {e}")
        return None

    def get_stock_sector(self, stock_code: str) -> Optional[Dict[str, Any]]:
        """获取股票板块数据。"""
        sources = DataSourceConfig.get_source_priority()
        for source_name in sources:
            if source_name not in self._source_map:
                continue
            fetcher = self._source_map[source_name]
            try:
                data = fetcher.get_stock_sector(stock_code)
                if data:
                    return data
            except Exception as e:
                logger.error(f"从 {source_name} 获取板块数据失败: {e}")
        return None

    def get_stock_sector_stocks(self, sector_name: str) -> Optional[list]:
        """获取板块内股票列表。"""
        sources = DataSourceConfig.get_source_priority()
        for source_name in sources:
            if source_name not in self._source_map:
                continue
            fetcher = self._source_map[source_name]
            try:
                data = fetcher.get_stock_sector_stocks(sector_name)
                if data:
                    return data
            except Exception as e:
                logger.error(f"从 {source_name} 获取板块股票列表失败: {e}")
        return None

    def get_stock_sector_index(self, sector_name: str) -> Optional[Dict[str, Any]]:
        """获取板块指数数据。"""
        sources = DataSourceConfig.get_source_priority()
        for source_name in sources:
            if source_name not in self._source_map:
                continue
            fetcher = self._source_map[source_name]
            try:
                data = fetcher.get_stock_sector_index(sector_name)
                if data:
                    return data
            except Exception as e:
                logger.error(f"从 {source_name} 获取板块指数数据失败: {e}")
        return None

    def get_stock_sector_index_stocks(self, sector_index_code: str) -> Optional[list]:
        """获取板块指数成分股列表。"""
        sources = DataSourceConfig.get_source_priority()
        for source_name in sources:
            if source_name not in self._source_map:
                continue
            fetcher = self._source_map[source_name]
            try:
                data = fetcher.get_stock_sector_index_stocks(sector_index_code)
                if data:
                    return data
            except Exception as e:
                logger.error(f"从 {source_name} 获取板块指数成分股失败: {e}")
        return None

    def get_stock_sector_index_data(self, sector_index_code: str) -> Optional[Dict[str, Any]]:
        """获取板块指数行情数据。"""
        sources = DataSourceConfig.get_source_priority()
        for source_name in sources:
            if source_name not in self._source_map:
                continue
            fetcher = self._source_map[source_name]
            try:
                data = fetcher.get_stock_sector_index_data(sector_index_code)
                if data:
                    return data
            except Exception as e:
                logger.error(f"从 {source_name} 获取板块指数行情失败: {e}")
        return None

    def get_stock_sector_index_data_history(self, sector_index_code: str) -> Optional[list]:
        """获取板块指数历史数据。"""
        sources = DataSourceConfig.get_source_priority()
        for source_name in sources:
            if source_name not in self._source_map:
                continue
            fetcher = self._source_map[source_name]
            try:
                data = fetcher.get_stock_sector_index_data_history(sector_index_code)
                if data:
                    return data
            except Exception as e:
                logger.error(f"从 {source_name} 获取板块指数历史数据失败: {e}")
        return None

    def get_stock_sector_index_data_realtime(self, sector_index_code: str) -> Optional[Dict[str, Any]]:
        """获取板块指数实时数据。"""
        sources = DataSourceConfig.get_source_priority()
        for source_name in sources:
            if source_name not in self._source_map:
                continue
            fetcher = self._source_map[source_name]
            try:
                data = fetcher.get_stock_sector_index_data_realtime(sector_index_code)
                if data:
                    return data
            except Exception as e:
                logger.error(f"从 {source_name} 获取板块指数实时数据失败: {e}")
        return None

    def get_stock_sector_index_data_minute(self, sector_index_code: str) -> Optional[list]:
        """获取板块指数分钟数据。"""
        sources = DataSourceConfig.get_source_priority()
        for source_name in sources:
            if source_name not in self._source_map:
                continue
            fetcher = self._source_map[source_name]
            try:
                data = fetcher.get_stock_sector_index_data_minute(sector_index_code)
                if data:
                    return data
            except Exception as e:
                logger.error(f"从 {source_name} 获取板块指数分钟数据失败: {e}")
        return None

    def get_stock_sector_index_data_daily(self, sector_index_code: str) -> Optional[list]:
        """获取板块指数日线数据。"""
        sources = DataSourceConfig.get_source_priority()
        for source_name in sources:
            if source_name not in self._source_map:
                continue
            fetcher = self._source_map[source_name]
            try:
                data = fetcher.get_stock_sector_index_data_daily(sector_index_code)
                if data:
                    return data
            except Exception as e:
                logger.error(f"从 {source_name} 获取板块指数日线数据失败: {e}")
        return None

    def close(self) -> None:
        """释放资源。"""
        for name, fetcher in self._source_map.items():
            try:
                fetcher.close()
            except Exception as e:
                logger.error(f"关闭 {name} 数据源失败: {e}")
        logger.info("终极数据获取器资源释放完成")