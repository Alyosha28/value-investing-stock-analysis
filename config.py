import os
import re
import pandas as pd
from datetime import datetime
from typing import Optional, List, Union
from pathlib import Path

_env_file = Path(__file__).parent / '.env'
if _env_file.exists():
    try:
        with open(_env_file, 'r', encoding='utf-8') as f:
            for line in f:
                line = line.strip()
                if not line or line.startswith('#'):
                    continue
                if '=' in line:
                    key, value = line.split('=', 1)
                    key = key.strip()
                    value = value.strip()
                    if key and value:
                        os.environ[key] = value
    except Exception:
        pass


class GrahamThresholds:
    PE_MAX = 15
    PB_MAX = 1.5
    DEBT_TO_EQUITY_MAX = 1.0
    CURRENT_RATIO_MIN = 2.0
    MARGIN_SAFETY_HIGH = 50
    MARGIN_SAFETY_MEDIUM = 33
    MARGIN_SAFETY_MINIMUM = 20
    GRAHAM_NUMBER_MULTIPLIER = 22.5
    AAA_BOND_YIELD_DEFAULT = 4.4
    EARNINGS_YEARS_REQUIRED = 10


class BuffettThresholds:
    ROE_MIN = 15
    ROE_STABILITY_YEARS = 5
    DIVIDEND_YIELD_MIN = 2
    FREE_CASHFLOW_GROWTH_MIN = 5
    DEBT_TO_EBITDA_MAX = 3.0
    CAPEX_TO_DEPRECIATION_MAX = 1.5


class MoatConfig:
    RESOURCE_MOAT_WEIGHT = 5
    POLICY_MOAT_WEIGHT = 4
    RESOURCE_MOAT_DESC = '资源壁垒（稀缺资源储量/行业龙头地位）'
    POLICY_MOAT_DESC = '政策壁垒（国家战略保护/出口管制/关键矿产目录）'
    STRATEGIC_RESOURCE_KEYWORDS = [
        '稀土', '锂', '铟', '锗', '镓', '铀', '钨', '锡', '钼', '锑', '锌', '铅', '铜', '金', '银', '铂'
    ]
    STRATEGIC_INDUSTRY_KEYWORDS = [
        '半导体', '芯片', '新能源', '光伏', '军工', '航天', '国防', '算力', 'AI', '5G'
    ]


class DCFConfig:
    DISCOUNT_RATE = 0.09
    TERMINAL_GROWTH_RATE = 0.03
    PROJECTION_YEARS = 10
    MARGIN_OF_SAFETY_BUY = 0.25
    MARGIN_OF_SAFETY_CONSIDER = 0.15


class TechnicalConfig:
    MA_PERIODS = [5, 20, 50, 200]
    RSI_PERIOD = 14
    RSI_OVERBOUGHT = 70
    RSI_OVERSOLD = 30
    BOLLINGER_PERIOD = 20
    BOLLINGER_STD = 2
    MACD_FAST = 12
    MACD_SLOW = 26
    MACD_SIGNAL = 9
    ADX_PERIOD = 14
    ADX_STRONG_TREND = 25
    ADX_VERY_STRONG = 40
    ATR_PERIOD = 14
    STOCHASTIC_PERIOD = 14
    STOCHASTIC_SIGNAL = 3
    KDJ_PERIOD = 9
    KDJ_SIGNAL_PERIOD = 3
    KDJ_OVERBOUGHT = 100
    KDJ_OVERSOLD = 0
    RSI_CENTERLINE = 50
    BOLLINGER_SQUEEZE_PERIOD = 20
    VOLUME_MA_PERIOD = 20
    OBV_PERIOD = 20
    ROC_PERIODS = [5, 10, 20]
    TREND_WEIGHT = 0.30
    MOMENTUM_WEIGHT = 0.20
    VOLATILITY_WEIGHT = 0.15
    VOLUME_WEIGHT = 0.15
    SUPPORT_RESISTANCE_WEIGHT = 0.10
    RELATIVE_STRENGTH_WEIGHT = 0.10


class APIConfig:
    DEEPSEEK_API_KEY = os.getenv('DEEPSEEK_API_KEY')
    DEEPSEEK_API_URL = 'https://api.deepseek.com/v1/chat/completions'
    DEEPSEEK_MODEL = 'deepseek-chat'
    DEEPSEEK_TEMPERATURE = 0.3
    DEEPSEEK_MAX_TOKENS = 1000
    DEEPSEEK_TIMEOUT = 60

    @classmethod
    def validate_api_keys(cls) -> List[str]:
        issues = []
        if cls.DEEPSEEK_API_KEY:
            if not re.match(r'^sk-[a-zA-Z0-9]+$', cls.DEEPSEEK_API_KEY):
                issues.append('DEEPSEEK_API_KEY 格式可能无效')
        else:
            issues.append('DEEPSEEK_API_KEY 未设置')
        return issues


class SystemConfig:
    OUTPUT_DIR = 'output'
    LOG_LEVEL = os.getenv('LOG_LEVEL', 'INFO')
    LOG_FILE = 'logs/stock_analysis.log'
    LOG_MAX_BYTES = 10 * 1024 * 1024
    LOG_BACKUP_COUNT = 5
    REQUEST_TIMEOUT = 30
    RETRY_MAX = 3
    RETRY_DELAY = 1
    SLEEP_BETWEEN_STOCKS = 1
    FINANCIAL_YEARS_HISTORY = 5
    CONCURRENT_MAX_WORKERS = None
    ENABLE_CONCURRENT = True


class DataConfig:
    DATA_SOURCES = ['xinhua', 'backup', 'tushare', 'akshare', 'tickflow', 'tavily']
    MIN_MARKET_CAP = 1e9
    ROE_MIN_VALID = 0
    ROE_MAX_VALID = 100

    @staticmethod
    def get_roe_years() -> List[int]:
        current_year = datetime.now().year
        return list(range(current_year - 5, current_year))

    DEFAULT_STOCK_POOL = ['600519', '000001', '601318']


class DataSourceConfig:
    XINHUA_ENABLED = os.getenv('XINHUA_ENABLED', 'true').lower() == 'true'
    TICKFLOW_ENABLED = os.getenv('TICKFLOW_ENABLED', 'true').lower() == 'true'
    AKSHARE_ENABLED = os.getenv('AKSHARE_ENABLED', 'true').lower() == 'true'
    TAVILY_ENABLED = os.getenv('TAVILY_ENABLED', 'true').lower() == 'true'
    BACKUP_ENABLED = os.getenv('BACKUP_ENABLED', 'true').lower() == 'true'
    TUSHARE_ENABLED = os.getenv('TUSHARE_ENABLED', 'true').lower() == 'true'

    XINHUA_TIMEOUT = int(os.getenv('XINHUA_TIMEOUT', '10'))
    TICKFLOW_TIMEOUT = int(os.getenv('TICKFLOW_TIMEOUT', '15'))
    AKSHARE_TIMEOUT = int(os.getenv('AKSHARE_TIMEOUT', '15'))
    TAVILY_TIMEOUT = int(os.getenv('TAVILY_TIMEOUT', '20'))

    AKSHARE_MAX_WORKERS = int(os.getenv('AKSHARE_MAX_WORKERS', '3'))
    AKSHARE_REQUEST_INTERVAL = float(os.getenv('AKSHARE_REQUEST_INTERVAL', '0.5'))

    TUSHARE_TOKEN = os.getenv('TUSHARE_TOKEN')
    TUSHARE_PROXY_URL = os.getenv('TUSHARE_PROXY_URL', 'http://118.89.66.41:8010/')

    @classmethod
    def get_source_priority(cls) -> List[str]:
        """返回数据源优先级列表。
        
        执行顺序约束：tushare 必须在 akshare 之前。
        原因：tushare 提供更完整的财务数据，akshare 作为补充源；
        两者同时请求可能造成资源竞争和接口限流。
        """
        priority = []
        if cls.XINHUA_ENABLED:
            priority.append('xinhua')
        if cls.BACKUP_ENABLED:
            priority.append('backup')
        if cls.TUSHARE_ENABLED:
            priority.append('tushare')
        if cls.AKSHARE_ENABLED:
            priority.append('akshare')
        if cls.TICKFLOW_ENABLED:
            priority.append('tickflow')
        if cls.TAVILY_ENABLED:
            priority.append('tavily')
        priority = cls._enforce_source_order(priority)
        return priority

    @classmethod
    def _enforce_source_order(cls, sources: List[str]) -> List[str]:
        """确保数据源顺序满足依赖约束：akshare 必须在 tushare 之后。"""
        if 'akshare' in sources and 'tushare' in sources:
            ak_idx = sources.index('akshare')
            ts_idx = sources.index('tushare')
            if ak_idx < ts_idx:
                sources.remove('tushare')
                sources.insert(ak_idx, 'tushare')
        return sources

    @classmethod
    def is_source_available(cls, source: str) -> bool:
        source_map = {
            'xinhua': cls.XINHUA_ENABLED,
            'tickflow': cls.TICKFLOW_ENABLED,
            'akshare': cls.AKSHARE_ENABLED,
            'tavily': cls.TAVILY_ENABLED,
            'backup': cls.BACKUP_ENABLED,
            'tushare': cls.TUSHARE_ENABLED and bool(cls.TUSHARE_TOKEN),
        }
        return source_map.get(source, False)


def safe_float(value: Union[str, float, int, None]) -> Optional[float]:
    """安全地将值转换为浮点数，处理各种无效输入。"""
    if value is None:
        return None
    value_str = str(value).strip()
    if value_str in {'-', '', 'None', 'nan', 'NaN', 'null', 'NULL'}:
        return None
    try:
        val = float(value_str)
        if val == float('inf') or val == float('-inf') or pd.isna(val):
            return None
        return val
    except (ValueError, TypeError):
        return None


def calculate_cagr(history: List[float]) -> Optional[float]:
    """计算复合年增长率 (CAGR)。"""
    if not history or len(history) < 2:
        return None
    valid = [v for v in history if v is not None and v > 0]
    if len(valid) < 2:
        return None
    # 历史数据通常按时间降序排列（最新在前），反转得到正确顺序
    valid = list(reversed(valid))
    earliest, latest = valid[0], valid[-1]
    years = len(valid) - 1
    if years <= 0:
        return None
    return ((latest / earliest) ** (1 / years) - 1) * 100


def normalize_stock_code(stock_code: str) -> str:
    """标准化股票代码格式为 .SH 或 .SZ 后缀。"""
    code = stock_code.strip()
    if code.endswith('.SH') or code.endswith('.SZ'):
        return code
    if code.startswith('6'):
        return f"{code}.SH"
    if code.startswith(('0', '3')):
        return f"{code}.SZ"
    return code


def to_sina_code(stock_code: str) -> str:
    """转换为新浪财经格式 (sh/sz 前缀)。"""
    code = stock_code.strip().replace('.SH', '').replace('.SZ', '')
    if code.startswith('6'):
        return f"sh{code}"
    return f"sz{code}"


def to_tushare_code(stock_code: str) -> str:
    """转换为 Tushare 格式 (.SH/.SZ 后缀)。"""
    return normalize_stock_code(stock_code)
