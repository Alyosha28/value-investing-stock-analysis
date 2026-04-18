import os
import re
import pandas as pd
from datetime import datetime
from typing import Optional, List
from dotenv import load_dotenv

load_dotenv()

class GrahamThresholds:
    PE_MAX = 15
    PB_MAX = 1.5
    DEBT_TO_EQUITY_MAX = 1.0
    CURRENT_RATIO_MIN = 2.0
    PE_SCORE_THRESHOLDS = [(10, 30), (15, 20), (20, 10)]
    PB_SCORE_THRESHOLDS = [(1, 30), (1.5, 20), (2, 10)]
    ROE_SCORE_THRESHOLDS = [(20, 40), (15, 30), (10, 20), (5, 10)]
    MARGIN_SAFETY_HIGH = 50
    MARGIN_SAFETY_MEDIUM = 33
    MARGIN_SAFETY_MINIMUM = 20
    GRAHAM_NUMBER_MULTIPLIER = 22.5
    AAA_BOND_YIELD_DEFAULT = 4.4
    EARNINGS_YEARS_REQUIRED = 10
    DIVIDEND_YEARS_REQUIRED = 20

class BuffettThresholds:
    ROE_MIN = 15
    ROE_STABILITY_YEARS = 5
    DIVIDEND_YIELD_MIN = 2
    FREE_CASHFLOW_GROWTH_MIN = 5
    DEBT_TO_EBITDA_MAX = 3.0
    CAPEX_TO_DEPRECIATION_MAX = 1.5
    ROE_SCORE_THRESHOLDS = [(20, 40), (15, 30), (10, 20), (5, 10)]
    DIVIDEND_SCORE_THRESHOLDS = [(4, 20), (3, 15), (2, 10), (1, 5)]
    MOAT_WIDE = (20, 20)
    MOAT_MEDIUM = (15, 15)
    MOAT_NARROW = 10
    REASONABLE_PE = 15
    SCORE_BUY = 80
    SCORE_CONSIDER = 60

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
    def validate_api_keys(cls):
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
    DATA_SOURCES = ['tickflow', 'akshare', 'tavily', 'backup']
    MIN_MARKET_CAP = 1e9
    ROE_MIN_VALID = 0
    ROE_MAX_VALID = 100
    @staticmethod
    def get_roe_years():
        current_year = datetime.now().year
        return list(range(current_year - 5, current_year))
    DEFAULT_STOCK_POOL = ['600519', '000001', '601318']

def safe_float(value) -> Optional[float]:
    if value is None or str(value) in ['-', '', 'None', 'nan', 'NaN']:
        return None
    try:
        val = float(value)
        return val if val != float('inf') and val != float('-inf') and not pd.isna(val) else None
    except (ValueError, TypeError):
        return None

def calculate_cagr(history: List[float]) -> Optional[float]:
    if not history or len(history) < 2:
        return None
    valid = [v for v in history if v and v > 0]
    if len(valid) < 2:
        return None
    earliest = valid[0]
    latest = valid[-1]
    if earliest <= 0:
        return None
    years = len(valid) - 1
    if years <= 0:
        return None
    return ((latest / earliest) ** (1 / years) - 1) * 100

def normalize_stock_code(stock_code: str) -> str:
    code = stock_code.strip()
    if not code.endswith('.SH') and not code.endswith('.SZ'):
        if code.startswith('6'):
            code = f"{code}.SH"
        elif code.startswith(('0', '3')):
            code = f"{code}.SZ"
    return code

def to_sina_code(stock_code: str) -> str:
    if stock_code.startswith('6'):
        return f"sh{stock_code}"
    return f"sz{stock_code}"

def to_tushare_code(stock_code: str) -> str:
    if stock_code.startswith('6'):
        return f"{stock_code}.SH"
    return f"{stock_code}.SZ"
