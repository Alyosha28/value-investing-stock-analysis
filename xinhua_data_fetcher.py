import requests
import re
import time
from typing import Dict, Optional

from logger_config import logger
from config import SystemConfig, safe_float


class XinhuaFinanceDataFetcher:
    """新华财经网数据获取器"""

    QUOTE_API_BASE = 'https://quotedata.cnfin.com'
    SEARCH_URL = 'https://search.cnfin.com/quotes'
    REAL_FIELDS = 'prod_name,last_px,open_px,high_px,low_px,preclose_px,business_amount,business_balance,market_value,px_change_rate,turnover_ratio,eps,amplitude,px_change'
    FIELD_INDEX_MAP = {
        'prod_name': 'name',
        'last_px': 'current',
        'open_px': 'open',
        'high_px': 'high',
        'low_px': 'low',
        'preclose_px': 'close',
        'business_amount': 'volume',
        'business_balance': 'amount',
        'market_value': 'total_mv',
        'px_change_rate': 'pct_chg',
        'turnover_ratio': 'turnover_rate',
        'eps': 'eps',
        'amplitude': 'amplitude',
        'px_change': 'px_change',
    }

    def __init__(self):
        self._timeout = SystemConfig.REQUEST_TIMEOUT
        self._retry_count = SystemConfig.RETRY_MAX
        self._retry_delay = SystemConfig.RETRY_DELAY
        self._last_request_time = 0
        self._min_request_interval = 0.3
        self._session = requests.Session()
        self._session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/html, */*',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Referer': 'https://www.cnfin.com/',
            'Origin': 'https://www.cnfin.com',
            'Connection': 'keep-alive',
        })
        self._available = True
        self._check_availability()

    def _check_availability(self):
        try:
            resp = self._safe_request(
                f"{self.QUOTE_API_BASE}/quote/v1/real",
                params={'en_prod_code': '600519.XSHG', 'fields': 'prod_name,last_px'},
                timeout=5
            )
            if resp and resp.status_code == 200:
                data = resp.json()
                if data.get('data', {}).get('snapshot'):
                    logger.info("新华财经数据源可用")
                    self._available = True
                else:
                    logger.warning("新华财经数据源响应异常，已禁用")
                    self._available = False
            else:
                logger.warning("新华财经数据源连接失败，已禁用")
                self._available = False
        except Exception:
            logger.warning("新华财经数据源检查失败，已禁用")
            self._available = False

    def is_available(self):
        return self._available

    def _safe_request(self, url: str, params: dict = None, headers: dict = None,
                      timeout: int = None) -> Optional[requests.Response]:
        if timeout is None:
            timeout = self._timeout

        req_headers = dict(self._session.headers)
        if headers:
            req_headers.update(headers)

        for attempt in range(self._retry_count):
            try:
                elapsed = time.time() - self._last_request_time
                if elapsed < self._min_request_interval:
                    time.sleep(self._min_request_interval - elapsed)
                self._last_request_time = time.time()

                response = self._session.get(url, params=params, headers=req_headers, timeout=timeout)
                response.raise_for_status()
                return response
            except requests.RequestException as e:
                logger.warning(f"请求失败 (尝试 {attempt + 1}/{self._retry_count}): {e}")
                if attempt < self._retry_count - 1:
                    time.sleep(self._retry_delay * (attempt + 1))

        return None

    @staticmethod
    def _to_cnfin_code(stock_code: str) -> str:
        code = stock_code.strip()
        if '.' not in code:
            if code.startswith('6'):
                return f"{code}.XSHG"
            if code.startswith(('0', '3')):
                return f"{code}.XSHE"
        return code

    def get_stock_info(self, stock_code: str) -> Optional[Dict]:
        if not self._available:
            return None

        quote = self.get_realtime_quote(stock_code)
        if quote and quote.get('name'):
            result = {
                'stock_name': quote['name'],
                'industry': None,
                'list_date': None,
                'total_share': quote.get('total_share'),
                'float_share': quote.get('float_share'),
            }
            logger.info(f"新华财经获取股票信息: {quote['name']} ({stock_code})")
            return result

        try:
            response = self._safe_request(
                self.SEARCH_URL,
                params={'q': stock_code}
            )
            if response and response.status_code == 200:
                content = response.text
                name_match = re.search(
                    r'<td>\s*<font[^>]*>' + re.escape(stock_code) + r'</font>\s*</td>\s*<td>\s*([^<]+?)\s*</td>',
                    content, re.S
                )
                if name_match:
                    stock_name = name_match.group(1).strip()
                    result = {
                        'stock_name': stock_name,
                        'industry': None,
                        'list_date': None,
                        'total_share': None,
                        'float_share': None,
                    }
                    logger.info(f"新华财经(搜索)获取股票名称: {stock_name} ({stock_code})")
                    return result
        except Exception as e:
            logger.warning(f"新华财经获取股票信息失败: {e}")

        return None

    def get_realtime_quote(self, stock_code: str) -> Optional[Dict]:
        if not self._available:
            return None

        try:
            cnfin_code = self._to_cnfin_code(stock_code)
            response = self._safe_request(
                f"{self.QUOTE_API_BASE}/quote/v1/real",
                params={'en_prod_code': cnfin_code, 'fields': self.REAL_FIELDS}
            )
            if response and response.status_code == 200:
                data = response.json()
                snapshot = data.get('data', {}).get('snapshot')
                if not snapshot:
                    return None
                # API 返回两种格式：列表 [fields, values] 或字典 {"fields": [...], "code.XSHG": [...]}
                if isinstance(snapshot, list) and len(snapshot) >= 2:
                    fields = snapshot[0]
                    values = snapshot[1]
                    return self._parse_snapshot(fields, values, stock_code)
                elif isinstance(snapshot, dict):
                    fields = snapshot.get('fields')
                    values = snapshot.get(cnfin_code)
                    if fields and values:
                        return self._parse_snapshot(fields, values, stock_code)
        except (ValueError, TypeError) as e:
            logger.warning(f"新华财经解析实时行情JSON失败: {e}")
        except Exception as e:
            logger.warning(f"新华财经获取实时行情失败: {e}")
        return None

    def _parse_snapshot(self, fields, values, stock_code: str) -> Optional[Dict]:
        try:
            field_dict = dict(zip(fields, values))
            result = {}
            for api_field, our_field in self.FIELD_INDEX_MAP.items():
                val = field_dict.get(api_field)
                if val is not None:
                    if isinstance(val, str):
                        result[our_field] = val
                    else:
                        result[our_field] = safe_float(val)
                else:
                    result[our_field] = None

            if result.get('current') and result['current'] > 0 and result.get('eps') and result['eps'] > 0:
                result['pe'] = round(result['current'] / result['eps'], 2)
            else:
                result['pe'] = None

            result['pb'] = None
            result['float_mv'] = None
            result['total_share'] = None
            result['float_share'] = None

            if result.get('current') and result['current'] > 0:
                if not result.get('close'):
                    result['close'] = result['current']
                logger.info(
                    f"新华财经行情 - {result.get('name', stock_code)}: "
                    f"价格={result['current']}, PE={result.get('pe')}, 市值={result.get('total_mv')}"
                )
                return result
        except Exception as e:
            logger.warning(f"新华财经解析行情数据失败: {e}")
        return None

    def get_financial_data(self, stock_code: str) -> Optional[Dict]:
        if not self._available:
            return None

        quote = self.get_realtime_quote(stock_code)
        if not quote:
            return None

        result = {
            'pe': quote.get('pe'),
            'pb': quote.get('pb'),
            'total_mv': quote.get('total_mv'),
            'float_mv': quote.get('float_mv'),
            'eps': quote.get('eps'),
            'current_price': quote.get('current'),
            'dividend_yield': None,
            'gross_margin': None,
            'net_margin': None,
            'revenue': None,
            'net_profit': None,
        }

        has_data = any(v is not None for v in result.values())
        if has_data:
            logger.info(f"新华财经财务数据 - PE={result.get('pe')}, PB={result.get('pb')}, EPS={result.get('eps')}")
            return result
        return None

    def get_stock_data(self, stock_code: str) -> Optional[Dict]:
        try:
            stock_info = self.get_stock_info(stock_code)
            realtime_quote = self.get_realtime_quote(stock_code)
            financial_data = self.get_financial_data(stock_code)

            if not realtime_quote and not financial_data:
                return None

            result = {}
            if stock_info:
                result.update(stock_info)
            if realtime_quote:
                for key, val in realtime_quote.items():
                    if val is not None:
                        result[key] = val
            if financial_data:
                for key, val in financial_data.items():
                    if val is not None and key not in result:
                        result[key] = val

            result['stock_code'] = stock_code
            result['source'] = 'xinhua'
            return result
        except Exception as e:
            logger.error(f"新华财经统一接口获取失败: {e}")
            return None

    def close(self):
        self._session.close()
        logger.info("新华财经数据源会话已关闭")


if __name__ == '__main__':
    fetcher = XinhuaFinanceDataFetcher()
    test_codes = ['600519', '000001', '601318']

    for code in test_codes:
        print(f"\n{'='*60}")
        print(f"测试股票: {code}")
        print('='*60)

        print("\n[1] 测试股票基本信息...")
        info = fetcher.get_stock_info(code)
        if info:
            print(f"  成功: 名称={info.get('stock_name')}, 总股本={info.get('total_share')}")
        else:
            print("  失败")

        print("\n[2] 测试实时行情...")
        quote = fetcher.get_realtime_quote(code)
        if quote:
            print(f"  成功: 价格={quote.get('current')}, 名称={quote.get('name')}")
            print(f"  PE={quote.get('pe')}, PB={quote.get('pb')}, 市值={quote.get('total_mv')}")
        else:
            print("  失败")

        print("\n[3] 测试财务数据...")
        financial = fetcher.get_financial_data(code)
        if financial:
            print(f"  成功: PE={financial.get('pe')}, EPS={financial.get('eps')}")
        else:
            print("  失败")

    fetcher.close()
    print("\n测试完成!")
