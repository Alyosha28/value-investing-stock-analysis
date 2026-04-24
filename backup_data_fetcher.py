import requests
import pandas as pd
import time
import re
import os
import json
from typing import Dict, Optional, List, Any
from datetime import datetime

from logger_config import logger
from config import SystemConfig, safe_float, to_sina_code, to_tushare_code


class BackupDataFetcher:

    EASTMONEY_FIELDS = {
        'f43': 'current', 'f44': 'high', 'f45': 'low', 'f46': 'open',
        'f47': 'volume', 'f48': 'amount', 'f50': 'volume_ratio',
        'f51': 'limit_up', 'f52': 'limit_down', 'f55': 'turnover_rate',
        'f57': 'code', 'f58': 'name',
        'f116': 'total_mv', 'f117': 'float_mv',
        'f162': 'pe_dynamic', 'f167': 'pe_static', 'f168': 'pe_ttm',
        'f169': 'pb', 'f170': 'total_share', 'f171': 'float_share',
        'f292': 'pct_chg'
    }

    EASTMONEY_STR_FIELDS = {'f57', 'f58'}
    EASTMONEY_RAW_FIELDS = {'f47', 'f48', 'f116', 'f117'}
    EASTMONEY_NEG_DIV100_FIELDS = {'f162', 'f167', 'f168', 'f169', 'f170', 'f171', 'f292'}
    PRICE_FIELDS = {'f43', 'f44', 'f45', 'f46', 'f51', 'f52'}
    PERCENT_FIELDS = {'f55'}

    FIELD_MAPS = {
        'xinhua': {
            'current_price': 'current', 'open': 'open', 'high': 'high', 'low': 'low',
            'close': 'close', 'volume': 'volume', 'amount': 'amount',
            'pe': 'pe', 'pb': 'pb', 'total_mv': 'total_mv', 'float_mv': 'float_mv',
            'pct_chg': 'pct_chg', 'turnover_rate': 'turnover_rate', 'name': 'name',
        },
        'eastmoney': {
            'current_price': 'current', 'open': 'open', 'high': 'high', 'low': 'low',
            'close': 'close', 'volume': 'volume', 'amount': 'amount',
            'total_mv': 'total_mv', 'float_mv': 'float_mv',
            'pe': 'pe_ttm', 'pb': 'pb', 'pct_chg': 'pct_chg',
            'turnover_rate': 'turnover_rate', 'name': 'name',
        },
        'tencent': {
            'current_price': 'current', 'open': 'open', 'high': 'high', 'low': 'low',
            'close': 'close', 'volume': 'volume', 'amount': 'amount',
            'pe': 'pe', 'total_mv': 'total_mv', 'float_mv': 'float_mv',
            'name': 'name',
        },
        'sina': {
            'current_price': 'current', 'open': 'open', 'high': 'high', 'low': 'low',
            'close': 'close', 'volume': 'volume', 'amount': 'amount',
            'name': 'name',
        },
        'tushare': {
            'current_price': 'price', 'open': 'open', 'high': 'high', 'low': 'low',
            'close': 'close', 'volume': 'volume', 'amount': 'amount',
            'pct_chg': 'pct_chg',
        },
    }

    SOURCE_PRIORITY = ['xinhua', 'eastmoney', 'tencent', 'sina', 'tushare']

    def __init__(self):
        self._timeout = SystemConfig.REQUEST_TIMEOUT
        self._retry_count = SystemConfig.RETRY_MAX
        self._retry_delay = SystemConfig.RETRY_DELAY
        self._last_request_time = 0
        self._min_request_interval = 0.5
        self._session = requests.Session()
        self._session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36 (KHTML, like Gecko) Chrome/120.0.0.0 Safari/537.36',
            'Accept': 'application/json, text/html, */*',
            'Accept-Language': 'zh-CN,zh;q=0.9,en;q=0.8',
            'Connection': 'keep-alive',
        })
        self._tushare = None
        self._xinhua = None
        self._init_tushare()
        self._init_xinhua()

    def _init_tushare(self):
        try:
            import tushare as ts
            token = os.getenv('TUSHARE_TOKEN')
            proxy_url = os.getenv('TUSHARE_PROXY_URL', 'http://118.89.66.41:8010/')
            if token:
                pro = ts.pro_api(token)
                pro._DataApi__http_url = proxy_url
                self._tushare = pro
                logger.info(f"Tushare 初始化成功，代理地址: {proxy_url}")
            else:
                logger.warning("未找到 TUSHARE_TOKEN，跳过Tushare")
                self._tushare = None
        except Exception as e:
            logger.warning(f"Tushare 初始化失败: {e}")
            self._tushare = None

    def _init_xinhua(self):
        try:
            from xinhua_data_fetcher import XinhuaFinanceDataFetcher
            self._xinhua = XinhuaFinanceDataFetcher()
            if self._xinhua.is_available:
                logger.info("新华财经网数据源初始化成功")
            else:
                self._xinhua = None
                logger.warning("新华财经网数据源不可用，已跳过")
        except Exception as e:
            self._xinhua = None
            logger.warning(f"新华财经网数据源初始化失败: {e}")

    def _safe_request(self, url: str, params: dict = None, headers: dict = None,
                      method: str = 'GET', timeout: int = None) -> Optional[requests.Response]:
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

                if method.upper() == 'GET':
                    response = self._session.get(url, params=params, headers=req_headers, timeout=timeout)
                else:
                    response = self._session.post(url, json=params, headers=req_headers, timeout=timeout)

                response.raise_for_status()
                return response
            except requests.RequestException as e:
                logger.warning(f"请求失败 (尝试 {attempt + 1}/{self._retry_count}): {e}")
                if attempt < self._retry_count - 1:
                    time.sleep(self._retry_delay * (attempt + 1))

        return None

    @staticmethod
    def _to_eastmoney_secid(stock_code: str) -> str:
        if stock_code.startswith('6'):
            return f"1.{stock_code}"
        return f"0.{stock_code}"

    def _convert_eastmoney_value(self, field: str, raw_value) -> Any:
        if raw_value is None or raw_value == '-':
            return None
        if field in self.EASTMONEY_STR_FIELDS:
            return str(raw_value)
        if field in self.EASTMONEY_RAW_FIELDS:
            try:
                return float(raw_value)
            except (ValueError, TypeError):
                return None
        try:
            val = float(raw_value)
            if field in self.PRICE_FIELDS:
                val = val / 100
            elif field in self.PERCENT_FIELDS:
                val = val / 100
            elif field in self.EASTMONEY_NEG_DIV100_FIELDS:
                val = abs(val) / 100
            return val
        except (ValueError, TypeError):
            return None

    def get_realtime_quote_from_eastmoney(self, stock_code: str) -> Optional[Dict]:
        try:
            secid = self._to_eastmoney_secid(stock_code)
            fields_str = ','.join(self.EASTMONEY_FIELDS.keys())
            url = f"http://push2.eastmoney.com/api/qt/stock/get"
            params = {
                'secid': secid,
                'fields': fields_str,
                '_': int(time.time() * 1000),
            }
            response = self._safe_request(url, params=params)

            if response and response.status_code == 200:
                data = response.json()
                stock_data = data.get('data', {})
                if not stock_data:
                    return None

                result = {}
                for field, name in self.EASTMONEY_FIELDS.items():
                    raw = stock_data.get(field)
                    if raw is not None:
                        result[name] = self._convert_eastmoney_value(field, raw)
                    else:
                        result[name] = None

                result['close'] = result.get('current')

                if result.get('current') and result.get('current') > 0:
                    logger.info(f"东方财富行情 - {result.get('name', stock_code)}: "
                                f"价格={result['current']}, PE(TTM)={result.get('pe_ttm')}, "
                                f"PB={result.get('pb')}, 总市值={result.get('total_mv')}")
                    return result

        except Exception as e:
            logger.warning(f"东方财富实时行情获取失败: {e}")

        return None

    def get_kline_from_eastmoney(self, stock_code: str, period: str = '1y',
                                  klt: int = 101, limit: int = 250) -> Optional[pd.DataFrame]:
        try:
            secid = self._to_eastmoney_secid(stock_code)
            url = "http://push2his.eastmoney.com/api/qt/stock/kline/get"
            params = {
                'secid': secid,
                'fields1': 'f1,f2,f3,f4,f5,f6',
                'fields2': 'f51,f52,f53,f54,f55,f56,f57,f58',
                'klt': klt,
                'fqt': 1,
                'end': '20500101',
                'lmt': limit,
                '_': int(time.time() * 1000),
            }
            response = self._safe_request(url, params=params)

            if response and response.status_code == 200:
                data = response.json()
                klines = data.get('data', {}).get('klines', [])
                if not klines:
                    return None

                rows = []
                for line in klines:
                    parts = line.split(',')
                    if len(parts) >= 8:
                        rows.append({
                            'Date': parts[0],
                            'Open': safe_float(parts[1]),
                            'Close': safe_float(parts[2]),
                            'High': safe_float(parts[3]),
                            'Low': safe_float(parts[4]),
                            'Volume': safe_float(parts[5]),
                            'Amount': safe_float(parts[6]),
                            'ChangePct': safe_float(parts[7]),
                        })

                if rows:
                    df = pd.DataFrame(rows)
                    df['Date'] = pd.to_datetime(df['Date'])
                    df.set_index('Date', inplace=True)
                    logger.info(f"东方财富K线获取成功: {stock_code}, 共 {len(df)} 条")
                    return df

        except Exception as e:
            logger.warning(f"东方财富K线获取失败: {e}")

        return None

    def get_stock_info_from_sina(self, stock_code: str) -> Optional[Dict]:
        try:
            sina_code = to_sina_code(stock_code)
            url = f"http://hq.sinajs.cn/list={sina_code}"
            headers = {
                'Referer': 'https://finance.sina.com.cn',
            }
            response = self._safe_request(url, headers=headers)

            if response and response.status_code == 200:
                content = response.content.decode('gbk')
                match = re.search(r'"([^"]*)"', content)
                if match:
                    data_str = match.group(1)
                    if not data_str.strip():
                        logger.warning(f"新浪财经返回空数据: {stock_code}")
                        return None
                    data = data_str.split(',')
                    if len(data) > 31:
                        return {
                            'name': data[0],
                            'open': safe_float(data[1]),
                            'close': safe_float(data[2]),
                            'current': safe_float(data[3]),
                            'high': safe_float(data[4]),
                            'low': safe_float(data[5]),
                            'volume': int(safe_float(data[8]) or 0) or None,
                            'amount': safe_float(data[9]),
                            'date': data[30],
                            'time': data[31],
                        }
        except Exception as e:
            logger.warning(f"新浪财经获取失败: {e}")

        return None

    def get_realtime_quote_from_tencent(self, stock_code: str) -> Optional[Dict]:
        try:
            tc_code = to_sina_code(stock_code)
            url = f"http://qt.gtimg.cn/q={tc_code}"
            response = self._safe_request(url)

            if response and response.status_code == 200:
                content = response.content.decode('gbk')
                match = re.search(r'"([^"]*)"', content)
                if match:
                    data_str = match.group(1)
                    if not data_str.strip():
                        logger.warning(f"腾讯财经返回空数据: {stock_code}")
                        return None
                    data = data_str.split('~')
                    if len(data) > 48:
                        current = safe_float(data[3])
                        close = safe_float(data[4])
                        open_price = safe_float(data[5])
                        volume = safe_float(data[6])
                        high = safe_float(data[33])
                        low = safe_float(data[34])
                        pe = safe_float(data[39])
                        total_mv = safe_float(data[45])
                        float_mv = safe_float(data[46])

                        if total_mv:
                            total_mv = total_mv * 10000
                        if float_mv:
                            float_mv = float_mv * 10000

                        return {
                            'name': data[1],
                            'current': current,
                            'close': close,
                            'open': open_price,
                            'volume': int(volume) if volume else None,
                            'high': high if high and high > 0 else None,
                            'low': low if low and low > 0 else None,
                            'pe': pe,
                            'total_mv': total_mv,
                            'float_mv': float_mv,
                            'amount': safe_float(data[37]),
                            'pct_chg': safe_float(data[32]),
                        }
        except Exception as e:
            logger.warning(f"腾讯财经获取失败: {e}")
    def get_realtime_quote_from_xinhua(self, stock_code: str) -> Optional[Dict]:
        if not self._xinhua:
            return None
        try:
            quote = self._xinhua.get_realtime_quote(stock_code)
            if quote and quote.get('current') and quote['current'] > 0:
                logger.info(f"新华财经网行情 - {quote.get('name', stock_code)}: "
                            f"价格={quote['current']}, PE={quote.get('pe')}, "
                            f"PB={quote.get('pb')}")
                return quote
        except Exception as e:
            logger.warning(f"新华财经网获取失败: {e}")
        return None


        return None

    def get_stock_info_from_tushare(self, stock_code: str) -> Optional[Dict]:
        if not self._tushare:
            return None

        try:
            ts_code = to_tushare_code(stock_code)
            df = self._tushare.stock_basic(ts_code=ts_code,
                                           fields='ts_code,symbol,name,area,industry,list_date,market,exchange')
            if df is not None and not df.empty:
                row = df.iloc[0]
                return {
                    'name': row.get('name'),
                    'industry': row.get('industry'),
                    'area': row.get('area'),
                    'list_date': row.get('list_date'),
                    'market': row.get('market'),
                }
        except Exception as e:
            logger.warning(f"Tushare 基本信息获取失败: {e}")

        return None

    def get_financial_data_from_tushare(self, stock_code: str) -> Optional[Dict]:
        if not self._tushare:
            return None

        result = {
            'roe_history': [],
            'net_profit_history': [],
            'revenue_history': [],
        }

        try:
            ts_code = to_tushare_code(stock_code)

            df = self._tushare.fina_indicator(ts_code=ts_code,
                                              fields='ts_code,ann_date,roe,net_profit_ratio,gross_profit_margin,debt_to_assets')
            if df is not None and not df.empty:
                for _, row in df.head(5).iterrows():
                    roe_val = row.get('roe')
                    if roe_val is not None and not (isinstance(roe_val, float) and pd.isna(roe_val)):
                        result['roe_history'].append(float(roe_val))

            income_df = self._tushare.income(ts_code=ts_code,
                                             fields='ts_code,ann_date,total_revenue,nethmq_profit')
            if income_df is not None and not income_df.empty:
                for _, row in income_df.head(5).iterrows():
                    rev = row.get('total_revenue')
                    if rev is not None and not (isinstance(rev, float) and pd.isna(rev)):
                        result['revenue_history'].append(float(rev))
                    profit = row.get('nethmq_profit')
                    if profit is not None and not (isinstance(profit, float) and pd.isna(profit)):
                        result['net_profit_history'].append(float(profit))

            if result['roe_history'] or result['revenue_history']:
                logger.info(f"Tushare 财务数据 - ROE: {len(result['roe_history'])}年, "
                            f"营收: {len(result['revenue_history'])}年")
                return result

        except Exception as e:
            logger.warning(f"Tushare 财务数据获取失败: {e}")

        return None

    def get_realtime_data_from_tushare(self, stock_code: str) -> Optional[Dict]:
        if not self._tushare:
            return None

        try:
            ts_code = to_tushare_code(stock_code)
            df = self._tushare.realtime_quote(ts_code=ts_code,
                                               fields='ts_code,open,high,low,price,volume,amount,pct_chg')
            if df is not None and not df.empty:
                row = df.iloc[0]
                return {
                    'open': row.get('open'),
                    'high': row.get('high'),
                    'low': row.get('low'),
                    'price': row.get('price'),
                    'close': row.get('price'),
                    'volume': row.get('volume'),
                    'amount': row.get('amount'),
                    'pct_chg': row.get('pct_chg'),
                }
        except Exception as e:
            logger.warning(f"Tushare 实时行情获取失败: {e}")

        return None

    def _validate_quote_data(self, quote: Dict) -> bool:
        if not quote:
            return False
        current = quote.get('current') or quote.get('current_price')
        if current is None or current <= 0:
            return False
        high = quote.get('high')
        low = quote.get('low')
        if high and low and high < low:
            logger.warning(f"数据异常: 最高价({high}) < 最低价({low})")
            return False
        if high and current > high * 1.1:
            logger.warning(f"数据异常: 当前价({current}) 远高于最高价({high})")
            return False
        if low and current < low * 0.9:
            logger.warning(f"数据异常: 当前价({current}) 远低于最低价({low})")
            return False
        return True

    def _cross_validate_quotes(self, quotes: Dict[str, Dict]) -> Optional[Dict]:
        if not quotes:
            return None

        prices = {}
        for source, data in quotes.items():
            price = data.get('current') or data.get('current_price')
            if price and price > 0:
                prices[source] = price

        if len(prices) >= 2:
            price_values = list(prices.values())
            avg_price = sum(price_values) / len(price_values)
            for source, price in prices.items():
                if abs(price - avg_price) / avg_price > 0.05:
                    logger.warning(f"数据源 {source} 价格({price}) 与均价({avg_price:.2f}) 偏差超过5%")

        for source in self.SOURCE_PRIORITY:
            if source in quotes and self._validate_quote_data(quotes[source]):
                return quotes[source]

        for source, data in quotes.items():
            if self._validate_quote_data(data):
                return data

        return None

    def get_all_realtime_quotes(self, stock_code: str) -> Dict[str, Any]:
        results = {}
        quote = self.get_realtime_quote_from_xinhua(stock_code)
        if quote:
            results['xinhua'] = quote
            logger.info(f"新华财经网成功获取 {stock_code} 行情")
        logger.info(f"尝试从多个数据源获取 {stock_code} 实时行情...")

        quote = self.get_realtime_quote_from_eastmoney(stock_code)
        if quote:
            results['eastmoney'] = quote
            logger.info(f"东方财富成功获取 {stock_code} 行情")

        quote = self.get_realtime_quote_from_tencent(stock_code)
        if quote:
            results['tencent'] = quote
            logger.info(f"腾讯财经成功获取 {stock_code} 行情")

        quote = self.get_stock_info_from_sina(stock_code)
        if quote:
            results['sina'] = quote
            logger.info(f"新浪财经成功获取 {stock_code} 行情")

        if not results:
            quote = self.get_realtime_data_from_tushare(stock_code)
            if quote:
                results['tushare'] = quote
                logger.info(f"Tushare成功获取 {stock_code} 行情")

        return results

    def get_all_stock_info(self, stock_code: str) -> Dict[str, Any]:
        results = {}
        if self._xinhua:
            try:
                xinhua_info = self._xinhua.get_stock_info(stock_code)
                if xinhua_info and xinhua_info.get('stock_name'):
                    results['xinhua'] = {
                        'name': xinhua_info['stock_name'],
                        'code': stock_code,
                        'industry': xinhua_info.get('industry'),
                        'list_date': xinhua_info.get('list_date'),
                    }
                    logger.info(f"新华财经网成功获取 {stock_code} 基本信息")
            except Exception as e:
                logger.warning(f"新华财经网获取基本信息失败: {e}")
        logger.info(f"尝试从多个数据源获取 {stock_code} 基本信息...")

        quote = self.get_realtime_quote_from_eastmoney(stock_code)
        if quote and quote.get('name'):
            results['eastmoney'] = {
                'name': quote.get('name'),
                'code': stock_code,
            }
            logger.info(f"东方财富成功获取 {stock_code} 基本信息")

        info = self.get_stock_info_from_tushare(stock_code)
        if info:
            results['tushare'] = info
            logger.info(f"Tushare成功获取 {stock_code} 基本信息")

        return results

    def get_all_financial_data(self, stock_code: str) -> Dict[str, Any]:
        results = {}
        if self._xinhua:
            try:
                xinhua_financial = self._xinhua.get_financial_data(stock_code)
                if xinhua_financial:
                    results['xinhua'] = xinhua_financial
                    logger.info(f"新华财经网成功获取 {stock_code} 财务指标")
            except Exception as e:
                logger.warning(f"新华财经网获取财务数据失败: {e}")
        logger.info(f"尝试从多个数据源获取 {stock_code} 财务数据...")

        quote = self.get_realtime_quote_from_eastmoney(stock_code)
        if quote:
            eastmoney_financial = {}
            if quote.get('pe_ttm'):
                eastmoney_financial['pe'] = quote['pe_ttm']
            elif quote.get('pe_dynamic'):
                eastmoney_financial['pe'] = quote['pe_dynamic']
            if quote.get('pb'):
                eastmoney_financial['pb'] = quote['pb']
            if quote.get('total_mv'):
                eastmoney_financial['total_mv'] = quote['total_mv']
            if quote.get('float_mv'):
                eastmoney_financial['float_mv'] = quote['float_mv']
            if quote.get('turnover_rate'):
                eastmoney_financial['turnover_rate'] = quote['turnover_rate']
            if eastmoney_financial:
                results['eastmoney'] = eastmoney_financial
                logger.info(f"东方财富成功获取 {stock_code} 财务指标")

        financial = self.get_financial_data_from_tushare(stock_code)
        if financial:
            results['tushare'] = financial
            logger.info(f"Tushare成功获取 {stock_code} 财务数据")

        return results

    def get_unified_quote(self, stock_code: str) -> Optional[Dict]:
        quotes = self.get_all_realtime_quotes(stock_code)

        if not quotes:
            return None

        unified = {
            'current_price': None,
            'open': None,
            'high': None,
            'low': None,
            'close': None,
            'volume': None,
            'amount': None,
            'pe': None,
            'pb': None,
            'total_mv': None,
            'float_mv': None,
            'pct_chg': None,
            'turnover_rate': None,
            'name': None,
            'sources': list(quotes.keys()),
        }

        for source in self.SOURCE_PRIORITY:
            if source not in quotes:
                continue
            data = quotes[source]
            field_map = self.FIELD_MAPS.get(source, {})
            for target_field, source_field in field_map.items():
                if source_field and unified.get(target_field) is None and data.get(source_field) is not None:
                    unified[target_field] = data[source_field]

        best = self._cross_validate_quotes(quotes)
        if best:
            price = best.get('current') or best.get('current_price')
            if price and (unified.get('current_price') is None or abs(price - unified['current_price']) / price > 0.02):
                unified['current_price'] = price
                unified['close'] = price

        logger.info(f"聚合行情数据成功: {unified['sources']}, 价格={unified.get('current_price')}")
        if self._xinhua:
            try:
                self._xinhua.close()
            except Exception:
                pass
        return unified

    def get_stock_data(self, stock_code: str) -> Optional[Dict]:
        """统一数据获取接口 - 整合 backup 内所有子源"""
        try:
            unified_quote = self.get_unified_quote(stock_code)
            stock_info = self.get_all_stock_info(stock_code)
            financial_data = self.get_all_financial_data(stock_code)

            if not unified_quote:
                return None

            result = dict(unified_quote)
            result['stock_code'] = stock_code
            result['source'] = 'backup'

            # 合并基本信息（取第一个成功源的 name/industry）
            if stock_info:
                for source, info in stock_info.items():
                    if info.get('name') and not result.get('stock_name'):
                        result['stock_name'] = info['name']
                    if info.get('industry') and not result.get('industry'):
                        result['industry'] = info['industry']

            # 合并财务数据（优先取第一个成功源）
            if financial_data:
                for source, fin in financial_data.items():
                    for key in ['pe', 'pb', 'total_mv', 'float_mv', 'turnover_rate',
                                'roe_history', 'net_profit_history', 'revenue_history']:
                        if fin.get(key) is not None and result.get(key) is None:
                            result[key] = fin[key]

            return result
        except Exception as e:
            logger.error(f"BackupDataFetcher 统一接口获取失败: {e}")
            return None

    def close(self):
        self._session.close()
        logger.info("备用数据源会话已关闭")


def test_backup_fetcher():
    fetcher = BackupDataFetcher()

    test_codes = ['600519', '000001', '603019']

    for code in test_codes:
        print(f"\n{'='*60}")
        print(f"测试股票: {code}")
        print('='*60)

        print("\n[1] 测试东方财富实时行情...")
        quote = fetcher.get_realtime_quote_from_eastmoney(code)
        if quote:
            print(f"  成功: 价格={quote.get('current')}, PE={quote.get('pe_ttm')}, PB={quote.get('pb')}")
            print(f"  总市值={quote.get('total_mv')}, 流通市值={quote.get('float_mv')}")
        else:
            print("  失败")

        print("\n[2] 测试腾讯财经实时行情...")
        quote = fetcher.get_realtime_quote_from_tencent(code)
        if quote:
            print(f"  成功: 价格={quote.get('current')}, PE={quote.get('pe')}")
        else:
            print("  失败")

        print("\n[3] 测试新浪财经...")
        sina_data = fetcher.get_stock_info_from_sina(code)
        if sina_data:
            print(f"  成功: 价格={sina_data.get('current')}, 名称={sina_data.get('name')}")
        else:
            print("  失败")

        print("\n[4] 测试聚合实时行情...")
        unified = fetcher.get_unified_quote(code)
        if unified:
            print(f"  成功: 价格={unified.get('current_price')}, PE={unified.get('pe')}, PB={unified.get('pb')}")
            print(f"  数据源: {unified.get('sources')}")
        else:
            print("  失败")

        print("\n[5] 测试东方财富K线...")
        kline = fetcher.get_kline_from_eastmoney(code, limit=5)
        if kline is not None:
            print(f"  成功: 获取 {len(kline)} 条K线")
            print(kline.tail(3))
        else:
            print("  失败")

        print("\n[6] 测试Tushare财务数据...")
        financial = fetcher.get_financial_data_from_tushare(code)
        if financial:
            print(f"  成功: ROE历史={financial.get('roe_history')}")
        else:
            print("  失败")

    fetcher.close()
    print("\n测试完成!")


if __name__ == '__main__':
    test_backup_fetcher()
