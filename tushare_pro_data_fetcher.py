import os
import time
import pandas as pd
from typing import Dict, Optional, List

from logger_config import logger
from config import SystemConfig, safe_float, to_tushare_code


class TushareProDataFetcher:
    """Tushare Pro 独立数据源获取器"""

    def __init__(self):
        self._pro = None
        self._retry_count = SystemConfig.RETRY_MAX
        self._retry_delay = SystemConfig.RETRY_DELAY
        self._last_request_time = 0
        self._min_request_interval = 0.5
        self._init_tushare()

    def _init_tushare(self):
        try:
            import tushare as ts
            self._ts = ts
            token = os.getenv('TUSHARE_TOKEN')
            proxy_url = os.getenv('TUSHARE_PROXY_URL', 'http://118.89.66.41:8010/')
            if token:
                ts.set_token(token)
                self._pro = ts.pro_api()
                # 必须设置代理地址，否则 Token 可能验证失败
                if hasattr(self._pro, '_DataApi__http_url'):
                    self._pro._DataApi__http_url = proxy_url
                logger.info(f"Tushare Pro 初始化成功，代理地址: {proxy_url}")
            else:
                logger.warning("未设置 TUSHARE_TOKEN，Tushare Pro 数据源不可用")
        except Exception as e:
            logger.warning(f"Tushare Pro 初始化失败: {e}")

    def _safe_call(self, func, *args, **kwargs):
        """带重试和频率控制的 Tushare API 调用"""
        if not self._pro:
            return None

        for attempt in range(self._retry_count):
            try:
                elapsed = time.time() - self._last_request_time
                if elapsed < self._min_request_interval:
                    time.sleep(self._min_request_interval - elapsed)
                self._last_request_time = time.time()

                result = func(*args, **kwargs)
                if result is not None and not (isinstance(result, pd.DataFrame) and result.empty):
                    return result
            except Exception as e:
                logger.warning(f"Tushare Pro 调用失败 (尝试 {attempt + 1}/{self._retry_count}): {e}")
                if attempt < self._retry_count - 1:
                    time.sleep(self._retry_delay * (attempt + 1))
        return None

    def get_stock_info(self, stock_code: str) -> Optional[Dict]:
        if not self._pro:
            return None

        try:
            ts_code = to_tushare_code(stock_code)
            df = self._safe_call(
                self._pro.stock_basic,
                ts_code=ts_code,
                fields='ts_code,symbol,name,area,industry,list_date,market,exchange'
            )
            if df is not None and not df.empty:
                row = df.iloc[0]
                return {
                    'stock_name': row.get('name'),
                    'industry': row.get('industry'),
                    'area': row.get('area'),
                    'list_date': row.get('list_date'),
                    'market': row.get('market'),
                }
        except Exception as e:
            logger.warning(f"Tushare Pro 基本信息获取失败: {e}")
        return None

    def get_realtime_quote(self, stock_code: str) -> Optional[Dict]:
        if not self._pro:
            return None

        ts_code = to_tushare_code(stock_code)
        result = {}

        # 1. 实时行情
        try:
            df = self._safe_call(
                self._pro.realtime_quote,
                ts_code=ts_code
            )
            if df is not None and not df.empty:
                row = df.iloc[0]
                result.update({
                    'current_price': safe_float(row.get('price')),
                    'open': safe_float(row.get('open')),
                    'high': safe_float(row.get('high')),
                    'low': safe_float(row.get('low')),
                    'close': safe_float(row.get('price')),
                    'volume': safe_float(row.get('volume')),
                    'amount': safe_float(row.get('amount')),
                    'pct_chg': safe_float(row.get('pct_chg')),
                })
        except Exception as e:
            logger.warning(f"Tushare Pro 实时行情获取失败: {e}")

        # 2. 每日指标（补充 PE/PB/市值等）
        try:
            trade_date = pd.Timestamp.now().strftime('%Y%m%d')
            for offset in [0, -1, -2]:
                check_date = (pd.Timestamp.now() + pd.Timedelta(days=offset)).strftime('%Y%m%d')
                df = self._safe_call(
                    self._pro.daily_basic,
                    ts_code=ts_code,
                    trade_date=check_date,
                    fields='ts_code,trade_date,close,turnover_rate,pe,pb,ps,dv_ratio,total_mv,circ_mv'
                )
                if df is not None and not df.empty:
                    row = df.iloc[0]
                    close_price = safe_float(row.get('close'))
                    result.update({
                        'current_price': result.get('current_price') or close_price,
                        'close': result.get('close') or close_price,
                        'pe': safe_float(row.get('pe')),
                        'pb': safe_float(row.get('pb')),
                        'ps': safe_float(row.get('ps')),
                        'dividend_yield': safe_float(row.get('dv_ratio')),
                        'total_mv': safe_float(row.get('total_mv')),
                        'float_mv': safe_float(row.get('circ_mv')),
                        'turnover_rate': safe_float(row.get('turnover_rate')),
                    })
                    break
        except Exception as e:
            logger.warning(f"Tushare Pro 每日指标获取失败: {e}")

        if result:
            logger.info(
                f"Tushare Pro 行情 - {stock_code}: "
                f"价格={result.get('current_price')}, PE={result.get('pe')}, PB={result.get('pb')}"
            )
            return result
        return None

    def get_financial_data(self, stock_code: str) -> Optional[Dict]:
        if not self._pro:
            return None

        ts_code = to_tushare_code(stock_code)
        result = {
            'pe': None,
            'pb': None,
            'roe': None,
            'total_mv': None,
            'float_mv': None,
            'eps': None,
            'current_price': None,
            'dividend_yield': None,
            'gross_margin': None,
            'net_margin': None,
            'revenue': None,
            'net_profit': None,
        }

        # 1. 财务指标（ROE 等）
        try:
            df = self._safe_call(
                self._pro.fina_indicator,
                ts_code=ts_code,
                fields='ts_code,ann_date,roe,net_profit_ratio,gross_profit_margin,debt_to_assets'
            )
            if df is not None and not df.empty:
                latest = df.iloc[0]
                roe_val = safe_float(latest.get('roe'))
                if roe_val is not None:
                    result['roe'] = roe_val
                result['net_margin'] = safe_float(latest.get('net_profit_ratio'))
                result['gross_margin'] = safe_float(latest.get('gross_profit_margin'))
        except Exception as e:
            logger.warning(f"Tushare Pro 财务指标获取失败: {e}")

        # 2. 利润表（营收/净利润）
        try:
            df = self._safe_call(
                self._pro.income,
                ts_code=ts_code,
                fields='ts_code,ann_date,total_revenue,n_income'
            )
            if df is not None and not df.empty:
                latest = df.iloc[0]
                result['revenue'] = safe_float(latest.get('total_revenue'))
                result['net_profit'] = safe_float(latest.get('n_income'))
        except Exception as e:
            logger.warning(f"Tushare Pro 利润表获取失败: {e}")

        # 3. 每日指标（补充估值数据）
        try:
            trade_date = pd.Timestamp.now().strftime('%Y%m%d')
            for offset in [0, -1, -2]:
                check_date = (pd.Timestamp.now() + pd.Timedelta(days=offset)).strftime('%Y%m%d')
                df = self._safe_call(
                    self._pro.daily_basic,
                    ts_code=ts_code,
                    trade_date=check_date,
                    fields='ts_code,trade_date,close,pe,pb,total_mv,circ_mv'
                )
                if df is not None and not df.empty:
                    row = df.iloc[0]
                    result['current_price'] = safe_float(row.get('close'))
                    result['pe'] = safe_float(row.get('pe'))
                    result['pb'] = safe_float(row.get('pb'))
                    result['total_mv'] = safe_float(row.get('total_mv'))
                    result['float_mv'] = safe_float(row.get('circ_mv'))
                    break
        except Exception as e:
            logger.warning(f"Tushare Pro 每日指标(财务补充)获取失败: {e}")

        has_data = any(v is not None for v in result.values())
        if has_data:
            logger.info(
                f"Tushare Pro 财务数据 - {stock_code}: "
                f"ROE={result.get('roe')}, PE={result.get('pe')}, PB={result.get('pb')}"
            )
            return result
        return None

    def get_historical_financials(self, stock_code: str, years: int = 5) -> Optional[Dict]:
        """获取历史财务数据（ROE、营收、净利润历史）"""
        if not self._pro:
            return None

        ts_code = to_tushare_code(stock_code)
        result = {
            'roe_history': [],
            'earnings_history': [],
            'revenue_history': [],
            'fcf_history': [],
            'dividend_history': [],
        }

        try:
            df = self._safe_call(
                self._pro.fina_indicator,
                ts_code=ts_code,
                fields='ts_code,ann_date,roe'
            )
            if df is not None and not df.empty:
                for _, row in df.head(years).iterrows():
                    roe = safe_float(row.get('roe'))
                    if roe is not None and -500 < roe < 500:
                        result['roe_history'].append(roe)
        except Exception as e:
            logger.warning(f"Tushare Pro 历史ROE获取失败: {e}")

        try:
            df = self._safe_call(
                self._pro.income,
                ts_code=ts_code,
                fields='ts_code,ann_date,total_revenue,n_income'
            )
            if df is not None and not df.empty:
                for _, row in df.head(years).iterrows():
                    rev = safe_float(row.get('total_revenue'))
                    profit = safe_float(row.get('n_income'))
                    if rev is not None:
                        result['revenue_history'].append(rev)
                    if profit is not None:
                        result['earnings_history'].append(profit)
        except Exception as e:
            logger.warning(f"Tushare Pro 历史营收获取失败: {e}")

        logger.info(
            f"Tushare Pro 历史数据 - {stock_code}: "
            f"ROE={len(result['roe_history'])}年, 营收={len(result['revenue_history'])}年"
        )
        return result

    def get_historical_data(self, stock_code: str, period: str = '1y') -> Optional[pd.DataFrame]:
        """获取日线历史数据（复权），供技术分析使用"""
        if not self._pro:
            return None

        ts_code = to_tushare_code(stock_code)
        try:
            end_date = pd.Timestamp.now()
            if period == '1y':
                start_date = end_date - pd.DateOffset(years=1)
            elif period == '2y':
                start_date = end_date - pd.DateOffset(years=2)
            elif period == '3y':
                start_date = end_date - pd.DateOffset(years=3)
            else:
                start_date = end_date - pd.DateOffset(years=1)

            df = self._safe_call(
                self._ts.pro_bar,
                api=self._pro,
                ts_code=ts_code,
                adj='qfq',
                start_date=start_date.strftime('%Y%m%d'),
                end_date=end_date.strftime('%Y%m%d')
            )
            if df is not None and not df.empty:
                df['trade_date'] = pd.to_datetime(df['trade_date'])
                df.set_index('trade_date', inplace=True)
                df.sort_index(inplace=True)

                # 统一列名，适配 technical_analyzer
                col_map = {
                    'open': 'Open',
                    'high': 'High',
                    'low': 'Low',
                    'close': 'Close',
                    'vol': 'Volume',
                    'amount': 'Amount',
                }
                df.rename(columns={k: v for k, v in col_map.items() if k in df.columns}, inplace=True)
                logger.info(f"Tushare Pro 成功获取 {stock_code} 历史数据，共 {len(df)} 条")
                return df
        except Exception as e:
            logger.warning(f"Tushare Pro 历史数据获取失败: {e}")
        return None

    def get_stock_data(self, stock_code: str) -> Optional[Dict]:
        """统一数据获取接口 - 整合 Tushare Pro 所有数据源"""
        try:
            stock_info = self.get_stock_info(stock_code)
            realtime_quote = self.get_realtime_quote(stock_code)
            financial_data = self.get_financial_data(stock_code)
            historical_financials = self.get_historical_financials(stock_code)
            historical_data = self.get_historical_data(stock_code)

            if not realtime_quote and not financial_data:
                return None

            result = {}

            if stock_info:
                for key, val in stock_info.items():
                    if val is not None:
                        result[key] = val

            if realtime_quote:
                for key, val in realtime_quote.items():
                    if val is not None:
                        result[key] = val

            if financial_data:
                for key, val in financial_data.items():
                    if val is not None and key not in result:
                        result[key] = val

            if historical_financials:
                for key, val in historical_financials.items():
                    if val is not None and key not in result:
                        result[key] = val

            if historical_data is not None and not historical_data.empty:
                result['historical'] = historical_data

            result['stock_code'] = stock_code
            result['source'] = 'tushare'
            return result
        except Exception as e:
            logger.error(f"Tushare Pro 统一接口获取失败: {e}")
            return None

    def close(self):
        logger.info("Tushare Pro 数据源会话已关闭")


if __name__ == '__main__':
    fetcher = TushareProDataFetcher()
    test_codes = ['600519', '000001', '601318']

    for code in test_codes:
        print(f"\n{'='*60}")
        print(f"测试股票: {code}")
        print('='*60)

        print("\n[1] 测试股票基本信息...")
        info = fetcher.get_stock_info(code)
        if info:
            print(f"  成功: 名称={info.get('stock_name')}, 行业={info.get('industry')}")
        else:
            print("  失败")

        print("\n[2] 测试实时行情...")
        quote = fetcher.get_realtime_quote(code)
        if quote:
            print(f"  成功: 价格={quote.get('current_price')}, PE={quote.get('pe')}, PB={quote.get('pb')}")
            print(f"  市值={quote.get('total_mv')}, 股息率={quote.get('dividend_yield')}")
        else:
            print("  失败")

        print("\n[3] 测试财务数据...")
        financial = fetcher.get_financial_data(code)
        if financial:
            print(f"  成功: ROE={financial.get('roe')}, PE={financial.get('pe')}, EPS={financial.get('eps')}")
            print(f"  营收={financial.get('revenue')}, 净利润={financial.get('net_profit')}")
        else:
            print("  失败")

        print("\n[4] 测试历史财务...")
        hist = fetcher.get_historical_financials(code)
        if hist:
            print(f"  ROE历史: {hist.get('roe_history')}")
            print(f"  营收历史: {hist.get('revenue_history')}")
        else:
            print("  失败")

        print("\n[5] 测试统一接口...")
        data = fetcher.get_stock_data(code)
        if data:
            print(f"  成功: 数据源={data.get('source')}, 价格={data.get('current_price')}, PE={data.get('pe')}")
        else:
            print("  失败")

    fetcher.close()
    print("\n测试完成!")
