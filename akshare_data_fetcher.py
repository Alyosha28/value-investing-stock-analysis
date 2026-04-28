import pandas as pd
import time
import logging
from typing import Dict, Optional, List
from logger_config import logger
from config import SystemConfig, safe_float, to_sina_code

class AkshareDataFetcher:
    def __init__(self):
        self._ak = None
        self._retry_count = SystemConfig.RETRY_MAX
        self._retry_delay = SystemConfig.RETRY_DELAY
        self._cached_spot_df = None
        self._cache_timestamp = 0
        self._cache_ttl = 100  # 缓存5分钟，避免频繁请求接口
        self._init_akshare()
    
    def _init_akshare(self):
        try:
            import akshare as ak
            self._ak = ak
            logger.info("akshare 初始化成功")
        except Exception as e:
            logger.error(f"akshare 初始化失败: {e}")
    
    def _retry_on_failure(self, func, *args, **kwargs):
        last_error = None
        for attempt in range(self._retry_count):
            try:
                result = func(*args, **kwargs)
                if result is not None:
                    return result
                last_error = f"{func.__name__} 返回空数据"
            except Exception as e:
                last_error = str(e)
                logger.warning(f"{func.__name__} 第 {attempt + 1} 次尝试失败: {last_error}")
            
            if attempt < self._retry_count - 1:
                time.sleep(self._retry_delay)
        
        logger.warning(f"{func.__name__} 重试 {self._retry_count} 次后仍然失败")
        return None
    
    def get_extended_info(self, stock_code: str) -> Optional[Dict]:
        if not self._ak:
            return None
        
        result = {
            'revenue': None,
            'total_share': None,
            'float_share': None,
            'dividend_yield': None,
            'industry': None,
            'listing_date': None
        }
        
        # 快速跳过容易失败的接口，避免浪费时间
        try:
            # 只尝试1次，不重试，避免触发反爬
            info_df = self._ak.stock_individual_info_em(symbol=stock_code)
            if info_df is not None and not info_df.empty:
                info_dict = dict(zip(info_df['item'], info_df['value']))
                
                result['total_share'] = safe_float(info_dict.get('总股本'))
                result['float_share'] = safe_float(info_dict.get('流通股'))
                result['industry'] = info_dict.get('行业')
                result['listing_date'] = info_dict.get('上市时间')
                result['dividend_yield'] = safe_float(info_dict.get('股息率') or info_dict.get('股息率(TTM)'))
                
        except Exception as e:
            logger.warning(f"akshare 个股信息获取失败: {e}")
        
        # 分红数据也暂时跳过，避免更多接口调用
        logger.debug(f"跳过分红数据获取，避免接口调用过多")
        
        return result if any(v is not None for v in result.values()) else None
    
    def get_income_data(self, stock_code: str) -> Optional[Dict]:
        if not self._ak:
            return None
        
        try:
            income_df = self._retry_on_failure(self._ak.stock_financial_report_sina, stock=stock_code, symbol='利润表')
            if income_df is not None and not income_df.empty:
                latest = income_df.iloc[0]
                
                result = {
                    'revenue': safe_float(latest.get('营业总收入')) or safe_float(latest.get('营业收入')),
                    'operating_income': safe_float(latest.get('营业总收入')),
                    'net_profit': safe_float(latest.get('净利润')) or safe_float(latest.get('归属母公司股东的净利润')),
                    'operating_profit': safe_float(latest.get('营业利润')),
                    'gross_margin': None,
                    'net_margin': None,
                    'report_date': str(latest.get('报告日', ''))
                }
                
                if result['revenue'] and result['net_profit']:
                    if result['operating_income']:
                        result['gross_margin'] = round((result['operating_income'] - safe_float(latest.get('营业成本')) or 0) / result['operating_income'] * 100, 2) if result['operating_income'] else None
                    result['net_margin'] = round(result['net_profit'] / result['revenue'] * 100, 2) if result['revenue'] else None
                
                logger.info(f"akshare 营收数据 - 营收: {result.get('revenue')}, 净利润: {result.get('net_profit')}")
                return result
                
        except Exception as e:
            logger.warning(f"akshare 利润表获取失败: {e}")
        
        return None
    
    def get_cashflow_data(self, stock_code: str) -> Optional[Dict]:
        if not self._ak:
            return None
        
        try:
            cashflow_df = self._retry_on_failure(self._ak.stock_financial_report_sina, stock=stock_code, symbol='现金流量表')
            if cashflow_df is not None and not cashflow_df.empty:
                latest = cashflow_df.iloc[0]
                
                result = {
                    'operating_cashflow': safe_float(latest.get('经营活动产生的现金流量净额')),
                    'investing_cashflow': safe_float(latest.get('投资活动产生的现金流量净额')),
                    'financing_cashflow': safe_float(latest.get('筹资活动产生的现金流量净额')),
                    'cash_change': safe_float(latest.get('现金及现金等价物净增加额')),
                    'free_cashflow': None
                }
                
                if result['operating_cashflow']:
                    capex = abs(safe_float(latest.get('购建固定资产、无形资产和其他长期资产支付的现金')) or 0)
                    result['free_cashflow'] = result['operating_cashflow'] - capex
                    logger.info(f"akshare 现金流 - 经营: {result['operating_cashflow']}, 自由: {result['free_cashflow']}")
                
                return result
                
        except Exception as e:
            logger.warning(f"akshare 现金流量表获取失败: {e}")
        
        return None
    
    def get_balance_sheet(self, stock_code: str) -> Optional[Dict]:
        if not self._ak:
            return None

        try:
            balance_df = self._retry_on_failure(self._ak.stock_financial_report_sina, stock=stock_code, symbol='资产负债表')
            if balance_df is not None and not balance_df.empty:
                latest = balance_df.iloc[0]

                def _bs(key):
                    return safe_float(latest.get(key))

                total_assets = _bs('资产总计')
                total_liabilities = _bs('负债合计')
                current_assets = _bs('流动资产合计')
                current_liabilities = _bs('流动负债合计')
                cash = _bs('货币资金')
                total_equity = _bs('股东权益合计') or (total_assets - total_liabilities if total_assets and total_liabilities else None)

                short_term_debt = _bs('短期借款') or 0
                long_term_debt = _bs('长期借款') or 0
                total_debt = short_term_debt + long_term_debt

                # ── 财报解读专家所需扩展字段 ──
                accounts_receivable = _bs('应收账款')          # 收入质量、提前确认收入红旗
                inventory = _bs('存货')                        # 存货周转率趋势
                goodwill = _bs('商誉')                         # 商誉/净资产>30%红旗
                fixed_assets = _bs('固定资产')                 # 固定资产增长vs折旧
                intangible_assets = _bs('无形资产')            # 资本化分析
                advances_from_customers = _bs('预收账款')       # 预收收入（收入质量）
                accounts_payable = _bs('应付账款')             # 供应商关系
                deferred_revenue = _bs('递延收益')             # 递延收入
                construction_in_progress = _bs('在建工程')      # 资本化可能

                result = {
                    'total_assets': total_assets,
                    'total_liabilities': total_liabilities,
                    'current_assets': current_assets,
                    'current_liabilities': current_liabilities,
                    'cash_and_equivalents': cash,
                    'total_equity': total_equity,
                    'total_debt': total_debt,
                    'short_term_debt': short_term_debt,
                    'long_term_debt': long_term_debt,
                    'accounts_receivable': accounts_receivable,      # NEW
                    'inventory': inventory,                          # NEW
                    'goodwill': goodwill,                            # NEW
                    'fixed_assets': fixed_assets,                    # NEW
                    'intangible_assets': intangible_assets,          # NEW
                    'advances_from_customers': advances_from_customers,
                    'accounts_payable': accounts_payable,
                }

                if current_assets and current_liabilities and current_liabilities > 0:
                    result['current_ratio'] = round(current_assets / current_liabilities, 2)
                if total_equity and total_equity > 0 and total_debt:
                    result['debt_to_equity'] = round(total_debt / total_equity, 2)
                if total_assets and total_liabilities:
                    result['asset_liability_ratio'] = round(total_liabilities / total_assets * 100, 2)
                if total_assets and goodwill:
                    result['goodwill_to_assets'] = round(goodwill / total_assets * 100, 2)

                logger.info(f"akshare 资产负债表 - 总资产:{total_assets/1e8 if total_assets else 0:.0f}亿")
                return result

        except Exception as e:
            logger.warning(f"akshare 资产负债表获取失败: {e}")

        return None

    def get_financial_reports_history(self, stock_code: str, periods: int = 5) -> Optional[Dict[str, list]]:
        """获取多期财务报表序列（利润表+资产负债表+现金流量表），用于三表交叉验证。
        返回 { 'income': [{...}], 'balance': [{...}], 'cashflow': [{...}] }，按时间倒序排列。
        """
        if not self._ak:
            return None

        result = {}
        for report_name, symbol, key_fields in [
            ('income', '利润表', ['营业总收入', '营业收入', '净利润', '归属母公司股东的净利润',
                                  '营业利润', '营业成本', '非经常性损益']),
            ('cashflow', '现金流量表', ['经营活动产生的现金流量净额', '投资活动产生的现金流量净额',
                                       '筹资活动产生的现金流量净额', '现金及现金等价物净增加额',
                                       '购建固定资产、无形资产和其他长期资产支付的现金']),
            ('balance', '资产负债表', ['资产总计', '负债合计', '流动资产合计', '流动负债合计',
                                       '货币资金', '股东权益合计', '短期借款', '长期借款',
                                       '应收账款', '存货', '商誉', '固定资产', '无形资产', '预收账款',
                                       '应付账款']),
        ]:
            try:
                df = self._retry_on_failure(self._ak.stock_financial_report_sina, stock=stock_code, symbol=symbol)
                if df is not None and not df.empty:
                    reports = []
                    for i in range(min(periods, len(df))):
                        row = df.iloc[i]
                        entry = {k: safe_float(row.get(k)) for k in key_fields if row.get(k) is not None}
                        entry['report_date'] = str(row.get('报告日', ''))
                        reports.append(entry)
                    result[report_name] = reports
                    logger.info(f"akshare {symbol} 多期数据获取成功，{len(reports)}期")
                else:
                    result[report_name] = []
            except Exception as e:
                logger.warning(f"akshare {symbol} 多期获取失败: {e}")
                result[report_name] = []

        if any(v for v in result.values()):
            return result
        return None

    def get_realtime_quote(self, stock_code: str) -> Optional[Dict]:
        if not self._ak:
            return None

        # 尝试从缓存获取，减少接口调用次数
        import time
        now = time.time()
        if self._cached_spot_df is not None and (now - self._cache_timestamp) < self._cache_ttl:
            try:
                row = self._cached_spot_df[self._cached_spot_df['代码'] == stock_code]
                if not row.empty:
                    r = row.iloc[0]
                    result = {
                        'current_price': safe_float(r.get('最新价')),
                        'pe': safe_float(r.get('市盈率')),
                        'pb': safe_float(r.get('市净率')),
                        'total_mv': safe_float(r.get('总市值')),
                        'float_mv': safe_float(r.get('流通市值')),
                        'total_share': safe_float(r.get('总股本')),
                        'float_share': safe_float(r.get('流通股')),
                        'eps': None,
                    }
                    logger.info(f"akshare(cache)行情 - 价格:{result['current_price']}, PE:{result['pe']}")
                    return result
            except Exception as e:
                logger.debug(f"从缓存获取行情失败: {e}")

        # 优先使用 stock_zh_a_spot_em（带缓存机制）
        try:
            df = self._retry_on_failure(self._ak.stock_zh_a_spot_em)
            if df is not None and not df.empty:
                # 更新缓存
                self._cached_spot_df = df
                self._cache_timestamp = now
                
                row = df[df['代码'] == stock_code]
                if not row.empty:
                    r = row.iloc[0]
                    result = {
                        'current_price': safe_float(r.get('最新价')),
                        'pe': safe_float(r.get('市盈率')),
                        'pb': safe_float(r.get('市净率')),
                        'total_mv': safe_float(r.get('总市值')),
                        'float_mv': safe_float(r.get('流通市值')),
                        'total_share': safe_float(r.get('总股本')),
                        'float_share': safe_float(r.get('流通股')),
                        'eps': None,
                    }
                    logger.info(f"akshare(spot_em)行情 - 价格:{result['current_price']}, PE:{result['pe']}")
                    return result
        except Exception as e:
            logger.warning(f"akshare spot_em 行情获取失败: {e}")

        # 完全跳过 stock_zh_a_spot_em 的重试，避免触发反爬
        logger.warning(f"spot_em 不可用，跳过该接口")
        return None
    
    def get_historical_data(self, stock_code: str, period: str = '1y') -> Optional[pd.DataFrame]:
        if not self._ak:
            return None
        
        try:
            bs_code = to_sina_code(stock_code)
            daily_df = self._retry_on_failure(self._ak.stock_zh_a_daily, symbol=bs_code, adjust='qfq')
            if daily_df is not None and not daily_df.empty:
                date_col = 'date' if 'date' in daily_df.columns else 'trade_date'
                daily_df['Date'] = pd.to_datetime(daily_df[date_col])
                daily_df.set_index('Date', inplace=True)
                
                col_map = {col: col.title() for col in daily_df.columns 
                          if col.lower() in ['open', 'high', 'low', 'close', 'volume', 'amount']}
                daily_df.rename(columns=col_map, inplace=True)
                
                logger.info(f"akshare 成功获取股票 {stock_code} 的历史数据，共 {len(daily_df)} 条")
                return daily_df
        except Exception as e:
            logger.warning(f"akshare 获取历史数据失败: {e}")
        
        return None
    
    def get_historical_financials(self, stock_code: str, years: int = 5) -> Optional[Dict]:
        if not self._ak:
            return None
        
        result = {
            'roe_history': [],
            'earnings_history': [],
            'fcf_history': [],
            'dividend_history': [],
            'revenue_history': []
        }
        
        try:
            key_data_df = self._retry_on_failure(self._ak.stock_financial_analysis_indicator, symbol=stock_code)
            if key_data_df is not None and not key_data_df.empty:
                for _, row in key_data_df.head(years).iterrows():
                    roe = safe_float(row.get('加权净资产收益率(%)'))
                    if roe is not None and -500 < roe < 500:
                        result['roe_history'].append(roe)
                    
                    net_profit = safe_float(row.get('净利润(元)'))
                    if net_profit is not None:
                        result['earnings_history'].append(net_profit)
                    
                    revenue = safe_float(row.get('营业总收入(元)'))
                    if revenue is not None:
                        result['revenue_history'].append(revenue)
                        
        except Exception as e:
            logger.warning(f"akshare 财务指标历史获取失败: {e}")
        
        try:
            dividend_df = self._retry_on_failure(self._ak.stock_history_dividend_detail, symbol=stock_code)
            if dividend_df is not None and not dividend_df.empty:
                for _, row in dividend_df.head(years).iterrows():
                    dividend = safe_float(row.get('派息'))
                    if dividend is not None:
                        result['dividend_history'].append(dividend / 10)
                        
        except Exception as e:
            logger.warning(f"akshare 分红历史获取失败: {e}")
        
        try:
            for _ in range(min(years, 5)):
                cashflow_df = self._ak.stock_financial_report_sina(stock=stock_code, symbol='现金流量表')
                if cashflow_df is not None and not cashflow_df.empty and len(cashflow_df) > _:
                    row = cashflow_df.iloc[_]
                    op_cf = safe_float(row.get('经营活动产生的现金流量净额'))
                    capex = abs(safe_float(row.get('购建固定资产、无形资产和其他长期资产支付的现金')) or 0)
                    if op_cf is not None:
                        result['fcf_history'].append(op_cf - capex)
                        
        except Exception as e:
            logger.warning(f"akshare 历史现金流获取失败: {e}")
        
        logger.info(f"akshare 历史数据 - ROE:{len(result['roe_history'])}年, FCF:{len(result['fcf_history'])}年")
        return result
    
    def _get_current_price(self, stock_code: str) -> Optional[float]:
        if not self._ak:
            return None
        
        # 优先从缓存获取
        import time
        now = time.time()
        if self._cached_spot_df is not None and (now - self._cache_timestamp) < self._cache_ttl:
            try:
                stock_row = self._cached_spot_df[self._cached_spot_df['代码'] == stock_code]
                if not stock_row.empty:
                    return safe_float(stock_row.iloc[0].get('最新价'))
            except Exception:
                pass
        
        # 如果没有缓存，就不调用接口了，避免触发反爬
        return None

    def get_stock_data(self, stock_code: str) -> Optional[Dict]:
        """统一数据获取接口，聚合各类数据"""
        if not self._ak:
            return None

        result = {'stock_code': stock_code}

        # 实时行情
        quote = self.get_realtime_quote(stock_code)
        if quote:
            result.update(quote)

        # 扩展信息
        info = self.get_extended_info(stock_code)
        if info:
            result.update(info)

        # 利润表
        income = self.get_income_data(stock_code)
        if income:
            result.update(income)

        # 资产负债表
        balance = self.get_balance_sheet(stock_code)
        if balance:
            result.update(balance)

        # 现金流
        cashflow = self.get_cashflow_data(stock_code)
        if cashflow:
            result.update(cashflow)

        # 历史财务
        hist = self.get_historical_financials(stock_code)
        if hist:
            result.update(hist)

        # 历史行情
        historical = self.get_historical_data(stock_code)
        if historical is not None and not historical.empty:
            result['historical'] = historical

        has_any_data = (
            result.get('current_price') is not None
            or result.get('pe') is not None
            or result.get('pb') is not None
            or result.get('free_cashflow') is not None
            or result.get('current_ratio') is not None
            or result.get('total_share') is not None
            or result.get('operating_cashflow') is not None
        )
        return result if has_any_data else None

if __name__ == '__main__':
    fetcher = AkshareDataFetcher()
    
    for code in ['002261', '600406']:
        print(f"\n测试股票: {code}")
        
        info = fetcher.get_extended_info(code)
        if info:
            print(f"总股本: {info.get('total_share')}, 流通股: {info.get('float_share')}")
            print(f"行业: {info.get('industry')}, 股息率: {info.get('dividend_yield')}")
        
        income = fetcher.get_income_data(code)
        if income:
            print(f"营收: {income.get('revenue')}, 净利润: {income.get('net_profit')}")
        
        cashflow = fetcher.get_cashflow_data(code)
        if cashflow:
            print(f"经营现金流: {cashflow.get('operating_cashflow')}, 自由现金流: {cashflow.get('free_cashflow')}")
        
        balance = fetcher.get_balance_sheet(code)
        if balance:
            print(f"总资产: {balance.get('total_assets')}, 流动比率: {balance.get('current_ratio')}, 负债率: {balance.get('asset_liability_ratio')}%")
        
        historical = fetcher.get_historical_financials(code)
        if historical:
            print(f"ROE历史: {historical.get('roe_history')}")
            print(f"分红历史: {historical.get('dividend_history')}")
