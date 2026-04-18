import pandas as pd
import logging
import os
import re
import requests
import time
from typing import Dict, Optional, List
from functools import lru_cache
from config import DataConfig, SystemConfig, safe_float, normalize_stock_code, to_sina_code, to_tushare_code
from logger_config import logger

class UltimateDataFetcher:
    _instance = None
    _init_flag = False
    
    def __new__(cls, *args, **kwargs):
        if cls._instance is None:
            cls._instance = super().__new__(cls)
        return cls._instance
    
    def __init__(self):
        if UltimateDataFetcher._init_flag:
            return
        UltimateDataFetcher._init_flag = True
        self._tf = None
        self._tavily = None
        self._ak = None
        self._backup = None
        self._validation_cache = {}
        self._init_data_sources()
    
    @classmethod
    def reset(cls):
        cls._instance = None
        cls._init_flag = False
    
    def _init_data_sources(self):
        try:
            from tickflow import TickFlow
            api_key = os.getenv('TICKFLOW_API_KEY')
            if api_key:
                self._tf = TickFlow(api_key=api_key)
                logger.info("TickFlow 初始化成功")
            else:
                logger.warning("未找到 TICKFLOW_API_KEY")
        except Exception as e:
            logger.error(f"TickFlow 初始化失败: {e}")
        
        try:
            from tavily import TavilyClient
            api_key = os.getenv('TAVILY_API_KEY')
            if api_key:
                self._tavily = TavilyClient(api_key=api_key)
                logger.info("Tavily 初始化成功")
            else:
                logger.warning("未找到 TAVILY_API_KEY")
        except Exception as e:
            logger.error(f"Tavily 初始化失败: {e}")
        
        try:
            from akshare_data_fetcher import AkshareDataFetcher
            self._ak = AkshareDataFetcher()
            logger.info("akshare 数据获取器初始化成功")
        except Exception as e:
            logger.error(f"akshare 数据获取器初始化失败: {e}")
        
        try:
            from backup_data_fetcher import BackupDataFetcher
            self._backup = BackupDataFetcher()
            logger.info("备用数据源初始化成功")
        except Exception as e:
            logger.warning(f"备用数据源初始化失败: {e}")
    
    def get_stock_data(self, stock_code: str, period: str = '1y') -> Optional[Dict]:
        try:
            logger.info(f"开始获取股票 {stock_code} 的数据...")
            
            info = self.get_stock_info(stock_code)
            financial = self.get_financial_data(stock_code)
            historical = self.get_historical_data(stock_code, period)
            
            data = {
                'info': info,
                'financial': financial,
                'historical': historical,
                'dividend': None
            }
            
            data['data_quality'] = self._evaluate_data_quality(data)
            
            data['data_validation'] = self._validate_data_with_web_search(data, stock_code)
            
            return data
            
        except Exception as e:
            logger.error(f"获取股票 {stock_code} 数据失败: {e}")
            return None
    
    def get_stock_info(self, stock_code: str) -> Optional[Dict]:
        default_info = {
            'stock_code': stock_code,
            'stock_name': f'股票{stock_code}',
            'industry': '',
            'market': 'A股',
            'list_date': '',
            'total_share': 0,
            'float_share': 0
        }
        
        if self._tf:
            try:
                ts_code = normalize_stock_code(stock_code)
                quote = self._tf.quotes.get(symbols=[ts_code])
                if quote and len(quote) > 0:
                    ext = quote[0].get('ext', {})
                    if default_info['stock_name'].startswith('股票'):
                        default_info['stock_name'] = ext.get('name', default_info['stock_name'])
            except Exception as e:
                logger.warning(f"TickFlow 获取股票信息失败: {e}")
        
        if self._ak:
            try:
                extended_info = self._ak.get_extended_info(stock_code)
                if extended_info:
                    if extended_info.get('industry'):
                        default_info['industry'] = extended_info['industry']
                    if extended_info.get('listing_date'):
                        default_info['list_date'] = extended_info['listing_date']
                    if extended_info.get('total_share'):
                        default_info['total_share'] = extended_info['total_share']
                    if extended_info.get('float_share'):
                        default_info['float_share'] = extended_info['float_share']
            except Exception as e:
                logger.warning(f"akshare 获取股票信息失败: {e}")
        
        if not default_info.get('industry') or not default_info.get('list_date'):
            if self._backup:
                try:
                    backup_info = self._backup.get_all_stock_info(stock_code)
                    if backup_info:
                        if backup_info.get('eastmoney'):
                            em_info = backup_info['eastmoney']
                            if not default_info.get('stock_name') and em_info.get('name'):
                                default_info['stock_name'] = em_info['name']
                        if backup_info.get('tushare'):
                            ts_info = backup_info['tushare']
                            if not default_info.get('industry') and ts_info.get('industry'):
                                default_info['industry'] = ts_info['industry']
                            if not default_info.get('list_date') and ts_info.get('list_date'):
                                default_info['list_date'] = ts_info['list_date']
                            logger.info(f"备用数据源获取基本信息 - 行业: {ts_info.get('industry')}")
                except Exception as e:
                    logger.warning(f"备用数据源获取股票信息失败: {e}")
        
        return default_info
    
    def get_financial_data(self, stock_code: str) -> Optional[Dict]:
        financial_data = self._create_empty_financial_data()
        
        self._fill_from_tickflow(stock_code, financial_data)
        self._fill_from_akshare(stock_code, financial_data)
        self._fill_from_tavily(stock_code, financial_data)
        self._fill_from_backup(stock_code, financial_data)
        
        if not financial_data['eps'] and financial_data['net_profit'] and financial_data.get('total_share'):
            total_share = financial_data.get('total_share')
            if total_share and total_share > 0:
                financial_data['eps'] = round(financial_data['net_profit'] / total_share, 2)
        
        return financial_data
    
    def _create_empty_financial_data(self) -> Dict:
        return {
            'pe': None, 'pb': None, 'total_mv': None, 'float_mv': None,
            'roe': None, 'net_profit': None, 'revenue': None, 'cash_flow': None,
            'free_cashflow': None, 'dividend_yield': None, 'eps': None,
            'current_price': None, 'current_ratio': None, 'debt_to_equity': None,
            'total_debt': None, 'cash_and_equivalents': None, 'debt_to_ebitda': None,
            'capex_to_depreciation': None, 'retained_earnings_efficiency': None,
            'earnings_history': [], 'dividend_history': [], 'roe_history': [],
            'fcf_history': [], 'revenue_history': [], 'gross_margin': None,
            'net_margin': None, 'total_share': None, 'float_share': None
        }
    
    def _fill_from_tickflow(self, stock_code: str, financial_data: Dict):
        if not self._tf:
            return
        try:
            ts_code = normalize_stock_code(stock_code)
            quote = self._tf.quotes.get(symbols=[ts_code])
            if quote and len(quote) > 0:
                ext = quote[0].get('ext', {})
                if not financial_data['total_mv']:
                    financial_data['total_mv'] = ext.get('total_mv')
                if not financial_data['float_mv']:
                    financial_data['float_mv'] = ext.get('float_mv')
                if not financial_data['current_price']:
                    financial_data['current_price'] = ext.get('current_price')
        except Exception as e:
            logger.warning(f"TickFlow 获取财务数据失败: {e}")
    
    def _fill_from_akshare(self, stock_code: str, financial_data: Dict):
        if not self._ak:
            return
        
        try:
            quote = self._ak.get_realtime_quote(stock_code)
            if quote:
                for field in ['current_price', 'total_mv', 'float_mv', 'eps', 'pe', 'pb', 'total_share', 'float_share']:
                    if not financial_data[field]:
                        financial_data[field] = quote.get(field)
        except Exception as e:
            logger.warning(f"akshare 实时行情获取失败: {e}")
        
        try:
            income_data = self._ak.get_income_data(stock_code)
            if income_data:
                for field in ['revenue', 'net_profit', 'gross_margin', 'net_margin']:
                    if not financial_data[field]:
                        financial_data[field] = income_data.get(field)
                if not financial_data['eps'] and financial_data['net_profit'] and financial_data.get('total_share'):
                    total_share = financial_data.get('total_share')
                    if total_share and total_share > 0:
                        financial_data['eps'] = round(financial_data['net_profit'] / total_share, 2)
        except Exception as e:
            logger.warning(f"akshare 营收数据获取失败: {e}")
        
        try:
            cashflow_data = self._ak.get_cashflow_data(stock_code)
            if cashflow_data:
                if not financial_data['cash_flow']:
                    financial_data['cash_flow'] = cashflow_data.get('operating_cashflow')
                if not financial_data['free_cashflow']:
                    financial_data['free_cashflow'] = cashflow_data.get('free_cashflow')
        except Exception as e:
            logger.warning(f"akshare 现金流数据获取失败: {e}")
        
        try:
            balance_data = self._ak.get_balance_sheet(stock_code)
            if balance_data:
                for field in ['current_ratio', 'debt_to_equity', 'total_debt', 'cash_and_equivalents']:
                    if not financial_data[field]:
                        financial_data[field] = balance_data.get(field)
        except Exception as e:
            logger.warning(f"akshare 资产负债表获取失败: {e}")
        
        try:
            historical_financials = self._ak.get_historical_financials(stock_code, years=5)
            if historical_financials:
                for field in ['roe_history', 'earnings_history', 'fcf_history', 'dividend_history', 'revenue_history']:
                    if not financial_data[field]:
                        financial_data[field] = historical_financials.get(field, [])
                
                if financial_data['roe_history']:
                    financial_data['roe'] = round(sum(financial_data['roe_history']) / len(financial_data['roe_history']), 2)
        except Exception as e:
            logger.warning(f"akshare 历史财务数据获取失败: {e}")
        
        try:
            extended_info = self._ak.get_extended_info(stock_code)
            if extended_info:
                if not financial_data['dividend_yield']:
                    financial_data['dividend_yield'] = extended_info.get('dividend_yield')
        except Exception as e:
            logger.warning(f"akshare 扩展信息获取失败: {e}")
    
    def _fill_from_tavily(self, stock_code: str, financial_data: Dict):
        if not self._tavily:
            return
        if financial_data['pe'] and financial_data['pb'] and financial_data['roe']:
            return
        
        try:
            stock_name = self._get_stock_name_from_info(stock_code)
            query = f"{stock_name} {stock_code} 市盈率 PE 市净率 PB 净资产收益率 ROE"
            results = self._tavily.search(query=query, search_depth="advanced", max_results=5)
            
            if results and 'results' in results:
                for result in results['results']:
                    content = result.get('content', '')
                    
                    if not financial_data['pe']:
                        pe_val = self._extract_numeric(content, r'市盈率[为:：]?\s*(\d+\.?\d*)', r'PE[^0-9]*([0-9]+\.?[0-9]*)', 0, 1000)
                        if pe_val is not None:
                            financial_data['pe'] = pe_val
                            logger.info(f"Tavily 提取到 PE: {pe_val}")
                    
                    if not financial_data['pb']:
                        pb_val = self._extract_numeric(content, r'市净率[为:：]?\s*(\d+\.?\d*)', r'PB[^0-9]*([0-9]+\.?[0-9]*)', 0, 100)
                        if pb_val is not None:
                            financial_data['pb'] = pb_val
                            logger.info(f"Tavily 提取到 PB: {pb_val}")
                    
                    if not financial_data['roe']:
                        roe_val = self._extract_numeric(content, r'净资产收益率[为:：]?\s*(\d+\.?\d*)%?', r'ROE[^0-9]*([0-9]+\.?[0-9]*)%?', 0, 100)
                        if roe_val is not None:
                            financial_data['roe'] = roe_val
                            logger.info(f"Tavily 提取到 ROE: {roe_val}%")
        except Exception as e:
            logger.warning(f"Tavily 财务数据搜索失败: {e}")
    
    def _extract_numeric(self, content: str, pattern1: str, pattern2: str, min_val: float, max_val: float) -> Optional[float]:
        for pattern in [pattern1, pattern2]:
            match = re.search(pattern, content, re.I)
            if match:
                try:
                    val = float(match.group(1))
                    if min_val < val < max_val:
                        return val
                except (ValueError, TypeError):
                    continue
        return None
    
    def _fill_from_backup(self, stock_code: str, financial_data: Dict):
        if not self._backup:
            return

        if not financial_data['roe_history']:
            try:
                backup_financial = self._backup.get_all_financial_data(stock_code)
                if backup_financial:
                    if backup_financial.get('tushare'):
                        ts_financial = backup_financial['tushare']
                        if ts_financial.get('roe_history'):
                            financial_data['roe_history'] = ts_financial['roe_history']
                            if not financial_data['roe']:
                                financial_data['roe'] = round(sum(ts_financial['roe_history']) / len(ts_financial['roe_history']), 2)
                            logger.info(f"备用数据源获取ROE历史: {financial_data['roe_history']}")
                        if ts_financial.get('revenue_history'):
                            financial_data['revenue_history'] = ts_financial['revenue_history']
                        if ts_financial.get('net_profit_history'):
                            financial_data['earnings_history'] = ts_financial['net_profit_history']

                    if backup_financial.get('eastmoney'):
                        em_financial = backup_financial['eastmoney']
                        if not financial_data.get('pe') and em_financial.get('pe'):
                            financial_data['pe'] = em_financial['pe']
                        if not financial_data.get('pb') and em_financial.get('pb'):
                            financial_data['pb'] = em_financial['pb']
                        if not financial_data.get('total_mv') and em_financial.get('total_mv'):
                            financial_data['total_mv'] = em_financial['total_mv']
                        if not financial_data.get('float_mv') and em_financial.get('float_mv'):
                            financial_data['float_mv'] = em_financial['float_mv']
                        logger.info(f"东方财富补充财务指标: PE={em_financial.get('pe')}, PB={em_financial.get('pb')}")
            except Exception as e:
                logger.warning(f"备用数据源获取财务数据失败: {e}")

        if not financial_data['current_price']:
            try:
                backup_quote = self._backup.get_unified_quote(stock_code)
                if backup_quote:
                    if not financial_data['current_price'] and backup_quote.get('current_price'):
                        financial_data['current_price'] = backup_quote['current_price']
                        logger.info(f"备用数据源获取实时价格: {financial_data['current_price']}")
                    if not financial_data.get('pe') and backup_quote.get('pe'):
                        financial_data['pe'] = backup_quote['pe']
                    if not financial_data.get('pb') and backup_quote.get('pb'):
                        financial_data['pb'] = backup_quote['pb']
                    if not financial_data.get('total_mv') and backup_quote.get('total_mv'):
                        financial_data['total_mv'] = backup_quote['total_mv']
                    if not financial_data.get('float_mv') and backup_quote.get('float_mv'):
                        financial_data['float_mv'] = backup_quote['float_mv']
            except Exception as e:
                logger.warning(f"备用数据源获取实时行情失败: {e}")
    
    def _validate_data_with_web_search(self, data: Dict, stock_code: str) -> Dict:
        if stock_code in self._validation_cache:
            return self._validation_cache[stock_code]
        
        validation_result = {
            'is_validated': False,
            'confidence': 'low',
            'matched_fields': [],
            'discrepancies': [],
            'sources': []
        }
        
        if not self._tavily:
            logger.warning("Tavily 未初始化，无法验证数据")
            self._validation_cache[stock_code] = validation_result
            return validation_result
        
        try:
            stock_name = data.get('info', {}).get('stock_name', f'股票{stock_code}')
            financial = data.get('financial', {})
            
            query = f"{stock_name} {stock_code} 股价 市值 PE PB ROE 财务数据"
            
            try:
                results = self._tavily.search(query=query, search_depth="advanced", max_results=5)
                all_results = results.get('results', []) if results else []
            except Exception as e:
                logger.warning(f"验证搜索失败: {e}")
                self._validation_cache[stock_code] = validation_result
                return validation_result
            
            if not all_results:
                self._validation_cache[stock_code] = validation_result
                return validation_result
            
            validation_result['sources'] = [r.get('url', '') for r in all_results[:3]]
            
            for result in all_results:
                content = result.get('content', '')
                
                if financial.get('pe'):
                    pe_match = re.search(r'市盈率[为:：]?\s*(\d+\.?\d*)', content)
                    if pe_match:
                        reported_pe = float(pe_match.group(1))
                        if reported_pe > 0:
                            validation_result['matched_fields'].append('pe')
                            diff = abs(reported_pe - financial['pe']) / reported_pe * 100
                            if diff > 20:
                                validation_result['discrepancies'].append(f"PE差异较大: 报告{reported_pe}, 获取{financial['pe']}")
                
                if financial.get('pb'):
                    pb_match = re.search(r'市净率[为:：]?\s*(\d+\.?\d*)', content)
                    if pb_match:
                        reported_pb = float(pb_match.group(1))
                        if reported_pb > 0:
                            validation_result['matched_fields'].append('pb')
                
                if financial.get('roe'):
                    roe_match = re.search(r'净资产收益率[为:：]?\s*(\d+\.?\d*)%?', content)
                    if roe_match:
                        reported_roe = float(roe_match.group(1))
                        if 0 < reported_roe < 100:
                            validation_result['matched_fields'].append('roe')
            
            matched_count = len(set(validation_result['matched_fields']))
            if matched_count >= 2 and len(validation_result['discrepancies']) == 0:
                validation_result['confidence'] = 'high'
                validation_result['is_validated'] = True
            elif matched_count >= 1:
                validation_result['confidence'] = 'medium'
                validation_result['is_validated'] = True
            
            logger.info(f"数据验证结果 - 置信度: {validation_result['confidence']}, "
                       f"匹配字段: {validation_result['matched_fields']}, "
                       f"差异: {validation_result['discrepancies']}")
            
        except Exception as e:
            logger.warning(f"数据验证失败: {e}")
        
        self._validation_cache[stock_code] = validation_result
        return validation_result
    
    def get_historical_data(self, stock_code: str, period: str = '1y') -> Optional[pd.DataFrame]:
        if self._tf:
            try:
                df = self._tf.klines.get(
                    normalize_stock_code(stock_code),
                    as_dataframe=True
                )
                
                if df is not None and not df.empty:
                    date_col = 'date' if 'date' in df.columns else 'trade_date'
                    df['Date'] = pd.to_datetime(df[date_col])
                    df.set_index('Date', inplace=True)
                    
                    df.columns = [col.title() if col.lower() in ['open', 'high', 'low', 'close', 'volume', 'amount'] else col 
                                  for col in df.columns]
                    
                    logger.info(f"TickFlow 成功获取股票 {stock_code} 的历史数据，共 {len(df)} 条")
                    return df
            except Exception as e:
                logger.warning(f"TickFlow 获取历史数据失败: {e}")
        
        if self._ak:
            try:
                df = self._ak.get_historical_data(stock_code, period)
                if df is not None and not df.empty:
                    logger.info(f"akshare 备用源成功获取股票 {stock_code} 的历史数据，共 {len(df)} 条")
                    return df
            except Exception as e:
                logger.warning(f"akshare 备用源获取历史数据失败: {e}")
        
        return None
    
    def _get_stock_name_from_info(self, stock_code: str) -> str:
        info = self.get_stock_info(stock_code)
        if info:
            return info.get('stock_name', f'股票{stock_code}')
        return f'股票{stock_code}'
    
    def _evaluate_data_quality(self, data: Dict) -> Dict:
        quality = {
            'score': 0,
            'issues': [],
            'completeness': 0,
            'sources': []
        }
        
        total_fields = 0
        valid_fields = 0
        
        if data.get('info'):
            total_fields += 2
            if data['info'].get('stock_name') and not data['info']['stock_name'].startswith('股票'):
                valid_fields += 1
                quality['sources'].append('TickFlow')
            if data['info'].get('industry'):
                valid_fields += 1
        
        if data.get('financial'):
            fields = ['pe', 'pb', 'roe', 'total_mv', 'free_cashflow', 'current_ratio']
            total_fields += len(fields)
            for field in fields:
                if data['financial'].get(field) is not None:
                    valid_fields += 1
        
        if data.get('historical') is not None:
            total_fields += 1
            if len(data['historical']) > 50:
                valid_fields += 1
        
        quality['completeness'] = (valid_fields / total_fields * 100) if total_fields > 0 else 0
        
        if quality['completeness'] < 50:
            quality['issues'].append('数据完整性不足（<50%）')
        
        if data.get('financial'):
            fin = data['financial']
            if fin.get('roe') and (fin['roe'] < 0 or fin['roe'] > 100):
                quality['issues'].append(f'ROE 数据可能异常: {fin["roe"]}%')
            if fin.get('pe') and (fin['pe'] < 0 or fin['pe'] > 1000):
                quality['issues'].append(f'PE 数据可能异常: {fin["pe"]}')
            if fin.get('total_mv') and fin['total_mv'] < DataConfig.MIN_MARKET_CAP:
                quality['issues'].append(f'市值数据可能错误: {fin["total_mv"]}')
        
        if data.get('data_validation'):
            validation = data['data_validation']
            if validation.get('confidence') == 'high':
                quality['sources'].append('Web验证通过')
            elif validation.get('discrepancies'):
                quality['issues'].extend([f"数据差异: {d}" for d in validation['discrepancies']])
        
        quality['score'] = max(0, 100 - len(quality['issues']) * 15)
        
        return quality
    
    def close(self):
        if self._tf:
            try:
                self._tf.close()
            except Exception:
                pass
        
        if self._backup:
            try:
                self._backup.close()
            except Exception:
                pass
        
        self._validation_cache.clear()
        logger.info("所有数据源连接已关闭")

if __name__ == "__main__":
    fetcher = UltimateDataFetcher()
    
    print("\n" + "="*60)
    print("数据获取器测试")
    print("="*60)
    
    test_codes = [
        ('600519', '贵州茅台'),
        ('000001', '平安银行'),
    ]
    
    for stock_code, stock_name in test_codes:
        print(f"\n测试股票: {stock_name} ({stock_code})")
        
        data = fetcher.get_stock_data(stock_code)
        
        if data:
            print(f"数据质量评分: {data['data_quality']['score']}/100")
            fin = data['financial']
            print(f"PE: {fin.get('pe', 'N/A')}, PB: {fin.get('pb', 'N/A')}, ROE: {fin.get('roe', 'N/A')}")
            print(f"流动比率: {fin.get('current_ratio', 'N/A')}, 债务权益比: {fin.get('debt_to_equity', 'N/A')}")
            print(f"自由现金流: {fin.get('free_cashflow', 'N/A')}")
            print(f"ROE历史: {fin.get('roe_history', [])}")
            
            if data.get('data_validation'):
                val = data['data_validation']
                print(f"\n数据验证: 置信度={val.get('confidence')}, "
                      f"匹配字段={val.get('matched_fields')}")
        else:
            print("数据获取失败!")
    
    fetcher.close()
