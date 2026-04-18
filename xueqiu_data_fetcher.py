from typing import Dict, Optional
from logger_config import logger
from config import safe_float

class XueqiuDataFetcher:
    def __init__(self):
        self._ef = None
        try:
            import efinance as ef
            self._ef = ef
            logger.info("efinance 初始化成功")
        except Exception as e:
            logger.error(f"efinance 初始化失败: {e}")
    
    def get_stock_info(self, stock_code: str) -> Optional[Dict]:
        if not self._ef:
            return None
        
        try:
            info = self._ef.stock.get_base_info(stock_code)
            
            if info is None or info.empty:
                return None
            
            result = {
                'stock_name': info.get('股票名称'),
                'industry': info.get('所处行业'),
                'list_date': info.get('上市时间'),
                'total_share': safe_float(info.get('总股本')),
                'float_share': safe_float(info.get('流通股本')),
                'current_price': safe_float(info.get('最新价')),
            }
            
            return result
            
        except Exception as e:
            logger.warning(f"efinance 获取股票信息失败: {e}")
        
        return None
    
    def get_stock_data(self, stock_code: str) -> Optional[Dict]:
        if not self._ef:
            return None
        
        try:
            info = self._ef.stock.get_base_info(stock_code)
            
            if info is None or info.empty:
                return None
            
            result = {
                'name': info.get('股票名称'),
                'pe': safe_float(info.get('市盈率(动)')),
                'pb': safe_float(info.get('市净率')),
                'roe': safe_float(info.get('ROE')),
                'total_mv': safe_float(info.get('总市值')),
                'float_mv': safe_float(info.get('流通市值')),
                'current_price': safe_float(info.get('最新价')),
                'eps': safe_float(info.get('每股收益')),
                'dividend_yield': safe_float(info.get('股息率')),
                'industry': info.get('所处行业'),
                'net_profit': safe_float(info.get('净利润')),
                'gross_margin': safe_float(info.get('毛利率')),
                'net_margin': safe_float(info.get('净利率')),
                'total_share': safe_float(info.get('总股本')),
                'float_share': safe_float(info.get('流通股本')),
            }
            
            if result['pe'] or result['roe']:
                logger.info(f"efinance数据 - {result['name']}: PE={result['pe']}, PB={result['pb']}, ROE={result['roe']}")
                logger.info(f"efinance补充 - 总市值:{result['total_mv']}, EPS:{result['eps']}, 当前价:{result['current_price']}")
                return result
            
        except Exception as e:
            logger.warning(f"efinance 获取数据失败: {e}")
        
        return None
