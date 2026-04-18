#!/usr/bin/env python3
import argparse
import logging
import time
import os
import threading
from typing import List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

from ultimate_data_fetcher import UltimateDataFetcher
from graham_analyzer import GrahamAnalyzer
from buffett_analyzer import BuffettAnalyzer
from technical_analyzer import TechnicalAnalyzer
from ai_analyzer import AIAnalyzer
from report_generator import ReportGenerator
from logger_config import logger
from config import SystemConfig, DataConfig

class StockAnalysisSystem:
    def __init__(self, max_workers: int = None):
        self.data_fetcher = UltimateDataFetcher()
        self.graham_analyzer = GrahamAnalyzer()
        self.buffett_analyzer = BuffettAnalyzer()
        self.technical_analyzer = TechnicalAnalyzer()
        self.ai_analyzer = AIAnalyzer()
        self.report_generator = ReportGenerator()
        self._max_workers = max_workers or self._calculate_optimal_workers()
        self._results_lock = Lock()
        logger.info(f"股票分析系统初始化完成，最大并发线程数: {self._max_workers}")
    
    def _calculate_optimal_workers(self) -> int:
        cpu_count = os.cpu_count() or 4
        return min(cpu_count, 8)
    
    def analyze_stock(self, stock_code: str) -> Optional[dict]:
        try:
            thread_name = threading.current_thread().name
            logger.info(f"[{thread_name}] 开始分析股票: {stock_code}")
            
            stock_data = self.data_fetcher.get_stock_data(stock_code)
            if not stock_data:
                logger.error(f"无法获取股票 {stock_code} 的数据")
                return None
            
            graham_result = self.graham_analyzer.analyze(stock_data)
            buffett_result = self.buffett_analyzer.analyze(stock_data)
            technical_result = self.technical_analyzer.analyze(stock_data)
            ai_result = self.ai_analyzer.analyze(
                stock_data,
                graham_result,
                buffett_result,
                technical_result
            )
            
            report = self.report_generator.generate_report(
                stock_data,
                graham_result,
                buffett_result,
                technical_result,
                ai_result
            )
            
            return {
                'stock_code': stock_code,
                'stock_data': stock_data,
                'graham_analysis': graham_result,
                'buffett_analysis': buffett_result,
                'technical_analysis': technical_result,
                'ai_analysis': ai_result,
                'report_path': report
            }
            
        except Exception as e:
            logger.error(f"分析股票 {stock_code} 时出错: {e}")
            return None
    
    def _analyze_single_stock(self, stock_code: str) -> dict:
        result = self.analyze_stock(stock_code)
        return {'stock_code': stock_code, 'result': result}
    
    def analyze_stocks(self, stock_codes: List[str]) -> List[dict]:
        results = []
        success_count = 0
        failed_stocks = []
        
        logger.info(f"开始并发分析 {len(stock_codes)} 只股票，使用 {self._max_workers} 个线程")
        
        with ThreadPoolExecutor(max_workers=self._max_workers) as executor:
            future_to_stock = {
                executor.submit(self._analyze_single_stock, code): code 
                for code in stock_codes
            }
            
            for future in as_completed(future_to_stock):
                stock_code = future_to_stock[future]
                try:
                    task_result = future.result()
                    result = task_result['result']
                    
                    with self._results_lock:
                        if result:
                            results.append(result)
                            success_count += 1
                            logger.info(f"✓ {stock_code} 分析完成")
                        else:
                            failed_stocks.append(stock_code)
                            logger.error(f"✗ {stock_code} 分析失败")
                            
                except Exception as e:
                    with self._results_lock:
                        failed_stocks.append(stock_code)
                        logger.error(f"✗ {stock_code} 执行异常: {e}")
        
        logger.info(f"并发分析完成，成功 {success_count}/{len(stock_codes)} 只股票")
        if failed_stocks:
            logger.warning(f"失败的股票: {', '.join(failed_stocks)}")
        
        return results

def main():
    parser = argparse.ArgumentParser(description='自动化股票分析脚本')
    parser.add_argument('stock_codes', nargs='*', default=DataConfig.DEFAULT_STOCK_POOL, help='要分析的A股股票代码列表')
    parser.add_argument('--output', '-o', type=str, default='console', choices=['console', 'file'],
                      help='输出方式: console (控制台) 或 file (文件)')
    parser.add_argument('--verbose', '-v', action='store_true', help='详细输出')
    
    args = parser.parse_args()
    
    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)
    
    system = StockAnalysisSystem()
    
    logger.info(f"开始分析股票: {args.stock_codes}")
    start_time = time.time()
    
    results = system.analyze_stocks(args.stock_codes)
    
    end_time = time.time()
    logger.info(f"分析完成，成功 {len(results)}/{len(args.stock_codes)} 只股票，耗时: {end_time - start_time:.2f} 秒")
    
    for result in results:
        if args.output == 'console':
            print("=" * 80)
            print(f"股票代码: {result['stock_code']}")
            print("=" * 80)
            if result.get('report_path'):
                try:
                    with open(result['report_path'], 'r', encoding='utf-8') as f:
                        print(f.read())
                except (IOError, OSError) as e:
                    logger.error(f"读取报告文件失败: {e}")
            else:
                print("报告生成失败")
            print("\n")
        else:
            if result.get('report_path'):
                logger.info(f"分析报告已保存到: {result['report_path']}")
            else:
                logger.error(f"报告生成失败: {result['stock_code']}")

if __name__ == "__main__":
    import sys
    import io
    if sys.platform == 'win32':
        sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8')
        sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8')
    main()
