#!/usr/bin/env python3
import argparse
import json
import logging
import sys
import time
import os
import threading
from typing import List, Optional
from concurrent.futures import ThreadPoolExecutor, as_completed
from threading import Lock

from ultimate_data_fetcher import UltimateDataFetcher
from graham_analyzer import GrahamAnalyzer
from buffett_analyzer import BuffettAnalyzer
from lynch_analyzer import LynchAnalyzer
from munger_analyzer import MungerAnalyzer
from dalio_analyzer import DalioAnalyzer
from technical_analyzer import TechnicalAnalyzer
from ai_analyzer import AIAnalyzer
from report_generator import ReportGenerator
from market_regime import MarketRegimeAnalyzer
from stock_screener import StockScreener
from notification import Notifier
from logger_config import logger
from config import SystemConfig, DataConfig
from agent_router import AgentRouter, AgentOrchestrator

class StockAnalysisSystem:
    def __init__(self, max_workers: int = None):
        self.data_fetcher = UltimateDataFetcher()
        self.graham_analyzer = GrahamAnalyzer()
        self.buffett_analyzer = BuffettAnalyzer()
        self.lynch_analyzer = LynchAnalyzer()
        self.munger_analyzer = MungerAnalyzer()
        self.dalio_analyzer = DalioAnalyzer()
        self.technical_analyzer = TechnicalAnalyzer()
        self.ai_analyzer = AIAnalyzer()
        self.report_generator = ReportGenerator()
        self.market_regime_analyzer = MarketRegimeAnalyzer()
        self.notifier = Notifier()
        self._market_regime = None
        self._max_workers = max_workers or self._calculate_optimal_workers()
        self._results_lock = Lock()
        logger.info(f"股票分析系统初始化完成，最大并发线程数: {self._max_workers}")
    
    def _calculate_optimal_workers(self) -> int:
        cpu_count = os.cpu_count() or 4
        return min(cpu_count, 8)
    
    def analyze_stock(self, stock_code: str, market_regime: dict = None) -> Optional[dict]:
        try:
            thread_name = threading.current_thread().name
            logger.info(f"[{thread_name}] 开始分析股票: {stock_code}")

            stock_data = self.data_fetcher.get_stock_data(stock_code)
            if not stock_data:
                logger.error(f"无法获取股票 {stock_code} 的数据")
                return None

            graham_result = self.graham_analyzer.analyze(stock_data)
            buffett_result = self.buffett_analyzer.analyze(stock_data)
            lynch_result = self.lynch_analyzer.analyze(stock_data)
            munger_result = self.munger_analyzer.analyze(stock_data)
            dalio_result = self.dalio_analyzer.analyze(stock_data)
            technical_result = self.technical_analyzer.analyze(stock_data)
            ai_result = self.ai_analyzer.analyze(
                stock_data,
                graham_result,
                buffett_result,
                lynch_result,
                munger_result,
                dalio_result,
                technical_result
            )

            if market_regime is None:
                if self._market_regime is None:
                    self._market_regime = self.market_regime_analyzer.analyze()
                market_regime = self._market_regime

            report = self.report_generator.generate_report(
                stock_data,
                graham_result,
                buffett_result,
                lynch_result,
                munger_result,
                dalio_result,
                technical_result,
                ai_result,
                market_regime
            )

            return {
                'stock_code': stock_code,
                'stock_data': stock_data,
                'graham_analysis': graham_result,
                'buffett_analysis': buffett_result,
                'lynch_analysis': lynch_result,
                'munger_analysis': munger_result,
                'dalio_analysis': dalio_result,
                'technical_analysis': technical_result,
                'ai_analysis': ai_result,
                'market_regime': market_regime,
                'report_path': report
            }

        except Exception as e:
            logger.error(f"分析股票 {stock_code} 时出错: {e}")
            return None
    
    def analyze_position(self, stock_code: str, cost_price: float, market_regime: dict = None) -> Optional[dict]:
        try:
            thread_name = threading.current_thread().name
            logger.info(f"[{thread_name}] 开始持仓分析: {stock_code} 成本价: {cost_price}")

            stock_data = self.data_fetcher.get_stock_data(stock_code)
            if not stock_data:
                logger.error(f"无法获取股票 {stock_code} 的数据")
                return None

            graham_result = self.graham_analyzer.analyze(stock_data)
            buffett_result = self.buffett_analyzer.analyze(stock_data)
            lynch_result = self.lynch_analyzer.analyze(stock_data)
            munger_result = self.munger_analyzer.analyze(stock_data)
            dalio_result = self.dalio_analyzer.analyze(stock_data)
            technical_result = self.technical_analyzer.analyze(stock_data)

            if market_regime is None:
                if self._market_regime is None:
                    self._market_regime = self.market_regime_analyzer.analyze()
                market_regime = self._market_regime

            from agent_bridge import agent_risk, AgentCache
            risk_result = agent_risk(stock_code, AgentCache())
            risk_data = risk_result.get('result', {}) if risk_result else {}

            report_path = self.report_generator.generate_position_report(
                stock_data=stock_data,
                cost_price=cost_price,
                graham_result=graham_result,
                buffett_result=buffett_result,
                lynch_result=lynch_result,
                munger_result=munger_result,
                dalio_result=dalio_result,
                technical_result=technical_result,
                market_regime=market_regime,
                risk_result=risk_data,
            )

            current_price = stock_data.get('financial', {}).get('current_price', 0) or 0
            pnl_pct = ((current_price - cost_price) / cost_price * 100) if cost_price > 0 and current_price > 0 else 0

            return {
                'stock_code': stock_code,
                'cost_price': cost_price,
                'current_price': current_price,
                'pnl_pct': pnl_pct,
                'graham_analysis': graham_result,
                'buffett_analysis': buffett_result,
                'lynch_analysis': lynch_result,
                'munger_analysis': munger_result,
                'dalio_analysis': dalio_result,
                'technical_analysis': technical_result,
                'market_regime': market_regime,
                'risk_assessment': risk_data,
                'report_path': report_path
            }

        except Exception as e:
            logger.error(f"持仓分析 {stock_code} 时出错: {e}")
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
    parser.add_argument('--hold', action='store_true', help='持仓分析模式（需配合股票代码和成本价使用）')
    parser.add_argument('--cost', type=float, nargs='+', default=None,
                        help='持仓成本价（与 --hold 配合使用，可同时指定多个股票的成本价）')
    parser.add_argument('--market', '-m', action='store_true', help='仅显示当前市场环境分析')
    parser.add_argument('--screen', '-s', action='store_true', help='执行全市场批量筛选')
    parser.add_argument('--strategy', type=str, default='comprehensive', choices=['graham', 'buffett', 'comprehensive'],
                      help='筛选策略: graham / buffett / comprehensive (默认)')
    parser.add_argument('--top-n', type=int, default=50, help='筛选返回前N名，默认50')
    parser.add_argument('--export-csv', type=str, default=None, help='将筛选结果导出到指定CSV路径')
    parser.add_argument('--notify', '-n', action='store_true', help='分析完成后推送通知（飞书/邮箱）')
    parser.add_argument('--agent', '-a', type=str, default=None,
                      choices=['data', 'value', 'technical', 'macro', 'risk', 'industry', 'financial_report', 'full', 'decision'],
                      help='指定Agent类型进行路由分析')
    parser.add_argument('--query', '-q', type=str, default=None,
                      help='自然语言查询，Agent路由器将自动匹配最合适的Agent')
    parser.add_argument('--format', '-f', type=str, default='text', choices=['text', 'json'],
                      help='Agent模式输出格式: text (默认) 或 json')
    parser.add_argument('--no-cache', action='store_true', help='Agent模式禁用缓存')
    parser.add_argument('--list-agents', action='store_true', help='列出所有可用Agent')

    args = parser.parse_args()

    if args.verbose:
        logging.getLogger().setLevel(logging.DEBUG)

    if args.list_agents:
        router = AgentRouter()
        agents = router.get_all_agents()
        print("=" * 60)
        print("可用Agent列表")
        print("=" * 60)
        for agent_type, info in agents.items():
            print(f"\n  [{info['type']}] {info['name']}")
            print(f"    优先级: {info['priority']}")
            print(f"    触发关键词: {', '.join(info['keywords'][:5])}...")
            if info.get('co_trigger'):
                print(f"    协同触发: 是")
        print("\n多Agent协作场景:")
        for name, scenario in router._multi_agent_scenarios.items():
            print(f"\n  [{name}]")
            print(f"    参与Agent: {', '.join(scenario['agents'])}")
            print(f"    执行模式: {scenario['execution_order']}")
        return

    if args.agent or args.query:
        orchestrator = AgentOrchestrator()
        stock_code = args.stock_codes[0] if args.stock_codes else DataConfig.DEFAULT_STOCK_POOL[0]
        user_input = args.query or stock_code

        logger.info(f"Agent路由模式: stock_code={stock_code}, agent={args.agent}, query={user_input}")

        try:
            result = orchestrator.analyze(
                stock_code=stock_code,
                user_input=user_input,
                agent_type=args.agent,
                no_cache=args.no_cache,
            )

            if args.format == 'json':
                from agent_bridge import _safe_serialize
                output = json.dumps(_safe_serialize(result), ensure_ascii=False, indent=2)
            else:
                from agent_bridge import format_text
                if 'agent_results' in result:
                    parts = [
                        "=" * 60,
                        f"协作场景: {result.get('scenario', 'N/A')}",
                        f"股票代码: {result.get('stock_code', 'N/A')}",
                        f"执行Agent: {', '.join(result.get('agents_executed', []))}",
                        "=" * 60,
                    ]
                    if result.get('consensus'):
                        parts.append(f"共识: {result['consensus']}")
                        parts.append(f"行动建议: {result['action']}")
                        parts.append(f"风控约束: {result['risk_constraint']}")
                        parts.append('')
                        parts.append('投票:')
                        for v in result.get('votes', []):
                            parts.append(f'  {v}')
                    elif result.get('conclusion'):
                        parts.append(f"综合结论: {result['conclusion']}")
                        parts.append(f"风险等级: {result.get('risk_level', 'N/A')}")
                    for agent_type, agent_result in result.get('agent_results', {}).items():
                        parts.append('')
                        parts.append(format_text(agent_result))
                    output = '\n'.join(parts)
                else:
                    output = format_text(result)

            try:
                sys.stdout.write(output + '\n')
                sys.stdout.flush()
            except (ValueError, OSError, AttributeError):
                sys.stdout = sys.__stdout__
                sys.stdout.write(output + '\n')
                sys.stdout.flush()

        except Exception as e:
            logger.error(f"Agent分析失败: {e}")
            if args.verbose:
                import traceback
                traceback.print_exc()
        return

    if args.hold:
        if not args.cost:
            print("错误: --hold 模式需要配合 --cost 参数指定持仓成本价")
            print(f"示例: python main.py 600338 002261 --hold --cost 21.298 36.506")
            return
        if len(args.stock_codes) != len(args.cost):
            if len(args.cost) == 1 and len(args.stock_codes) > 1:
                cost_prices = [args.cost[0]] * len(args.stock_codes)
            else:
                print(f"错误: 股票代码数量({len(args.stock_codes)})与成本价数量({len(args.cost)})不匹配")
                print("请为每只股票指定一个成本价，或指定一个统一成本价")
                return
        else:
            cost_prices = args.cost

        system = StockAnalysisSystem()
        for i, stock_code in enumerate(args.stock_codes):
            cost_price = cost_prices[i]
            print(f"\n{'=' * 60}")
            print(f"持仓分析: {stock_code} (成本价: {cost_price:.3f})")
            print(f"{'=' * 60}")

            result = system.analyze_position(stock_code, cost_price)
            if result and result.get('report_path'):
                print(f"\n✅ 报告已生成: {result['report_path']}")
                print(f"   当前价: {result.get('current_price', 'N/A')}")
                pnl = result.get('pnl_pct', 0)
                pnl_s = f"+{pnl:.2f}% ✅" if pnl >= 0 else f"{pnl:.2f}% 🔴"
                print(f"   盈亏: {pnl_s}")

                if args.output == 'console':
                    print(f"\n{'─' * 60}")
                    print("报告预览:")
                    try:
                        with open(result['report_path'], 'r', encoding='utf-8') as f:
                            content = f.read()
                            print(content)
                    except Exception as e:
                        print(f"读取报告文件失败: {e}")
            else:
                print(f"\n❌ {stock_code} 持仓分析失败")

            if args.notify:
                try:
                    notifier = Notifier()
                    notifier.send_stock_report(result)
                except Exception as e:
                    logger.error(f"推送通知失败: {e}")
        return

    notifier = Notifier()

    if args.market:
        analyzer = MarketRegimeAnalyzer()
        regime = analyzer.analyze()
        print("=" * 90)
        print("A 股市场大盘综合分析".center(82))
        print("=" * 90)
        print(f"分析日期: {regime.get('analysis_date', 'N/A')}  |  "
              f"综合判断: {regime.get('composite_regime', '未知')}  |  "
              f"建议仓位: {regime.get('recommend_position', 'N/A')}%")
        bc = regime.get('bullish_count', 0)
        tc = regime.get('total_index_count', 0)
        br = regime.get('breadth_ratio', 0) * 100
        bw_signal = ('●●●●○ 强势多方' if br >= 80 else
                     '●●●○○ 多方占优' if br >= 60 else
                     '●●●●○ 强势空方' if br <= 20 else
                     '●●○○○ 空方占优' if br <= 40 else
                     '●●●○○ 多空均衡')
        print(f"市场宽度: {bc}/{tc} 偏多 | 广度比率: {br:.1f}% | {bw_signal}")
        print(f"趋势强度: {regime.get('trend_strength', 'N/A')}/100  |  "
              f"波动率状态: {regime.get('volatility_regime', '未知')}")
        print()
        print(analyzer.get_position_advice(regime))
        print("-" * 90)
        header = f" {'指数名称':<8} {'代码':<8} {'最新价':>10} {'涨跌幅':>8} {'趋势阶段':<24} {'趋势分':>6}"
        print(header)
        print("-" * 90)
        for name, r in regime.get('index_regimes', {}).items():
            code = analyzer.INDEX_CODES.get(name, 'N/A')
            price = r.get('latest_close', 'N/A')
            chg = r.get('change_pct')
            chg_str = f"{chg:+.2f}%" if chg is not None else 'N/A'
            stage = r.get('stage', '未知')
            score = r.get('trend_score', 'N/A')
            print(f" {name:<7} {code:<8} {str(price):>10} {chg_str:>8} {stage:<24} {score:>6}")
        print("-" * 90)
        print()
        print("* 涨跌幅和最新价来自 akshare 实时行情快照，趋势阶段基于历史均线系统分析。")
        if args.notify:
            notifier.send_market_regime(regime)
        return

    if args.screen:
        screener = StockScreener()
        result = screener.screen(strategy=args.strategy, top_n=args.top_n)
        print("=" * 80)
        print(f"全市场筛选结果 (策略: {args.strategy})")
        print("=" * 80)
        if not result['success']:
            print(f"筛选失败: {result.get('error', '未知错误')}")
            return

        print(f"扫描总数: {result['total_scanned']} 只")
        print(f"基础过滤后: {result['after_basic_filter']} 只")
        print(f"返回数量: Top {len(result['stocks'])}")
        print("-" * 40)
        print(result['summary'])
        print("-" * 40)
        for s in result['stocks'][:20]:
            print(f"{s['rank']:>3}. {s['stock_name']}({s['stock_code']}) | "
                  f"评分:{s['total_score']} | PE:{s['pe']} | PB:{s['pb']} | ROE:{s['roe']}% | {s['suggestion']}")

        if args.export_csv:
            screener.export_to_csv(result, args.export_csv)
        if args.notify:
            notifier.send_screener_summary(result)
        return

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

        if args.notify:
            notifier.send_stock_report(result)

if __name__ == "__main__":
    import io
    if sys.platform == 'win32':
        try:
            if hasattr(sys.stdout, 'buffer') and sys.stdout.buffer is not None:
                if not isinstance(sys.stdout, io.TextIOWrapper) or sys.stdout.encoding != 'utf-8':
                    sys.stdout = io.TextIOWrapper(sys.stdout.buffer, encoding='utf-8', errors='replace')
            if hasattr(sys.stderr, 'buffer') and sys.stderr.buffer is not None:
                if not isinstance(sys.stderr, io.TextIOWrapper) or sys.stderr.encoding != 'utf-8':
                    sys.stderr = io.TextIOWrapper(sys.stderr.buffer, encoding='utf-8', errors='replace')
        except (ValueError, AttributeError, IOError):
            pass
    main()
