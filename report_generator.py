import pandas as pd
import matplotlib.pyplot as plt
import matplotlib
import logging
from typing import Dict, Optional, Any
import os
import sys
from config import SystemConfig
from logger_config import logger

if sys.platform == 'win32':
    for font_path in [
        'C:/Windows/Fonts/msyh.ttc',
        'C:/Windows/Fonts/simsun.ttc',
        'C:/Windows/Fonts/mingliu.ttc',
    ]:
        if os.path.exists(font_path):
            matplotlib.font_manager.fontManager.addfont(font_path)
            plt.rcParams['font.sans-serif'] = ['Microsoft YaHei', 'SimSun', 'DejaVu Sans']
            plt.rcParams['axes.unicode_minus'] = False
            break

class ReportGenerator:
    def __init__(self):
        self.output_dir = SystemConfig.OUTPUT_DIR
        if not os.path.exists(self.output_dir):
            os.makedirs(self.output_dir)
    
    def generate_report(self, stock_data: Dict[str, Any], graham_result: Dict[str, Any], buffett_result: Dict[str, Any], technical_result: Dict[str, Any], ai_result: Dict[str, Any]) -> str:
        try:
            stock_info = stock_data.get('info', {})
            stock_name = stock_info.get('stock_name', '未知股票')
            stock_code = stock_info.get('stock_code', '未知代码')
            
            report_content = self._generate_text_report(
                stock_name, stock_code, stock_data, 
                graham_result, buffett_result, technical_result, ai_result
            )
            
            report_path = os.path.join(self.output_dir, f'{stock_code}_{stock_name}_价值投资分析报告.md')
            with open(report_path, 'w', encoding='utf-8') as f:
                f.write(report_content)
            
            self._generate_charts(stock_data, technical_result)
            
            logger.info(f"报告生成完成: {report_path}")
            return report_path
            
        except Exception as e:
            logger.error(f"生成报告失败: {e}")
            return None
    
    def _generate_text_report(self, stock_name: str, stock_code: str, stock_data: Dict[str, Any], 
                           graham_result: Dict[str, Any], buffett_result: Dict[str, Any], 
                           technical_result: Dict[str, Any], ai_result: Dict[str, Any]) -> str:
        report = []
        
        report.append(f"# {stock_name}（{stock_code}）分析报告")
        report.append(f"生成时间: {pd.Timestamp.now().strftime('%Y-%m-%d %H:%M:%S')}")
        report.append("=" * 80)
        
        def safe_format(value, format_spec=''):
            if value is None:
                return 'N/A'
            if format_spec:
                try:
                    return f"{value:{format_spec}}"
                except Exception:
                    return str(value)
            return str(value)
        
        self._append_basic_info(report, stock_data, safe_format)
        self._append_financial_data(report, stock_data, safe_format)
        self._append_graham_analysis(report, graham_result, safe_format)
        self._append_buffett_analysis(report, buffett_result, safe_format)
        self._append_technical_analysis(report, technical_result, safe_format)
        self._append_ai_analysis(report, ai_result)
        
        report.append("\n## 7. 总结")
        report.append("=" * 80)
        report.append("免责声明：本报告仅供参考，不构成投资建议。投资有风险，入市需谨慎。")
        
        return '\n'.join(report)
    
    def _append_basic_info(self, report: list, stock_data: Dict[str, Any], safe_format):
        report.append("\n## 1. 股票基本信息")
        info = stock_data.get('info', {})
        financial = stock_data.get('financial', {})
        report.append(f"- 股票名称: {info.get('stock_name', '未知')}")
        report.append(f"- 股票代码: {info.get('stock_code', '未知')}")
        report.append(f"- 所属行业: {info.get('industry', '未知')}")
        report.append(f"- 上市日期: {info.get('list_date', '未知')}")
        
        total_share = info.get('total_share') or financial.get('total_share')
        float_share = info.get('float_share') or financial.get('float_share')
        report.append(f"- 总股本: {safe_format(total_share, ',')} 股")
        report.append(f"- 流通股本: {safe_format(float_share, ',')} 股")
    
    def _append_financial_data(self, report: list, stock_data: Dict[str, Any], safe_format):
        report.append("\n## 2. 财务数据")
        financial = stock_data.get('financial', {})
        report.append(f"- 市盈率 (PE): {safe_format(financial.get('pe'))}")
        report.append(f"- 市净率 (PB): {safe_format(financial.get('pb'))}")
        report.append(f"- 每股收益 (EPS): {safe_format(financial.get('eps'))}")
        report.append(f"- 总市值: {safe_format(financial.get('total_mv'), ',')} 元")
        report.append(f"- 流通市值: {safe_format(financial.get('float_mv'), ',')} 元")
        report.append(f"- 净资产收益率 (ROE): {safe_format(financial.get('roe'))}%")
        report.append(f"- 净利润: {safe_format(financial.get('net_profit'), ',')} 元")
        report.append(f"- 营收: {safe_format(financial.get('revenue'), ',')} 元")
        report.append(f"- 自由现金流: {safe_format(financial.get('free_cashflow'), ',')} 元")
        report.append(f"- 流动比率: {safe_format(financial.get('current_ratio'))}")
        report.append(f"- 债务权益比: {safe_format(financial.get('debt_to_equity'))}")
        
        if financial.get('roe_history'):
            report.append(f"- ROE历史(近{len(financial['roe_history'])}年): {', '.join(f'{r:.1f}%' for r in financial['roe_history'])}")
    
    def _append_graham_analysis(self, report: list, graham_result: Dict[str, Any], safe_format):
        report.append("\n## 3. 格雷厄姆价值投资分析")
        if graham_result:
            report.append(f"- 市盈率 (PE): {safe_format(graham_result.get('pe'))}")
            report.append(f"- 市净率 (PB): {safe_format(graham_result.get('pb'))}")
            report.append(f"- 每股收益 (EPS): {safe_format(graham_result.get('eps'))}")
            report.append(f"- 净资产收益率 (ROE): {safe_format(graham_result.get('roe'))}%")
            report.append(f"- 流动比率: {safe_format(graham_result.get('current_ratio'))}")
            report.append(f"- 债务权益比: {safe_format(graham_result.get('debt_to_equity'))}")
            report.append(f"- 当前价格: {safe_format(graham_result.get('current_price'))}")
            report.append(f"- 内在价值: {safe_format(graham_result.get('intrinsic_value'))}")
            report.append(f"- 格雷厄姆评分: {safe_format(graham_result.get('graham_score'))}/100")
            report.append(f"- 安全边际: {safe_format(graham_result.get('margin_of_safety'))}%")
            report.append(f"- 符合格雷厄姆标准: {'是' if graham_result.get('meets_graham_standards', False) else '否'}")
            report.append(f"- 建议: {graham_result.get('suggestion', '无')}")
            details = graham_result.get('criteria_details', {}).get('details', [])
            if details:
                report.append("\n详细检查项:")
                for detail in details:
                    report.append(f"  {detail}")
        else:
            report.append("- 无数据")
    
    def _append_buffett_analysis(self, report: list, buffett_result: Dict[str, Any], safe_format):
        report.append("\n## 4. 巴菲特价值投资分析")
        if buffett_result:
            report.append(f"- 净资产收益率 (ROE): {safe_format(buffett_result.get('roe'))}%")
            report.append(f"- 股息率: {safe_format(buffett_result.get('dividend_yield'))}%")
            report.append(f"- 自由现金流: {safe_format(buffett_result.get('free_cashflow'), ',')} 元")
            report.append(f"- FCF增长率: {safe_format(buffett_result.get('fcf_growth_rate'))}%")
            report.append(f"- 经济护城河: {buffett_result.get('moat_rating', '无')}")
            report.append(f"- 内在价值(DCF): {safe_format(buffett_result.get('intrinsic_value'), ',')} 元")
            report.append(f"- 安全边际: {safe_format(buffett_result.get('margin_of_safety'))}%")
            report.append(f"- 管理层评分: {safe_format(buffett_result.get('management_score'))}/100")
            report.append(f"- 资本配置评分: {safe_format(buffett_result.get('capital_allocation'))}/100")
            
            debt_analysis = buffett_result.get('debt_analysis', {})
            if debt_analysis:
                report.append(f"- 债务水平: {safe_format(debt_analysis.get('debt_to_ebitda'))}")
                report.append(f"- 债务风险: {debt_analysis.get('risk_level', '未知')}")
                debt_details = debt_analysis.get('details', [])
                if debt_details:
                    for detail in debt_details:
                        report.append(f"  {detail}")
            
            if buffett_result.get('retained_earnings_efficiency') is not None:
                report.append(f"- 留存收益效率: {safe_format(buffett_result.get('retained_earnings_efficiency'))}")
            
            report.append(f"- 巴菲特评分: {safe_format(buffett_result.get('buffett_score'))}/100")
            report.append(f"- 建议: {buffett_result.get('suggestion', '无')}")
            
            moat_details = buffett_result.get('moat_details', '')
            if moat_details and moat_details != '数据不足':
                report.append(f"\n护城河分析:")
                for detail in moat_details.split(';'):
                    report.append(f"  {detail.strip()}")
        else:
            report.append("- 无数据")
    
    def _append_technical_analysis(self, report: list, technical_result: Dict[str, Any], safe_format):
        report.append("\n## 5. 技术分析")
        if technical_result:
            composite_score = technical_result.get('composite_score', 0)
            signal_strength = technical_result.get('signal_strength', '无法评估')
            market_regime = technical_result.get('market_regime', 'unknown')
            
            report.append(f"- 综合评分: {safe_format(composite_score)}/100")
            report.append(f"- 信号强度: {signal_strength}")
            report.append(f"- 市场状态: {market_regime}")
            
            dimension_scores = technical_result.get('dimension_scores', {})
            if dimension_scores:
                report.append("\n维度评分:")
                dimension_names = {
                    'trend': '趋势强度',
                    'momentum': '动量指标',
                    'volatility': '波动率状态',
                    'volume': '量价配合',
                    'support_resistance': '支撑阻力',
                    'relative_strength': '相对强度'
                }
                for key, name in dimension_names.items():
                    if key in dimension_scores:
                        report.append(f"  {name}: {safe_format(dimension_scores[key])}/100")
            
            sr_levels = technical_result.get('support_resistance', {})
            if sr_levels:
                report.append("\n支撑阻力位:")
                report.append(f"  阻力位2: {safe_format(sr_levels.get('resistance_2'))}")
                report.append(f"  阻力位1: {safe_format(sr_levels.get('resistance_1'))}")
                report.append(f"  枢轴点: {safe_format(sr_levels.get('pivot'))}")
                report.append(f"  支撑位1: {safe_format(sr_levels.get('support_1'))}")
                report.append(f"  支撑位2: {safe_format(sr_levels.get('support_2'))}")
            
            latest_signals = technical_result.get('latest_signals', {})
            if latest_signals:
                report.append("\n交易信号:")
                signal_map = {1: '买入', -1: '卖出', 0: '持有'}
                for key, name in [('ma_signal', '移动平均线'), ('rsi_signal', 'RSI'), ('bb_signal', '布林带'), ('macd_signal', 'MACD'), ('composite_signal', '综合')]:
                    signal_val = latest_signals.get(key, 0)
                    report.append(f"  {name}: {signal_map.get(signal_val, '持有')}")
        else:
            report.append("- 无数据")
    
    def _append_ai_analysis(self, report: list, ai_result: Dict[str, Any]):
        report.append("\n## 6. AI 智能分析")
        if ai_result:
            if ai_result.get('is_structured'):
                report.append(f"- 投资建议: {ai_result.get('recommendation', '无法分析')}")
                report.append(f"- 置信度: {ai_result.get('confidence_level', '中')}")
                target_low = ai_result.get('target_price_low')
                target_high = ai_result.get('target_price_high')
                if target_low and target_high:
                    report.append(f"- 目标价区间: {target_low} - {target_high}")
                report.append(f"- 时间周期: {ai_result.get('time_horizon', '无法分析')}")
                
                key_reasons = ai_result.get('key_reasons', [])
                if key_reasons:
                    report.append("\n关键理由:")
                    for i, reason in enumerate(key_reasons, 1):
                        report.append(f"  {i}. {reason}")
                
                report.append(f"\n投资机会:")
                report.append(ai_result.get('investment_opportunity', '无'))
                report.append(f"\n风险提示:")
                report.append(ai_result.get('risk_factors', '无'))
            else:
                for section in ['investment_opportunity', 'risk_factors', 'recommendation', 'target_price', 'time_horizon', 'full_analysis']:
                    section_name_map = {
                        'investment_opportunity': '投资机会',
                        'risk_factors': '风险提示',
                        'recommendation': '投资建议',
                        'target_price': '目标价',
                        'time_horizon': '时间周期',
                        'full_analysis': '完整分析'
                    }
                    report.append(f"\n### {section_name_map.get(section, section)}")
                    report.append(ai_result.get(section, '无'))
        else:
            report.append("- 无数据")
    
    def _generate_charts(self, stock_data: Dict[str, Any], technical_result: Dict[str, Any]) -> Optional[str]:
        try:
            historical_data = stock_data.get('historical')
            if historical_data is None or historical_data.empty:
                return None
            
            stock_info = stock_data.get('info', {})
            stock_name = stock_info.get('stock_name', '未知股票')
            stock_code = stock_info.get('stock_code', '未知代码')
            
            plt.figure(figsize=(15, 12))
            
            ax1 = plt.subplot(3, 1, 1)
            ax1.plot(historical_data.index, historical_data['Close'], label='收盘价', color='blue')
            
            if technical_result and 'ma' in technical_result['indicators']:
                ma_data = technical_result['indicators']['ma']
                for key, ma_series in ma_data.items():
                    ax1.plot(historical_data.index, ma_series, label=key.upper())
            
            ax1.set_title(f'{stock_name}（{stock_code}）价格走势')
            ax1.set_ylabel('价格')
            ax1.legend()
            ax1.grid(True)
            
            ax2 = plt.subplot(3, 1, 2)
            if technical_result and 'rsi' in technical_result['indicators']:
                rsi = technical_result['indicators']['rsi']
                ax2.plot(historical_data.index, rsi, label='RSI', color='purple')
                ax2.axhline(70, linestyle='--', color='red', alpha=0.5)
                ax2.axhline(30, linestyle='--', color='green', alpha=0.5)
                ax2.set_title('RSI 指标')
                ax2.set_ylabel('RSI')
                ax2.legend()
                ax2.grid(True)
            
            ax3 = plt.subplot(3, 1, 3)
            if technical_result and 'macd' in technical_result['indicators']:
                macd = technical_result['indicators']['macd']
                ax3.plot(historical_data.index, macd['macd_line'], label='MACD', color='blue')
                ax3.plot(historical_data.index, macd['signal_line'], label='Signal', color='red')
                
                histogram = macd['histogram']
                colors = ['green' if val >= 0 else 'red' for val in histogram]
                ax3.bar(historical_data.index, histogram, color=colors, alpha=0.5, label='Histogram')
                
                ax3.set_title('MACD 指标')
                ax3.set_ylabel('MACD')
                ax3.legend()
                ax3.grid(True)
            
            plt.tight_layout()
            
            chart_path = os.path.join(self.output_dir, f'{stock_code}_{stock_name}_技术分析.png')
            plt.savefig(chart_path)
            plt.close()
            
            return chart_path
            
        except Exception as e:
            logger.error(f"生成图表失败: {e}")
            return None
