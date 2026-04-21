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
    
    def generate_report(self, stock_data: Dict[str, Any], graham_result: Dict[str, Any], buffett_result: Dict[str, Any],
                        lynch_result: Dict[str, Any], munger_result: Dict[str, Any], dalio_result: Dict[str, Any],
                        technical_result: Dict[str, Any], ai_result: Dict[str, Any], market_regime: Dict[str, Any] = None) -> str:
        try:
            stock_info = stock_data.get('info', {})
            stock_name = stock_info.get('stock_name', '未知股票')
            stock_code = stock_info.get('stock_code', '未知代码')

            report_content = self._generate_text_report(
                stock_name, stock_code, stock_data,
                graham_result, buffett_result, lynch_result, munger_result, dalio_result,
                technical_result, ai_result, market_regime
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
                           lynch_result: Dict[str, Any], munger_result: Dict[str, Any],
                           dalio_result: Dict[str, Any], technical_result: Dict[str, Any],
                           ai_result: Dict[str, Any], market_regime: Dict[str, Any] = None) -> str:
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
        
        self._append_market_regime(report, market_regime)
        self._append_basic_info(report, stock_data, safe_format)
        self._append_financial_data(report, stock_data, safe_format)
        self._append_graham_analysis(report, graham_result, safe_format)
        self._append_buffett_analysis(report, buffett_result, safe_format)
        self._append_lynch_analysis(report, lynch_result, safe_format)
        self._append_munger_analysis(report, munger_result, safe_format)
        self._append_dalio_analysis(report, dalio_result, safe_format)
        self._append_technical_analysis(report, technical_result, safe_format)
        self._append_ai_analysis(report, ai_result)

        report.append("\n## 11. 总结")
        report.append("=" * 80)
        report.append("免责声明：本报告仅供参考，不构成投资建议。投资有风险，入市需谨慎。")
        
        return '\n'.join(report)
    
    def _append_market_regime(self, report: list, market_regime: Dict[str, Any]):
        report.append("\n## 1. 市场环境")
        if not market_regime:
            report.append("- 无市场环境数据")
            return

        composite = market_regime.get('composite_regime', '未知')
        trend = market_regime.get('trend_strength', 'N/A')
        volatility = market_regime.get('volatility_regime', '未知')
        position = market_regime.get('recommend_position', 'N/A')

        report.append(f"- 综合判断: {composite}")
        report.append(f"- 趋势强度: {trend}/100")
        report.append(f"- 波动率状态: {volatility}")
        report.append(f"- 建议仓位: {position}%")

        details = market_regime.get('details', [])
        if details:
            report.append("\n指数详情:")
            for detail in details:
                report.append(f"  {detail}")

        index_regimes = market_regime.get('index_regimes', {})
        if index_regimes:
            report.append("\n技术指标概览:")
            for name, data in index_regimes.items():
                report.append(
                    f"  {name}: 收盘 {data.get('latest_close', 'N/A')} | "
                    f"MA20 {data.get('ma20', 'N/A')} | MA50 {data.get('ma50', 'N/A')} | "
                    f"MA200 {data.get('ma200', 'N/A')} | 波动率 {data.get('volatility_pct', 'N/A')}%"
                )

    def _append_basic_info(self, report: list, stock_data: Dict[str, Any], safe_format):
        report.append("\n## 2. 股票基本信息")
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
        report.append("\n## 3. 财务数据")
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
        report.append("\n## 4. 格雷厄姆价值投资分析")
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
        report.append("\n## 5. 巴菲特价值投资分析")
        if buffett_result:
            report.append(f"- 净资产收益率 (ROE): {safe_format(buffett_result.get('roe'))}%")
            report.append(f"- 股息率: {safe_format(buffett_result.get('dividend_yield'))}%")
            report.append(f"- 自由现金流: {safe_format(buffett_result.get('free_cashflow'), ',')} 元")
            report.append(f"- FCF增长率: {safe_format(buffett_result.get('fcf_growth_rate'))}%")
            report.append(f"- 经济护城河: {buffett_result.get('moat_rating', '无')}")
            report.append(f"- 内在价值(DCF): {safe_format(buffett_result.get('intrinsic_value'), ',')} 元")
            
            dcf_scenarios = buffett_result.get('dcf_scenarios')
            if dcf_scenarios:
                report.append("\n多情景DCF估值:")
                for scenario_name, scenario_data in dcf_scenarios.items():
                    report.append(f"  {scenario_name}: {safe_format(scenario_data.get('intrinsic_value'), ',')}元 (增长率:{scenario_data.get('growth_rate')}%, {scenario_data.get('description')})")
            
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
            
            resource_moat = buffett_result.get('resource_moat', {})
            if resource_moat and resource_moat.get('has_resource_moat'):
                report.append("\n资源壁垒护城河:")
                for detail in resource_moat.get('details', []):
                    report.append(f"  {detail}")
            
            policy_moat = buffett_result.get('policy_moat', {})
            if policy_moat and policy_moat.get('has_policy_moat'):
                report.append("\n政策壁垒护城河:")
                for detail in policy_moat.get('details', []):
                    report.append(f"  {detail}")
            
            report.append(f"- 巴菲特评分: {safe_format(buffett_result.get('buffett_score'))}/100")
            report.append(f"- 建议: {buffett_result.get('suggestion', '无')}")
            
            moat_details = buffett_result.get('moat_details', '')
            if moat_details and moat_details != '数据不足':
                report.append(f"\n护城河分析:")
                for detail in moat_details.split(';'):
                    report.append(f"  {detail.strip()}")
        else:
            report.append("- 无数据")
    
    def _append_lynch_analysis(self, report: list, lynch_result: Dict[str, Any], safe_format):
        report.append("\n## 6. 彼得·林奇分析")
        if lynch_result:
            report.append(f"- 市盈率 (PE): {safe_format(lynch_result.get('pe'))}")
            report.append(f"- PEG 指标: {safe_format(lynch_result.get('peg'))}")
            report.append(f"- 盈利增长率: {safe_format(lynch_result.get('growth_rate'))}%")
            report.append(f"- 公司分类: {lynch_result.get('category', '未知')}")
            report.append(f"- 林奇评分: {safe_format(lynch_result.get('lynch_score'))}/100")
            report.append(f"- 建议: {lynch_result.get('suggestion', '无')}")

            consumer = lynch_result.get('consumer_facing')
            if consumer is not None:
                report.append(f"- 消费品属性: {'是' if consumer else '否'}")

            valuation = lynch_result.get('valuation', {})
            if valuation and valuation.get('details'):
                report.append("\n估值分析:")
                for detail in valuation['details']:
                    report.append(f"  {detail}")

            debt = lynch_result.get('debt_analysis', {})
            if debt and debt.get('details'):
                report.append("\n债务与现金:")
                for detail in debt['details']:
                    report.append(f"  {detail}")

            inst = lynch_result.get('institutional_analysis', {})
            if inst and inst.get('details'):
                report.append("\n机构持股:")
                for detail in inst['details']:
                    report.append(f"  {detail}")
        else:
            report.append("- 无数据")

    def _append_munger_analysis(self, report: list, munger_result: Dict[str, Any], safe_format):
        report.append("\n## 7. 查理·芒格分析")
        if munger_result:
            report.append(f"- 净资产收益率 (ROE): {safe_format(munger_result.get('roe'))}%")
            report.append(f"- 投入资本回报率 (ROIC): {safe_format(munger_result.get('roic'))}%")
            report.append(f"- 芒格评分: {safe_format(munger_result.get('munger_score'))}/100")
            report.append(f"- Lollapalooza 效应数: {safe_format(munger_result.get('lollapalooza_score'))}")
            report.append(f"- 建议: {munger_result.get('suggestion', '无')}")

            quality = munger_result.get('quality_analysis', {})
            if quality:
                report.append(f"- 企业质量评级: {quality.get('rating', '未知')}")
                if quality.get('details'):
                    report.append("\n质量分析:")
                    for detail in quality['details']:
                        report.append(f"  {detail}")

            capital = munger_result.get('capital_efficiency', {})
            if capital and capital.get('details'):
                report.append("\n资本效率:")
                for detail in capital['details']:
                    report.append(f"  {detail}")

            invert = munger_result.get('invert_checklist', {})
            if invert:
                if invert.get('failures'):
                    report.append("\n反向检查 - 失败项:")
                    for item in invert['failures']:
                        report.append(f"  {item}")
                if invert.get('passes'):
                    report.append("\n反向检查 - 通过项:")
                    for item in invert['passes']:
                        report.append(f"  {item}")
        else:
            report.append("- 无数据")

    def _append_dalio_analysis(self, report: list, dalio_result: Dict[str, Any], safe_format):
        report.append("\n## 8. 瑞·达里奥分析")
        if dalio_result:
            report.append(f"- 达里奥评分: {safe_format(dalio_result.get('dalio_score'))}/100")
            report.append(f"- 建议: {dalio_result.get('suggestion', '无')}")

            quadrant = dalio_result.get('all_weather_quadrant', {})
            if quadrant:
                report.append(f"- 全天候象限定位: {quadrant.get('quadrant', '未知')}")
                report.append(f"- 风险平价角色: {quadrant.get('risk_parity_role', '未知')}")

            debt = dalio_result.get('debt_cycle_analysis', {})
            if debt:
                report.append(f"- 债务周期健康度: {debt.get('rating', '未知')}")
                if debt.get('details'):
                    report.append("\n债务周期分析:")
                    for detail in debt['details']:
                        report.append(f"  {detail}")

            stability = dalio_result.get('earnings_stability', {})
            if stability:
                report.append(f"- 盈利稳定性: {stability.get('rating', '未知')}")
                if stability.get('details'):
                    report.append("\n盈利稳定性:")
                    for detail in stability['details']:
                        report.append(f"  {detail}")

            inflation = dalio_result.get('inflation_hedge', {})
            if inflation:
                report.append(f"- 通胀对冲能力: {inflation.get('rating', '弱')}")
                if inflation.get('details'):
                    report.append("\n通胀对冲:")
                    for detail in inflation['details']:
                        report.append(f"  {detail}")

            div = dalio_result.get('diversification_value', {})
            if div:
                report.append(f"- 分散化价值: {div.get('rating', '一般')}")
                if div.get('details'):
                    report.append("\n分散化价值:")
                    for detail in div['details']:
                        report.append(f"  {detail}")

            real_ret = dalio_result.get('real_return_estimate', {})
            if real_ret and real_ret.get('estimated_real_return') is not None:
                report.append(f"- 估算真实回报: {real_ret['estimated_real_return']:.1f}%")
                if real_ret.get('details'):
                    for detail in real_ret['details']:
                        report.append(f"  {detail}")

            sys_imp = dalio_result.get('system_importance', {})
            if sys_imp:
                report.append(f"- 系统重要性: {sys_imp.get('rating', '低')}")
                if sys_imp.get('details'):
                    for detail in sys_imp['details']:
                        report.append(f"  {detail}")
        else:
            report.append("- 无数据")

    def _append_technical_analysis(self, report: list, technical_result: Dict[str, Any], safe_format):
        report.append("\n## 9. 技术分析")
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
        report.append("\n## 10. AI 智能分析")
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
