#!/usr/bin/env python3
"""
数据源验证脚本
对各数据源进行回测，评估其准确性和可靠性
增强了对akshare和网页搜索验证的支持
"""
import pandas as pd
import logging
import os
from datetime import datetime
from typing import Dict, List, Any, Optional
from dataclasses import dataclass, field

from logger_config import logger

@dataclass
class DataSourceMetrics:
    name: str
    total_requests: int = 0
    successful_requests: int = 0
    failed_requests: int = 0
    error_messages: List[str] = field(default_factory=list)
    data_quality_scores: List[float] = field(default_factory=list)
    pe_values: List[Optional[float]] = field(default_factory=list)
    pb_values: List[Optional[float]] = field(default_factory=list)
    roe_values: List[Optional[float]] = field(default_factory=list)
    validation_confidences: List[str] = field(default_factory=list)
    
    @property
    def success_rate(self) -> float:
        return (self.successful_requests / self.total_requests * 100) if self.total_requests > 0 else 0
    
    @property
    def avg_quality_score(self) -> float:
        return sum(self.data_quality_scores) / len(self.data_quality_scores) if self.data_quality_scores else 0
    
    @property
    def avg_validation_confidence(self) -> float:
        confidence_map = {'high': 100, 'medium': 60, 'low': 20}
        if not self.validation_confidences:
            return 0
        return sum(confidence_map.get(c, 0) for c in self.validation_confidences) / len(self.validation_confidences)
    
    def is_valid_value(self, value: Optional[float], min_val: float, max_val: float) -> bool:
        if value is None:
            return False
        return min_val <= value <= max_val
    
    def get_valid_pe_count(self) -> int:
        return sum(1 for v in self.pe_values if self.is_valid_value(v, 0, 1000))
    
    def get_valid_pb_count(self) -> int:
        return sum(1 for v in self.pb_values if self.is_valid_value(v, 0, 100))
    
    def get_valid_roe_count(self) -> int:
        return sum(1 for v in self.roe_values if self.is_valid_value(v, 0, 100))

class DataSourceValidator:
    def __init__(self):
        self.metrics: Dict[str, DataSourceMetrics] = {}
        self.test_stocks = [
            ('600519', '贵州茅台'),
            ('000001', '平安银行'),
            ('601318', '中国平安'),
            ('002475', '立讯精密'),
            ('300274', '阳光电源'),
        ]
        self.quality_report_path = 'logs/datasource_quality_report.txt'
        
    def validate(self, data: Dict[str, Any]) -> bool:
        """快速验证数据是否包含基本有效字段"""
        if not data or not isinstance(data, dict):
            return False
        financial = data.get('financial', {}) or {}
        pe = financial.get('pe') if financial else data.get('pe')
        pb = financial.get('pb') if financial else data.get('pb')
        if pe is not None or pb is not None or data.get('current_price') is not None:
            return True
        # 也接受包含核心财务指标的数据（如资产负债表现金流量表数据）
        if (financial.get('free_cashflow') is not None
                or financial.get('current_ratio') is not None
                or data.get('free_cashflow') is not None
                or data.get('current_ratio') is not None):
            return True
        return False

    def _init_metrics(self, name: str):
        if name not in self.metrics:
            self.metrics[name] = DataSourceMetrics(name=name)
    
    def validate_data_source(self, stock_code: str, data: Dict[str, Any], source_name: str):
        """验证单个数据源的返回数据（支持嵌套 financial 和扁平结构）"""
        self._init_metrics(source_name)
        metrics = self.metrics[source_name]
        metrics.total_requests += 1

        if data is None:
            metrics.failed_requests += 1
            metrics.error_messages.append(f"{stock_code}: 返回空数据")
            return

        # 支持嵌套 financial 结构和扁平结构
        financial = data.get('financial', {}) or {}
        pe = financial.get('pe') if financial else data.get('pe')
        pb = financial.get('pb') if financial else data.get('pb')
        roe = financial.get('roe') if financial else data.get('roe')

        if not financial and pe is None and pb is None and roe is None:
            metrics.failed_requests += 1
            metrics.error_messages.append(f"{stock_code}: 无财务数据")
            return

        metrics.successful_requests += 1

        quality_score = self._calculate_quality_score(data)
        metrics.data_quality_scores.append(quality_score)

        metrics.pe_values.append(pe)
        metrics.pb_values.append(pb)
        metrics.roe_values.append(roe)

        if data.get('data_validation'):
            metrics.validation_confidences.append(data['data_validation'].get('confidence', 'unknown'))
    
    def _calculate_quality_score(self, data: Dict[str, Any]) -> float:
        """计算数据质量评分（支持嵌套 financial 和扁平结构）"""
        score = 0
        financial = data.get('financial', {}) or {}
        info = data.get('info', {})

        required_fields = ['pe', 'pb', 'roe', 'total_mv', 'eps']
        for field in required_fields:
            value = financial.get(field) if financial else data.get(field)
            if value is not None:
                score += 20

        # 支持 stock_name 在 info 中或顶层
        stock_name = info.get('stock_name') or data.get('stock_name')
        if stock_name and not str(stock_name).startswith('股票'):
            score += 10

        industry = info.get('industry') or data.get('industry')
        if industry:
            score += 10

        if data.get('data_validation'):
            validation = data['data_validation']
            pb = financial.get('pb')
            if pe is not None and pb is not None:
                return True

        # 支持扁平结构
        pe = data.get('pe')
        pb = data.get('pb')
        if pe is not None and pb is not None:
            return True
        if data.get('current_price') or data.get('close'):
            return True

        return False
    
    def validate_all_sources(self):
        """对所有数据源进行全面验证"""
        logger.info("=" * 70)
        logger.info("开始数据源验证测试")
        logger.info("=" * 70)
        
        for stock_code, stock_name in self.test_stocks:
            logger.info(f"\n测试股票: {stock_name} ({stock_code})")
            
            from ultimate_data_fetcher import UltimateDataFetcher
            fetcher = UltimateDataFetcher()
            
            try:
                data = fetcher.get_stock_data(stock_code)
                
                if data:
                    logger.info(f"  数据质量评分: {data.get('data_quality', {}).get('score', 0)}/100")
                    
                    financial = data.get('financial', {})
                    logger.info(f"  PE: {financial.get('pe')}, PB: {financial.get('pb')}, ROE: {financial.get('roe')}")
                    logger.info(f"  市值: {financial.get('total_mv')}")
                    logger.info(f"  净利润: {financial.get('net_profit')}")
                    
                    if data.get('data_validation'):
                        val = data['data_validation']
                        logger.info(f"  数据验证: 置信度={val.get('confidence')}, "
                                  f"匹配字段={val.get('matched_fields')}, "
                                  f"差异={val.get('discrepancies')}")
                    
                    self.validate_data_source(stock_code, data, 'UltimateDataFetcher')
                else:
                    logger.error(f"  获取数据失败")
                    
            except Exception as e:
                logger.error(f"  测试异常: {e}")
            finally:
                fetcher.close()
    
    def analyze_results(self) -> Dict[str, Any]:
        """分析验证结果"""
        analysis = {
            'timestamp': datetime.now().strftime('%Y-%m-%d %H:%M:%S'),
            'test_stocks': [f"{code}({name})" for code, name in self.test_stocks],
            'sources': {}
        }
        
        for source_name, metrics in self.metrics.items():
            source_analysis = {
                'total_requests': metrics.total_requests,
                'successful_requests': metrics.successful_requests,
                'failed_requests': metrics.failed_requests,
                'success_rate': f"{metrics.success_rate:.1f}%",
                'avg_quality_score': f"{metrics.avg_quality_score:.1f}",
                'avg_validation_confidence': f"{metrics.avg_validation_confidence:.1f}%",
                'valid_pe_count': metrics.get_valid_pe_count(),
                'valid_pb_count': metrics.get_valid_pb_count(),
                'valid_roe_count': metrics.get_valid_roe_count(),
                'error_rate': f"{(metrics.failed_requests / metrics.total_requests * 100) if metrics.total_requests > 0 else 0:.1f}%"
            }
            analysis['sources'][source_name] = source_analysis
            
            logger.info(f"\n{source_name} 统计:")
            logger.info(f"  总请求: {metrics.total_requests}")
            logger.info(f"  成功: {metrics.successful_requests}")
            logger.info(f"  失败: {metrics.failed_requests}")
            logger.info(f"  成功率: {metrics.success_rate:.1f}%")
            logger.info(f"  平均质量分: {metrics.avg_quality_score:.1f}")
            logger.info(f"  平均验证置信度: {metrics.avg_validation_confidence:.1f}%")
            
            if metrics.error_messages:
                logger.info(f"  错误信息:")
                for error in metrics.error_messages[:5]:
                    logger.info(f"    - {error}")
        
        return analysis
    
    def generate_quality_report(self, analysis: Dict[str, Any]):
        """生成质量报告"""
        report_lines = []
        report_lines.append("=" * 70)
        report_lines.append("数据源质量评估报告")
        report_lines.append(f"生成时间: {analysis['timestamp']}")
        report_lines.append("=" * 70)
        report_lines.append("")
        report_lines.append(f"测试股票: {', '.join(analysis['test_stocks'])}")
        report_lines.append("")
        
        for source_name, stats in analysis['sources'].items():
            report_lines.append("-" * 70)
            report_lines.append(f"数据源: {source_name}")
            report_lines.append("-" * 70)
            for key, value in stats.items():
                report_lines.append(f"  {key}: {value}")
            report_lines.append("")
        
        report_lines.append("=" * 70)
        report_lines.append("结论与建议")
        report_lines.append("=" * 70)
        
        recommendations = self._generate_recommendations(analysis)
        for rec in recommendations:
            report_lines.append(f"  {rec}")
        
        report_content = '\n'.join(report_lines)
        
        os.makedirs(os.path.dirname(self.quality_report_path), exist_ok=True)
        with open(self.quality_report_path, 'w', encoding='utf-8') as f:
            f.write(report_content)
        
        logger.info(f"\n质量报告已保存到: {self.quality_report_path}")
        print("\n" + report_content)
        
        return recommendations
    
    def _generate_recommendations(self, analysis: Dict[str, Any]) -> List[str]:
        """生成优化建议"""
        recommendations = []
        
        for source_name, stats in analysis['sources'].items():
            success_rate = float(stats['success_rate'].rstrip('%'))
            avg_quality = float(stats['avg_quality_score'])
            error_rate = float(stats['error_rate'].rstrip('%'))
            validation_confidence = float(stats['avg_validation_confidence'].rstrip('%'))
            
            if error_rate > 30:
                recommendations.append(f"⚠️ {source_name}: 错误率 {error_rate}% 过高，建议禁用或优化")
            elif validation_confidence > 70:
                recommendations.append(f"✅ {source_name}: 验证置信度 {validation_confidence:.1f}%，表现优秀")
            elif success_rate < 70:
                recommendations.append(f"⚡ {source_name}: 成功率 {success_rate}% 较低，考虑作为备用数据源")
            elif avg_quality < 50:
                recommendations.append(f"⚡ {source_name}: 数据质量 {avg_quality:.1f} 较低，需要改进")
            else:
                recommendations.append(f"✅ {source_name}: 表现良好，建议保留")
        
        recommendations.append("")
        recommendations.append("主要改进:")
        recommendations.append("  - 已禁用baostock（数据质量不稳定）")
        recommendations.append("  - 已禁用efinance（连接失败率>90%）")
        recommendations.append("  - 已增强akshare错误处理和重试机制")
        recommendations.append("  - 已添加网页搜索验证数据准确性")
        
        return recommendations
    
    def run_validation(self):
        """运行完整验证流程"""
        self.validate_all_sources()
        analysis = self.analyze_results()
        recommendations = self.generate_quality_report(analysis)
        return analysis, recommendations

def main():
    validator = DataSourceValidator()
    analysis, recommendations = validator.run_validation()
    
    print("\n" + "=" * 70)
    print("优化建议汇总")
    print("=" * 70)
    for rec in recommendations:
        print(rec)

if __name__ == "__main__":
    main()
