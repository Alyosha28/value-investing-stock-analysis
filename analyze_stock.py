#!/usr/bin/env python3
import sys
import os
sys.path.insert(0, os.path.dirname(os.path.abspath(__file__)))

from ultimate_data_fetcher import UltimateDataFetcher
import json

def analyze_stock(stock_code):
    fetcher = UltimateDataFetcher()
    print('='*60)
    print(f'开始获取{stock_code}数据...')
    print('='*60)

    data = fetcher.get_stock_data(stock_code)

    if data:
        print('\n【基本信息】')
        info = data.get('info', {})
        print(f"股票名称: {info.get('stock_name', '未知')}")
        print(f"股票代码: {info.get('stock_code', '未知')}")
        print(f"行业: {info.get('industry', '未知')}")
        print(f"上市日期: {info.get('list_date', '未知')}")

        print('\n【财务数据】')
        fin = data.get('financial', {})
        print(f"市盈率(PE): {fin.get('pe')}")
        print(f"市净率(PB): {fin.get('pb')}")
        print(f"净资产收益率(ROE): {fin.get('roe')}%")
        print(f"每股收益(EPS): {fin.get('eps')}")
        print(f"总市值: {fin.get('total_mv', 0)/1e8:.2f}亿")
        print(f"流通市值: {fin.get('float_mv', 0)/1e8:.2f}亿")
        print(f"当前股价: {fin.get('current_price')}")

        print('\n【盈利能力】')
        gm = fin.get('gross_margin')
        nm = fin.get('net_margin')
        print(f"毛利率: {gm}%" if gm else "毛利率: N/A")
        print(f"净利率: {nm}%" if nm else "净利率: N/A")
        np = fin.get('net_profit')
        rev = fin.get('revenue')
        print(f"净利润: {np/1e8:.2f}亿" if np else "净利润: N/A")
        print(f"营业收入: {rev/1e8:.2f}亿" if rev else "营业收入: N/A")

        print('\n【ROE历史】')
        roe_history = fin.get('roe_history', [])
        if roe_history:
            roe_strs = [f"{r:.2f}%" for r in roe_history]
            print(f"近{len(roe_history)}年ROE: {', '.join(roe_strs)}")
        else:
            print("ROE历史: 无数据")

        print('\n【现金流】')
        cf = fin.get('cash_flow')
        fcf = fin.get('free_cashflow')
        print(f"经营现金流: {cf/1e8:.2f}亿" if cf else "经营现金流: N/A")
        print(f"自由现金流: {fcf/1e8:.2f}亿" if fcf else "自由现金流: N/A")

        print('\n【财务健康】')
        cr = fin.get('current_ratio')
        de = fin.get('debt_to_equity')
        td = fin.get('total_debt')
        print(f"流动比率: {cr}" if cr else "流动比率: N/A")
        print(f"债务权益比: {de}" if de else "债务权益比: N/A")
        print(f"总负债: {td/1e8:.2f}亿" if td else "总负债: N/A")

        print('\n【股息】')
        dy = fin.get('dividend_yield')
        print(f"股息率: {dy}%" if dy else "股息率: N/A")
        div_history = fin.get('dividend_history', [])
        if div_history:
            div_strs = [f"{d:.2f}" for d in div_history[:5]]
            print(f"历史分红: {', '.join(div_strs)}元/股")

        print('\n【数据质量评估】')
        quality = data.get('data_quality', {})
        print(f"质量评分: {quality.get('score', 0)}/100")
        print(f"数据完整性: {quality.get('completeness', 0):.1f}%")
        sources = quality.get('sources', ['未知'])
        print(f"数据来源: {', '.join(sources)}")
        issues = quality.get('issues', [])
        if issues:
            print(f"问题: {', '.join(issues)}")

        print('\n【数据验证】')
        validation = data.get('data_validation', {})
        validated = validation.get('is_validated', False)
        print(f"验证状态: {'已验证' if validated else '未验证'}")
        print(f"置信度: {validation.get('confidence', 'unknown')}")
        matched = validation.get('matched_fields', [])
        if matched:
            print(f"匹配字段: {', '.join(matched)}")
        else:
            print("匹配字段: 无")
        discrepancies = validation.get('discrepancies', [])
        if discrepancies:
            print(f"数据差异: {', '.join(discrepancies)}")
        else:
            print("数据差异: 无")

        return data
    else:
        print('数据获取失败!')
        return None

if __name__ == '__main__':
    stock_code = sys.argv[1] if len(sys.argv) > 1 else '603019'
    analyze_stock(stock_code)
