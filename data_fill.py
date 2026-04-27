#!/usr/bin/env python3
"""
data_fill.py — 数据爬取补充方案

确保所有缺失数据字段均能被成功获取。

工作机制：
1. 接收 DataHub 已爬取但存在缺失字段的数据
2. 逐字段尝试从备选数据源补充
3. 内置个股级别的回退爬取器
4. 生成补全报告

使用方式：
    python data_fill.py 600338          # 补充单只股票
    python data_fill.py 600338 600519   # 批量补充
    python data_fill.py --list          # 列出已知数据源
    python data_fill.py 600338 --report # 生成补全报告
"""

import json
import logging
import time
from typing import Dict, List, Optional, Any, Set
from datetime import datetime

logger = logging.getLogger(__name__)


class DataCompletenessChecker:
    """数据完整性检查器"""

    REQUIRED_INFO_FIELDS = {
        'stock_name', 'industry', 'list_date',
    }

    REQUIRED_FINANCIAL_FIELDS = {
        'pb', 'pe', 'roe', 'eps',
        'total_mv', 'float_mv',
        'revenue', 'net_profit', 'gross_margin', 'net_margin',
        'total_share', 'float_share',
        'dividend_yield',
        'current_ratio', 'debt_to_equity',
        'free_cashflow', 'cash_and_equivalents', 'total_debt',
    }

    REQUIRED_HISTORY_FIELDS = {
        'revenue_history', 'earnings_history', 'roe_history',
        'fcf_history', 'dividend_history',
    }

    OPTIONAL_FIELDS = {
        'operating_cashflow', 'book_value_per_share',
        'total_assets', 'total_liabilities',
        'high_52w', 'low_52w',
    }

    ALL_REQUIRED = (REQUIRED_INFO_FIELDS |
                    REQUIRED_FINANCIAL_FIELDS |
                    REQUIRED_HISTORY_FIELDS)

    @classmethod
    def check(cls, data: Dict) -> Dict:
        """
        检查数据完整性，返回缺失字段列表
        """
        import pandas as pd
        info = data.get('info', {})
        financial = data.get('financial', {})

        missing_info = [f for f in cls.REQUIRED_INFO_FIELDS
                        if not cls._has_value(info.get(f))]
        missing_financial = [f for f in cls.REQUIRED_FINANCIAL_FIELDS
                             if not cls._has_value(financial.get(f)) and not cls._has_value(data.get(f))]
        missing_history = [f for f in cls.REQUIRED_HISTORY_FIELDS
                           if not cls._has_value(data.get(f)) and not cls._has_value(financial.get(f))]

        all_missing = missing_info + missing_financial + missing_history

        total = len(cls.ALL_REQUIRED)
        found = total - len(all_missing)
        score = found / total * 100 if total > 0 else 0

        hist = data.get('historical')
        needs_historical = hist is None or (isinstance(hist, pd.DataFrame) and hist.empty)

        return {
            'completeness_score': round(score, 1),
            'missing_info': missing_info,
            'missing_financial': missing_financial,
            'missing_history': missing_history,
            'needs_historical': needs_historical,
            'total_fields': total,
            'found_fields': found,
            'is_complete': len(all_missing) == 0 and not needs_historical,
        }

    @staticmethod
    def _has_value(val) -> bool:
        if val is None:
            return False
        if isinstance(val, (list, tuple)) and len(val) == 0:
            return False
        if isinstance(val, str) and val.strip() == '':
            return False
        return True


class FieldSupplementFetcher:
    """
    逐字段补充爬取器

    对于 DataHub 主数据源无法获取的字段，尝试从特定专有API补充。
    """

    def __init__(self):
        self._fetcher = None
        self._stats: Dict[str, int] = {
            'attempted': 0,
            'filled': 0,
            'failed': 0,
        }

    def _get_fetcher(self):
        if self._fetcher is None:
            from ultimate_data_fetcher import UltimateDataFetcher
            self._fetcher = UltimateDataFetcher()
        return self._fetcher

    def get_stats(self) -> Dict[str, int]:
        return dict(self._stats)

    def fill_missing(self, stock_code: str, data: Dict) -> Dict:
        """
        补充缺失字段

        对于每个缺失字段，尝试从专有数据方法获取。
        """
        completeness = DataCompletenessChecker.check(data)
        result = dict(data)
        result.setdefault('financial', {})
        result.setdefault('info', {})

        missing = (completeness.get('missing_info', []) +
                   completeness.get('missing_financial', []) +
                   completeness.get('missing_history', []))

        if not missing and not completeness.get('needs_historical'):
            logger.info(f"[data_fill] {stock_code} 数据完整，无需补充")
            return result

        logger.info(f"[data_fill] {stock_code} 数据完整度 "
                    f"{completeness['completeness_score']}%，"
                    f"缺 {len(missing)} 个字段，"
                    f"历史数据: {'需补充' if completeness['needs_historical'] else '已有'}")

        filled = []
        still_missing = []

        for field in missing:
            self._stats['attempted'] += 1
            val = self._fetch_single_field(stock_code, field)
            if val is not None:
                self._stats['filled'] += 1
                self._set_field(result, field, val)
                filled.append(field)
                logger.info(f"[data_fill] [+] {field}")
            else:
                self._stats['failed'] += 1
                still_missing.append(field)
                logger.warning(f"[data_fill] [-] {field}")

        if completeness.get('needs_historical'):
            hist = self._fetch_historical(stock_code)
            if hist is not None:
                result['historical'] = hist
                logger.info(f"[data_fill] [+] historical")

        new_completeness = DataCompletenessChecker.check(result)
        result['_fill_metadata'] = {
            'original_score': completeness['completeness_score'],
            'new_score': new_completeness['completeness_score'],
            'filled': filled,
            'still_missing': still_missing,
            'stats': dict(self._stats),
            'fill_time': datetime.now().isoformat(),
        }

        return result

    def _set_field(self, data: Dict, field: str, val: Any):
        if field in DataCompletenessChecker.REQUIRED_INFO_FIELDS:
            data['info'][field] = val
        else:
            data['financial'][field] = val
        data[field] = val

    def _fetch_single_field(self, stock_code: str, field: str) -> Optional[Any]:
        """尝试从多种途径获取单个字段"""
        fetcher = self._get_fetcher()

        method_map = {
            'stock_name': self._try_akshare_stock_info,
            'industry': self._try_akshare_stock_info,
            'list_date': self._try_akshare_stock_info,
            'pb': self._try_xinhua_financial,
            'pe': self._try_xinhua_financial,
            'eps': self._try_xinhua_financial,
            'total_mv': self._try_xinhua_market,
            'float_mv': self._try_xinhua_market,
            'revenue': self._try_xinhua_financial,
            'net_profit': self._try_xinhua_financial,
            'gross_margin': self._try_akshare_financial,
            'net_margin': self._try_akshare_financial,
            'total_share': self._try_akshare_stock_info,
            'float_share': self._try_akshare_stock_info,
            'dividend_yield': self._try_xinhua_financial,
            'current_ratio': self._try_akshare_financial,
            'debt_to_equity': self._try_akshare_financial,
            'free_cashflow': self._try_akshare_financial,
            'cash_and_equivalents': self._try_akshare_financial,
            'total_debt': self._try_akshare_financial,
            'revenue_history': self._try_historical,
            'earnings_history': self._try_historical,
            'roe_history': self._try_historical,
            'fcf_history': self._try_historical,
            'dividend_history': self._try_historical,
        }

        handler = method_map.get(field)
        if handler:
            try:
                return handler(stock_code, field)
            except Exception as e:
                logger.debug(f"[data_fill] {field} 获取失败: {e}")

        for source_name in fetcher._source_map:
            if source_name == 'primary':
                continue
            try:
                source = fetcher._source_map[source_name]
                data = source.get_stock_data(stock_code)
                if data:
                    for container in [data, data.get('info', {}), data.get('financial', {})]:
                        val = container.get(field)
                        if val is not None:
                            return val
            except Exception:
                continue

        return None

    def _try_xinhua_financial(self, stock_code: str, field: str) -> Optional[Any]:
        fetcher = self._get_fetcher()
        xinhua = fetcher._source_map.get('xinhua')
        if not xinhua:
            return None
        data = xinhua.get_stock_data(stock_code)
        if not data:
            return None
        return data.get(field) or data.get('financial', {}).get(field)

    def _try_xinhua_market(self, stock_code: str, field: str) -> Optional[Any]:
        return self._try_xinhua_financial(stock_code, field)

    def _try_akshare_stock_info(self, stock_code: str, field: str) -> Optional[Any]:
        fetcher = self._get_fetcher()
        akshare = fetcher._source_map.get('akshare')
        if not akshare:
            return None
        data = akshare.get_stock_data(stock_code)
        if not data:
            return None
        return data.get(field) or data.get('info', {}).get(field)

    def _try_akshare_financial(self, stock_code: str, field: str) -> Optional[Any]:
        return self._try_akshare_stock_info(stock_code, field)

    def _try_historical(self, stock_code: str, field: str) -> Optional[Any]:
        import pandas as pd
        fetcher = self._get_fetcher()
        for source_name in ['xinhua', 'tencent', 'sina']:
            source = fetcher._source_map.get(source_name)
            if not source:
                continue
            try:
                hist = source.get_historical_data(stock_code)
                if hist is not None and isinstance(hist, dict):
                    val = hist.get(field)
                    if val:
                        return val
            except Exception:
                continue
        return None

    def _fetch_historical(self, stock_code: str) -> Optional[Any]:
        import pandas as pd
        fetcher = self._get_fetcher()
        for source_name in ['xinhua', 'tencent', 'sina']:
            source = fetcher._source_map.get(source_name)
            if not source:
                continue
            try:
                hist = source.get_historical_data(stock_code)
                if hist is not None and not (isinstance(hist, pd.DataFrame) and hist.empty):
                    return hist
            except Exception:
                continue
        return None


def fill_stock(stock_code: str) -> Optional[Dict]:
    """补充单只股票数据"""
    from data_hub import DataHub

    hub = DataHub.get_instance()
    data = hub.get_stock_data(stock_code)
    if not data:
        logger.error(f"[data_fill] 无法获取 {stock_code} 的原始数据")
        return None

    filler = FieldSupplementFetcher()
    result = filler.fill_missing(stock_code, data)

    completeness = DataCompletenessChecker.check(result)
    logger.info(f"[data_fill] {stock_code} 补全后完整度: "
                f"{completeness['completeness_score']}% "
                f"({'完整' if completeness['is_complete'] else '仍有缺失'})")

    return result


def batch_fill(stock_codes: List[str]) -> Dict[str, Dict]:
    """批量补充多只股票"""
    results = {}
    for code in stock_codes:
        logger.info(f"\n{'='*50}")
        logger.info(f"[data_fill] 处理 {code}...")
        try:
            data = fill_stock(code)
            results[code] = data
        except Exception as e:
            logger.error(f"[data_fill] {code} 补充失败: {e}")
            results[code] = {'error': str(e)}
    return results


def generate_report(results: Dict[str, Dict]) -> str:
    """生成补全报告"""
    def safe_str(s):
        return str(s).encode('utf-8', errors='replace').decode('utf-8')

    lines = []
    lines.append('=' * 60)
    lines.append('Data Fill Report')
    lines.append(f'Time: {datetime.now().strftime("%Y-%m-%d %H:%M:%S")}')
    lines.append('=' * 60)

    total_complete = 0
    average_before = 0
    average_after = 0
    count = 0

    for code, data in results.items():
        if 'error' in safe_str(str(data)):
            lines.append(f'\n{code}: FAIL - {safe_str(str(data["error"]))[:100]}')
            continue

        meta = data.get('_fill_metadata', {})
        comp = DataCompletenessChecker.check(data)

        before = meta.get('original_score', 0)
        after = meta.get('new_score', 0)
        improvement = after - before

        average_before += before
        average_after += after
        count += 1
        if comp['is_complete']:
            total_complete += 1

        lines.append(f'\n## {code}')
        lines.append(f'  Original: {before:.1f}%')
        lines.append(f'  After:    {after:.1f}%')
        lines.append(f'  Improved: +{improvement:.1f}%')
        lines.append(f'  Filled ({len(meta.get("filled", []))}): {", ".join(meta.get("filled", [])[:10])}')
        still = meta.get('still_missing', [])
        if still:
            lines.append(f'  Still Missing ({len(still)}): {", ".join(still[:10])}')
        else:
            lines.append(f'  Status: COMPLETE')

    if count > 0:
        lines.append(f'\n' + '-' * 40)
        lines.append(f'Summary:')
        lines.append(f'  Stocks: {count}')
        lines.append(f'  Complete: {total_complete}/{count}')
        lines.append(f'  Avg Before: {average_before/count:.1f}%')
        lines.append(f'  Avg After:  {average_after/count:.1f}%')
        lines.append(f'  Avg Gain:   {(average_after-average_before)/count:.1f}%')

    return '\n'.join(lines)


def main():
    import argparse
    import sys

    parser = argparse.ArgumentParser(
        description='数据爬取补充方案 — 确保缺失数据均可获取'
    )
    parser.add_argument('codes', nargs='*', help='股票代码列表')
    parser.add_argument('--list', action='store_true', help='列出已知数据源和字段')
    parser.add_argument('--report', action='store_true', help='输出补全报告')
    parser.add_argument('--json', action='store_true', help='JSON格式输出')

    args = parser.parse_args()

    if args.list:
        print("必填信息字段:", DataCompletenessChecker.REQUIRED_INFO_FIELDS)
        print("必填财务字段:", DataCompletenessChecker.REQUIRED_FINANCIAL_FIELDS)
        print("必填历史字段:", DataCompletenessChecker.REQUIRED_HISTORY_FIELDS)
        print("可选字段:", DataCompletenessChecker.OPTIONAL_FIELDS)
        return

    if not args.codes:
        parser.print_help()
        return

    logging.basicConfig(level=logging.INFO, format='%(levelname)s: %(message)s')

    results = batch_fill(args.codes)

    if args.report:
        print(generate_report(results))
    elif args.json:
        serializable = {}
        for code, data in results.items():
            if isinstance(data, dict):
                data_clean = {k: v for k, v in data.items()
                              if k != 'historical' and not k.startswith('_')}
                meta = data.get('_fill_metadata', {})
                data_clean['fill_summary'] = {
                    'before': meta.get('original_score', 0),
                    'after': meta.get('new_score', 0),
                    'filled': meta.get('filled', []),
                    'still_missing': meta.get('still_missing', []),
                }
                serializable[code] = data_clean
            else:
                serializable[code] = str(data)
        print(json.dumps(serializable, ensure_ascii=False, indent=2, default=str))
    else:
        for code, data in results.items():
            if isinstance(data, dict) and '_fill_metadata' in data:
                meta = data['_fill_metadata']
                m = meta.get('still_missing', [])
                print(f'{code}: {meta["original_score"]:.0f}% → {meta["new_score"]:.0f}% '
                      f'(+{meta["filled"]}) | 缺失: {m if m else "无"}')


if __name__ == '__main__':
    main()
