# 股票分析系统运行指南

## 环境要求
- Python 3.10+
- 依赖: `akshare`, `tushare`, `pandas`, `requests`, `numpy`
- TUSHARE_TOKEN（可选，用于财务数据）

## 快速运行

```bash
# 分析单只股票
python main.py 600519

# 分析多只股票
python main.py 600519 000001 601318

# 市场环境分析
python main.py --market

# 全市场筛选（Top 50）
python main.py --screen

# 筛选Top 10并导出CSV
python main.py --screen --top-n 10 --export-csv results.csv

# 详细输出
python main.py 600519 --verbose
```

## 常用参数

| 参数 | 说明 |
|------|------|
| `--market, -m` | 仅显示市场环境分析 |
| `--screen, -s` | 执行全市场批量筛选 |
| `--strategy` | 筛选策略: `graham` / `buffett` / `comprehensive` |
| `--top-n` | 筛选返回前N名，默认50 |
| `--export-csv` | 将筛选结果导出到CSV |
| `--notify, -n` | 分析完成后推送通知 |
| `--verbose, -v` | 详细日志输出 |

## 数据源优先级

1. 新华财经网（实时行情）
2. Backup聚合源（腾讯/东方财富/新浪）
3. Akshare（财务数据）
4. Tushare Pro（需TOKEN）

## 输出目录

- 个股报告: `output/股票代码_股票名称_价值投资分析报告.md`
- 筛选缓存: `data/screener_cache.pkl`
- 日志: `logs/`

## 常见问题

1. **连接超时**: 国内数据源偶发断开，程序会自动重试3次并切换备用源
2. **数据缺失**: 部分财务字段（PB/ROE）可能缺失，不影响报告生成
3. **通知未配置**: 飞书/邮箱需在 `config.py` 或环境变量中配置
