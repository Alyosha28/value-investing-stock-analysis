# 基于格雷厄姆与巴菲特价值投资法股票分析

基于本杰明·格雷厄姆《聪明的投资者》与沃伦·巴菲特致股东信核心原则，结合技术分析与大语言模型，构建的 A 股价值投资智能分析系统。

## 核心功能

### 格雷厄姆防御型投资者分析
- 9 项严格筛选标准：企业规模、PE、PB、PE×PB、ROE、流动比率、债务权益比、盈利稳定性、分红记录
- 格雷厄姆内在价值公式：`V = EPS × (8.5 + 2g) × (4.4 / Y)`
- 安全边际计算：`(内在价值 - 当前价格) / 内在价值 × 100%`
- 百分制评分体系，自动生成投资建议

### 巴菲特价值投资分析
- **护城河评估**：基于 ROE/毛利率/净利率/ROE 稳定性/市值规模，判定宽/中等/窄/无护城河
- **管理层评价**：资本配置能力 + 留存收益再投资效率
- **DCF 估值**：自由现金流折现模型，10 年预测期 + 永续增长终值
- **债务分析**：Debt/EBITDA + 净现金状态判断
- **安全边际**：内在价值 vs 当前市值

### 技术分析（6 维度量化评分）
| 维度 | 权重 | 核心指标 |
|------|------|----------|
| 趋势强度 | 30% | ADX、均线排列、MA200 位置 |
| 动量指标 | 20% | RSI、MACD 柱状图、ROC、随机指标 |
| 波动率状态 | 15% | ATR 比率、布林带宽度 |
| 量价配合 | 15% | OBV、涨跌量比、成交量活跃度 |
| 支撑阻力 | 10% | 布林带位置、均线支撑 |
| 相对强度 | 10% | 20/60 日价格变动率 |

### AI 智能分析
- 接入 DeepSeek 大语言模型
- 综合格雷厄姆/巴菲特/技术分析结果，生成结构化投资建议
- 输出：投资机会、风险提示、目标价区间、持有周期、置信度

### 多源数据整合
- TickFlow API → efinance → baostock → Tavily 搜索，四级自动故障转移
- 并发分析支持，自动根据 CPU 核心数优化线程数

## 项目结构

```
├── main.py                      # 主程序入口（支持单股/多股并发分析）
├── graham_analyzer.py           # 格雷厄姆价值投资分析器
├── buffett_analyzer.py          # 巴菲特价值投资分析器
├── technical_analyzer.py        # 技术分析器（6 维度评分）
├── ai_analyzer.py               # AI 智能分析器（DeepSeek）
├── report_generator.py          # 报告生成器（Markdown + 图表）
├── ultimate_data_fetcher.py     # 多源数据获取器
├── config.py                    # 统一配置（阈值/参数/API）
├── .env.example                 # 环境变量模板
├── requirements.txt             # Python 依赖
├── output/                      # 分析报告输出
└── docs/                        # 文档
```

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置环境变量

复制 `.env.example` 为 `.env`，填入你的 API 密钥：

```bash
cp .env.example .env
```

| 变量 | 必需 | 用途 |
|------|------|------|
| `DEEPSEEK_API_KEY` | 可选 | AI 智能分析（无则跳过 AI 模块） |
| `TICKFLOW_API_KEY` | 可选 | TickFlow 行情数据（无则使用免费数据源） |
| `TAVILY_API_KEY` | 可选 | 网络搜索补充数据 |

### 3. 运行分析

```bash
# 分析单只股票
python main.py 600519

# 分析多只股票（并发）
python main.py 600519 000001 601318

# 详细输出
python main.py 600519 -v

# 输出到文件
python main.py 600519 -o file
```

## 分析报告示例

分析完成后在 `output/` 目录生成：
- `{代码}_{名称}_价值投资分析报告.md` — 完整分析报告
- `{代码}_{名称}_技术分析.png` — K 线 + 均线 + RSI + MACD 图表

## 配置说明

所有分析阈值均可通过 `config.py` 调整：

- `GrahamThresholds` — 格雷厄姆筛选标准（PE≤15, PB≤1.5 等）
- `BuffettThresholds` — 巴菲特筛选标准（ROE≥15%, Debt/EBITDA≤3 等）
- `DCFConfig` — DCF 折现参数（折现率 9%, 永续增长率 3% 等）
- `TechnicalConfig` — 技术指标参数与权重

## 免责声明

本系统仅供学习研究使用，不构成任何投资建议。股票投资有风险，入市需谨慎。

## 许可证

MIT License
