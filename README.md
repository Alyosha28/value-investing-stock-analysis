# AI 价值投资分析系统

基于本杰明·格雷厄姆、沃伦·巴菲特、彼得·林奇、查理·芒格与瑞·达里奥五大投资大师核心原则，结合现代量化技术分析与大型语言模型，构建的 A 股智能价值投资分析平台。注意，使用Akshare数据库需要挂梯子

---

## 核心功能

### 五大大师价值投资分析

| 大师 | 核心方法论 | 输出指标 |
|------|-----------|----------|
| **本杰明·格雷厄姆** | 防御型/进取型投资者筛选、内在价值公式、安全边际 | `graham_score`/100、安全边际、符合标准判断 |
| **沃伦·巴菲特** | 护城河评估、管理层评价、DCF 自由现金流折现、债务风险 | `buffett_score`/100、护城河等级、内在价值(DCF) |
| **彼得·林奇** | GARP/PEG 策略、公司分类（快成长/稳健/慢速/周期/隐形资产） | `lynch_score`/100、PEG、分类标签 |
| **查理·芒格** | 优质企业公平价、ROIC 筛选、逆向检查清单、Lollapalooza 效应叠加、资本效率 | `munger_score`/100、企业质量评级、逆向扣分项 |
| **瑞·达里奥** | 全天候资产配置、债务周期健康度、通胀对冲、宏观象限适配 | `dalio_score`/100、全天候象限、真实回报估计 |

### 技术分析（6 维度量化评分）

| 维度 | 权重 | 核心指标 |
|------|------|----------|
| 趋势强度 | 30% | ADX、均线排列、MA200 位置 |
| 动量指标 | 20% | RSI、MACD 柱状图、ROC、随机指标 |
| 波动率状态 | 15% | ATR 比率、布林带宽度 |
| 量价配合 | 15% | OBV、涨跌量比、成交量活跃度 |
| 支撑阻力 | 10% | 布林带位置、均线支撑 |
| 相对强度 | 10% | 20/60 日价格变动率 |

### AI 智能综合研判

- 接入 **DeepSeek** 大语言模型
- 综合五大大师分析 + 技术分析 + 市场环境，生成结构化投资建议
- 输出：投资机会、风险提示、目标价区间、持有周期、置信度、关键理由

### 市场环境分析

- 基于 A 股主要指数（沪深 300、上证 50、中证 500、创业板等）综合判断
- 输出：综合市场环境状态、趋势强度、波动率状态、建议仓位比例

### 全市场批量筛选

- 支持三种筛选策略：`graham` / `buffett` / `comprehensive`
- 基础过滤后按综合评分排序，导出 Top N 结果
- 支持导出为 CSV

### 多渠道通知推送

- **飞书机器人**：支持 Webhook + HMAC-SHA256 签名验证，推送交互式卡片消息
- **SMTP 邮箱**：支持 SSL/TLS，推送 HTML 格式邮件
- 按需触发，无定时任务，不产生冗余消息

---

## 项目结构

```
├── main.py                      # 主程序入口（单股/多股并发/市场分析/筛选）
├── graham_analyzer.py           # 格雷厄姆价值投资分析器
├── buffett_analyzer.py          # 巴菲特价值投资分析器
├── lynch_analyzer.py            # 彼得·林奇 GARP 分析器
├── munger_analyzer.py           # 查理·芒格质量分析器
├── dalio_analyzer.py            # 瑞·达里奥全天候/宏观分析器
├── technical_analyzer.py        # 技术分析器（6 维度评分）
├── ai_analyzer.py               # AI 智能分析器（DeepSeek API）
├── market_regime.py             # 市场环境分析器
├── stock_screener.py            # 全市场批量筛选器
├── notification.py              # 飞书 + 邮箱通知推送器
├── report_generator.py          # 报告生成器（Markdown 完整报告）
├── ultimate_data_fetcher.py     # 多源数据获取器（akshare/efinance 等）
├── config.py                    # 统一配置（阈值/参数/API）
├── logger_config.py             # 日志配置
├── requirements.txt             # Python 依赖
├── output/                      # 分析报告输出目录
└── logs/                        # 运行日志目录
```

---

## 快速开始

### 1. 安装依赖

```bash
pip install -r requirements.txt
```

### 2. 配置环境变量

创建 `.env` 文件（或在系统环境变量中设置）：

```bash
# AI 分析（可选，未设置则跳过 AI 模块）
DEEPSEEK_API_KEY=sk-xxxxxxxxxxxxxxxxxxxxxxxx

# 飞书推送（可选）
FEISHU_WEBHOOK=https://open.feishu.cn/open-apis/bot/v2/hook/xxxxxx
FEISHU_SECRET=xxxxxxxxxxxx

# 邮箱推送（可选）
SMTP_HOST=smtp.example.com
SMTP_PORT=465
SMTP_USER=your_email@example.com
SMTP_PASSWORD=your_password
SMTP_TO=receiver@example.com
SMTP_SSL=true
```

### 3. 运行分析

#### 分析单只或多只股票

```bash
# 分析单只股票
python main.py 600519

# 分析多只股票（自动并发）
python main.py 600519 000001 601318

# 详细日志输出
python main.py 600519 -v

# 输出到文件（保存到 output/ 目录）
python main.py 600519 -o file
```

#### 市场环境分析

```bash
# 查看当前市场环境判断与建议仓位
python main.py --market

# 同时推送飞书/邮箱通知
python main.py --market --notify
```

#### 全市场批量筛选

```bash
# 使用综合策略筛选 Top 50
python main.py --screen

# 使用巴菲特策略筛选 Top 30
python main.py --screen --strategy buffett --top-n 30

# 筛选并导出 CSV
python main.py --screen --top-n 100 --export-csv results.csv

# 筛选完成后推送通知
python main.py --screen --notify
```

#### 分析完成后推送报告

```bash
# 分析单股并推送飞书/邮箱
python main.py 600519 --notify

# 分析多股并推送
python main.py 600519 000001 601318 --notify
```

---

## 配置说明

所有分析阈值与参数均可通过 `config.py` 调整：

| 配置类 | 说明 |
|--------|------|
| `GrahamThresholds` | 格雷厄姆筛选标准（PE≤15, PB≤1.5, 流动比率≥2.0 等） |
| `BuffettThresholds` | 巴菲特筛选标准（ROE≥15%, Debt/EBITDA≤3, 股息率≥2% 等） |
| `MoatConfig` | 护城河资源壁垒与政策壁垒关键词、权重 |
| `DCFConfig` | DCF 折现参数（折现率 9%, 永续增长率 3%, 预测期 10 年） |
| `TechnicalConfig` | 技术指标周期、超买超卖阈值、六维度权重分配 |
| `APIConfig` | DeepSeek API 地址、模型、温度、超时设置 |
| `SystemConfig` | 输出目录、日志级别、重试策略、并发线程数 |
| `DataConfig` | 数据源优先级、默认股票池、市值过滤阈值 |

---

## 分析报告内容

单股分析报告包含以下章节：

1. **股票基本信息** — 名称、代码、行业、市值、当前价格
2. **市场环境** — 综合判断、趋势强度、建议仓位
3. **格雷厄姆分析** — 评分、安全边际、内在价值、符合标准判断
4. **巴菲特分析** — 评分、护城河、管理层、DCF 估值、债务风险
5. **彼得·林奇分析** — 评分、PEG、公司分类、增长率
6. **查理·芒格分析** — 评分、企业质量、资本效率、逆向检查、Lollapalooza
7. **瑞·达里奥分析** — 评分、债务周期、全天候象限、真实回报、系统重要性
8. **技术分析** — 六维度评分、信号强度、支撑阻力位、各指标状态
9. **AI 智能分析** — DeepSeek 综合研判、目标价区间、关键理由
10. **总结** — 综合评分汇总与投资建议

---

## 通知推送格式

### 飞书消息示例

以交互式卡片形式推送，包含：
- 市场环境与建议仓位
- 五大大师评分表格
- 关键财务指标（PE / PB / ROE）
- AI 综合建议与关键理由
- 免责声明

### 邮件示例

以 HTML 表格形式呈现，排版清晰，支持：
- 标题与引用块
- 评分表格与边框
- 加粗/斜体强调
- 列表项

---

## 并发与性能

- 多股分析时自动启用线程池并发，默认线程数为 `min(CPU 核心数, 8)`
- 市场环境分析在批次内只执行一次，避免重复请求指数数据
- 单股分析内部按顺序串行执行各分析器，确保数据一致性

---

## 免责声明

本系统仅供学习研究使用，不构成任何投资建议。股票投资有风险，入市需谨慎。所有分析结果基于历史数据与算法模型，无法预测未来市场走势。

---

## 许可证

MIT License
