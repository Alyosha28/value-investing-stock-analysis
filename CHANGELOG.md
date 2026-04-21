# 更新日志

## [版本更新] 2026-04-21

### 新增功能

#### 1. 三大投资大师分析器
- **[dalio_analyzer.py](d:%5C自做ai股票分析%5Cdalio_analyzer.py)** - 瑞·达里奥分析器
  - 债务周期健康度分析
  - 盈利稳定性评估
  - 通胀对冲能力分析
  - 全天候象限定位
  - 分散化价值评估
  - 系统重要性评估

- **[lynch_analyzer.py](d:%5C自做ai股票分析%5Clynch_analyzer.py)** - 彼得·林奇分析器
  - PEG指标计算与估值分析
  - 公司分类（缓慢增长型/稳定增长型/快速增长型/周期型/隐蔽资产型）
  - 债务与现金分析
  - 存货增长分析
  - 机构持股分析
  - 消费品属性识别

- **[munger_analyzer.py](d:%5C自做ai股票分析%5Cmunger_analyzer.py)** - 查理·芒格分析器
  - 企业质量分析（ROIC/ROE/毛利率/净利率）
  - 资本效率分析
  - 反向检查清单（Invert, always invert）
  - Lollapalooza效应评估

#### 2. 市场阶段判断器
- **[market_regime.py](d:%5C自做ai股票分析%5Cmarket_regime.py)** - MarketRegimeAnalyzer
  - 基于上证指数和沪深300的技术分析
  - 多维度市场阶段判断（牛市/熊市/震荡）
  - 趋势强度量化评分
  - 波动率状态评估
  - 建议仓位计算

#### 3. 多渠道通知推送系统
- **[notification.py](d:%5C自做ai股票分析%5Cnotification.py)** - Notifier
  - 飞书机器人推送（支持签名校验）
  - SMTP邮箱推送（支持QQ/163/Gmail等）
  - PushPlus微信推送
  - Server酱3微信推送
  - 自定义Webhook支持
  - Markdown转HTML渲染
  - 消息字节数截断

#### 4. 全市场批量筛选器
- **[stock_screener.py](d:%5C自做ai股票分析%5Cstock_screener.py)** - StockScreener
  - 基于akshare全市场实时数据
  - 支持格雷厄姆/巴菲特/综合三种筛选策略
  - 多维度评分系统
  - 护城河评级
  - CSV导出功能

### 功能增强

#### 巴菲特分析器增强 ([buffett_analyzer.py](d:%5C自做ai股票分析%5Cbuffett_analyzer.py))
- 新增**资源壁垒护城河**分析（稀土、锂、铟、锗、镓等战略资源）
- 新增**政策壁垒护城河**分析（半导体、芯片、新能源、军工等国家战略行业）
- 新增多情景DCF估值（保守/中性/乐观三种假设）
- 管理层评分体系优化
- 留存收益效率评估

#### AI分析器增强 ([ai_analyzer.py](d:%5C自做ai股票分析%5Cai_analyzer.py))
- 支持更多分析模块数据输入
- 彼得·林奇分析结果整合
- 查理·芒格分析结果整合
- 瑞·达里奥分析结果整合
- 技术分析详细信号整合
- 结构化JSON输出优化

#### 报告生成器增强 ([report_generator.py](d:%5C自做ai股票分析%5Creport_generator.py))
- 新增市场环境章节
- 新增彼得·林奇分析章节
- 新增查理·芒格分析章节
- 新增瑞·达里奥分析章节
- 图表生成功能（均线/RSI/MACD）
- 多情景DCF估值展示
- 资源/政策护城河展示

#### 主程序增强 ([main.py](d:%5C自做ai股票分析%5Cmain.py))
- 命令行参数 `--market` 仅显示市场环境分析
- 命令行参数 `--screen` 执行全市场批量筛选
- 命令行参数 `--strategy` 选择筛选策略
- 命令行参数 `--top-n` 设置筛选返回数量
- 命令行参数 `--export-csv` 导出筛选结果
- 命令行参数 `--notify` 分析完成后推送通知
- 并发线程优化

#### 配置模块增强 ([config.py](d:%5C自做ai股票分析%5Cconfig.py))
- 新增 `MoatConfig` 类（资源/政策护城河配置）
- 新增 `DCFConfig` 类（折现率/增长率配置）
- 新增 `TechnicalConfig` 类（技术分析参数）
- 新增 `APIConfig` 类（DeepSeek API配置）
- 战略资源关键词定义
- 战略行业关键词定义

#### 环境配置文档 ([.env.example](d:%5C自做ai股票分析%5C.env.example))
- 完整的中文注释说明
- 自选股列表配置
- DeepSeek API配置（密钥/模型/温度/Token限制）
- 飞书机器人配置
- SMTP邮箱配置（QQ/163/Gmail/企业邮箱）
- PushPlus配置
- Server酱3配置
- 自定义Webhook配置
- 回测参数配置
- 系统与日志配置
- 代理配置

### 系统特性

- **五大大师模型**：格雷厄姆、巴菲特、彼得·林奇、查理·芒格、瑞·达里奥
- **技术分析**：均线、RSI、布林带、MACD等技术指标
- **AI智能分析**：基于DeepSeek大模型的综合分析建议
- **市场环境判断**：大盘阶段识别与仓位建议
- **全市场筛选**：批量发现符合价值投资的标的
- **多渠道通知**：飞书、邮件、微信等多平台推送
- **并发分析**：支持多只股票同时分析
- **中文优化**：完整的简体中文支持

### 使用方式

```bash
# 分析单只股票
python main.py 600519

# 分析多只股票
python main.py 600519 000001 601318

# 仅查看市场环境
python main.py --market

# 全市场筛选
python main.py --screen --strategy comprehensive --top-n 50

# 筛选结果导出
python main.py --screen --export-csv results.csv

# 分析完成后推送通知
python main.py 600519 --notify

# 查看详细日志
python main.py 600519 -v
```

### 注意事项

- 本系统仅供参考和学习研究，不构成投资建议
- 投资有风险，入市需谨慎
- 请根据自身情况理性投资
