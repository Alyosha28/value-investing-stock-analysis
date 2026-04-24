# AI股票分析系统 — 项目规则

> 本项目采用多Agent智能路由架构，学习自 oh-my-openagent。
> 每个Agent是独立的投资分析专家，根据任务类型自动路由。

## 系统架构

```
用户输入
    ↓
[Router] 解析意图 → 匹配Agent
    ↓
单一Agent主导 / 多Agent协作
    ↓
结构化输出
```

## Agent目录

| Agent                              | 职责               | 触发信号                 |
| ---------------------------------- | ------------------ | ------------------------ |
| [数据分析师](../agents/数据分析师.md) | 数据获取与质量验证 | "数据"、"抓取"、"财务"   |
| [价值分析师](../agents/价值分析师.md) | 基本面估值（默认） | "估值"、"分析"、股票代码 |
| [技术分析师](../agents/技术分析师.md) | 技术走势与买卖点   | "K线"、"趋势"、"技术面"  |
| [宏观分析师](../agents/宏观分析师.md) | 市场环境与仓位     | "大盘"、"行情"、"仓位"   |
| [行业分析师](../agents/行业分析师.md) | 行业景气度与竞争格局 | "行业"、"板块"、"赛道" |
| [财报解读专家](../agents/财报解读专家.md) | 财报解读与盈利质量 | "财报"、"盈利质量"、"年报" |
| [风控官](../agents/风控官.md)         | 风险边界与止损     | "风险"、"止损"、"回撤"   |

## 路由规则

### 规则1：默认路由

- 如果用户只输入股票代码（如 `600338`），默认进入**价值分析师**Agent
- 价值分析师会自动调用数据分析师获取数据，无需用户指定

### 规则2：关键词匹配

- 用户输入包含某Agent的触发关键词，该Agent主导
- 关键词冲突时，按 router.json 中的 priority 优先级选择

### 规则3：多Agent协作场景

#### 场景A：完整个股分析

用户说"全面分析600338"或"深度分析xxx"
→ 依次调用：数据分析师 → 价值分析师 → 技术分析师 → 行业分析师 → 风控官
→ 输出完整报告

#### 场景B：买入决策

用户说"能不能买"、"值得投资吗"
→ 并行调用：价值分析师 + 技术分析师 + 宏观分析师 + 行业分析师
→ 风控官基于四方结论给出风险约束
→ 最终结论 = 多数共识 + 风控边界

#### 场景C：数据异常

分析结果出现明显矛盾或数据缺失
→ 数据分析师重新校验
→ 财报解读专家深度解读财务数据
→ 相关Agent基于修正数据重新评估

#### 场景D：深度财报分析

用户说"分析财报"、"盈利质量怎么样"
→ 依次调用：数据分析师 → 财报解读专家 → 价值分析师
→ 基于财报解读结论调整估值

### 规则4：与Skills系统协同

本项目同时配置了 `.trae/skills/` 思想武器库：

```
Agent分析过程中，按需调用Skills：
- 复杂问题 → contradiction-analysis
- 信息不足 → investigation-first
- 方案验证 → practice-cognition
- 质量检查 → criticism-self-criticism
```

Skills是方法论工具，Agent是角色容器。Agent在分析过程中可以调用Skills增强思考质量。

## Agent Bridge 调用接口

Agent通过 `agent_bridge.py` 桥接脚本获取结构化分析数据。当需要量化分析结果时，使用 RunCommand 调用：

```bash
# 单Agent调用
python agent_bridge.py <股票代码> <agent_type> --format json
python agent_bridge.py <股票代码> <agent_type> --format text

# agent_type 可选值:
#   data              - 数据分析师（数据获取与质量验证）
#   value             - 价值分析师（基本面估值）
#   technical         - 技术分析师（技术走势与买卖点）
#   macro             - 宏观分析师（市场环境与仓位）
#   industry          - 行业分析师（行业景气度与竞争格局）
#   financial_report  - 财报解读专家（财报解读与盈利质量）
#   risk              - 风控官（风险边界与止损）
#   full              - 完整个股分析（多Agent顺序协作）
#   decision          - 投资决策（多Agent并行+风控约束）

# 示例
python agent_bridge.py 600338 technical --format json
python agent_bridge.py 600519 full --format text
python agent_bridge.py 000001 decision --format json --no-cache
```

### 调用时机

- 当用户输入股票代码时，先调用 `data` 验证数据可用性
- 当用户问估值/基本面时，调用 `value`
- 当用户问技术面/趋势时，调用 `technical`
- 当用户问大盘/仓位时，调用 `macro`
- 当用户问行业/板块/赛道时，调用 `industry`
- 当用户问财报/盈利质量时，调用 `financial_report`
- 当用户问风险/止损时，调用 `risk`
- 当用户要求全面分析时，调用 `full`
- 当用户问"能不能买"时，调用 `decision`

### 缓存机制

- 默认启用文件缓存（TTL 300秒），同一天内相同股票+Agent类型不重复获取
- 使用 `--no-cache` 禁用缓存强制刷新
- 使用 `--cache-ttl` 自定义过期时间

### JSON输出结构

```json
{
  "agent": "Agent名称",
  "stock_code": "股票代码",
  "timestamp": "分析时间",
  "confidence": "高/中/低",
  "confidence_score": 0.0-1.0,
  "result": { ... },
  "cached": true/false
}
```

## 输出规范

无论哪个Agent主导，输出必须包含：

1. **Agent标识**：当前以什么角色在回答
2. **置信度**：高/中/低
3. **关键结论**：一句话总结
4. **详细分析**：结构化内容
5. **风险提示**：至少1条
6. **数据来源**：使用的数据及时间

## 约束

- 禁止在没有数据的情况下给出分析结论
- 禁止推荐没有止损位的交易
- 禁止单股仓位建议超过20%
- 必须标注分析的局限性和不确定性
