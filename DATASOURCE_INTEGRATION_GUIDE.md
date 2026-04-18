# 数据源接入指南

## 当前可靠数据源

### ✅ 已验证可靠的数据源

| 数据源 | 用途 | 成功率 | 依赖 |
|--------|------|--------|------|
| **TickFlow** | 历史K线数据 | 100% | TICKFLOW_API_KEY |
| **baostock** | 财务数据、ROE、净利润 | 100% | 无 |
| **Tavily** | 网络搜索补充PE/PB | 100% | TAVILY_API_KEY |
| **akshare sina** | 实时行情、价格、市值 | 100% | 无 |

### ❌ 已禁用的数据源

| 数据源 | 问题 | 禁用原因 |
|--------|------|----------|
| efinance (东方财富) | 连接失败率>90% | API不稳定，严重拖慢性能 |
| akshare财务报表 | JSON解析失败 | API格式已变化 |

---

## 接入新数据源

如果您有更好的数据源API，可以按以下格式接入：

### 1. 数据源适配器接口

```python
class BaseDataSourceAdapter:
    """数据源适配器基类"""
    
    def get_stock_info(self, stock_code: str) -> Optional[Dict]:
        """获取股票基本信息"""
        pass
    
    def get_financial_data(self, stock_code: str) -> Optional[Dict]:
        """获取财务数据"""
        pass
    
    def get_realtime_quote(self, stock_code: str) -> Optional[Dict]:
        """获取实时行情"""
        pass
```

### 2. 需要返回的数据字段

#### 股票基本信息
```python
{
    'stock_code': str,      # 股票代码
    'stock_name': str,      # 股票名称
    'industry': str,         # 所属行业
    'list_date': str,       # 上市日期
    'total_share': float,   # 总股本
    'float_share': float,   # 流通股本
}
```

#### 财务数据
```python
{
    'pe': float,            # 市盈率
    'pb': float,            # 市净率
    'roe': float,           # 净资产收益率(%)
    'total_mv': float,      # 总市值
    'float_mv': float,      # 流通市值
    'eps': float,           # 每股收益
    'current_price': float, # 当前价格
    'net_profit': float,    # 净利润
    'revenue': float,       # 营业收入
    'gross_margin': float,   # 毛利率(%)
    'net_margin': float,     # 净利率(%)
    'free_cashflow': float, # 自由现金流
    'current_ratio': float, # 流动比率
    'debt_to_equity': float,# 债务权益比
    'roe_history': List[float],   # ROE历史
    'earnings_history': List[float],  # 盈利历史
    'fcf_history': List[float],  # 现金流历史
    'dividend_history': List[float], # 分红历史
}
```

#### 实时行情
```python
{
    'current_price': float, # 当前价格
    'total_mv': float,     # 总市值
    'float_mv': float,     # 流通市值
    'total_share': float,  # 总股本
    'float_share': float,  # 流通股本
    'pe': float,           # 市盈率
    'pb': float,           # 市净率
}
```

### 3. 接入步骤

1. 创建适配器类（继承 BaseDataSourceAdapter）
2. 在 `ultimate_data_fetcher.py` 中注册新数据源
3. 测试数据完整性和准确性
4. 验证与现有数据源的数据一致性

### 4. 集成到 UltimateDataFetcher

```python
# 在 _init_data_sources 中添加
try:
    from your_data_source import YourDataSourceAdapter
    self._your_source = YourDataSourceAdapter()
    logger.info("YourDataSource 初始化成功")
except Exception as e:
    logger.error(f"YourDataSource 初始化失败: {e}")

# 在 get_stock_data 中调用
def get_stock_data(self, stock_code: str):
    # ... 其他数据源调用
    
    # 添加新数据源
    if self._your_source:
        your_data = self._your_source.get_stock_info(stock_code)
        # 合并数据
```

---

## 数据源质量标准

新数据源需要满足以下标准才能接入：

| 指标 | 要求 | 验证方法 |
|------|------|----------|
| API可用性 | > 90% | 连续测试10次 |
| 数据完整性 | > 80% | 关键字段非空率 |
| 数据准确性 | 偏差 < 10% | 与已知准确数据对比 |
| 响应速度 | < 5秒 | 单次请求计时 |
| 数据时效性 | < 24小时 | 数据更新时间戳 |

---

## 推荐的新数据源类型

如果您有以下类型的API，可以考虑接入：

1. **实时行情API** - 替代/补充 akshare sina
2. **财务报表API** - 替代/补充 baostock（财务指标、历史ROE）
3. **基本面数据API** - 提供更全面的财务指标
4. **新闻/公告API** - 补充 Tavily（AI分析用）

---

## 联系提供API

如需接入新数据源，请提供：
1. API地址和文档链接
2. 认证方式（API Key / OAuth / 无认证）
3. 支持的数据类型
4. 请求频率限制
5. 测试用的股票代码
