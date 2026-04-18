# 股票数据API获取指南

## 一、免费API推荐（个人用户首选）

### 1. Tushare Pro ⭐推荐
**官网**: https://tushare.pro/

| 项目 | 说明 |
|------|------|
| **数据覆盖** | A股日线/分钟线、财务数据、龙虎榜、北向资金、指数、基金、期货 |
| **免费额度** | 注册后120积分，每日8000次基础调用 |
| **数据格式** | pandas DataFrame，直接可用 |
| **积分获取** | 注册+实名认证=120积分，2000积分≈200元/年 |

**安装**:
```bash
pip install tushare -i https://pypi.tuna.tsinghua.edu.cn/simple
```

**使用示例**:
```python
import tushare as ts

# 初始化（替换为你的token）
pro = ts.pro_api('你的TOKEN')

# 获取日线数据
df = pro.daily(ts_code='600519.SH', start_date='20250101')

# 获取财务指标
df = pro.fina_indicator(ts_code='600519.SH')
```

---

### 2. baostock（已集成）
**官网**: https://www.baostock.com/

| 项目 | 说明 |
|------|------|
| **数据覆盖** | A股日线/分钟线、财务报表、指数数据 |
| **免费额度** | 完全免费，无需注册 |
| **数据格式** | pandas DataFrame |

**优势**: ✅ 已集成到你的系统，无需额外配置

---

### 3. 东方财富/新浪（已禁用）
- ❌ efinance: 连接不稳定，已禁用
- ✅ akshare sina: 实时行情可用，已保留

---

### 4. iTick
**官网**: https://itick.org/

| 项目 | 说明 |
|------|------|
| **数据覆盖** | A股实时行情、K线 |
| **免费额度** | 永久免费套餐（基础数据） |
| **特点** | 稳定可靠，适合个人开发者 |

---

## 二、付费API推荐（专业量化）

### 1. Tushare Pro 付费版

| 套餐 | 价格 | 每日额度 | 适合场景 |
|------|------|---------|----------|
| 基础 | 200元/年 | 100万次 | 个人量化 |
| 专业 | 500元/年 | 无限制 | 高频策略 |
| 机构 | 2000元/年 | 无限制 | 团队使用 |

**付费项目**:
- 历史分钟数据: 2000元/年
- 实时分钟数据: 1000元/月
- 港股数据: 1000元/年

---

### 2. 东方财富 Choice
**官网**: https://choice.eastmoney.com/

| 项目 | 价格 |
|------|------|
| 基础接口 | 3000-5000元/年 |
| 实时行情 | 500-2000元/月 |
| 机构版 | 数万元/年 |

**特点**: 
- 数据最全面（机构级）
- L2十档行情
- 主力资金流向

---

### 3. 聚宽 JoinQuant
**官网**: https://www.joinquant.com/

| 项目 | 价格 |
|------|------|
| 回测/模拟 | 免费 |
| 实盘对接 | 50-500元/月 |

---

## 三、官方免费数据源

### 1. 交易所官网
- **上交所**: https://www.sse.com.cn/
- **深交所**: https://www.szse.cn/

**数据类型**:
- ✅ 每日收盘行情（CSV下载）
- ✅ 财务报告
- ✅ 公告信息
- ⚠️ 需要自己解析

### 2. 证监会/证券业协会
- 北向资金数据
- 融资融券余额

---

## 四、针对你的项目推荐

### 方案一：低成本方案（Tushare免费版）
1. 注册 Tushare Pro: https://tushare.pro/register
2. 实名认证获得120积分
3. 获取Token
4. 安装: `pip install tushare`
5. 在 `ultimate_data_fetcher.py` 中集成

**优点**: 免费、简单、数据全面
**缺点**: 免费版有频率限制

---

### 方案二：提升Tushare积分
1. 社区贡献获取积分
2. 付费升级（200元/年=2000积分）
3. 积分权益:
   - 2000积分: 每日100万次调用
   - 5000积分: 无限制+特色数据

---

### 方案三：商业数据（推荐机构用户）
- **Choice数据**: 数千元/年，适合专业量化
- **万得Wind**: 数万元/年，机构首选

---

## 五、Tushare接入代码模板

```python
# 在 ultimate_data_fetcher.py 中添加

def __init__(self):
    # ... 现有代码 ...
    
    # 添加Tushare
    try:
        import tushare as ts
        self._tushare = ts
        self._tushare_pro = ts.pro_api('你的TOKEN')
        logger.info("Tushare 初始化成功")
    except Exception as e:
        logger.warning(f"Tushare 初始化失败: {e}")
        self._tushare = None
        self._tushare_pro = None

def get_tushare_data(self, stock_code):
    """使用Tushare获取数据"""
    if not self._tushare_pro:
        return None
    
    try:
        # 日线数据
        df = self._tushare_pro.daily(
            ts_code=self._convert_tushare_code(stock_code),
            start_date='20200101'
        )
        
        # 财务指标
        fi = self._tushare_pro.fina_indicator(
            ts_code=self._convert_tushare_code(stock_code)
        )
        
        return {'daily': df, 'financial': fi}
    except Exception as e:
        logger.warning(f"Tushare数据获取失败: {e}")
        return None
```

---

## 六、快速开始清单

1. ☐ 注册 Tushare Pro: https://tushare.pro/register
2. ☐ 完成实名认证（获得120积分）
3. ☐ 获取API Token
4. ☐ 安装tushare: `pip install tushare`
5. ☐ 集成到 `ultimate_data_fetcher.py`
6. ☐ 测试数据获取
7. ☐ 验证数据准确性

---

## 七、推荐组合

### 个人用户（免费）
- ✅ baostock（已集成，财务数据）
- ✅ akshare sina（已集成，实时行情）
- ✅ Tushare免费版（补充分钟数据）
- ✅ TickFlow（历史K线，已配置）

### 专业用户（付费）
- ✅ Tushare专业版（200-500元/年）
- ✅ baostock（备份）
- ✅ Choice数据（如需L2行情）

---

## 八、注意事项

1. **频率限制**: 免费API都有调用频率限制，需要添加延迟
2. **数据质量**: 不同来源数据可能有微小差异，建议优先使用一个主要来源
3. **时效性**: 实时数据需要付费API，免费API通常有15分钟延迟
4. **合规使用**: 遵守API使用条款，不要高频爬取
