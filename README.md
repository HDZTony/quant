# jvQuant集成交易系统

基于jvQuant API的实时行情数据接入、技术指标计算和自动交易系统，支持MACD、RSI、KDJ、量比等技术指标的实时计算、图表展示和智能交易。

## 功能特性

### 📊 技术指标
- **MACD**：移动平均收敛发散指标（使用Nautilus Trader官方实现）
- **RSI**：相对强弱指数（使用Nautilus Trader官方实现）
- **KDJ**：随机指标（自定义实现）
- **量比**：成交量比率
- **K线图**：实时K线数据
- **成交量**：实时成交量数据

### 🔄 数据源
- **Level1**：基础行情数据（价格、成交量、涨跌幅等）
- **Level2**：逐笔成交数据
- **Level10**：十档盘口数据

### 📈 图表展示
- 使用mplfinance绘制专业K线图
- 实时更新技术指标图表
- 多指标组合展示

### 💹 交易功能
- **登录柜台**：支持券商柜台登录
- **交易委托**：买入/卖出股票
- **撤销委托**：撤销未成交委托
- **查询功能**：查询持仓和交易记录
- **自动交易**：基于技术指标的智能交易策略
- **图形界面**：友好的GUI操作界面

## 文件说明

### 核心文件
- `integrated_trading_system.py` - 完整的集成交易系统（GUI界面）
- `trading_system.py` - 交易功能模块
- `jvquant_realtime_system.py` - 完整的实时系统（包含图表功能）
- `simple_realtime_system.py` - 简化的实时系统（用于测试）
- `macd_strategy.py` - 参照官方示例的MACD策略实现
- `test_integrated_system.py` - 集成系统测试脚本
- `kdj_indicator.py` - KDJ指标实现
- `ema_comparison.py` - EMA指标对比示例

### 配置文件
- `requirements.txt` - Python依赖包列表
- `README.md` - 项目说明文档

## 安装和配置

### 1. 环境要求
- Python 3.11+
- 虚拟环境（推荐）

### 2. 安装依赖
```bash
# 激活虚拟环境
& "C:\Users\hedz\Downloads\nautilus_trader-develop\.venv\Scripts\Activate.ps1"

# 安装依赖
pip install -r requirements.txt
```

### 3. 配置Token
在代码中配置你的jvQuant Token：
```python
TOKEN = "d0c519adcd47d266f1c96750d4e80aa6"  # 你的Token
STOCK_CODE = "159506"  # 股票代码
```

## 使用方法

### 1. 测试连接
首先测试Token和服务器连接是否正常：
```bash
python test_jvquant_connection.py
```

### 2. 运行简化版本
测试基本的数据接收和指标计算：
```bash
python simple_realtime_system.py
```

### 3. 运行完整版本
运行包含图表功能的完整系统：
```bash
python jvquant_realtime_system.py
```

### 4. 运行集成交易系统
运行包含GUI界面的完整交易系统：
```bash
python integrated_trading_system.py
```

### 5. 运行交易系统
运行命令行版本的交易系统：
```bash
python trading_system.py
```

### 6. 运行系统测试
运行集成系统功能测试：
```bash
python test_integrated_system.py
```

## 数据格式说明

### Level1数据格式
```
lv1_证券代码=推送时间,证券名称,最新价格,涨幅,成交额,成交量,买五档,卖五档
```

### Level2数据格式
```
lv2_证券代码=成交时间1,成交编号1,成交价格1,成交数量1|成交时间2,成交编号2,成交价格2,成交数量2...
```

### Level10数据格式
```
lv10_证券代码=推送时间,证券名称,最新价格,昨收,成交额,成交量,买十档,卖十档
```

## 技术指标说明

### MACD (移动平均收敛发散)
- **快线周期**: 12
- **慢线周期**: 26
- **信号线周期**: 9
- **用途**: 判断趋势变化和买卖时机
- **实现**: 使用Nautilus Trader官方MovingAverageConvergenceDivergence指标
- **策略**: 参照官方示例实现，支持做多和做空

### RSI (相对强弱指数)
- **周期**: 14
- **超买线**: 70
- **超卖线**: 30
- **用途**: 判断超买超卖状态
- **实现**: 使用Nautilus Trader官方RelativeStrengthIndex指标

### KDJ (随机指标)
- **K周期**: 9
- **D周期**: 3
- **J计算**: J = 3K - 2D
- **用途**: 判断价格动量和买卖信号

### 量比
- **周期**: 5
- **计算**: 当前成交量 / 过去5期平均成交量
- **用途**: 判断成交量活跃程度

## 交易功能说明

### 登录柜台
- 支持券商柜台登录验证
- 获取交易凭证（ticket）
- 自动管理登录状态和凭证有效期

### 交易委托
- **买入委托**：支持市价和限价买入
- **卖出委托**：支持市价和限价卖出
- **委托参数**：证券代码、名称、价格、数量

### 查询功能
- **持仓查询**：查看当前持仓情况
- **交易记录**：查看历史交易记录
- **委托状态**：查看委托执行状态

### 自动交易策略
- **信号生成**：基于MACD、RSI、KDJ综合信号
- **风险控制**：持仓管理和资金控制
- **策略监控**：实时监控策略执行状态

## 数据保存

系统会自动保存历史数据到本地文件：
- 格式：`data_股票代码_时间戳.pkl`
- 包含：OHLC数据、逐笔数据、技术指标值

## 注意事项

### 1. 交易时间
- 沪深市场：9:30-11:30, 13:00-15:00
- 非交易时间可能无法获取实时数据

### 2. 数据频率
- Level1：基础行情，更新频率较低
- Level2：逐笔成交，更新频率高
- Level10：十档盘口，更新频率中等

### 3. 网络要求
- 稳定的网络连接
- 支持WebSocket协议
- 建议使用有线网络

### 4. 资源消耗
- 实时数据处理会消耗一定CPU和内存
- 长时间运行建议监控系统资源

## 故障排除

### 1. 连接失败
- 检查Token是否正确
- 确认网络连接正常
- 查看服务器分配是否成功

### 2. 数据解析错误
- 检查数据格式是否符合预期
- 确认股票代码是否正确
- 查看日志输出

### 3. 指标计算异常
- 确认数据量是否足够
- 检查指标参数设置
- 验证计算逻辑

## 重构说明

### 删除自定义MACD实现
为了与Nautilus Trader官方示例保持一致，项目已删除所有自定义的`SimpleMACD`实现：

#### 删除的文件和类：
- `simple_realtime_system.py` 中的 `SimpleMACD` 类
- `integrated_trading_system.py` 中的 `SimpleMACD` 类  
- `tests/test_integrated_system.py` 中的 `SimpleMACD` 类

#### 重构的内容：
- 所有MACD指标现在使用Nautilus Trader的 `MovingAverageConvergenceDivergence`
- 所有RSI指标现在使用Nautilus Trader的 `RelativeStrengthIndex`
- 新增 `macd_strategy.py` 文件，参照官方示例实现MACD策略
- 更新了所有相关的指标更新和信号生成逻辑

#### 优势：
- 更好的性能和准确性
- 与官方API保持一致
- 更容易维护和扩展
- 支持更多高级功能

## 扩展功能

### 1. 添加新指标
在`RealTimeDataProcessor`类中添加新的技术指标：
```python
def add_new_indicator(self):
    # 实现新指标的计算逻辑
    pass
```

### 2. 自定义图表
修改`generate_charts`方法来自定义图表样式：
```python
def generate_charts(self, indicators: dict):
    # 自定义图表绘制逻辑
    pass
```

### 3. 数据导出
添加数据导出功能：
```python
def export_data(self, format='csv'):
    # 实现数据导出逻辑
    pass
```

## 许可证

本项目基于GNU Lesser General Public License v3.0开源协议。

## 联系方式

如有问题或建议，请通过以下方式联系：
- 项目Issues
- 邮件联系
- 技术讨论群

---

**免责声明**: 本系统仅供学习和研究使用，不构成投资建议。使用本系统进行实际交易的风险由用户自行承担。 
# 手动立刻执行一次
schtasks /run /tn ETF159506_1min_Collector

# 查看任务状态
schtasks /query /tn ETF159506_1min_Collector

# 删除任务
schtasks /delete /tn ETF159506_1min_Collector /f