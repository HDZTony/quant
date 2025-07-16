# 159506 ETF基于NautilusTrader Cache的实时数据系统

## 🎯 系统概述

本系统基于NautilusTrader的Cache组件，实现了159506 ETF的实时数据采集、存储、分析和多策略共享功能。

## 📁 文件结构

```
quant/
├── etf_159506_cache_collector.py      # 基于Cache的数据采集器
├── etf_159506_cache_strategy.py       # 单策略示例
├── etf_159506_multi_strategy_demo.py  # 多策略共享演示
├── README_Cache_System.md             # 本说明文档
└── catalog/
    └── etf_159506_cache/              # Cache数据保存目录
```

## 🚀 核心功能

### 1. 实时数据存储
- **WebSocket连接**: 实时接收159506 ETF的Level1数据
- **Cache存储**: 将数据存储到NautilusTrader Cache中
- **Redis持久化**: 支持Redis数据库持久化存储
- **自动保存**: 定期将Cache数据保存为Parquet格式

### 2. 策略回测支持
- **历史数据访问**: 通过Cache API访问历史数据
- **数据格式标准化**: 使用NautilusTrader标准数据格式
- **回测兼容**: 数据可直接用于NautilusTrader回测引擎

### 3. 数据持久化
- **Redis支持**: 数据持久化到Redis数据库
- **内存缓存**: 高性能内存缓存
- **自动清理**: 定期清理旧数据

### 4. 多策略共享
- **数据共享**: 多个策略访问相同的Cache数据
- **策略协同**: 策略间可以共享分析结果
- **共识机制**: 综合多个策略的信号

## 🛠️ 安装和配置

### 1. 安装依赖

```bash
# 使用uv安装依赖
uv add nautilus-trader
uv add websocket-client
uv add pandas
uv add numpy
uv add requests
```

### 2. Redis配置（可选）

如果需要Redis持久化，请安装并启动Redis：

```bash
# Windows (使用WSL或Docker)
docker run -d -p 6379:6379 redis:latest

# 或者安装Redis服务器
# 下载地址: https://redis.io/download
```

### 3. 配置参数

在脚本中修改以下配置：

```python
# 基本配置
TOKEN = "your_jvquant_token"           # JVQuant API Token
STOCK_CODE = "159506"                  # 股票代码
USE_REDIS = True                       # 是否使用Redis
REDIS_HOST = "localhost"               # Redis主机
REDIS_PORT = 6379                      # Redis端口
```

## 📊 使用方法

### 1. 启动数据采集器

```bash
# 启动基于Cache的数据采集器
python etf_159506_cache_collector.py
```

**功能特点：**
- 实时接收WebSocket数据
- 存储到NautilusTrader Cache
- 支持Redis持久化
- 自动保存为Parquet格式
- 每1分钟输出诊断状态

### 2. 运行单策略分析

```bash
# 启动单策略分析
python etf_159506_cache_strategy.py
```

**策略功能：**
- 价格趋势分析
- 成交量分析
- 买卖盘压力分析
- 综合信号生成
- 每10秒分析一次

### 3. 运行多策略演示

```bash
# 启动多策略共享演示
python etf_159506_multi_strategy_demo.py
```

**策略列表：**
1. **Momentum策略**: 基于价格动量
2. **Volume策略**: 基于成交量异常
3. **Spread策略**: 基于买卖价差
4. **Consensus策略**: 综合其他策略结果

## 🔧 Cache API使用

### 1. 基本数据访问

```python
# 获取最新报价
latest_quote = cache.quote_tick(instrument_id)

# 获取最新交易
latest_trade = cache.trade_tick(instrument_id)

# 获取历史报价数据
quote_ticks = cache.quote_ticks(instrument_id)

# 获取历史交易数据
trade_ticks = cache.trade_ticks(instrument_id)
```

### 2. 数据统计

```python
# 获取数据数量
quote_count = cache.quote_tick_count(instrument_id)
trade_count = cache.trade_tick_count(instrument_id)

# 检查数据可用性
has_quotes = cache.has_quote_ticks(instrument_id)
has_trades = cache.has_trade_ticks(instrument_id)
```

### 3. 自定义数据存储

```python
# 存储自定义数据
cache.add("my_key", b"my_data")

# 获取自定义数据
data = cache.get("my_key")
```

## 📈 策略开发指南

### 1. 创建基础策略

```python
class MyStrategy(BaseStrategy):
    def __init__(self, cache: Cache, instrument_id: InstrumentId):
        super().__init__("My策略", cache, instrument_id)
        # 初始化策略参数
    
    def analyze(self):
        # 获取数据
        quote_ticks = self.cache.quote_ticks(self.instrument_id)
        
        # 进行分析
        # ...
        
        # 存储结果
        self.cache.add("strategy_analysis_My策略", json.dumps(result).encode())
```

### 2. 策略参数配置

```python
# 策略参数
self.lookback_period = 20      # 回看周期
self.threshold = 0.01          # 阈值
self.signal_strength = 0.5     # 信号强度
```

### 3. 信号生成

```python
# 生成交易信号
if condition > threshold:
    signal = "BUY"
    strength = min(condition * 100, 100)
elif condition < -threshold:
    signal = "SELL"
    strength = min(abs(condition) * 100, 100)
else:
    signal = "HOLD"
    strength = 0
```

## 🔍 监控和诊断

### 1. 系统状态监控

```python
# 获取系统状态
status = client.get_status()
print(f"连接状态: {status['connected']}")
print(f"数据接收: {status['data_receive_count']} 条")
print(f"Cache数据: {status['cache_status']['total_data']} 条")
```

### 2. 策略状态监控

```python
# 获取策略状态
strategy_status = strategy.get_strategy_status()
print(f"策略名称: {strategy_status['name']}")
print(f"分析次数: {strategy_status['analysis_count']}")
print(f"运行时间: {strategy_status['runtime']}")
```

### 3. 日志监控

系统会生成详细的日志文件：
- `etf_159506_cache_collector.log`: 数据采集器日志
- `etf_159506_cache_strategy.log`: 策略分析日志
- `etf_159506_multi_strategy.log`: 多策略系统日志

## 🎯 最佳实践

### 1. 数据管理

- **定期清理**: 使用`clear_old_data()`清理旧数据
- **容量控制**: 设置合适的`tick_capacity`和`bar_capacity`
- **持久化**: 重要数据及时保存到Parquet文件

### 2. 策略开发

- **数据检查**: 分析前检查数据充足性
- **异常处理**: 添加完善的异常处理机制
- **性能优化**: 避免频繁的数据查询

### 3. 系统集成

- **模块化设计**: 将数据采集和策略分析分离
- **配置管理**: 使用配置文件管理参数
- **监控告警**: 添加系统监控和告警机制

## 🔧 故障排除

### 1. 常见问题

**Q: 无法连接到Redis**
```
A: 检查Redis服务是否启动，端口是否正确
```

**Q: 数据接收中断**
```
A: 检查网络连接，WebSocket重连机制会自动处理
```

**Q: 策略分析失败**
```
A: 检查数据充足性，确保有足够的历史数据
```

### 2. 性能优化

- **内存使用**: 监控Cache内存使用情况
- **CPU使用**: 优化策略计算逻辑
- **网络延迟**: 选择合适的数据中心

## 📚 扩展开发

### 1. 添加新策略

1. 继承`BaseStrategy`类
2. 实现`analyze()`方法
3. 在策略管理器中注册

### 2. 集成其他数据源

1. 创建数据适配器
2. 转换为NautilusTrader格式
3. 存储到Cache中

### 3. 添加回测功能

1. 使用NautilusTrader的BacktestEngine
2. 配置历史数据源
3. 运行策略回测

## 📞 技术支持

如有问题，请检查：
1. 日志文件中的错误信息
2. 网络连接状态
3. Redis服务状态
4. 数据充足性

## 🎉 总结

本系统提供了完整的159506 ETF实时数据处理解决方案：

✅ **实时数据采集**: WebSocket + Cache存储
✅ **策略回测支持**: 标准化数据格式
✅ **数据持久化**: Redis + Parquet双重保障
✅ **多策略共享**: 协同分析和共识机制
✅ **易于扩展**: 模块化设计，支持自定义策略

通过NautilusTrader Cache的强大功能，您可以构建高性能、可扩展的量化交易系统！ 