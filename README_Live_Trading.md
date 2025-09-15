# 159506 ETF 实时交易系统使用指南

## 概述

本系统是基于NautilusTrader框架开发的159506港股通医疗ETF富国实时交易系统。系统包含完整的实时数据接收、策略执行、风险管理和监控功能。

## 系统架构

```
ETF159506实时交易系统
├── 配置管理 (etf_159506_live_config.py)
├── 数据客户端 (etf_159506_data_client.py)
├── 执行客户端 (etf_159506_exec_client.py)
├── 风险管理 (etf_159506_risk_manager.py)
├── 主程序 (etf_159506_live_trading.py)
└── 测试系统 (test_etf_159506_live_system.py)
```

## 快速开始

### 1. 环境准备

确保已安装所需依赖：

```bash
# 使用uv安装依赖
uv add nautilus-trader
uv add pandas numpy matplotlib
uv add redis pyarrow
```

### 2. 启动Redis服务

```bash
# Windows
redis-server

# Linux/Mac
sudo systemctl start redis
```

### 3. 运行测试网络模式

```bash
# 运行测试网络模式（推荐首次使用）
uv run etf_159506_live_trading.py --mode testnet
```

### 4. 运行生产环境模式

```bash
# 运行生产环境模式（需要真实数据源和执行接口）
uv run etf_159506_live_trading.py --mode production
```

## 配置说明

### 主配置文件 (etf_159506_live_config.py)

```python
# 创建测试网络配置
config = create_testnet_live_config()

# 创建生产环境配置
config = create_production_live_config()

# 自定义配置
config = ETF159506LiveConfig(
    trader_id="MY-TRADER-001",
    testnet=True,
    starting_balance="100000 CNY",
    redis_host="localhost",
    redis_port=6379,
)
```

### 数据客户端配置

```python
data_config = ETF159506DataClientConfig(
    simulate_real_data=True,  # 是否使用模拟数据
    base_price=1.0,          # 基础价格
    volatility=0.02,         # 波动率
    update_interval_ms=1000, # 更新间隔
)
```

### 执行客户端配置

```python
exec_config = ETF159506ExecClientConfig(
    simulate_execution=True,  # 是否使用模拟执行
    execution_delay_ms=100,   # 执行延迟
    fill_probability=0.95,    # 成交概率
    slippage_bps=5,           # 滑点（基点）
)
```

## 系统功能

### 1. 实时数据接收

- **K线数据**: 1分钟K线数据生成和订阅
- **报价数据**: 实时买卖报价
- **交易数据**: 实时成交数据
- **模拟数据**: 基于几何布朗运动的模拟价格生成

### 2. 策略执行

- **MACD策略**: 基于MACD金叉死叉的交易策略
- **背离检测**: 价格与MACD背离信号检测
- **定时交易**: 每日2:50分定时买入功能
- **风险管理**: 止损止盈和持仓管理

### 3. 风险管理

- **持仓限制**: 最大持仓数量控制
- **日亏损限制**: 每日最大亏损控制
- **回撤控制**: 最大回撤百分比限制
- **保证金管理**: 保证金比例监控
- **实时警报**: 风险等级和警报系统

### 4. 监控功能

- **系统状态**: 实时系统状态监控
- **性能指标**: 交易性能统计
- **风险指标**: 实时风险指标计算
- **日志记录**: 完整的操作日志

## 使用示例

### 基本使用

```python
import asyncio
from etf_159506_live_trading import ETF159506LiveTradingSystem
from etf_159506_live_config import create_testnet_live_config
from etf_159506_data_client import ETF159506DataClientConfig
from etf_159506_exec_client import ETF159506ExecClientConfig

async def main():
    # 创建配置
    config = create_testnet_live_config()
    data_config = ETF159506DataClientConfig(simulate_real_data=True)
    exec_config = ETF159506ExecClientConfig(simulate_execution=True)
    
    # 创建交易系统
    trading_system = ETF159506LiveTradingSystem(
        config=config,
        data_config=data_config,
        exec_config=exec_config,
    )
    
    # 启动系统
    await trading_system.start()

if __name__ == "__main__":
    asyncio.run(main())
```

### 风险管理使用

```python
from etf_159506_risk_manager import RiskManager, RiskMonitor

# 创建风险管理器
risk_manager = RiskManager(
    max_position_size=100000,
    max_daily_loss=5000.0,
    max_drawdown_pct=0.10,
)

# 添加警报回调
def alert_callback(alert):
    print(f"风险警报: {alert.severity.value} - {alert.message}")

risk_manager.add_alert_callback(alert_callback)

# 创建风险监控器
risk_monitor = RiskMonitor(risk_manager)

# 启动监控
await risk_monitor.start_monitoring(interval_seconds=60)
```

## 测试和验证

### 运行系统测试

```bash
# 运行完整系统测试
uv run test_etf_159506_live_system.py
```

测试包括：
- 配置验证测试
- 数据客户端测试
- 执行客户端测试
- 风险管理器测试
- 集成系统测试
- 性能测试

### 测试结果示例

```
ETF159506 实时交易系统测试报告
================================================================================
总测试数: 5
通过测试: 5
失败测试: 0
成功率: 100.0%
测试耗时: 15.23秒

详细测试结果:
------------------------------------------------------------
✅ configuration: PASSED
✅ data_client: PASSED
✅ exec_client: PASSED
✅ risk_manager: PASSED
✅ integrated_system: PASSED
================================================================================
```

## 部署建议

### 测试环境部署

1. **使用模拟数据**: 设置`simulate_real_data=True`
2. **使用模拟执行**: 设置`simulate_execution=True`
3. **较低频率更新**: 设置`update_interval_ms=2000`
4. **保守风险参数**: 使用较小的持仓和亏损限制

### 生产环境部署

1. **连接真实数据源**: 设置`simulate_real_data=False`
2. **连接真实执行接口**: 设置`simulate_execution=False`
3. **高频更新**: 设置`update_interval_ms=500`
4. **严格风险控制**: 使用保守的风险管理参数

## 注意事项

### 安全提醒

1. **测试网络优先**: 首次使用请务必在测试网络模式下运行
2. **资金管理**: 设置合理的初始资金和风险限制
3. **监控日志**: 定期检查系统日志和风险警报
4. **备份配置**: 定期备份重要配置和交易数据

### 性能优化

1. **Redis配置**: 确保Redis服务稳定运行
2. **网络延迟**: 考虑网络延迟对交易的影响
3. **系统资源**: 监控CPU和内存使用情况
4. **数据存储**: 定期清理历史数据

### 故障排除

1. **连接问题**: 检查Redis连接和网络状态
2. **数据问题**: 验证数据源连接和数据格式
3. **执行问题**: 检查执行接口和订单状态
4. **风险问题**: 查看风险警报和处理建议

## 技术支持

如遇到问题，请：

1. 查看系统日志文件
2. 运行系统测试验证
3. 检查配置参数设置
4. 参考NautilusTrader官方文档

## 更新日志

- **v1.0.0**: 初始版本，包含基本实时交易功能
- 支持MACD策略和背离检测
- 完整的风险管理和监控系统
- 模拟数据和生产环境支持

---

**免责声明**: 本系统仅供学习和研究使用，实际交易存在风险，请谨慎使用并做好风险管理。
