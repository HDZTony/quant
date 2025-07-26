# 策略反向分析工具使用指南

## 概述

这个工具可以通过分析你的tick数据和买卖点标记，自动推算出策略逻辑。它使用机器学习方法从实际交易结果反推策略规则。

## 功能特点

### 🔍 **数据分析**
- 分析tick数据中的价格、成交量、买卖盘等特征
- 识别买卖信号的时间模式和特征模式
- 提取关键的技术指标和阈值

### 📊 **统计分析**
- 基于信号区分度计算特征重要性
- 自动识别最重要的特征
- 生成特征重要性排序

### 📊 **可视化**
- 价格和信号图表
- 特征重要性分析
- 信号时间分布
- 价格变化分布

### 💻 **代码生成**
- 自动生成可执行的策略代码
- 基于数据分析得出的参数
- 包含完整的交易逻辑

## 使用方法

### 1. 准备数据

你需要提供两个CSV文件：

#### tick_data.csv（tick数据）
```csv
timestamp,price,volume,bid_price,ask_price,bid_size,ask_size
2024-01-01 09:30:00,1.000,500000,0.999,1.001,5000,5000
2024-01-01 09:30:01,1.001,600000,1.000,1.002,6000,6000
...
```

**必需列：**
- `timestamp`: 时间戳
- `price`: 价格

**可选列：**
- `volume`: 成交量
- `bid_price`: 买价
- `ask_price`: 卖价
- `bid_size`: 买盘大小
- `ask_size`: 卖盘大小

#### signals.csv（买卖信号）
```csv
timestamp,signal
2024-01-01 09:30:00,0
2024-01-01 09:30:01,1
2024-01-01 09:30:02,-1
...
```

**信号含义：**
- `1`: 买入信号
- `-1`: 卖出信号
- `0`: 无信号

### 2. 运行分析

```bash
# 生成示例数据（可选）
python generate_sample_data.py

# 运行策略反向分析
python strategy_reverse_engineering.py
```

### 3. 使用方法

#### 方法一：使用CSV文件
在`strategy_reverse_engineering.py`中，找到以下代码段并取消注释：

```python
try:
    # 加载数据
    analyzer.load_data('tick_data.csv', 'signals.csv')
    
    # 分析模式
    analyzer.analyze_patterns()
    
    # 生成策略规则
    rules = analyzer.generate_strategy_rules()
    
    # 可视化分析
    analyzer.visualize_analysis()
    
    # 生成策略代码
    strategy_code = analyzer.generate_strategy_code(rules)
    
    # 保存策略代码
    with open('reverse_engineered_strategy.py', 'w', encoding='utf-8') as f:
        f.write(strategy_code)
    
    print("策略反向分析完成！")
    print("生成的策略代码已保存为: reverse_engineered_strategy.py")
    
except Exception as e:
    logger.error(f"分析失败: {e}")
```

#### 方法二：从catalog loader加载数据
```python
from strategy_reverse_engineering import StrategyReverseEngineering
from datetime import date

# 创建分析工具
analyzer = StrategyReverseEngineering()

# 从catalog loader加载今天的数据
analyzer.load_from_catalog_loader()

# 从catalog loader加载指定日期的数据
analyzer.load_from_catalog_loader(target_date=date(2024, 1, 15))

# 进行分析
analyzer.analyze_patterns()
rules = analyzer.generate_strategy_rules()
analyzer.visualize_analysis()
strategy_code = analyzer.generate_strategy_code(rules)
```

#### 方法三：从Redis cache加载数据
```python
from strategy_reverse_engineering import StrategyReverseEngineering

# 创建分析工具
analyzer = StrategyReverseEngineering()

# 从Redis cache加载数据
analyzer.load_from_redis_cache(redis_host="localhost", redis_port=6379, limit=1000)

# 进行分析
analyzer.analyze_patterns()
rules = analyzer.generate_strategy_rules()
analyzer.visualize_analysis()
strategy_code = analyzer.generate_strategy_code(rules)
```

## 输出结果

### 1. 分析报告
- 信号分布统计
- 特征重要性排序
- 模型准确率评估
- 买卖信号的特征模式

### 2. 可视化图表
- `strategy_analysis.png`: 包含4个子图的分析图表

### 3. 策略代码
- `reverse_engineered_strategy.py`: 自动生成的策略代码

## 生成的策略特点

### 📈 **特征驱动**
- 基于数据分析得出的特征阈值
- 多维度特征组合判断
- 动态权重分配

### 🎯 **规则清晰**
- 明确的买入/卖出条件
- 可解释的交易理由
- 信号强度评估

### ⚡ **实时执行**
- 支持实时tick数据处理
- 自动生成交易信号
- 完整的仓位管理

## 示例输出

```
=== 模型评估 ===
准确率: 0.8234

分类报告:
              precision    recall  f1-score   support
          -1       0.85      0.78      0.81        45
           0       0.82      0.85      0.83       890
           1       0.79      0.82      0.80        38

特征重要性Top10:
   feature  importance
0  price_ma_5    0.156
1  volume_ratio    0.134
2  bid_ask_ratio    0.098
3  momentum_5    0.087
4  volatility_5    0.076
...
```

## 注意事项

### ⚠️ **数据质量**
- 确保tick数据的时间戳准确
- 信号标记要基于实际交易决策
- 数据量要足够大（建议至少1000条记录）

### 🔧 **参数调优**
- 生成的特征阈值可能需要微调
- 根据实际市场情况调整参数
- 建议在模拟环境中先测试

### 📊 **结果验证**
- 生成的策略需要回测验证
- 注意过拟合问题
- 考虑市场环境变化的影响

## 扩展功能

### 🚀 **高级分析**
- 添加更多技术指标
- 支持多品种分析
- 时间序列特征工程

### 🤖 **深度学习**
- 使用神经网络模型
- 序列预测能力
- 更复杂的模式识别

### 📈 **策略优化**
- 遗传算法优化参数
- 多目标优化
- 风险调整收益

## 技术支持

如果遇到问题，请检查：
1. 数据格式是否正确
2. 依赖包是否安装完整
3. 文件路径是否正确
4. 数据量是否足够

## 依赖包

```bash
pip install pandas numpy matplotlib seaborn

# 可选依赖（用于Redis和catalog功能）
pip install redis nautilus-trader pytz
```

---

**提示**: 这个工具可以帮助你快速理解现有策略的逻辑，但生成的策略需要在实际使用前进行充分的测试和验证。 