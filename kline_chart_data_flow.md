# 回测K线图数据传递过程详解

## 完整数据流程

### 1. 数据源阶段
```
原始Tick数据 → Redis缓存 → Catalog存储
```

**数据格式：**
```python
[
    {
        'timestamp': Timestamp('2025-09-12 09:30:20.324734900+0800'),
        'price': 1.678,
        'volume': 147200,
        'trade_id': 'trade_5236',
        'source': 'catalog'
    },
    # ... 更多tick数据
]
```

### 2. 回测系统阶段
```
ETF159506OfficialBacktest.display_backtest_chart()
    ↓
创建 catalog_loader = ETF159506RedisKlineGenerator()
    ↓
传递参数：
- save_path: 图片保存路径
- target_date: 目标日期
- trade_signals: 交易信号列表
- technical_signals: 技术指标信号列表
```

### 3. 数据获取阶段
```
catalog_loader.create_realtime_kline_chart()
    ↓
调用 get_today_kline_data(target_date)
    ↓
数据来源优先级：
1. Redis缓存 (cache.trade_ticks())
2. Catalog文件 (ParquetDataCatalog)
    ↓
返回格式化的K线数据列表
```

### 4. 数据转换阶段
```
get_today_kline_data() 返回的数据
    ↓
_plot_kline_chart() 中的处理：
    ↓
pd.DataFrame(kline_data)  # 转换为DataFrame
    ↓
pd.to_datetime(df['timestamp'])  # 时间格式转换
    ↓
时区转换 (UTC → 北京时间)
    ↓
按时间排序和过滤交易时间
```

### 5. 图表绘制阶段
```
处理后的DataFrame
    ↓
matplotlib + mplfinance 绘制
    ↓
添加交易信号标记：
- trade_signals: 买卖点
- technical_signals: 技术指标信号
    ↓
保存为PNG文件
```

## 关键方法调用链

```
main()
    ↓
backtest_system.run_july_25_backtest()
    ↓
self.run_backtest(start_date, end_date)
    ↓
self.collect_trade_signals(result, backtest_node)  # 收集交易信号
    ↓
self.display_backtest_chart(start_date, end_date)  # 显示图表
    ↓
catalog_loader.create_realtime_kline_chart(
    save_path=image_filename,
    target_date=start_date,
    trade_signals=self.trade_signals,      # 交易信号传递
    technical_signals=self.technical_signals  # 技术信号传递
)
    ↓
_plot_kline_chart()  # 实际绘图方法
    ↓
get_today_kline_data()  # 获取K线数据
    ↓
matplotlib绘图 + 保存文件
```

## 数据传递的关键参数

### 1. 交易信号 (trade_signals)
```python
self.trade_signals = [
    {
        'timestamp': '2025-09-12 09:30:20',
        'side': 'BUY',
        'price': 1.678,
        'volume': 1000,
        'reason': 'golden_cross'
    },
    # ... 更多交易信号
]
```

### 2. 技术指标信号 (technical_signals)
```python
self.technical_signals = [
    {
        'timestamp': '2025-09-12 09:30:20',
        'signal_type': 'golden_cross',
        'price': 1.678,
        'ema_fast': 1.675,
        'ema_slow': 1.673
    },
    # ... 更多技术信号
]
```

### 3. K线数据 (kline_data)
```python
kline_data = [
    {
        'timestamp': '2025-09-12 09:30:00',
        'open': 1.678,
        'high': 1.682,
        'low': 1.675,
        'close': 1.681,
        'volume': 1027300
    },
    # ... 更多K线数据
]
```

## 图表生成的文件

1. **主K线图**: `etf_159506_backtest_20250912.png`
2. **买卖点图**: `etf_159506_trade_points_20250912.png`  
3. **极值点图**: `etf_159506_extremes_20250912.png`

## 数据流向总结

```
原始Tick数据 → Redis/Catalog → DataFrame → 图表绘制 → PNG文件
                ↑
            交易信号 + 技术信号 (从回测结果收集)
```
