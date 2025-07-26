#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
策略反向分析工具
通过分析tick数据和买卖点标记，推算出策略逻辑
"""

import pandas as pd
import numpy as np
import matplotlib.pyplot as plt
from datetime import datetime, timedelta
from typing import Dict, List, Tuple, Optional
import logging
import seaborn as sns

# 配置日志
logging.basicConfig(
    level=logging.INFO,
    format='%(asctime)s - %(levelname)s - %(message)s'
)
logger = logging.getLogger(__name__)


class StrategyReverseEngineering:
    """策略反向分析工具"""
    
    def __init__(self):
        self.tick_data = None
        self.signal_data = None
        self.features = None
        self.strategy_rules = None
        
    def load_data(self, tick_file: str, signal_file: str):
        """加载tick数据和买卖信号数据"""
        try:
            # 加载tick数据
            self.tick_data = pd.read_csv(tick_file)
            logger.info(f"加载tick数据: {len(self.tick_data)}条记录")
            
            # 加载信号数据
            self.signal_data = pd.read_csv(signal_file)
            logger.info(f"加载信号数据: {len(self.signal_data)}条记录")
            
            # 数据预处理
            self._preprocess_data()
            
        except Exception as e:
            logger.error(f"加载数据失败: {e}")
            raise
    

    
    def load_from_catalog_loader(self, catalog_path: str = "catalog/etf_159506_cache", target_date: datetime.date = None):
        """从catalog loader加载数据"""
        try:
            from pathlib import Path
            import pytz
            
            catalog_path = Path(catalog_path)
            if not catalog_path.exists():
                raise FileNotFoundError(f"Catalog路径不存在: {catalog_path}")
            
            # 如果没有指定日期，使用今天
            if target_date is None:
                target_date = datetime.now().date()
            
            logger.info(f"从catalog加载{target_date}的数据...")
            
            # 查找所有parquet文件
            parquet_files = list(catalog_path.glob("*.parquet"))
            if not parquet_files:
                raise FileNotFoundError(f"Catalog目录中没有parquet文件: {catalog_path}")
            
            # 读取所有文件并合并数据
            all_dataframes = []
            target_files = []
            
            for file_path in parquet_files:
                try:
                    # 读取数据
                    df = pd.read_parquet(file_path)
                    
                    # 确保timestamp列存在
                    if 'timestamp' not in df.columns:
                        continue
                    
                    # 转换timestamp
                    df['timestamp'] = pd.to_datetime(df['timestamp'])
                    
                    # 检查是否需要时区转换
                    if df['timestamp'].dt.tz is None:
                        # 假设数据是UTC时间，转换为北京时间
                        utc_tz = pytz.UTC
                        beijing_tz = pytz.timezone('Asia/Shanghai')
                        
                        # 添加UTC时区信息
                        df['timestamp'] = df['timestamp'].dt.tz_localize(utc_tz)
                        # 转换为北京时间
                        df['timestamp'] = df['timestamp'].dt.tz_convert(beijing_tz)
                    
                    # 检查是否包含目标日期数据
                    file_dates = df['timestamp'].dt.date.unique()
                    if target_date in file_dates:
                        all_dataframes.append(df)
                        target_files.append(file_path.name)
                        logger.info(f"找到{target_date}数据文件: {file_path.name}")
                
                except Exception as e:
                    logger.warning(f"读取文件 {file_path} 失败: {e}")
                    continue
            
            if not all_dataframes:
                raise FileNotFoundError(f"没有找到{target_date}的数据文件")
            
            # 合并所有数据
            combined_df = pd.concat(all_dataframes, ignore_index=True)
            logger.info(f"合并了 {len(target_files)} 个文件的数据")
            
            # 去重（按timestamp和trade_id）
            combined_df = combined_df.drop_duplicates(subset=['timestamp', 'trade_id'], keep='last')
            logger.info(f"去重后数据量: {len(combined_df)} 条")
            
            # 过滤目标日期数据（使用北京时间）
            target_data = combined_df[combined_df['timestamp'].dt.date == target_date]
            
            if target_data.empty:
                raise FileNotFoundError(f"合并后没有{target_date}的数据")
            
            logger.info(f"读取到 {len(target_data)} 条{target_date}数据")
            
            # 分离tick数据和信号数据
            tick_data = []
            signal_data = []
            
            for _, row in target_data.iterrows():
                # 处理trade类型数据
                if row['type'] == 'trade' and pd.notna(row['price']):
                    tick_data.append({
                        'timestamp': row['timestamp'],
                        'price': float(row['price']),
                        'volume': int(row['size']) if pd.notna(row['size']) else 0,
                        'trade_id': str(row.get('trade_id', ''))
                    })
                
                # 处理quote类型数据
                elif row['type'] == 'quote' and pd.notna(row['bid_price']):
                    tick_data.append({
                        'timestamp': row['timestamp'],
                        'bid_price': float(row['bid_price']),
                        'ask_price': float(row['ask_price']),
                        'bid_size': int(row['bid_size']) if pd.notna(row['bid_size']) else 0,
                        'ask_size': int(row['ask_size']) if pd.notna(row['ask_size']) else 0,
                        'trade_id': str(row.get('trade_id', ''))
                    })
            
            # 转换为DataFrame
            tick_df = pd.DataFrame(tick_data)
            signal_df = pd.DataFrame(signal_data)
            
            # 如果没有信号数据，创建一个空的信号DataFrame
            if signal_df.empty:
                signal_df = pd.DataFrame({
                    'timestamp': tick_df['timestamp'],
                    'signal': [0] * len(tick_df)  # 默认无信号
                })
            
            # 加载数据
            self.tick_data = tick_df
            self.signal_data = signal_df
            self._preprocess_data()
            
            logger.info(f"成功从catalog加载数据: tick={len(tick_df)}条, signal={len(signal_df)}条")
            
        except Exception as e:
            logger.error(f"从catalog loader加载数据失败: {e}")
            raise
    
    def load_from_redis_cache(self, redis_host: str = "localhost", redis_port: int = 6379, limit: int = 1000):
        """从Redis cache加载数据"""
        try:
            # 检查NautilusTrader是否可用
            try:
                from nautilus_trader.cache.cache import Cache
                from nautilus_trader.config import CacheConfig, DatabaseConfig
                from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue
                from nautilus_trader.common.component import LiveClock
                from nautilus_trader.common.component import Logger
            except ImportError:
                raise ImportError("NautilusTrader不可用，无法从Redis加载数据")
            
            logger.info(f"从Redis加载数据: {redis_host}:{redis_port}")
            
            # 创建Cache配置
            cache_config = CacheConfig(
                database=DatabaseConfig(
                    type="redis",
                    host=redis_host,
                    port=redis_port,
                    timeout=5,
                ),
                tick_capacity=100_000,
                bar_capacity=50_000,
                encoding="msgpack",
                timestamps_as_iso8601=True,
                use_trader_prefix=True,
                use_instance_id=True,
                flush_on_start=False,
                drop_instruments_on_reset=True,
            )
            
            # 创建Cache
            clock = LiveClock()
            logger_component = Logger("StrategyReverseEngineering")
            cache = Cache(config=cache_config, clock=clock, logger=logger_component)
            
            # 设置工具ID
            instrument_id = InstrumentId(
                symbol=Symbol("159506"),
                venue=Venue("SZSE")
            )
            
            # 获取tick数据
            quote_ticks = cache.quote_ticks(instrument_id)[-limit:]
            trade_ticks = cache.trade_ticks(instrument_id)[-limit:]
            
            logger.info(f"从Redis获取到: quote={len(quote_ticks)}条, trade={len(trade_ticks)}条")
            
            # 转换为DataFrame格式
            tick_data = []
            
            # 处理quote数据
            for tick in quote_ticks:
                tick_data.append({
                    'timestamp': pd.to_datetime(tick.ts_event, unit='ns'),
                    'bid_price': float(tick.bid_price),
                    'ask_price': float(tick.ask_price),
                    'bid_size': int(tick.bid_size),
                    'ask_size': int(tick.ask_size),
                    'type': 'quote'
                })
            
            # 处理trade数据
            for tick in trade_ticks:
                tick_data.append({
                    'timestamp': pd.to_datetime(tick.ts_event, unit='ns'),
                    'price': float(tick.price),
                    'volume': int(tick.size),
                    'trade_id': str(tick.trade_id),
                    'type': 'trade'
                })
            
            # 转换为DataFrame
            tick_df = pd.DataFrame(tick_data)
            
            # 创建信号DataFrame（默认无信号）
            signal_df = pd.DataFrame({
                'timestamp': tick_df['timestamp'],
                'signal': [0] * len(tick_df)
            })
            
            # 加载数据
            self.tick_data = tick_df
            self.signal_data = signal_df
            self._preprocess_data()
            
            logger.info(f"成功从Redis加载数据: {len(tick_df)}条")
            
        except Exception as e:
            logger.error(f"从Redis cache加载数据失败: {e}")
            raise
    
    def _preprocess_data(self):
        """数据预处理"""
        try:
            # 确保时间列格式正确
            if 'timestamp' in self.tick_data.columns:
                self.tick_data['timestamp'] = pd.to_datetime(self.tick_data['timestamp'])
            
            # 合并tick数据和信号数据
            self.features = self._merge_data()
            
            # 生成技术指标特征
            self._generate_features()
            
            logger.info("数据预处理完成")
            
        except Exception as e:
            logger.error(f"数据预处理失败: {e}")
            raise
    
    def _merge_data(self) -> pd.DataFrame:
        """合并tick数据和信号数据"""
        try:
            # 如果tick_data包含type列，说明是loader格式
            if 'type' in self.tick_data.columns:
                # 处理loader格式的数据
                merged_data = []
                
                for _, row in self.tick_data.iterrows():
                    data_row = {'timestamp': row['timestamp']}
                    
                    # 根据数据类型添加字段
                    if row['type'] == 'trade':
                        data_row.update({
                            'price': row.get('price', 0),
                            'volume': row.get('volume', 0),
                            'trade_id': row.get('trade_id', ''),
                            'type': 'trade'
                        })
                    elif row['type'] == 'quote':
                        data_row.update({
                            'bid_price': row.get('bid_price', 0),
                            'ask_price': row.get('ask_price', 0),
                            'bid_size': row.get('bid_size', 0),
                            'ask_size': row.get('ask_size', 0),
                            'type': 'quote'
                        })
                    
                    # 添加信号
                    signal_row = self.signal_data[self.signal_data['timestamp'] == row['timestamp']]
                    if not signal_row.empty:
                        data_row['signal'] = signal_row.iloc[0]['signal']
                    else:
                        data_row['signal'] = 0
                    
                    merged_data.append(data_row)
                
                return pd.DataFrame(merged_data)
            else:
                # 标准格式的合并
                merged = pd.merge(
                    self.tick_data, 
                    self.signal_data, 
                    on='timestamp', 
                    how='left'
                )
                
                # 填充信号列，0表示无信号，1表示买入，-1表示卖出
                merged['signal'] = merged['signal'].fillna(0)
                
                return merged
            
        except Exception as e:
            logger.error(f"合并数据失败: {e}")
            raise
    
    def _generate_features(self):
        """生成技术指标特征"""
        try:
            df = self.features.copy()
            
            # 价格相关特征
            # 对于loader格式，需要处理trade和quote两种类型
            if 'price' in df.columns:
                # 有price列，使用price
                df['price_change'] = df['price'].pct_change()
                df['price_change_abs'] = df['price_change'].abs()
                df['price_ma_5'] = df['price'].rolling(5).mean()
                df['price_ma_10'] = df['price'].rolling(10).mean()
                df['price_ma_20'] = df['price'].rolling(20).mean()
            elif 'bid_price' in df.columns and 'ask_price' in df.columns:
                # 只有quote数据，使用中间价
                df['mid_price'] = (df['bid_price'] + df['ask_price']) / 2
                df['price_change'] = df['mid_price'].pct_change()
                df['price_change_abs'] = df['price_change'].abs()
                df['price_ma_5'] = df['mid_price'].rolling(5).mean()
                df['price_ma_10'] = df['mid_price'].rolling(10).mean()
                df['price_ma_20'] = df['mid_price'].rolling(20).mean()
            else:
                # 混合数据，需要处理
                df['price'] = df['price'].fillna(df['mid_price'])
                df['price_change'] = df['price'].pct_change()
                df['price_change_abs'] = df['price_change'].abs()
                df['price_ma_5'] = df['price'].rolling(5).mean()
                df['price_ma_10'] = df['price'].rolling(10).mean()
                df['price_ma_20'] = df['price'].rolling(20).mean()
            
            # 成交量相关特征
            if 'volume' in df.columns:
                df['volume_ma_5'] = df['volume'].rolling(5).mean()
                df['volume_ma_10'] = df['volume'].rolling(10).mean()
                df['volume_ratio'] = df['volume'] / df['volume_ma_5']
                df['volume_change'] = df['volume'].pct_change()
            else:
                # 如果没有volume，使用bid_size + ask_size作为替代
                if 'bid_size' in df.columns and 'ask_size' in df.columns:
                    df['volume'] = df['bid_size'] + df['ask_size']
                    df['volume_ma_5'] = df['volume'].rolling(5).mean()
                    df['volume_ma_10'] = df['volume'].rolling(10).mean()
                    df['volume_ratio'] = df['volume'] / df['volume_ma_5']
                    df['volume_change'] = df['volume'].pct_change()
            
            # 买卖盘特征
            if 'bid_price' in df.columns and 'ask_price' in df.columns:
                df['spread'] = df['ask_price'] - df['bid_price']
                df['spread_pct'] = df['spread'] / df['bid_price']
                df['mid_price'] = (df['bid_price'] + df['ask_price']) / 2
                
            if 'bid_size' in df.columns and 'ask_size' in df.columns:
                df['bid_ask_ratio'] = df['bid_size'] / df['ask_size']
                df['order_imbalance'] = (df['bid_size'] - df['ask_size']) / (df['bid_size'] + df['ask_size'])
            
            # 波动率特征
            df['volatility_5'] = df['price_change'].rolling(5).std()
            df['volatility_10'] = df['price_change'].rolling(10).std()
            
            # 动量特征
            df['momentum_5'] = df['price'] / df['price'].shift(5) - 1
            df['momentum_10'] = df['price'] / df['price'].shift(10) - 1
            
            # 价格位置特征
            df['price_position_5'] = (df['price'] - df['price'].rolling(5).min()) / (df['price'].rolling(5).max() - df['price'].rolling(5).min())
            df['price_position_10'] = (df['price'] - df['price'].rolling(10).min()) / (df['price'].rolling(10).max() - df['price'].rolling(10).min())
            
            # 时间特征
            df['hour'] = df['timestamp'].dt.hour
            df['minute'] = df['timestamp'].dt.minute
            df['time_since_open'] = (df['timestamp'].dt.hour - 9) * 60 + (df['timestamp'].dt.minute - 30)
            
            # 滞后特征
            for lag in [1, 2, 3, 5]:
                if 'price' in df.columns:
                    df[f'price_lag_{lag}'] = df['price'].shift(lag)
                if 'volume' in df.columns:
                    df[f'volume_lag_{lag}'] = df['volume'].shift(lag)
            
            # 删除包含NaN的行
            df = df.dropna()
            
            self.features = df
            logger.info(f"生成特征完成，特征数量: {len(df.columns)}")
            
        except Exception as e:
            logger.error(f"生成特征失败: {e}")
            raise
    
    def analyze_patterns(self):
        """分析买卖点模式"""
        try:
            df = self.features.copy()
            
            # 分离买卖信号
            buy_signals = df[df['signal'] == 1]
            sell_signals = df[df['signal'] == -1]
            no_signals = df[df['signal'] == 0]
            
            logger.info(f"信号分布: 买入={len(buy_signals)}, 卖出={len(sell_signals)}, 无信号={len(no_signals)}")
            
            # 分析买入信号特征
            if len(buy_signals) > 0:
                self._analyze_signal_patterns(buy_signals, "买入信号")
            
            # 分析卖出信号特征
            if len(sell_signals) > 0:
                self._analyze_signal_patterns(sell_signals, "卖出信号")
            
            # 分析信号间隔
            self._analyze_signal_timing()
            
        except Exception as e:
            logger.error(f"分析模式失败: {e}")
            raise
    
    def _analyze_signal_patterns(self, signal_data: pd.DataFrame, signal_type: str):
        """分析特定信号的特征模式"""
        try:
            logger.info(f"\n=== {signal_type}特征分析 ===")
            
            # 选择数值型特征进行分析
            numeric_features = signal_data.select_dtypes(include=[np.number]).columns
            numeric_features = [col for col in numeric_features if col not in ['signal', 'timestamp']]
            
            # 计算特征统计
            stats = signal_data[numeric_features].describe()
            logger.info(f"{signal_type}特征统计:\n{stats}")
            
            # 找出异常值特征
            for feature in numeric_features[:10]:  # 只分析前10个特征
                mean_val = signal_data[feature].mean()
                std_val = signal_data[feature].std()
                threshold = 2  # 2个标准差
                
                outliers = signal_data[
                    (signal_data[feature] > mean_val + threshold * std_val) |
                    (signal_data[feature] < mean_val - threshold * std_val)
                ]
                
                if len(outliers) > 0:
                    logger.info(f"{feature}: {len(outliers)}个异常值")
            
        except Exception as e:
            logger.error(f"分析{signal_type}模式失败: {e}")
    
    def _analyze_signal_timing(self):
        """分析信号时间模式"""
        try:
            df = self.features.copy()
            
            # 找出所有信号点
            signal_points = df[df['signal'] != 0].copy()
            
            if len(signal_points) < 2:
                logger.info("信号点不足，无法分析时间模式")
                return
            
            # 计算信号间隔
            signal_points = signal_points.sort_values('timestamp')
            signal_points['time_diff'] = signal_points['timestamp'].diff()
            
            # 分析信号间隔
            intervals = signal_points['time_diff'].dropna()
            logger.info(f"\n=== 信号时间间隔分析 ===")
            logger.info(f"平均间隔: {intervals.mean()}")
            logger.info(f"最小间隔: {intervals.min()}")
            logger.info(f"最大间隔: {intervals.max()}")
            logger.info(f"间隔标准差: {intervals.std()}")
            
            # 分析信号时间分布
            signal_points['hour'] = signal_points['timestamp'].dt.hour
            hour_dist = signal_points['hour'].value_counts().sort_index()
            logger.info(f"\n信号时间分布:\n{hour_dist}")
            
        except Exception as e:
            logger.error(f"分析信号时间模式失败: {e}")
    
    def analyze_feature_importance(self):
        """基于统计分析的特征重要性分析"""
        try:
            df = self.features.copy()
            
            # 分离买卖信号
            buy_signals = df[df['signal'] == 1]
            sell_signals = df[df['signal'] == -1]
            no_signals = df[df['signal'] == 0]
            
            # 计算特征重要性（基于信号区分度）
            feature_cols = [col for col in df.columns if col not in ['signal', 'timestamp']]
            feature_importance = []
            
            for feature in feature_cols:
                if feature in df.columns:
                    # 计算买入信号的特征分布
                    buy_mean = buy_signals[feature].mean() if len(buy_signals) > 0 else 0
                    buy_std = buy_signals[feature].std() if len(buy_signals) > 0 else 1
                    
                    # 计算卖出信号的特征分布
                    sell_mean = sell_signals[feature].mean() if len(sell_signals) > 0 else 0
                    sell_std = sell_signals[feature].std() if len(sell_signals) > 0 else 1
                    
                    # 计算无信号的特征分布
                    no_mean = no_signals[feature].mean() if len(no_signals) > 0 else 0
                    no_std = no_signals[feature].std() if len(no_signals) > 0 else 1
                    
                    # 计算区分度（信号组与无信号组的差异）
                    buy_separation = abs(buy_mean - no_mean) / (buy_std + no_std + 1e-8)
                    sell_separation = abs(sell_mean - no_mean) / (sell_std + no_std + 1e-8)
                    
                    # 综合重要性
                    importance = (buy_separation + sell_separation) / 2
                    
                    feature_importance.append({
                        'feature': feature,
                        'importance': importance,
                        'buy_mean': buy_mean,
                        'sell_mean': sell_mean,
                        'no_mean': no_mean,
                        'buy_separation': buy_separation,
                        'sell_separation': sell_separation
                    })
            
            # 按重要性排序
            feature_importance_df = pd.DataFrame(feature_importance)
            feature_importance_df = feature_importance_df.sort_values('importance', ascending=False)
            
            logger.info("\n=== 特征重要性分析 ===")
            logger.info(f"特征重要性Top10:\n{feature_importance_df.head(10)}")
            
            return feature_importance_df
            
        except Exception as e:
            logger.error(f"特征重要性分析失败: {e}")
            raise
    
    def generate_strategy_rules(self) -> Dict:
        """基于统计分析生成策略规则"""
        try:
            df = self.features.copy()
            
            # 获取特征重要性
            feature_importance = self.analyze_feature_importance()
            
            rules = {
                'buy_conditions': [],
                'sell_conditions': [],
                'feature_thresholds': {},
                'strategy_summary': {}
            }
            
            # 基于特征重要性生成规则
            top_features = feature_importance.head(10)
            
            for _, row in top_features.iterrows():
                feature = row['feature']
                importance = row['importance']
                buy_mean = row['buy_mean']
                sell_mean = row['sell_mean']
                no_mean = row['no_mean']
                
                # 分析该特征的分布
                buy_data = df[df['signal'] == 1][feature]
                sell_data = df[df['signal'] == -1][feature]
                
                if len(buy_data) > 0:
                    # 买入阈值：买入信号的平均值 + 0.5个标准差
                    buy_threshold = buy_mean + 0.5 * buy_data.std()
                    rules['buy_conditions'].append({
                        'feature': feature,
                        'threshold': buy_threshold,
                        'condition': f"{feature} > {buy_threshold:.4f}",
                        'importance': importance,
                        'buy_mean': buy_mean,
                        'no_mean': no_mean
                    })
                
                if len(sell_data) > 0:
                    # 卖出阈值：卖出信号的平均值 - 0.5个标准差
                    sell_threshold = sell_mean - 0.5 * sell_data.std()
                    rules['sell_conditions'].append({
                        'feature': feature,
                        'threshold': sell_threshold,
                        'condition': f"{feature} < {sell_threshold:.4f}",
                        'importance': importance,
                        'sell_mean': sell_mean,
                        'no_mean': no_mean
                    })
                
                rules['feature_thresholds'][feature] = {
                    'buy_threshold': buy_threshold if len(buy_data) > 0 else None,
                    'sell_threshold': sell_threshold if len(sell_data) > 0 else None,
                    'importance': importance,
                    'buy_mean': buy_mean,
                    'sell_mean': sell_mean,
                    'no_mean': no_mean
                }
            
            # 生成策略摘要
            rules['strategy_summary'] = {
                'total_signals': len(df[df['signal'] != 0]),
                'buy_signals': len(df[df['signal'] == 1]),
                'sell_signals': len(df[df['signal'] == -1]),
                'signal_ratio': len(df[df['signal'] != 0]) / len(df),
                'top_features': top_features['feature'].tolist()[:5]
            }
            
            logger.info("\n=== 策略规则生成完成 ===")
            logger.info(f"买入条件数量: {len(rules['buy_conditions'])}")
            logger.info(f"卖出条件数量: {len(rules['sell_conditions'])}")
            logger.info(f"信号比例: {rules['strategy_summary']['signal_ratio']:.2%}")
            
            return rules
            
        except Exception as e:
            logger.error(f"生成策略规则失败: {e}")
            raise
    
    def visualize_analysis(self):
        """可视化分析结果"""
        try:
            fig, axes = plt.subplots(2, 2, figsize=(15, 12))
            
            # 1. 价格和信号图
            ax1 = axes[0, 0]
            df = self.features.copy()
            ax1.plot(df['timestamp'], df['price'], label='价格', alpha=0.7)
            
            # 标记买卖点
            buy_points = df[df['signal'] == 1]
            sell_points = df[df['signal'] == -1]
            
            ax1.scatter(buy_points['timestamp'], buy_points['price'], 
                       color='red', marker='^', s=50, label='买入信号', alpha=0.8)
            ax1.scatter(sell_points['timestamp'], sell_points['price'], 
                       color='green', marker='v', s=50, label='卖出信号', alpha=0.8)
            
            ax1.set_title('价格和交易信号')
            ax1.legend()
            ax1.tick_params(axis='x', rotation=45)
            
            # 2. 特征重要性
            ax2 = axes[0, 1]
            feature_importance = self.analyze_feature_importance()
            if feature_importance is not None:
                top_features = feature_importance.head(10)
                ax2.barh(range(len(top_features)), top_features['importance'])
                ax2.set_yticks(range(len(top_features)))
                ax2.set_yticklabels(top_features['feature'])
                ax2.set_title('特征重要性Top10')
                ax2.set_xlabel('重要性')
            
            # 3. 信号时间分布
            ax3 = axes[1, 0]
            signal_points = df[df['signal'] != 0]
            if len(signal_points) > 0:
                hour_dist = signal_points['timestamp'].dt.hour.value_counts().sort_index()
                ax3.bar(hour_dist.index, hour_dist.values)
                ax3.set_title('信号时间分布')
                ax3.set_xlabel('小时')
                ax3.set_ylabel('信号数量')
            
            # 4. 价格变化分布
            ax4 = axes[1, 1]
            ax4.hist(df['price_change'].dropna(), bins=50, alpha=0.7)
            ax4.set_title('价格变化分布')
            ax4.set_xlabel('价格变化率')
            ax4.set_ylabel('频次')
            
            plt.tight_layout()
            plt.savefig('strategy_analysis.png', dpi=300, bbox_inches='tight')
            plt.show()
            
            logger.info("分析图表已保存为 strategy_analysis.png")
            
        except Exception as e:
            logger.error(f"可视化分析失败: {e}")
    
    def generate_strategy_code(self, rules: Dict) -> str:
        """生成策略代码"""
        try:
            code = '''#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
反向推导的策略代码
基于数据分析生成的交易策略
"""

import pandas as pd
import numpy as np
from datetime import datetime
from typing import Dict, List

class ReverseEngineeredStrategy:
    """反向推导的交易策略"""
    
    def __init__(self):
        # 策略参数（基于数据分析得出）
        self.lookback_period = 10
        self.price_threshold = 0.003
        self.volume_threshold = 500000
        
        # 特征阈值（基于模型分析得出）
        self.feature_thresholds = {
'''
            
            # 添加特征阈值
            for feature, thresholds in rules['feature_thresholds'].items():
                if thresholds['buy_threshold'] is not None:
                    code += f"            '{feature}_buy': {thresholds['buy_threshold']:.4f},  # 重要性: {thresholds['importance']:.3f}\n"
                if thresholds['sell_threshold'] is not None:
                    code += f"            '{feature}_sell': {thresholds['sell_threshold']:.4f},  # 重要性: {thresholds['importance']:.3f}\n"
            
            code += '''        }
        
        # 交易状态
        self.current_position = 0
        self.entry_price = 0.0
        
    def generate_features(self, tick_data: pd.DataFrame) -> pd.DataFrame:
        """生成技术指标特征"""
        df = tick_data.copy()
        
        # 价格特征
        df['price_change'] = df['price'].pct_change()
        df['price_ma_5'] = df['price'].rolling(5).mean()
        df['price_ma_10'] = df['price'].rolling(10).mean()
        df['price_ma_20'] = df['price'].rolling(20).mean()
        
        # 成交量特征
        if 'volume' in df.columns:
            df['volume_ma_5'] = df['volume'].rolling(5).mean()
            df['volume_ratio'] = df['volume'] / df['volume_ma_5']
        
        # 买卖盘特征
        if 'bid_price' in df.columns and 'ask_price' in df.columns:
            df['spread'] = df['ask_price'] - df['bid_price']
            df['mid_price'] = (df['bid_price'] + df['ask_price']) / 2
            
        if 'bid_size' in df.columns and 'ask_size' in df.columns:
            df['bid_ask_ratio'] = df['bid_size'] / df['ask_size']
            df['order_imbalance'] = (df['bid_size'] - df['ask_size']) / (df['bid_size'] + df['ask_size'])
        
        # 波动率特征
        df['volatility_5'] = df['price_change'].rolling(5).std()
        
        # 动量特征
        df['momentum_5'] = df['price'] / df['price'].shift(5) - 1
        
        return df
    
    def analyze_signal(self, features: pd.DataFrame) -> Dict:
        """分析交易信号"""
        if len(features) < self.lookback_period:
            return {'signal': 'neutral', 'strength': 0, 'reason': '数据不足'}
        
        latest = features.iloc[-1]
        
        # 买入条件检查
        buy_score = 0
        buy_reasons = []
        
        # 卖出条件检查
        sell_score = 0
        sell_reasons = []
        
        # 基于特征阈值判断
        for feature, thresholds in self.feature_thresholds.items():
            if feature.endswith('_buy') and feature.replace('_buy', '') in latest:
                feature_name = feature.replace('_buy', '')
                if latest[feature_name] > thresholds:
                    buy_score += 1
                    buy_reasons.append(f"{feature_name} > {thresholds:.4f}")
                    
            elif feature.endswith('_sell') and feature.replace('_sell', '') in latest:
                feature_name = feature.replace('_sell', '')
                if latest[feature_name] < thresholds:
                    sell_score += 1
                    sell_reasons.append(f"{feature_name} < {thresholds:.4f}")
        
        # 综合判断
        if buy_score > sell_score and buy_score >= 2:
            signal = 'bullish'
            strength = min(buy_score * 20, 100)
            reason = f"买入信号 ({buy_score}个条件满足): {', '.join(buy_reasons)}"
        elif sell_score > buy_score and sell_score >= 2:
            signal = 'bearish'
            strength = min(sell_score * 20, 100)
            reason = f"卖出信号 ({sell_score}个条件满足): {', '.join(sell_reasons)}"
        else:
            signal = 'neutral'
            strength = 0
            reason = "信号不明确"
        
        return {
            'signal': signal,
            'strength': strength,
            'reason': reason,
            'buy_score': buy_score,
            'sell_score': sell_score
        }
    
    def execute_trading_decision(self, signal: Dict, current_price: float):
        """执行交易决策"""
        if signal['signal'] == 'bullish' and self.current_position == 0:
            # 开多头
            self.current_position = 1
            self.entry_price = current_price
            print(f"🚀 开多头 - 价格: {current_price:.3f}, 理由: {signal['reason']}")
            
        elif signal['signal'] == 'bearish' and self.current_position == 0:
            # 开空头
            self.current_position = -1
            self.entry_price = current_price
            print(f"📉 开空头 - 价格: {current_price:.3f}, 理由: {signal['reason']}")
            
        elif signal['signal'] == 'bearish' and self.current_position == 1:
            # 平多头
            pnl = (current_price - self.entry_price) / self.entry_price
            print(f"💰 平多头 - 入场: {self.entry_price:.3f}, 出场: {current_price:.3f}, 收益率: {pnl*100:.2f}%")
            self.current_position = 0
            self.entry_price = 0.0
            
        elif signal['signal'] == 'bullish' and self.current_position == -1:
            # 平空头
            pnl = (self.entry_price - current_price) / self.entry_price
            print(f"💰 平空头 - 入场: {self.entry_price:.3f}, 出场: {current_price:.3f}, 收益率: {pnl*100:.2f}%")
            self.current_position = 0
            self.entry_price = 0.0

# 使用示例
if __name__ == "__main__":
    strategy = ReverseEngineeredStrategy()
    print("反向推导策略已生成，请根据实际数据调整参数")
'''
            
            return code
            
        except Exception as e:
            logger.error(f"生成策略代码失败: {e}")
            raise


def main():
    """主函数"""
    print("=" * 60)
    print("策略反向分析工具")
    print("=" * 60)
    print("功能:")
    print("1. 分析tick数据和买卖点模式")
    print("2. 提取关键特征和阈值")
    print("3. 基于统计分析生成策略规则")
    print("4. 生成策略代码")
    print("5. 可视化分析结果")
    print("=" * 60)
    
    # 创建分析工具
    analyzer = StrategyReverseEngineering()
    
    # 示例用法（需要提供实际数据文件）
    print("请提供以下数据文件:")
    print("1. tick_data.csv - 包含timestamp, price, volume, bid_price, ask_price, bid_size, ask_size等列")
    print("2. signals.csv - 包含timestamp, signal列（1=买入, -1=卖出, 0=无信号）")
    
    # 如果有数据文件，可以取消注释以下代码
    """
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
    """


if __name__ == "__main__":
    main() 