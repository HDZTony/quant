#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
159506 ETF数据管理模块
包含数据缓存、预处理、质量检查等功能
"""

import logging
import json
import pickle
import gzip
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Tuple, Any
from collections import deque
import pandas as pd
import numpy as np
from threading import Lock

logger = logging.getLogger(__name__)


class DataManager:
    """数据管理器"""
    
    def __init__(self, config: Dict):
        self.config = config
        self.logger = logging.getLogger("DataManager")
        
        # 缓存配置
        self.max_cache_size = config.get('max_cache_size', 10000)
        self.cache_ttl = config.get('cache_ttl', 3600)  # 1小时
        
        # 数据缓存
        self.quote_cache = deque(maxlen=self.max_cache_size)
        self.trade_cache = deque(maxlen=self.max_cache_size)
        self.bar_cache = deque(maxlen=self.max_cache_size)
        
        # 缓存锁
        self.cache_lock = Lock()
        
        # 数据质量统计
        self.data_quality_stats = {
            'total_quotes': 0,
            'total_trades': 0,
            'total_bars': 0,
            'invalid_quotes': 0,
            'invalid_trades': 0,
            'invalid_bars': 0,
            'duplicate_quotes': 0,
            'duplicate_trades': 0,
            'missing_data': 0
        }
        
        # 数据预处理配置
        self.preprocessing_config = {
            'remove_outliers': True,
            'outlier_threshold': 3.0,  # 3个标准差
            'fill_missing': True,
            'smooth_data': True,
            'smoothing_window': 5
        }
        
        # 数据源配置
        self.data_sources = {
            'redis': config.get('redis_enabled', True),
            'local_file': config.get('local_file_enabled', True),
            'api': config.get('api_enabled', False)
        }
        
        # 数据文件路径
        self.data_path = config.get('data_path', './data')
        
    def add_quote_data(self, quote_data: Dict) -> bool:
        """添加报价数据"""
        try:
            with self.cache_lock:
                # 数据质量检查
                if not self._validate_quote_data(quote_data):
                    self.data_quality_stats['invalid_quotes'] += 1
                    return False
                
                # 重复检查
                if self._is_duplicate_quote(quote_data):
                    self.data_quality_stats['duplicate_quotes'] += 1
                    return False
                
                # 数据预处理
                processed_data = self._preprocess_quote_data(quote_data)
                
                # 添加到缓存
                self.quote_cache.append(processed_data)
                self.data_quality_stats['total_quotes'] += 1
                
                return True
                
        except Exception as e:
            self.logger.error(f"添加报价数据失败: {e}")
            return False
    
    def add_trade_data(self, trade_data: Dict) -> bool:
        """添加成交数据"""
        try:
            with self.cache_lock:
                # 数据质量检查
                if not self._validate_trade_data(trade_data):
                    self.data_quality_stats['invalid_trades'] += 1
                    return False
                
                # 重复检查
                if self._is_duplicate_trade(trade_data):
                    self.data_quality_stats['duplicate_trades'] += 1
                    return False
                
                # 数据预处理
                processed_data = self._preprocess_trade_data(trade_data)
                
                # 添加到缓存
                self.trade_cache.append(processed_data)
                self.data_quality_stats['total_trades'] += 1
                
                return True
                
        except Exception as e:
            self.logger.error(f"添加成交数据失败: {e}")
            return False
    
    def add_bar_data(self, bar_data: Dict) -> bool:
        """添加K线数据"""
        try:
            with self.cache_lock:
                # 数据质量检查
                if not self._validate_bar_data(bar_data):
                    self.data_quality_stats['invalid_bars'] += 1
                    return False
                
                # 数据预处理
                processed_data = self._preprocess_bar_data(bar_data)
                
                # 添加到缓存
                self.bar_cache.append(processed_data)
                self.data_quality_stats['total_bars'] += 1
                
                return True
                
        except Exception as e:
            self.logger.error(f"添加K线数据失败: {e}")
            return False
    
    def get_recent_quotes(self, count: int = 100) -> List[Dict]:
        """获取最近的报价数据"""
        try:
            with self.cache_lock:
                return list(self.quote_cache)[-count:]
        except Exception as e:
            self.logger.error(f"获取报价数据失败: {e}")
            return []
    
    def get_recent_trades(self, count: int = 100) -> List[Dict]:
        """获取最近的成交数据"""
        try:
            with self.cache_lock:
                return list(self.trade_cache)[-count:]
        except Exception as e:
            self.logger.error(f"获取成交数据失败: {e}")
            return []
    
    def get_recent_bars(self, count: int = 100) -> List[Dict]:
        """获取最近的K线数据"""
        try:
            with self.cache_lock:
                return list(self.bar_cache)[-count:]
        except Exception as e:
            self.logger.error(f"获取K线数据失败: {e}")
            return []
    
    def get_data_by_time_range(self, start_time: datetime, end_time: datetime, 
                              data_type: str = 'quote') -> List[Dict]:
        """根据时间范围获取数据"""
        try:
            with self.cache_lock:
                if data_type == 'quote':
                    cache = self.quote_cache
                elif data_type == 'trade':
                    cache = self.trade_cache
                elif data_type == 'bar':
                    cache = self.bar_cache
                else:
                    return []
                
                filtered_data = []
                for data in cache:
                    data_time = data.get('timestamp')
                    if isinstance(data_time, str):
                        data_time = datetime.fromisoformat(data_time)
                    
                    if start_time <= data_time <= end_time:
                        filtered_data.append(data)
                
                return filtered_data
                
        except Exception as e:
            self.logger.error(f"按时间范围获取数据失败: {e}")
            return []
    
    def _validate_quote_data(self, data: Dict) -> bool:
        """验证报价数据"""
        try:
            required_fields = ['timestamp', 'bid_price', 'ask_price', 'bid_size', 'ask_size']
            
            # 检查必需字段
            for field in required_fields:
                if field not in data:
                    return False
            
            # 检查价格合理性
            bid_price = float(data['bid_price'])
            ask_price = float(data['ask_price'])
            
            if bid_price <= 0 or ask_price <= 0 or bid_price > ask_price:
                return False
            
            # 检查数量合理性
            bid_size = int(data['bid_size'])
            ask_size = int(data['ask_size'])
            
            if bid_size < 0 or ask_size < 0:
                return False
            
            # 检查时间戳
            timestamp = data['timestamp']
            if isinstance(timestamp, str):
                datetime.fromisoformat(timestamp)
            
            return True
            
        except Exception as e:
            self.logger.error(f"验证报价数据失败: {e}")
            return False
    
    def _validate_trade_data(self, data: Dict) -> bool:
        """验证成交数据"""
        try:
            required_fields = ['timestamp', 'price', 'size']
            
            # 检查必需字段
            for field in required_fields:
                if field not in data:
                    return False
            
            # 检查价格合理性
            price = float(data['price'])
            if price <= 0:
                return False
            
            # 检查数量合理性
            size = int(data['size'])
            if size <= 0:
                return False
            
            # 检查时间戳
            timestamp = data['timestamp']
            if isinstance(timestamp, str):
                datetime.fromisoformat(timestamp)
            
            return True
            
        except Exception as e:
            self.logger.error(f"验证成交数据失败: {e}")
            return False
    
    def _validate_bar_data(self, data: Dict) -> bool:
        """验证K线数据"""
        try:
            required_fields = ['timestamp', 'open', 'high', 'low', 'close', 'volume']
            
            # 检查必需字段
            for field in required_fields:
                if field not in data:
                    return False
            
            # 检查价格合理性
            open_price = float(data['open'])
            high_price = float(data['high'])
            low_price = float(data['low'])
            close_price = float(data['close'])
            
            if any(p <= 0 for p in [open_price, high_price, low_price, close_price]):
                return False
            
            if not (low_price <= open_price <= high_price and 
                   low_price <= close_price <= high_price):
                return False
            
            # 检查成交量
            volume = int(data['volume'])
            if volume < 0:
                return False
            
            # 检查时间戳
            timestamp = data['timestamp']
            if isinstance(timestamp, str):
                datetime.fromisoformat(timestamp)
            
            return True
            
        except Exception as e:
            self.logger.error(f"验证K线数据失败: {e}")
            return False
    
    def _is_duplicate_quote(self, data: Dict) -> bool:
        """检查是否为重复报价"""
        try:
            if not self.quote_cache:
                return False
            
            latest_quote = self.quote_cache[-1]
            
            # 检查时间戳和价格是否相同
            return (data['timestamp'] == latest_quote['timestamp'] and
                   data['bid_price'] == latest_quote['bid_price'] and
                   data['ask_price'] == latest_quote['ask_price'])
            
        except Exception as e:
            self.logger.error(f"检查重复报价失败: {e}")
            return False
    
    def _is_duplicate_trade(self, data: Dict) -> bool:
        """检查是否为重复成交"""
        try:
            if not self.trade_cache:
                return False
            
            latest_trade = self.trade_cache[-1]
            
            # 检查时间戳、价格和数量是否相同
            return (data['timestamp'] == latest_trade['timestamp'] and
                   data['price'] == latest_trade['price'] and
                   data['size'] == latest_trade['size'])
            
        except Exception as e:
            self.logger.error(f"检查重复成交失败: {e}")
            return False
    
    def _preprocess_quote_data(self, data: Dict) -> Dict:
        """预处理报价数据"""
        try:
            processed_data = data.copy()
            
            # 数据平滑
            if self.preprocessing_config['smooth_data']:
                processed_data = self._smooth_quote_data(processed_data)
            
            # 异常值处理
            if self.preprocessing_config['remove_outliers']:
                processed_data = self._remove_quote_outliers(processed_data)
            
            return processed_data
            
        except Exception as e:
            self.logger.error(f"预处理报价数据失败: {e}")
            return data
    
    def _preprocess_trade_data(self, data: Dict) -> Dict:
        """预处理成交数据"""
        try:
            processed_data = data.copy()
            
            # 数据平滑
            if self.preprocessing_config['smooth_data']:
                processed_data = self._smooth_trade_data(processed_data)
            
            # 异常值处理
            if self.preprocessing_config['remove_outliers']:
                processed_data = self._remove_trade_outliers(processed_data)
            
            return processed_data
            
        except Exception as e:
            self.logger.error(f"预处理成交数据失败: {e}")
            return data
    
    def _preprocess_bar_data(self, data: Dict) -> Dict:
        """预处理K线数据"""
        try:
            processed_data = data.copy()
            
            # 数据平滑
            if self.preprocessing_config['smooth_data']:
                processed_data = self._smooth_bar_data(processed_data)
            
            # 异常值处理
            if self.preprocessing_config['remove_outliers']:
                processed_data = self._remove_bar_outliers(processed_data)
            
            return processed_data
            
        except Exception as e:
            self.logger.error(f"预处理K线数据失败: {e}")
            return data
    
    def _smooth_quote_data(self, data: Dict) -> Dict:
        """平滑报价数据"""
        try:
            if len(self.quote_cache) < self.preprocessing_config['smoothing_window']:
                return data
            
            # 获取最近的报价数据进行平滑
            recent_quotes = list(self.quote_cache)[-self.preprocessing_config['smoothing_window']:]
            
            bid_prices = [float(q['bid_price']) for q in recent_quotes]
            ask_prices = [float(q['ask_price']) for q in recent_quotes]
            
            # 计算移动平均
            smoothed_bid = np.mean(bid_prices)
            smoothed_ask = np.mean(ask_prices)
            
            data['bid_price'] = smoothed_bid
            data['ask_price'] = smoothed_ask
            
            return data
            
        except Exception as e:
            self.logger.error(f"平滑报价数据失败: {e}")
            return data
    
    def _smooth_trade_data(self, data: Dict) -> Dict:
        """平滑成交数据"""
        try:
            if len(self.trade_cache) < self.preprocessing_config['smoothing_window']:
                return data
            
            # 获取最近的成交数据进行平滑
            recent_trades = list(self.trade_cache)[-self.preprocessing_config['smoothing_window']:]
            
            prices = [float(t['price']) for t in recent_trades]
            
            # 计算移动平均
            smoothed_price = np.mean(prices)
            
            data['price'] = smoothed_price
            
            return data
            
        except Exception as e:
            self.logger.error(f"平滑成交数据失败: {e}")
            return data
    
    def _smooth_bar_data(self, data: Dict) -> Dict:
        """平滑K线数据"""
        try:
            if len(self.bar_cache) < self.preprocessing_config['smoothing_window']:
                return data
            
            # 获取最近的K线数据进行平滑
            recent_bars = list(self.bar_cache)[-self.preprocessing_config['smoothing_window']:]
            
            closes = [float(b['close']) for b in recent_bars]
            
            # 计算移动平均
            smoothed_close = np.mean(closes)
            
            data['close'] = smoothed_close
            
            return data
            
        except Exception as e:
            self.logger.error(f"平滑K线数据失败: {e}")
            return data
    
    def _remove_quote_outliers(self, data: Dict) -> Dict:
        """移除报价异常值"""
        try:
            if len(self.quote_cache) < 20:
                return data
            
            # 获取历史数据计算统计
            recent_quotes = list(self.quote_cache)[-20:]
            
            bid_prices = [float(q['bid_price']) for q in recent_quotes]
            ask_prices = [float(q['ask_price']) for q in recent_quotes]
            
            bid_mean = np.mean(bid_prices)
            bid_std = np.std(bid_prices)
            ask_mean = np.mean(ask_prices)
            ask_std = np.std(ask_prices)
            
            current_bid = float(data['bid_price'])
            current_ask = float(data['ask_price'])
            
            # 检查是否为异常值
            bid_threshold = bid_mean + self.preprocessing_config['outlier_threshold'] * bid_std
            ask_threshold = ask_mean + self.preprocessing_config['outlier_threshold'] * ask_std
            
            if current_bid > bid_threshold or current_ask > ask_threshold:
                # 使用历史平均值替代
                data['bid_price'] = bid_mean
                data['ask_price'] = ask_mean
            
            return data
            
        except Exception as e:
            self.logger.error(f"移除报价异常值失败: {e}")
            return data
    
    def _remove_trade_outliers(self, data: Dict) -> Dict:
        """移除成交异常值"""
        try:
            if len(self.trade_cache) < 20:
                return data
            
            # 获取历史数据计算统计
            recent_trades = list(self.trade_cache)[-20:]
            
            prices = [float(t['price']) for t in recent_trades]
            
            price_mean = np.mean(prices)
            price_std = np.std(prices)
            
            current_price = float(data['price'])
            
            # 检查是否为异常值
            threshold = price_mean + self.preprocessing_config['outlier_threshold'] * price_std
            
            if current_price > threshold:
                # 使用历史平均值替代
                data['price'] = price_mean
            
            return data
            
        except Exception as e:
            self.logger.error(f"移除成交异常值失败: {e}")
            return data
    
    def _remove_bar_outliers(self, data: Dict) -> Dict:
        """移除K线异常值"""
        try:
            if len(self.bar_cache) < 20:
                return data
            
            # 获取历史数据计算统计
            recent_bars = list(self.bar_cache)[-20:]
            
            closes = [float(b['close']) for b in recent_bars]
            
            close_mean = np.mean(closes)
            close_std = np.std(closes)
            
            current_close = float(data['close'])
            
            # 检查是否为异常值
            threshold = close_mean + self.preprocessing_config['outlier_threshold'] * close_std
            
            if current_close > threshold:
                # 使用历史平均值替代
                data['close'] = close_mean
            
            return data
            
        except Exception as e:
            self.logger.error(f"移除K线异常值失败: {e}")
            return data
    
    def save_data_to_file(self, filename: str, data_type: str = 'all') -> bool:
        """保存数据到文件"""
        try:
            with self.cache_lock:
                save_data = {}
                
                if data_type in ['quote', 'all']:
                    save_data['quotes'] = list(self.quote_cache)
                
                if data_type in ['trade', 'all']:
                    save_data['trades'] = list(self.trade_cache)
                
                if data_type in ['bar', 'all']:
                    save_data['bars'] = list(self.bar_cache)
                
                # 使用gzip压缩保存
                with gzip.open(f"{self.data_path}/{filename}.gz", 'wt', encoding='utf-8') as f:
                    json.dump(save_data, f, indent=2, default=str)
                
                self.logger.info(f"数据已保存到文件: {filename}.gz")
                return True
                
        except Exception as e:
            self.logger.error(f"保存数据到文件失败: {e}")
            return False
    
    def load_data_from_file(self, filename: str) -> bool:
        """从文件加载数据"""
        try:
            with gzip.open(f"{self.data_path}/{filename}.gz", 'rt', encoding='utf-8') as f:
                data = json.load(f)
            
            with self.cache_lock:
                if 'quotes' in data:
                    self.quote_cache.extend(data['quotes'])
                
                if 'trades' in data:
                    self.trade_cache.extend(data['trades'])
                
                if 'bars' in data:
                    self.bar_cache.extend(data['bars'])
            
            self.logger.info(f"数据已从文件加载: {filename}.gz")
            return True
            
        except Exception as e:
            self.logger.error(f"从文件加载数据失败: {e}")
            return False
    
    def get_data_quality_report(self) -> Dict:
        """获取数据质量报告"""
        try:
            total_data = (self.data_quality_stats['total_quotes'] + 
                         self.data_quality_stats['total_trades'] + 
                         self.data_quality_stats['total_bars'])
            
            invalid_data = (self.data_quality_stats['invalid_quotes'] + 
                           self.data_quality_stats['invalid_trades'] + 
                           self.data_quality_stats['invalid_bars'])
            
            duplicate_data = (self.data_quality_stats['duplicate_quotes'] + 
                             self.data_quality_stats['duplicate_trades'])
            
            quality_rate = ((total_data - invalid_data - duplicate_data) / total_data * 100) if total_data > 0 else 0
            
            return {
                'data_quality_stats': self.data_quality_stats.copy(),
                'quality_rate': quality_rate,
                'cache_status': {
                    'quote_cache_size': len(self.quote_cache),
                    'trade_cache_size': len(self.trade_cache),
                    'bar_cache_size': len(self.bar_cache),
                    'max_cache_size': self.max_cache_size
                }
            }
            
        except Exception as e:
            self.logger.error(f"生成数据质量报告失败: {e}")
            return {}
    
    def clear_cache(self, data_type: str = 'all') -> None:
        """清空缓存"""
        try:
            with self.cache_lock:
                if data_type in ['quote', 'all']:
                    self.quote_cache.clear()
                
                if data_type in ['trade', 'all']:
                    self.trade_cache.clear()
                
                if data_type in ['bar', 'all']:
                    self.bar_cache.clear()
            
            self.logger.info(f"缓存已清空: {data_type}")
            
        except Exception as e:
            self.logger.error(f"清空缓存失败: {e}") 