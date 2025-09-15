#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
159506 ETF 实时交易数据保存模块
基于cache_collector的数据保存机制，为live_trading系统提供数据持久化功能
"""

import json
import time
import threading
import pandas as pd
import numpy as np
from datetime import datetime, timedelta
from collections import deque
import os
from pathlib import Path
import pickle
from typing import Dict, List, Optional, Any, Union
import logging
import gc

# Redis连接检测
REDIS_AVAILABLE = False
REDIS_HEALTHY = False
try:
    import redis
    REDIS_AVAILABLE = True
    try:
        r = redis.StrictRedis(host="localhost", port=6379, socket_connect_timeout=3)
        result = r.ping()
        REDIS_HEALTHY = True
        print(f"[DataSaver] Redis服务可用: {result}")
    except Exception as e:
        print(f"[DataSaver] Redis服务不可用: {e}")
except ImportError:
    print("[DataSaver] 未安装redis-py，将使用文件保存模式")

# NautilusTrader imports
from nautilus_trader.model.data import Bar, BarType, QuoteTick, TradeTick
from nautilus_trader.model.identifiers import InstrumentId, Symbol, Venue
from nautilus_trader.model.objects import Price, Quantity
from nautilus_trader.model.enums import BarAggregation, PriceType
from decimal import Decimal

# 配置日志
logger = logging.getLogger(__name__)

# 数据保存配置
DATA_SAVER_CONFIG = {
    'save_interval': 300,          # 5分钟保存一次
    'merge_interval': 3600,        # 1小时合并一次文件
    'max_cache_size': 1000000,     # 最大缓存数据条数
    'cleanup_keep_hours': 24,      # 清理时保留的小时数
    'catalog_path': 'catalog/etf_159506_live',  # 数据保存路径
    'redis_ttl': 86400,           # Redis数据过期时间（24小时）
}


class LiveDataSaver:
    """实时交易数据保存器"""
    
    def __init__(
        self,
        instrument_id: InstrumentId,
        config: Optional[Dict[str, Any]] = None
    ):
        """
        初始化数据保存器
        
        Parameters
        ----------
        instrument_id : InstrumentId
            工具ID
        config : Optional[Dict[str, Any]]
            配置参数
        """
        self.instrument_id = instrument_id
        self.config = {**DATA_SAVER_CONFIG, **(config or {})}
        
        # 数据缓存
        self.quote_ticks_buffer = deque(maxlen=self.config['max_cache_size'])
        self.trade_ticks_buffer = deque(maxlen=self.config['max_cache_size'])
        self.bars_buffer = deque(maxlen=self.config['max_cache_size'])
        
        # 线程控制
        self.stop_save = False
        self.save_thread = None
        self.is_running = False
        
        # 统计信息
        self.total_quotes_saved = 0
        self.total_trades_saved = 0
        self.total_bars_saved = 0
        self.last_save_time = None
        self.start_time = datetime.now()
        
        # 创建保存目录
        self.catalog_path = Path(self.config['catalog_path'])
        self.catalog_path.mkdir(parents=True, exist_ok=True)
        
        # Redis连接
        self.redis_client = None
        if REDIS_AVAILABLE and REDIS_HEALTHY:
            try:
                self.redis_client = redis.StrictRedis(
                    host="localhost", 
                    port=6379, 
                    db=0,
                    decode_responses=True
                )
                logger.info("Redis连接成功，启用Redis缓存")
            except Exception as e:
                logger.warning(f"Redis连接失败，使用文件保存模式: {e}")
                self.redis_client = None
        
        logger.info(f"LiveDataSaver初始化完成: {instrument_id}")
    
    def add_quote_tick(self, tick: QuoteTick) -> None:
        """添加报价tick数据"""
        try:
            tick_data = {
                'timestamp': tick.ts_event,
                'ts_init': tick.ts_init,
                'bid_price': float(tick.bid_price),
                'ask_price': float(tick.ask_price),
                'bid_size': int(tick.bid_size),
                'ask_size': int(tick.ask_size),
                'instrument_id': str(tick.instrument_id),
                'type': 'quote'
            }
            
            self.quote_ticks_buffer.append(tick_data)
            
            # 同时保存到Redis（如果可用）
            if self.redis_client:
                self._save_to_redis('quote_tick', tick_data)
                
        except Exception as e:
            logger.error(f"添加报价tick失败: {e}")
    
    def add_trade_tick(self, tick: TradeTick) -> None:
        """添加交易tick数据"""
        try:
            tick_data = {
                'timestamp': tick.ts_event,
                'ts_init': tick.ts_init,
                'price': float(tick.price),
                'size': int(tick.size),
                'trade_id': str(tick.trade_id),
                'instrument_id': str(tick.instrument_id),
                'type': 'trade'
            }
            
            self.trade_ticks_buffer.append(tick_data)
            
            # 同时保存到Redis（如果可用）
            if self.redis_client:
                self._save_to_redis('trade_tick', tick_data)
                
        except Exception as e:
            logger.error(f"添加交易tick失败: {e}")
    
    def add_bar(self, bar: Bar) -> None:
        """添加K线数据"""
        try:
            bar_data = {
                'timestamp': bar.ts_event,
                'ts_init': bar.ts_init,
                'open': float(bar.open),
                'high': float(bar.high),
                'low': float(bar.low),
                'close': float(bar.close),
                'volume': int(bar.volume),
                'bar_type': str(bar.bar_type),
                'instrument_id': str(bar.bar_type.instrument_id),
                'type': 'bar'
            }
            
            self.bars_buffer.append(bar_data)
            
            # 同时保存到Redis（如果可用）
            if self.redis_client:
                self._save_to_redis('bar', bar_data)
                
        except Exception as e:
            logger.error(f"添加K线数据失败: {e}")
    
    def _save_to_redis(self, data_type: str, data: Dict[str, Any]) -> None:
        """保存数据到Redis"""
        if not self.redis_client:
            return
            
        try:
            key = f"etf_159506:{data_type}:{data['timestamp']}"
            self.redis_client.setex(
                key, 
                self.config['redis_ttl'], 
                json.dumps(data, ensure_ascii=False)
            )
        except Exception as e:
            logger.error(f"保存到Redis失败: {e}")
    
    def start_auto_save(self) -> None:
        """启动自动保存线程"""
        if self.is_running:
            logger.warning("数据保存器已在运行")
            return
            
        self.stop_save = False
        self.save_thread = threading.Thread(target=self._auto_save_loop, daemon=True)
        self.save_thread.start()
        self.is_running = True
        logger.info("自动保存线程已启动")
    
    def stop_auto_save(self) -> None:
        """停止自动保存线程"""
        if not self.is_running:
            return
            
        self.stop_save = True
        if self.save_thread and self.save_thread.is_alive():
            self.save_thread.join(timeout=10)
        self.is_running = False
        logger.info("自动保存线程已停止")
    
    def _auto_save_loop(self) -> None:
        """自动保存循环"""
        while not self.stop_save:
            try:
                time.sleep(self.config['save_interval'])
                
                if self.stop_save:
                    break
                
                # 检查是否有数据需要保存
                has_data = (
                    len(self.quote_ticks_buffer) > 0 or 
                    len(self.trade_ticks_buffer) > 0 or 
                    len(self.bars_buffer) > 0
                )
                
                if has_data:
                    logger.info(f"开始自动保存: 报价{len(self.quote_ticks_buffer)}条, "
                              f"交易{len(self.trade_ticks_buffer)}条, "
                              f"K线{len(self.bars_buffer)}条")
                    
                    # 保存数据到Parquet文件
                    self._save_buffer_data()
                    
                    # 清理旧数据
                    self._cleanup_old_data()
                    
            except Exception as e:
                logger.error(f"自动保存失败: {e}")
                import traceback
                logger.error(f"详细错误信息: {traceback.format_exc()}")
                time.sleep(30)  # 出错时等待30秒
    
    def _save_buffer_data(self) -> None:
        """保存缓冲区数据到Parquet文件"""
        try:
            # 使用文件锁避免并发写入
            lock_file = self.catalog_path / ".save_lock"
            
            if lock_file.exists():
                logger.debug("保存操作正在进行中，跳过本次保存")
                return
            
            # 创建锁文件
            try:
                lock_file.touch()
            except Exception as e:
                logger.warning(f"无法创建锁文件，跳过保存: {e}")
                return
            
            try:
                # 准备数据
                all_data = []
                
                # 添加报价数据
                for tick_data in list(self.quote_ticks_buffer):
                    all_data.append(tick_data)
                
                # 添加交易数据
                for tick_data in list(self.trade_ticks_buffer):
                    all_data.append(tick_data)
                
                # 添加K线数据
                for bar_data in list(self.bars_buffer):
                    all_data.append(bar_data)
                
                if not all_data:
                    logger.debug("没有数据需要保存")
                    return
                
                # 转换为DataFrame
                df = pd.DataFrame(all_data)
                
                # 生成文件名
                today = datetime.now()
                filename = f"live_data_{today.strftime('%Y%m%d')}.parquet"
                filepath = self.catalog_path / filename
                
                # 保存文件（使用临时文件避免并发写入问题）
                temp_filepath = filepath.with_suffix('.tmp')
                
                try:
                    logger.debug(f"开始保存数据到临时文件: {temp_filepath}")
                    df.to_parquet(temp_filepath, index=False)
                    
                    # 验证临时文件
                    if not temp_filepath.exists():
                        logger.error(f"临时文件创建失败: {temp_filepath}")
                        return
                    
                    # 移动临时文件到最终位置
                    if filepath.exists():
                        backup_path = filepath.with_suffix(f'.backup_{int(time.time())}')
                        filepath.rename(backup_path)
                    
                    temp_filepath.rename(filepath)
                    
                    # 验证最终文件
                    if not filepath.exists():
                        logger.error(f"最终文件创建失败: {filepath}")
                        return
                    
                    # 更新统计信息
                    self.total_quotes_saved += len(self.quote_ticks_buffer)
                    self.total_trades_saved += len(self.trade_ticks_buffer)
                    self.total_bars_saved += len(self.bars_buffer)
                    self.last_save_time = datetime.now()
                    
                    # 清空缓冲区
                    self.quote_ticks_buffer.clear()
                    self.trade_ticks_buffer.clear()
                    self.bars_buffer.clear()
                    
                    logger.info(f"数据保存完成: {filepath} ({len(df)} 条记录)")
                    
                except Exception as save_error:
                    logger.error(f"保存文件失败: {save_error}")
                    # 清理临时文件
                    if temp_filepath.exists():
                        temp_filepath.unlink()
                    raise
                    
            finally:
                # 清理锁文件
                try:
                    if lock_file.exists():
                        lock_file.unlink()
                except Exception as e:
                    logger.warning(f"清理锁文件失败: {e}")
                    
        except Exception as e:
            logger.error(f"保存缓冲区数据失败: {e}")
            import traceback
            logger.error(f"详细错误信息: {traceback.format_exc()}")
    
    def _cleanup_old_data(self) -> None:
        """清理旧数据"""
        try:
            cutoff_time = datetime.now() - timedelta(hours=self.config['cleanup_keep_hours'])
            
            # 清理Redis中的旧数据（如果使用Redis）
            if self.redis_client:
                try:
                    # 获取所有相关的Redis键
                    pattern = f"etf_159506:*:{int(cutoff_time.timestamp() * 1e9)}"
                    keys = self.redis_client.keys(f"etf_159506:*")
                    
                    for key in keys:
                        try:
                            # 检查时间戳并删除旧数据
                            key_parts = key.split(':')
                            if len(key_parts) >= 3:
                                timestamp = int(key_parts[2])
                                if timestamp < cutoff_time.timestamp() * 1e9:
                                    self.redis_client.delete(key)
                        except Exception:
                            continue
                            
                except Exception as e:
                    logger.warning(f"清理Redis旧数据失败: {e}")
            
            # 清理本地缓冲区（如果有内存压力）
            current_time = datetime.now()
            if (current_time - self.start_time).seconds > 3600:  # 运行超过1小时
                # 强制垃圾回收
                gc.collect()
                
        except Exception as e:
            logger.error(f"清理旧数据失败: {e}")
    
    def force_save_all_data(self) -> None:
        """强制保存所有数据"""
        try:
            logger.info("开始强制保存所有数据...")
            self._save_buffer_data()
            logger.info("强制保存完成")
        except Exception as e:
            logger.error(f"强制保存失败: {e}")
    
    def get_statistics(self) -> Dict[str, Any]:
        """获取统计信息"""
        return {
            'is_running': self.is_running,
            'total_quotes_saved': self.total_quotes_saved,
            'total_trades_saved': self.total_trades_saved,
            'total_bars_saved': self.total_bars_saved,
            'buffer_quotes': len(self.quote_ticks_buffer),
            'buffer_trades': len(self.trade_ticks_buffer),
            'buffer_bars': len(self.bars_buffer),
            'last_save_time': self.last_save_time.isoformat() if self.last_save_time else None,
            'start_time': self.start_time.isoformat(),
            'redis_available': self.redis_client is not None,
            'catalog_path': str(self.catalog_path),
        }
    
    def get_historical_data(
        self, 
        start_time: Optional[datetime] = None,
        end_time: Optional[datetime] = None,
        data_type: Optional[str] = None
    ) -> Dict[str, List[Dict[str, Any]]]:
        """
        获取历史数据
        
        Parameters
        ----------
        start_time : Optional[datetime]
            开始时间
        end_time : Optional[datetime]
            结束时间
        data_type : Optional[str]
            数据类型 ('quote', 'trade', 'bar', None表示所有)
            
        Returns
        -------
        Dict[str, List[Dict[str, Any]]]
            历史数据
        """
        try:
            result = {'quotes': [], 'trades': [], 'bars': []}
            
            # 如果没有指定时间范围，使用今天
            if not start_time:
                start_time = datetime.now().replace(hour=0, minute=0, second=0, microsecond=0)
            if not end_time:
                end_time = datetime.now()
            
            # 从Parquet文件读取数据
            parquet_files = list(self.catalog_path.glob("live_data_*.parquet"))
            
            for file_path in sorted(parquet_files):
                try:
                    df = pd.read_parquet(file_path)
                    
                    # 过滤时间范围
                    if 'timestamp' in df.columns:
                        df['datetime'] = pd.to_datetime(df['timestamp'], unit='ns')
                        df = df[(df['datetime'] >= start_time) & (df['datetime'] <= end_time)]
                    
                    # 按类型分类
                    if data_type is None or data_type == 'quote':
                        quotes = df[df['type'] == 'quote'].to_dict('records')
                        result['quotes'].extend(quotes)
                    
                    if data_type is None or data_type == 'trade':
                        trades = df[df['type'] == 'trade'].to_dict('records')
                        result['trades'].extend(trades)
                    
                    if data_type is None or data_type == 'bar':
                        bars = df[df['type'] == 'bar'].to_dict('records')
                        result['bars'].extend(bars)
                        
                except Exception as e:
                    logger.warning(f"读取文件失败 {file_path}: {e}")
                    continue
            
            # 从Redis读取数据（如果可用）
            if self.redis_client:
                try:
                    pattern = f"etf_159506:*"
                    keys = self.redis_client.keys(pattern)
                    
                    for key in keys:
                        try:
                            data_str = self.redis_client.get(key)
                            if data_str:
                                data = json.loads(data_str)
                                
                                # 检查时间范围
                                if 'timestamp' in data:
                                    data_time = datetime.fromtimestamp(data['timestamp'] / 1e9)
                                    if start_time <= data_time <= end_time:
                                        data_type_key = data.get('type', 'unknown')
                                        if data_type is None or data_type == data_type_key:
                                            if data_type_key == 'quote':
                                                result['quotes'].append(data)
                                            elif data_type_key == 'trade':
                                                result['trades'].append(data)
                                            elif data_type_key == 'bar':
                                                result['bars'].append(data)
                        except Exception:
                            continue
                            
                except Exception as e:
                    logger.warning(f"从Redis读取数据失败: {e}")
            
            # 按时间排序
            for key in result:
                result[key].sort(key=lambda x: x.get('timestamp', 0))
            
            return result
            
        except Exception as e:
            logger.error(f"获取历史数据失败: {e}")
            return {'quotes': [], 'trades': [], 'bars': []}
    
    def disconnect(self) -> None:
        """断开连接并保存数据"""
        try:
            logger.info("断开数据保存器连接...")
            
            # 停止自动保存
            self.stop_auto_save()
            
            # 强制保存所有数据
            self.force_save_all_data()
            
            # 关闭Redis连接
            if self.redis_client:
                try:
                    self.redis_client.close()
                except Exception:
                    pass
            
            logger.info("数据保存器已断开")
            
        except Exception as e:
            logger.error(f"断开数据保存器失败: {e}")


# 全局数据保存器实例
_global_data_saver: Optional[LiveDataSaver] = None


def set_global_data_saver(data_saver: LiveDataSaver) -> None:
    """设置全局数据保存器实例"""
    global _global_data_saver
    _global_data_saver = data_saver
    logger.info("全局数据保存器已设置")


def get_global_data_saver() -> Optional[LiveDataSaver]:
    """获取全局数据保存器实例"""
    return _global_data_saver


def create_default_data_saver(instrument_id: InstrumentId) -> LiveDataSaver:
    """创建默认数据保存器"""
    return LiveDataSaver(instrument_id=instrument_id)


if __name__ == "__main__":
    # 测试代码
    from nautilus_trader.model.identifiers import Symbol, Venue
    
    # 创建测试用的InstrumentId
    instrument_id = InstrumentId(Symbol("159506"), Venue("SZSE"))
    
    # 创建数据保存器
    data_saver = create_default_data_saver(instrument_id)
    
    # 启动自动保存
    data_saver.start_auto_save()
    
    try:
        # 模拟添加一些测试数据
        print("添加测试数据...")
        # 这里可以添加一些测试数据
        
        # 等待一段时间
        time.sleep(10)
        
        # 获取统计信息
        stats = data_saver.get_statistics()
        print(f"统计信息: {stats}")
        
    finally:
        # 断开连接
        data_saver.disconnect()
        print("测试完成")
