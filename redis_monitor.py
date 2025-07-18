#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
Redis监控脚本
"""

import redis
import time
from datetime import datetime

def monitor_redis():
    """监控Redis状态"""
    try:
        r = redis.StrictRedis(host='localhost', port=6379)
        
        while True:
            # 获取Redis信息
            info = r.info()
            memory_info = r.info('memory')
            persistence_info = r.info('persistence')
            
            # 显示状态
            print(f"\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Redis状态:")
            print(f"  连接数: {info.get('connected_clients', 0)}")
            print(f"  内存使用: {memory_info.get('used_memory_human', 'N/A')}")
            print(f"  内存峰值: {memory_info.get('used_memory_peak_human', 'N/A')}")
            print(f"  RDB保存次数: {persistence_info.get('rdb_saves', 0)}")
            print(f"  AOF文件大小: {persistence_info.get('aof_current_size_human', 'N/A')}")
            
            # 检查关键数据
            keys = r.keys('*')
            print(f"  总keys数: {len(keys)}")
            
            # 检查159506相关数据
            etf_keys = [k for k in keys if b'159506' in k]
            print(f"  159506相关keys: {len(etf_keys)}")
            
            time.sleep(30)  # 每30秒检查一次
            
    except KeyboardInterrupt:
        print("\n监控已停止")
    except Exception as e:
        print(f"监控失败: {e}")

if __name__ == "__main__":
    monitor_redis()
