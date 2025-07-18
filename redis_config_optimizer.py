#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
Redis配置优化脚本
用于提高159506 ETF项目的数据安全性
"""

import redis
import json
import os
from datetime import datetime
import logging

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

class RedisConfigOptimizer:
    """Redis配置优化器"""
    
    def __init__(self, redis_host: str = "localhost", redis_port: int = 6379):
        self.redis_host = redis_host
        self.redis_port = redis_port
        self.r = redis.StrictRedis(host=redis_host, port=redis_port)
        
    def check_current_config(self):
        """检查当前配置"""
        print("=" * 60)
        print("Redis当前配置检查")
        print("=" * 60)
        
        try:
            # 检查持久化配置
            save_config = self.r.config_get('save')
            appendonly_config = self.r.config_get('appendonly')
            dir_config = self.r.config_get('dir')
            
            print(f"RDB持久化配置: {save_config['save']}")
            print(f"AOF持久化配置: {appendonly_config['appendonly']}")
            print(f"数据目录: {dir_config['dir']}")
            
            # 检查数据状态
            info = self.r.info('persistence')
            print(f"RDB保存次数: {info.get('rdb_saves', 0)}")
            print(f"最后保存时间: {info.get('rdb_last_save_time', 0)}")
            
            # 检查内存使用
            memory_info = self.r.info('memory')
            print(f"已用内存: {memory_info.get('used_memory_human', 'N/A')}")
            print(f"内存峰值: {memory_info.get('used_memory_peak_human', 'N/A')}")
            
            return {
                'save_config': save_config['save'],
                'appendonly': appendonly_config['appendonly'],
                'data_dir': dir_config['dir'],
                'rdb_saves': info.get('rdb_saves', 0),
                'memory_used': memory_info.get('used_memory_human', 'N/A')
            }
            
        except Exception as e:
            logger.error(f"检查配置失败: {e}")
            return None
    
    def optimize_for_quant_trading(self):
        """为量化交易优化配置"""
        print("\n" + "=" * 60)
        print("Redis量化交易优化配置")
        print("=" * 60)
        
        try:
            # 1. 启用AOF持久化（更安全）
            print("1. 启用AOF持久化...")
            self.r.config_set('appendonly', 'yes')
            self.r.config_set('appendfsync', 'everysec')  # 每秒同步一次
            
            # 2. 优化RDB配置（更频繁保存）
            print("2. 优化RDB配置...")
            # 更频繁的保存：5分钟1个key变化，1分钟100个key变化
            self.r.config_set('save', '300 1 60 100')
            
            # 3. 设置最大内存限制
            print("3. 设置内存限制...")
            self.r.config_set('maxmemory', '512mb')  # 限制512MB内存
            self.r.config_set('maxmemory-policy', 'allkeys-lru')  # LRU淘汰策略
            
            # 4. 启用压缩
            print("4. 启用数据压缩...")
            self.r.config_set('rdbcompression', 'yes')
            
            # 5. 保存配置
            print("5. 保存配置到文件...")
            self.r.config_rewrite()
            
            print("✅ Redis配置优化完成！")
            
            # 显示新配置
            print("\n新配置:")
            new_config = self.check_current_config()
            
            return new_config
            
        except Exception as e:
            logger.error(f"优化配置失败: {e}")
            return None
    
    def create_backup_script(self):
        """创建备份脚本"""
        backup_script = '''#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
Redis数据备份脚本
"""

import redis
import json
import os
from datetime import datetime
import shutil

def backup_redis_data():
    """备份Redis数据"""
    try:
        # 连接Redis
        r = redis.StrictRedis(host='localhost', port=6379)
        
        # 创建备份目录
        backup_dir = f"redis_backup_{datetime.now().strftime('%Y%m%d_%H%M%S')}"
        os.makedirs(backup_dir, exist_ok=True)
        
        # 获取所有keys
        keys = r.keys('*')
        print(f"发现 {len(keys)} 个keys")
        
        # 备份数据
        data_backup = {}
        for key in keys:
            try:
                key_str = key.decode('utf-8') if isinstance(key, bytes) else str(key)
                value = r.get(key)
                if value:
                    data_backup[key_str] = value.decode('utf-8') if isinstance(value, bytes) else str(value)
            except Exception as e:
                print(f"备份key {key} 失败: {e}")
        
        # 保存备份文件
        backup_file = os.path.join(backup_dir, 'redis_data.json')
        with open(backup_file, 'w', encoding='utf-8') as f:
            json.dump(data_backup, f, ensure_ascii=False, indent=2)
        
        print(f"✅ 备份完成: {backup_file}")
        print(f"备份了 {len(data_backup)} 个keys")
        
        return backup_file
        
    except Exception as e:
        print(f"❌ 备份失败: {e}")
        return None

if __name__ == "__main__":
    backup_redis_data()
'''
        
        # 保存备份脚本
        with open('redis_backup.py', 'w', encoding='utf-8') as f:
            f.write(backup_script)
        
        print("✅ 备份脚本已创建: redis_backup.py")
        print("使用方法: python redis_backup.py")
    
    def create_monitoring_script(self):
        """创建监控脚本"""
        monitoring_script = '''#!/usr/bin/env python3
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
            print(f"\\n[{datetime.now().strftime('%Y-%m-%d %H:%M:%S')}] Redis状态:")
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
        print("\\n监控已停止")
    except Exception as e:
        print(f"监控失败: {e}")

if __name__ == "__main__":
    monitor_redis()
'''
        
        # 保存监控脚本
        with open('redis_monitor.py', 'w', encoding='utf-8') as f:
            f.write(monitoring_script)
        
        print("✅ 监控脚本已创建: redis_monitor.py")
        print("使用方法: python redis_monitor.py")

def main():
    """主函数"""
    print("Redis配置优化工具")
    print("=" * 60)
    
    optimizer = RedisConfigOptimizer()
    
    # 检查当前配置
    current_config = optimizer.check_current_config()
    
    if current_config:
        print(f"\n当前配置分析:")
        if current_config['appendonly'] == 'no':
            print("⚠️  AOF持久化未启用 - 数据安全性较低")
        else:
            print("✅ AOF持久化已启用 - 数据安全性较高")
        
        print(f"📊 当前内存使用: {current_config['memory_used']}")
        print(f"💾 RDB保存次数: {current_config['rdb_saves']}")
    
    # 询问是否优化
    print("\n是否要优化Redis配置以提高数据安全性？")
    print("优化内容包括:")
    print("1. 启用AOF持久化")
    print("2. 更频繁的RDB保存")
    print("3. 设置内存限制")
    print("4. 启用数据压缩")
    
    try:
        choice = input("是否优化？(y/n): ").strip().lower()
        
        if choice == 'y':
            optimizer.optimize_for_quant_trading()
            
            # 创建辅助脚本
            print("\n创建辅助脚本...")
            optimizer.create_backup_script()
            optimizer.create_monitoring_script()
            
            print("\n" + "=" * 60)
            print("优化完成！建议:")
            print("1. 定期运行: python redis_backup.py")
            print("2. 监控状态: python redis_monitor.py")
            print("3. 重启Redis服务以应用新配置")
            print("=" * 60)
        else:
            print("跳过优化")
            
    except KeyboardInterrupt:
        print("\n用户中断")
    except Exception as e:
        print(f"操作失败: {e}")

if __name__ == "__main__":
    main() 