#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
159506 ETF适配器连接管理工具
解决并发连接问题
"""

import asyncio
import logging
import time
from datetime import datetime, timedelta
from etf_159506_adapter import ETF159506Adapter

logging.basicConfig(level=logging.INFO, format='%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)


class AdapterConnectionManager:
    """适配器连接管理器"""
    
    def __init__(self, token: str = "d0c519adcd47d266f1c96750d4e80aa6"):
        self.token = token
        self.config = {
            'token': token,
            'stock_code': '159506'
        }
        self.adapter = None
        self.connection_history = []
        
    async def create_adapter(self) -> ETF159506Adapter:
        """创建新的适配器实例"""
        if self.adapter:
            await self.adapter.disconnect()
        
        self.adapter = ETF159506Adapter(self.config)
        return self.adapter
    
    async def connect_with_retry(self, max_retries: int = 3, retry_delay: int = 5) -> bool:
        """带重试的连接"""
        for attempt in range(max_retries):
            try:
                logger.info(f"第{attempt + 1}次尝试连接...")
                
                # 创建新的适配器实例
                adapter = await self.create_adapter()
                
                # 尝试连接
                connected = await adapter.connect()
                
                if connected:
                    self.adapter = adapter
                    self.connection_history.append({
                        'timestamp': datetime.now(),
                        'attempt': attempt + 1,
                        'success': True
                    })
                    logger.info("✅ 连接成功！")
                    return True
                else:
                    # 检查连接健康状态
                    health = await adapter.check_connection_health()
                    if not health['can_retry']:
                        logger.warning("⚠️ 连接尝试次数已达上限，需要等待")
                        await self.wait_for_connection_reset()
                        continue
                    
                    self.connection_history.append({
                        'timestamp': datetime.now(),
                        'attempt': attempt + 1,
                        'success': False,
                        'reason': 'connection_failed'
                    })
                    
                    if attempt < max_retries - 1:
                        logger.info(f"连接失败，{retry_delay}秒后重试...")
                        await asyncio.sleep(retry_delay)
                    else:
                        logger.error("❌ 所有重试都失败了")
                        return False
                        
            except Exception as e:
                logger.error(f"连接过程中发生错误: {e}")
                self.connection_history.append({
                    'timestamp': datetime.now(),
                    'attempt': attempt + 1,
                    'success': False,
                    'reason': str(e)
                })
                
                if attempt < max_retries - 1:
                    await asyncio.sleep(retry_delay)
                else:
                    return False
        
        return False
    
    async def wait_for_connection_reset(self, wait_time: int = 60):
        """等待连接重置"""
        logger.info(f"等待{wait_time}秒让服务器重置连接计数...")
        await asyncio.sleep(wait_time)
    
    async def test_historical_data(self) -> bool:
        """测试历史数据获取"""
        if not self.adapter or not self.adapter.is_connected:
            logger.error("适配器未连接")
            return False
        
        try:
            logger.info("测试历史数据获取...")
            data_client = self.adapter.get_data_client()
            
            historical_data = await data_client.get_historical_data(
                symbol='159506',
                start_date='2024-01-01',
                end_date='2024-01-31',
                kline_type='day',
                fq='前复权',
                limit=5
            )
            
            if historical_data:
                logger.info(f"✅ 历史数据获取成功: {len(historical_data)}条记录")
                return True
            else:
                logger.warning("⚠️ 历史数据获取失败")
                return False
                
        except Exception as e:
            logger.error(f"历史数据测试失败: {e}")
            return False
    
    async def test_realtime_data(self, duration: int = 10) -> bool:
        """测试实时数据订阅"""
        if not self.adapter or not self.adapter.is_connected:
            logger.error("适配器未连接")
            return False
        
        try:
            logger.info(f"测试实时数据订阅({duration}秒)...")
            data_client = self.adapter.get_data_client()
            
            data_received = False
            
            async def quote_callback(data):
                nonlocal data_received
                data_received = True
                logger.info(f"收到实时数据: 价格={data.get('price')}, 成交量={data.get('volume')}")
            
            # 订阅数据
            data_client.subscribe_quotes('159506', quote_callback)
            
            # 等待数据
            await asyncio.sleep(duration)
            
            if data_received:
                logger.info("✅ 实时数据订阅成功")
                return True
            else:
                logger.warning("⚠️ 未收到实时数据")
                return False
                
        except Exception as e:
            logger.error(f"实时数据测试失败: {e}")
            return False
    
    async def get_connection_summary(self) -> dict:
        """获取连接摘要"""
        if not self.adapter:
            return {'status': 'no_adapter'}
        
        try:
            status = await self.adapter.get_status()
            health = await self.adapter.check_connection_health()
            
            return {
                'status': 'connected' if self.adapter.is_connected else 'disconnected',
                'adapter_status': status,
                'health_status': health,
                'connection_history': self.connection_history[-5:] if self.connection_history else []
            }
        except Exception as e:
            return {'status': 'error', 'error': str(e)}
    
    async def cleanup(self):
        """清理资源"""
        if self.adapter:
            await self.adapter.disconnect()
            self.adapter = None
        logger.info("连接管理器已清理")


async def main():
    """主函数"""
    print("🔧 159506 ETF适配器连接管理工具")
    print("="*60)
    
    manager = AdapterConnectionManager()
    
    try:
        # 尝试连接
        print("📡 尝试连接适配器...")
        connected = await manager.connect_with_retry(max_retries=3, retry_delay=10)
        
        if connected:
            print("✅ 适配器连接成功！")
            
            # 获取连接摘要
            summary = await manager.get_connection_summary()
            print("📊 连接摘要:")
            for key, value in summary.items():
                print(f"   {key}: {value}")
            
            # 测试历史数据
            print("\n📈 测试历史数据...")
            historical_ok = await manager.test_historical_data()
            
            # 测试实时数据
            print("\n📡 测试实时数据...")
            realtime_ok = await manager.test_realtime_data(duration=5)
            
            # 总结
            print("\n" + "="*60)
            print("测试结果:")
            print(f"   连接状态: {'✅ 成功' if connected else '❌ 失败'}")
            print(f"   历史数据: {'✅ 成功' if historical_ok else '❌ 失败'}")
            print(f"   实时数据: {'✅ 成功' if realtime_ok else '❌ 失败'}")
            
            if connected and historical_ok and realtime_ok:
                print("\n🎉 所有功能测试通过！")
            else:
                print("\n⚠️ 部分功能测试失败，请检查配置")
                
        else:
            print("❌ 适配器连接失败")
            print("\n💡 建议:")
            print("1. 检查网络连接")
            print("2. 确认token是否有效")
            print("3. 等待一段时间后重试（可能达到并发连接上限）")
            print("4. 关闭其他使用相同token的程序")
            
    except Exception as e:
        print(f"❌ 程序执行失败: {e}")
        import traceback
        traceback.print_exc()
        
    finally:
        # 清理资源
        await manager.cleanup()


if __name__ == "__main__":
    asyncio.run(main()) 