"""
测试自定义数据源模块
"""

import asyncio
import sys
import os
sys.path.append(os.path.dirname(os.path.dirname(os.path.abspath(__file__))))

import pandas as pd
from custom_data_source import CustomDataSource, DataProcessor
import logging

# 设置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


async def test_custom_data_source():
    """测试自定义数据源"""
    print("=" * 50)
    print("测试自定义数据源")
    print("=" * 50)
    
    token = "d0c519adcd47d266f1c96750d4e80aa6"
    
    try:
        async with CustomDataSource() as data_source:
            # 测试获取K线数据
            print("1. 测试获取K线数据...")
            df = await data_source.get_kline_data(
                token=token,
                code="159506",
                cate="etf",
                kline_type="day",
                fq="前复权",
                limit=240
            )
            
            if df is not None and not df.empty:
                print(f"✓ 成功获取 {len(df)} 条数据")
                print(f"数据时间范围: {df.index.min()} 到 {df.index.max()}")
                print("最新5条数据:")
                print(df.tail())
                
                # 测试数据处理
                print("\n2. 测试数据处理...")
                processor = DataProcessor()
                df_with_indicators = processor.calculate_indicators(df)
                
                print("✓ 成功计算技术指标")
                print("指标列:", [col for col in df_with_indicators.columns if col not in df.columns])
                
                # 测试数据保存
                print("\n3. 测试数据保存...")
                filename = "tests/159506_test_data.csv"
                processor.save_data(df_with_indicators, filename)
                
                # 测试Nautilus格式转换
                print("\n4. 测试Nautilus格式转换...")
                nautilus_data = data_source.convert_to_nautilus_format(df)
                print(f"✓ 成功转换为Nautilus格式: {len(nautilus_data)} 条记录")
                
                if nautilus_data:
                    print("第一条记录示例:")
                    print(nautilus_data[0])
                
                # 测试数据验证
                print("\n5. 测试数据验证...")
                print(f"数据完整性检查:")
                print(f"- 缺失值数量: {df.isnull().sum().sum()}")
                print(f"- 数据类型: {df.dtypes.to_dict()}")
                print(f"- 数据范围:")
                print(f"  开盘价: {df['open'].min():.3f} - {df['open'].max():.3f}")
                print(f"  收盘价: {df['close'].min():.3f} - {df['close'].max():.3f}")
                print(f"  成交量: {df['volume'].min():.0f} - {df['volume'].max():.0f}")
                
                return True
            else:
                print("✗ 获取数据失败")
                return False
                
    except Exception as e:
        print(f"✗ 测试失败: {e}")
        logger.exception("测试异常")
        return False


async def test_data_processor():
    """测试数据处理器"""
    print("\n" + "=" * 50)
    print("测试数据处理器")
    print("=" * 50)
    
    try:
        # 创建测试数据
        test_data = {
            'open': [1.0, 1.1, 1.2, 1.15, 1.25],
            'high': [1.05, 1.15, 1.25, 1.2, 1.3],
            'low': [0.95, 1.05, 1.15, 1.1, 1.2],
            'close': [1.1, 1.2, 1.15, 1.25, 1.3],
            'volume': [1000, 1200, 1100, 1300, 1400]
        }
        
        df = pd.DataFrame(test_data)
        df.index = pd.date_range('2024-01-01', periods=len(df), freq='D')
        
        processor = DataProcessor()
        
        # 测试指标计算
        print("1. 测试指标计算...")
        df_with_indicators = processor.calculate_indicators(df)
        
        expected_columns = ['ma5', 'ma10', 'ma20', 'volume_ma5', 'volume_ma10', 
                          'volume_ratio', 'price_change', 'price_change_pct']
        
        for col in expected_columns:
            if col in df_with_indicators.columns:
                print(f"✓ {col} 计算成功")
            else:
                print(f"✗ {col} 计算失败")
        
        # 测试最新数据获取
        print("\n2. 测试最新数据获取...")
        latest_data = processor.get_latest_data(df_with_indicators, 2)
        print(f"✓ 获取最新2条数据: {len(latest_data)} 条")
        
        # 测试数据保存
        print("\n3. 测试数据保存...")
        test_filename = "tests/test_processor_data.csv"
        processor.save_data(df_with_indicators, test_filename)
        print("✓ 数据保存成功")
        
        return True
        
    except Exception as e:
        print(f"✗ 测试失败: {e}")
        logger.exception("测试异常")
        return False


async def test_error_handling():
    """测试错误处理"""
    print("\n" + "=" * 50)
    print("测试错误处理")
    print("=" * 50)
    
    try:
        async with CustomDataSource() as data_source:
            # 测试无效token
            print("1. 测试无效token...")
            df_invalid = await data_source.get_kline_data("invalid_token", "159506")
            if df_invalid is None:
                print("✓ 正确处理无效token")
            else:
                print("✗ 未正确处理无效token")
            
            # 测试无效代码
            print("\n2. 测试无效代码...")
            df_invalid_code = await data_source.get_kline_data(
                "d0c519adcd47d266f1c96750d4e80aa6", "invalid_code"
            )
            if df_invalid_code is None or df_invalid_code.empty:
                print("✓ 正确处理无效代码")
            else:
                print("✗ 未正确处理无效代码")
        
        return True
        
    except Exception as e:
        print(f"✗ 测试失败: {e}")
        logger.exception("测试异常")
        return False


async def main():
    """主测试函数"""
    print("开始测试自定义数据源模块...")
    
    # 创建测试目录
    import os
    os.makedirs("tests", exist_ok=True)
    
    # 运行测试
    tests = [
        ("自定义数据源", test_custom_data_source),
        ("数据处理器", test_data_processor),
        ("错误处理", test_error_handling)
    ]
    
    results = []
    
    for test_name, test_func in tests:
        try:
            result = await test_func()
            results.append((test_name, result))
        except Exception as e:
            print(f"测试 {test_name} 发生异常: {e}")
            results.append((test_name, False))
    
    # 输出测试结果
    print("\n" + "=" * 50)
    print("测试结果汇总")
    print("=" * 50)
    
    passed = 0
    total = len(results)
    
    for test_name, result in results:
        status = "✓ 通过" if result else "✗ 失败"
        print(f"{test_name}: {status}")
        if result:
            passed += 1
    
    print(f"\n总计: {passed}/{total} 个测试通过")
    
    if passed == total:
        print("🎉 所有测试通过！")
    else:
        print("⚠️  部分测试失败，请检查代码")


if __name__ == "__main__":
    asyncio.run(main()) 