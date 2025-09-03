import pandas as pd
import sys
import numpy as np

def analyze_parquet_file(filepath):
    try:
        df = pd.read_parquet(filepath)
        print('数据形状:', df.shape)
        print('列名:', df.columns.tolist())
        print('数据类型:')
        print(df.dtypes)
        print('前50行数据:')
        print(df.head(50))
        print('数据统计:')
        print(df.describe())
        
        # 检查是否有空值
        print('\n空值统计:')
        print(df.isnull().sum())
        
        # 检查数据类型问题
        print('\n数据类型检查:')
        for col in df.columns:
            print(f'{col}: {df[col].dtype}')
            if df[col].dtype == 'object':
                print(f'  {col} 前几个值: {df[col].head().tolist()}')
        
        # 分析数据质量问题
        print('\n数据质量分析:')
        
        # 检查价格数据
        if 'bid_price' in df.columns and 'ask_price' in df.columns:
            quote_data = df[df['type'] == 'quote'].copy()
            if not quote_data.empty:
                print(f'报价数据统计: {len(quote_data)} 条')
                print(f'买价范围: {quote_data["bid_price"].min():.4f} - {quote_data["bid_price"].max():.4f}')
                print(f'卖价范围: {quote_data["ask_price"].min():.4f} - {quote_data["ask_price"].max():.4f}')
                print(f'价差范围: {(quote_data["ask_price"] - quote_data["bid_price"]).min():.4f} - {(quote_data["ask_price"] - quote_data["bid_price"]).max():.4f}')
        
        # 检查交易数据
        if 'price' in df.columns:
            trade_data = df[df['type'] == 'trade'].copy()
            if not trade_data.empty:
                print(f'交易数据统计: {len(trade_data)} 条')
                print(f'价格范围: {trade_data["price"].min():.4f} - {trade_data["price"].max():.4f}')
                print(f'成交量范围: {trade_data["size"].min():.0f} - {trade_data["size"].max():.0f}')
        
        return df
    except Exception as e:
        print(f"读取文件失败: {e}")
        return None

def identify_data_issues(df):
    """识别数据问题"""
    issues = []
    
    # 检查数据类型问题
    for col in df.columns:
        if col in ['bid_size', 'ask_size', 'size'] and df[col].dtype == 'float64':
            issues.append(f"列 {col} 应该是整数类型，但现在是浮点数")
        
        if col in ['bid_price', 'ask_price', 'price'] and df[col].dtype == 'float64':
            # 检查精度问题
            non_null_data = df[col].dropna()
            if not non_null_data.empty:
                # 检查是否有精度不一致的问题
                decimal_places = non_null_data.apply(lambda x: len(str(x).split('.')[-1]) if '.' in str(x) else 0)
                unique_precisions = decimal_places.unique()
                if len(unique_precisions) > 1:
                    issues.append(f"列 {col} 存在精度不一致问题: {unique_precisions}")
    
    # 检查数据完整性问题
    quote_data = df[df['type'] == 'quote']
    trade_data = df[df['type'] == 'trade']
    
    if not quote_data.empty:
        # 检查报价数据的完整性
        missing_bid = quote_data['bid_price'].isnull().sum()
        missing_ask = quote_data['ask_price'].isnull().sum()
        if missing_bid > 0 or missing_ask > 0:
            issues.append(f"报价数据缺失: bid_price缺失{missing_bid}条, ask_price缺失{missing_ask}条")
    
    if not trade_data.empty:
        # 检查交易数据的完整性
        missing_price = trade_data['price'].isnull().sum()
        missing_size = trade_data['size'].isnull().sum()
        if missing_price > 0 or missing_size > 0:
            issues.append(f"交易数据缺失: price缺失{missing_price}条, size缺失{missing_size}条")
    
    return issues

if __name__ == "__main__":
    filepath = 'catalog/etf_159506_cache/cache_data_20250903.parquet'
    df = analyze_parquet_file(filepath)
    
    if df is not None:
        print('\n' + '='*60)
        print('数据问题识别:')
        issues = identify_data_issues(df)
        if issues:
            for i, issue in enumerate(issues, 1):
                print(f"{i}. {issue}")
        else:
            print("未发现明显的数据问题")
