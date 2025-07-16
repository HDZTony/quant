"""
自定义数据源模块
专门处理HTTP API数据，支持jvQuant数据格式
"""

import asyncio
import aiohttp
import pandas as pd
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import json
import logging

logger = logging.getLogger(__name__)


class CustomDataSource:
    """自定义数据源，支持HTTP API数据获取"""
    
    def __init__(self, base_url: str = "http://121.43.57.182:21936"):
        self.base_url = base_url
        self.session = None
        
    async def __aenter__(self):
        self.session = aiohttp.ClientSession()
        return self
        
    async def __aexit__(self, exc_type, exc_val, exc_tb):
        if self.session:
            await self.session.close()
    
    async def get_kline_data(self, 
                           token: str,
                           code: str = "159506",
                           cate: str = "etf",
                           kline_type: str = "day",
                           fq: str = "前复权",
                           limit: int = 240) -> Optional[pd.DataFrame]:
        """
        获取K线数据
        
        Args:
            token: API令牌
            code: 股票代码
            cate: 类别 (etf, stock等)
            kline_type: K线类型 (day, week, month, 1m, 5m等)
            fq: 复权类型 (前复权, 后复权, 不复权)
            limit: 数据条数限制
            
        Returns:
            DataFrame格式的K线数据
        """
        try:
            url = f"{self.base_url}/sql"
            params = {
                "token": token,
                "mode": "kline",
                "cate": cate,
                "code": code,
                "type": kline_type,
                "fq": fq,
                "limit": limit
            }
            
            async with self.session.get(url, params=params) as response:
                if response.status == 200:
                    data = await response.json()
                    
                    if data.get("code") == 0 and data.get("data"):
                        return self._parse_kline_data(data["data"])
                    else:
                        logger.error(f"API返回错误: {data}")
                        return None
                else:
                    logger.error(f"HTTP请求失败: {response.status}")
                    return None
                    
        except Exception as e:
            logger.error(f"获取K线数据失败: {e}")
            return None
    
    def _parse_kline_data(self, data: Dict[str, Any]) -> pd.DataFrame:
        """
        解析K线数据
        
        Args:
            data: API返回的数据字典
            
        Returns:
            标准化的DataFrame
        """
        try:
            # 字段映射
            field_mapping = {
                "日期": "datetime",
                "开盘": "open",
                "收盘": "close", 
                "最高": "high",
                "最低": "low",
                "成交量": "volume",
                "成交额": "amount",
                "振幅": "amplitude",
                "涨跌幅": "change_pct",
                "涨跌额": "change_amount",
                "换手率": "turnover_rate"
            }
            
            # 获取字段列表
            fields = data.get("fields", [])
            kline_list = data.get("list", [])
            
            if not kline_list:
                return pd.DataFrame()
            
            # 创建DataFrame
            df = pd.DataFrame(kline_list, columns=fields)
            
            # 重命名列
            df = df.rename(columns=field_mapping)
            
            # 转换数据类型
            numeric_columns = ["open", "close", "high", "low", "volume", "amount", 
                             "amplitude", "change_pct", "change_amount", "turnover_rate"]
            
            for col in numeric_columns:
                if col in df.columns:
                    df[col] = pd.to_numeric(df[col], errors='coerce')
            
            # 转换日期
            if "datetime" in df.columns:
                df["datetime"] = pd.to_datetime(df["datetime"])
                df.set_index("datetime", inplace=True)
            
            # 按时间排序
            df.sort_index(inplace=True)
            
            return df
            
        except Exception as e:
            logger.error(f"解析K线数据失败: {e}")
            return pd.DataFrame()
    
    async def get_realtime_data(self, token: str, code: str = "159506") -> Optional[Dict]:
        """
        获取实时数据（如果有实时API）
        
        Args:
            token: API令牌
            code: 股票代码
            
        Returns:
            实时数据字典
        """
        # 这里可以根据实际的实时API接口来实现
        # 目前先返回None，等待实时API接口信息
        return None
    
    def convert_to_nautilus_format(self, df: pd.DataFrame) -> List[Dict]:
        """
        将DataFrame转换为Nautilus Trader格式
        
        Args:
            df: K线数据DataFrame
            
        Returns:
            Nautilus格式的数据列表
        """
        nautilus_data = []
        
        for timestamp, row in df.iterrows():
            bar_data = {
                "timestamp": timestamp,
                "open": float(row["open"]),
                "high": float(row["high"]),
                "low": float(row["low"]),
                "close": float(row["close"]),
                "volume": float(row["volume"]),
                "amount": float(row["amount"]) if "amount" in row else 0.0,
                "amplitude": float(row["amplitude"]) if "amplitude" in row else 0.0,
                "change_pct": float(row["change_pct"]) if "change_pct" in row else 0.0,
                "turnover_rate": float(row["turnover_rate"]) if "turnover_rate" in row else 0.0
            }
            nautilus_data.append(bar_data)
        
        return nautilus_data


class DataProcessor:
    """数据处理类，用于处理和分析数据"""
    
    def __init__(self):
        self.data_cache = {}
    
    def calculate_indicators(self, df: pd.DataFrame) -> pd.DataFrame:
        """
        计算技术指标
        
        Args:
            df: 原始数据DataFrame
            
        Returns:
            包含技术指标的DataFrame
        """
        result_df = df.copy()
        
        # 计算移动平均线
        result_df['ma5'] = result_df['close'].rolling(window=5).mean()
        result_df['ma10'] = result_df['close'].rolling(window=10).mean()
        result_df['ma20'] = result_df['close'].rolling(window=20).mean()
        
        # 计算成交量移动平均
        result_df['volume_ma5'] = result_df['volume'].rolling(window=5).mean()
        result_df['volume_ma10'] = result_df['volume'].rolling(window=10).mean()
        
        # 计算成交量比率
        result_df['volume_ratio'] = result_df['volume'] / result_df['volume_ma5']
        
        # 计算价格变化
        result_df['price_change'] = result_df['close'].diff()
        result_df['price_change_pct'] = result_df['close'].pct_change() * 100
        
        return result_df
    
    def get_latest_data(self, df: pd.DataFrame, periods: int = 1) -> pd.DataFrame:
        """
        获取最新数据
        
        Args:
            df: 数据DataFrame
            periods: 获取的期数
            
        Returns:
            最新的数据
        """
        return df.tail(periods)
    
    def save_data(self, df: pd.DataFrame, filename: str):
        """
        保存数据到文件
        
        Args:
            df: 数据DataFrame
            filename: 文件名
        """
        try:
            df.to_csv(filename, encoding='utf-8-sig')
            logger.info(f"数据已保存到: {filename}")
        except Exception as e:
            logger.error(f"保存数据失败: {e}")


# 使用示例
async def main():
    """使用示例"""
    token = "d0c519adcd47d266f1c96750d4e80aa6"
    
    async with CustomDataSource() as data_source:
        # 获取K线数据
        df = await data_source.get_kline_data(token, "159506", "etf", "day", "前复权", 240)
        
        if df is not None and not df.empty:
            print(f"获取到 {len(df)} 条数据")
            print("最新数据:")
            print(df.tail())
            
            # 计算指标
            processor = DataProcessor()
            df_with_indicators = processor.calculate_indicators(df)
            
            # 保存数据
            processor.save_data(df_with_indicators, "159506_kline_data.csv")
            
            # 转换为Nautilus格式
            nautilus_data = data_source.convert_to_nautilus_format(df)
            print(f"转换为Nautilus格式: {len(nautilus_data)} 条记录")
        else:
            print("获取数据失败")


if __name__ == "__main__":
    asyncio.run(main()) 