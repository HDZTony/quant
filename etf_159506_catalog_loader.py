#!/usr/bin/env python3
# -*- coding:utf-8 -*-
"""
159506 ETF Catalog数据加载器
专门用于加载和分析159506 ETF的catalog数据，支持回测和数据分析
"""

import pandas as pd
import numpy as np
from pathlib import Path
import json
import matplotlib.pyplot as plt
import seaborn as sns
from datetime import datetime, timedelta
from typing import Dict, List, Optional, Any
import logging

# 配置日志
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)


class ETF159506CatalogLoader:
    """159506 ETF Catalog数据加载器"""
    
    def __init__(self, catalog_path: str = "catalog/etf_159506"):
        self.catalog_path = Path(catalog_path)
        self.metadata = None
        self.data_files = []
        
        logger.info(f"初始化159506 ETF Catalog加载器: {self.catalog_path}")
        
        # 检查目录是否存在
        if not self.catalog_path.exists():
            logger.warning(f"Catalog目录不存在: {self.catalog_path}")
            return
        
        # 加载元数据
        self._load_metadata()
        
        # 扫描数据文件
        self._scan_data_files()
    
    def _load_metadata(self):
        """加载元数据"""
        metadata_file = self.catalog_path / 'metadata.json'
        if metadata_file.exists():
            try:
                with open(metadata_file, 'r', encoding='utf-8') as f:
                    self.metadata = json.load(f)
                logger.info(f"加载元数据成功: {self.metadata}")
            except Exception as e:
                logger.error(f"加载元数据失败: {e}")
        else:
            logger.warning("元数据文件不存在")
    
    def _scan_data_files(self):
        """扫描数据文件"""
        try:
            # 查找所有parquet文件
            parquet_files = list(self.catalog_path.glob("*.parquet"))
            self.data_files = sorted(parquet_files)
            
            logger.info(f"找到 {len(self.data_files)} 个数据文件")
            for file in self.data_files:
                logger.info(f"  - {file.name}")
                
        except Exception as e:
            logger.error(f"扫描数据文件失败: {e}")
    
    def get_data_files_info(self) -> List[Dict]:
        """获取数据文件信息"""
        file_info = []
        
        for file in self.data_files:
            try:
                # 读取文件基本信息
                df = pd.read_parquet(file)
                
                info = {
                    'filename': file.name,
                    'filepath': str(file),
                    'size_mb': file.stat().st_size / (1024 * 1024),
                    'records': len(df),
                    'start_time': df['timestamp'].min() if 'timestamp' in df.columns else None,
                    'end_time': df['timestamp'].max() if 'timestamp' in df.columns else None,
                    'columns': list(df.columns)
                }
                file_info.append(info)
                
            except Exception as e:
                logger.error(f"读取文件信息失败 {file.name}: {e}")
        
        return file_info
    
    def load_all_data(self) -> pd.DataFrame:
        """加载所有数据"""
        all_data = []
        
        for file in self.data_files:
            try:
                logger.info(f"加载数据文件: {file.name}")
                df = pd.read_parquet(file)
                all_data.append(df)
                
            except Exception as e:
                logger.error(f"加载文件失败 {file.name}: {e}")
        
        if all_data:
            # 合并所有数据
            combined_df = pd.concat(all_data, ignore_index=True)
            
            # 去重和排序
            combined_df = combined_df.drop_duplicates(subset=['timestamp', 'stock_code'])
            combined_df = combined_df.sort_values('timestamp')
            
            logger.info(f"数据加载完成: {len(combined_df)} 条记录")
            return combined_df
        else:
            logger.warning("没有数据可加载")
            return pd.DataFrame()
    
    def load_data_by_date_range(self, start_date: str, end_date: str) -> pd.DataFrame:
        """按日期范围加载数据"""
        try:
            start_dt = pd.to_datetime(start_date)
            end_dt = pd.to_datetime(end_date)
            
            all_data = []
            
            for file in self.data_files:
                try:
                    df = pd.read_parquet(file)
                    
                    # 过滤日期范围
                    if 'timestamp' in df.columns:
                        df['timestamp'] = pd.to_datetime(df['timestamp'])
                        mask = (df['timestamp'] >= start_dt) & (df['timestamp'] <= end_dt)
                        df_filtered = df[mask]
                        
                        if len(df_filtered) > 0:
                            all_data.append(df_filtered)
                            
                except Exception as e:
                    logger.error(f"加载文件失败 {file.name}: {e}")
            
            if all_data:
                combined_df = pd.concat(all_data, ignore_index=True)
                combined_df = combined_df.drop_duplicates(subset=['timestamp', 'stock_code'])
                combined_df = combined_df.sort_values('timestamp')
                
                logger.info(f"按日期范围加载数据完成: {len(combined_df)} 条记录")
                return combined_df
            else:
                logger.warning(f"在指定日期范围内没有找到数据: {start_date} 到 {end_date}")
                return pd.DataFrame()
                
        except Exception as e:
            logger.error(f"按日期范围加载数据失败: {e}")
            return pd.DataFrame()
    
    def analyze_data(self, df: pd.DataFrame) -> Dict:
        """分析数据"""
        if df.empty:
            return {}
        
        try:
            analysis = {
                'basic_info': {
                    'total_records': len(df),
                    'date_range': {
                        'start': df['timestamp'].min().isoformat(),
                        'end': df['timestamp'].max().isoformat()
                    },
                    'unique_days': df['timestamp'].dt.date.nunique(),
                    'columns': list(df.columns)
                },
                'price_analysis': {
                    'price_range': {
                        'min': float(df['price'].min()),
                        'max': float(df['price'].max()),
                        'mean': float(df['price'].mean()),
                        'std': float(df['price'].std())
                    },
                    'volume_analysis': {
                        'total_volume': float(df['volume'].sum()),
                        'avg_volume': float(df['volume'].mean()),
                        'max_volume': float(df['volume'].max())
                    }
                },
                'time_analysis': {
                    'trading_hours': df['timestamp'].dt.hour.value_counts().to_dict(),
                    'weekday_distribution': df['timestamp'].dt.dayofweek.value_counts().to_dict()
                }
            }
            
            # 计算价格变化统计
            if len(df) > 1:
                df_sorted = df.sort_values('timestamp')
                price_changes = df_sorted['price'].diff().dropna()
                
                analysis['price_analysis']['price_changes'] = {
                    'positive_changes': int((price_changes > 0).sum()),
                    'negative_changes': int((price_changes < 0).sum()),
                    'no_changes': int((price_changes == 0).sum()),
                    'max_change': float(price_changes.max()),
                    'min_change': float(price_changes.min()),
                    'avg_change': float(price_changes.mean())
                }
            
            return analysis
            
        except Exception as e:
            logger.error(f"数据分析失败: {e}")
            return {}
    
    def generate_summary_report(self) -> str:
        """生成摘要报告"""
        try:
            # 获取文件信息
            file_info = self.get_data_files_info()
            
            # 加载所有数据
            df = self.load_all_data()
            
            if df.empty:
                return "没有数据可分析"
            
            # 分析数据
            analysis = self.analyze_data(df)
            
            # 生成报告
            report = []
            report.append("=" * 60)
            report.append("159506 ETF Catalog数据摘要报告")
            report.append("=" * 60)
            
            # 文件信息
            report.append(f"\n📁 数据文件信息:")
            report.append(f"   目录: {self.catalog_path}")
            report.append(f"   文件数量: {len(file_info)}")
            
            total_size = sum(info['size_mb'] for info in file_info)
            total_records = sum(info['records'] for info in file_info)
            report.append(f"   总大小: {total_size:.2f} MB")
            report.append(f"   总记录数: {total_records}")
            
            # 数据文件详情
            for info in file_info:
                report.append(f"   - {info['filename']}: {info['records']} 条记录, {info['size_mb']:.2f} MB")
            
            # 基本统计
            if analysis:
                basic_info = analysis['basic_info']
                report.append(f"\n📊 基本统计:")
                report.append(f"   总记录数: {basic_info['total_records']}")
                report.append(f"   时间范围: {basic_info['date_range']['start']} 到 {basic_info['date_range']['end']}")
                report.append(f"   交易日数: {basic_info['unique_days']}")
                
                # 价格分析
                price_analysis = analysis['price_analysis']
                report.append(f"\n💰 价格分析:")
                report.append(f"   价格范围: {price_analysis['price_range']['min']:.4f} - {price_analysis['price_range']['max']:.4f}")
                report.append(f"   平均价格: {price_analysis['price_range']['mean']:.4f}")
                report.append(f"   价格标准差: {price_analysis['price_range']['std']:.4f}")
                
                # 成交量分析
                volume_analysis = price_analysis['volume_analysis']
                report.append(f"\n📈 成交量分析:")
                report.append(f"   总成交量: {volume_analysis['total_volume']:,.0f}")
                report.append(f"   平均成交量: {volume_analysis['avg_volume']:,.0f}")
                report.append(f"   最大成交量: {volume_analysis['max_volume']:,.0f}")
                
                # 价格变化分析
                if 'price_changes' in price_analysis:
                    changes = price_analysis['price_changes']
                    report.append(f"\n📉 价格变化分析:")
                    report.append(f"   上涨次数: {changes['positive_changes']}")
                    report.append(f"   下跌次数: {changes['negative_changes']}")
                    report.append(f"   平盘次数: {changes['no_changes']}")
                    report.append(f"   最大涨幅: {changes['max_change']:.4f}")
                    report.append(f"   最大跌幅: {changes['min_change']:.4f}")
                    report.append(f"   平均变化: {changes['avg_change']:.4f}")
            
            report.append("\n" + "=" * 60)
            
            return "\n".join(report)
            
        except Exception as e:
            logger.error(f"生成摘要报告失败: {e}")
            return f"生成报告失败: {e}"
    
    def plot_price_chart(self, df: pd.DataFrame, save_path: str = None):
        """绘制价格图表"""
        try:
            if df.empty:
                logger.warning("没有数据可绘制")
                return
            
            # 准备数据
            df_plot = df.copy()
            df_plot['timestamp'] = pd.to_datetime(df_plot['timestamp'])
            df_plot = df_plot.sort_values('timestamp')
            
            # 创建图表
            fig, axes = plt.subplots(2, 1, figsize=(15, 10))
            fig.suptitle('159506 ETF 价格和成交量分析', fontsize=16)
            
            # 价格图
            ax1 = axes[0]
            ax1.plot(df_plot['timestamp'], df_plot['price'], linewidth=1, alpha=0.8)
            ax1.set_title('价格走势')
            ax1.set_ylabel('价格')
            ax1.grid(True, alpha=0.3)
            
            # 成交量图
            ax2 = axes[1]
            ax2.bar(df_plot['timestamp'], df_plot['volume'], alpha=0.6, width=0.001)
            ax2.set_title('成交量')
            ax2.set_ylabel('成交量')
            ax2.set_xlabel('时间')
            ax2.grid(True, alpha=0.3)
            
            plt.tight_layout()
            
            if save_path:
                plt.savefig(save_path, dpi=300, bbox_inches='tight')
                logger.info(f"图表已保存到: {save_path}")
            
            plt.show()
            
        except Exception as e:
            logger.error(f"绘制价格图表失败: {e}")
    
    def export_to_csv(self, df: pd.DataFrame, output_path: str):
        """导出数据到CSV"""
        try:
            df.to_csv(output_path, index=False, encoding='utf-8-sig')
            logger.info(f"数据已导出到: {output_path}")
        except Exception as e:
            logger.error(f"导出CSV失败: {e}")


def main():
    """主函数"""
    print("=" * 60)
    print("159506 ETF Catalog数据加载器")
    print("=" * 60)
    
    # 创建加载器
    loader = ETF159506CatalogLoader()
    
    # 生成摘要报告
    report = loader.generate_summary_report()
    print(report)
    
    # 加载所有数据
    print("\n正在加载数据...")
    df = loader.load_all_data()
    
    if not df.empty:
        print(f"✅ 数据加载成功: {len(df)} 条记录")
        
        # 显示前几行数据
        print("\n前5行数据:")
        print(df.head())
        
        # 显示数据列信息
        print(f"\n数据列: {list(df.columns)}")
        
        # 绘制价格图表
        print("\n正在生成价格图表...")
        loader.plot_price_chart(df, "etf_159506_price_chart.png")
        
        # 导出数据
        print("\n正在导出数据...")
        loader.export_to_csv(df, "etf_159506_data.csv")
        
    else:
        print("❌ 没有数据可加载")
    
    print("\n" + "=" * 60)


if __name__ == "__main__":
    main() 