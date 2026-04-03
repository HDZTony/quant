"""
ML 回测后端逻辑封装 —— 避免 api.py 过度膨胀。

提供给 api.py 调用的两个核心函数：
- run_ml_backtest_for_date(date_str) → dict   ML 回测全量结果
- get_feature_importance()           → list    特征重要性排名
"""

from __future__ import annotations

import logging
import sys
from pathlib import Path

logger = logging.getLogger(__name__)

PROJECT_ROOT = Path(__file__).resolve().parent.parent
if str(PROJECT_ROOT) not in sys.path:
    sys.path.insert(0, str(PROJECT_ROOT))


def run_ml_backtest_for_date(date_str: str) -> dict:
    """
    运行指定日期（或全量）的 ML 回测，返回前端 ``MLBacktestResponse`` 所需的数据。

    Parameters
    ----------
    date_str : str
        "YYYY-MM-DD" 格式的日期字符串。传 "" 或 None 表示使用全量数据。

    Returns
    -------
    dict
        包含 equity_curves / metrics / signals / feature_importance 四个键。
    """
    from ml_backtest_runner import run_ml_backtest_for_viewer

    try:
        return run_ml_backtest_for_viewer(date_str if date_str else None)
    except FileNotFoundError as e:
        logger.error("ML 回测模型缺失: %s", e)
        return {"equity_curves": [], "metrics": [], "signals": [], "feature_importance": [], "error": str(e)}
    except Exception as e:
        logger.error("ML 回测失败: %s", e, exc_info=True)
        return {"equity_curves": [], "metrics": [], "signals": [], "feature_importance": [], "error": str(e)}


def get_feature_importance() -> list[dict]:
    """读取已缓存的特征重要性数据。"""
    from ml_backtest_runner import _load_feature_importance

    try:
        return _load_feature_importance()
    except Exception as e:
        logger.error("读取特征重要性失败: %s", e)
        return []
