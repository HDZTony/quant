"""
回测执行器 —— 被 kline-viewer API 以子进程方式调用。

用法: python run_backtest_for_viewer.py <date> <output_json_path>
示例: python run_backtest_for_viewer.py 2025-12-24 data/backtest_signals/2025-12-24.json

将回测产生的交易信号写入 JSON 文件供前端展示。
"""

from __future__ import annotations

import json
import sys
from datetime import date, datetime
from pathlib import Path

import pandas as pd


def main() -> None:
    if len(sys.argv) < 3:
        print(f"用法: {sys.argv[0]} <YYYY-MM-DD> <output.json>", file=sys.stderr)
        sys.exit(1)

    date_str = sys.argv[1]
    output_path = Path(sys.argv[2])
    target_date = datetime.strptime(date_str, "%Y-%m-%d").date()

    from etf_159506_official_backtest import ETF159506OfficialBacktest

    bt = ETF159506OfficialBacktest()
    result, node = bt.run_backtest(target_date, target_date)
    bt.collect_trade_signals(result, node)

    signals: list[dict] = []
    for sig in bt.trade_signals:
        ts = sig.get("timestamp")
        if isinstance(ts, pd.Timestamp):
            beijing = ts.tz_localize("UTC").tz_convert("Asia/Shanghai") if ts.tzinfo is None else ts.tz_convert("Asia/Shanghai")
            t = int(beijing.timestamp())
        else:
            t = 0

        signals.append({
            "time": t,
            "side": "buy" if sig.get("side") == "BUY" else "sell",
            "price": float(sig.get("price", 0)),
            "signal_type": sig.get("signal_type", ""),
        })

    output_path.parent.mkdir(parents=True, exist_ok=True)
    output_path.write_text(json.dumps(signals, ensure_ascii=False), encoding="utf-8")
    print(f"已保存 {len(signals)} 个信号到 {output_path}")


if __name__ == "__main__":
    main()
