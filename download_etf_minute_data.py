"""
ETF 159506 · 1 分钟 K 线 · 增量采集脚本

数据源: pytdx（通达信协议，免费、稳定）
存储:   data/159506_1min.parquet（单文件，增量追加）

使用方式:
  首次运行 → 下载通达信全部可用历史（约 3-4 个月）
  每日运行 → 只追加缺失的新数据，自动跳过已有部分

建议: 每个交易日收盘后运行一次，长期积累即可攒齐完整年度数据。
"""

from __future__ import annotations

from datetime import datetime
from pathlib import Path

import pandas as pd

SYMBOL = "159506"
OUTPUT_DIR = Path("data")
PARQUET_PATH = OUTPUT_DIR / f"{SYMBOL}_1min.parquet"
CSV_PATH = OUTPUT_DIR / f"{SYMBOL}_1min.csv"

TDX_HOSTS = [
    ("218.75.126.9", 7709),
    ("115.238.56.198", 7709),
    ("124.160.88.183", 7709),
    ("60.12.136.250", 7709),
    ("218.108.98.244", 7709),
    ("218.108.47.69", 7709),
    ("180.153.39.51", 7709),
    ("119.147.212.81", 7709),
    ("14.17.75.71", 7709),
]
TDX_MARKET = 0
TDX_CAT_1MIN = 8
TDX_BATCH_SIZE = 800


def load_existing() -> pd.DataFrame:
    """加载已有的本地数据"""
    if PARQUET_PATH.exists():
        df = pd.read_parquet(PARQUET_PATH)
        df["datetime"] = pd.to_datetime(df["datetime"])
        print(f"[本地] 已有 {len(df):,} 条  ({df['datetime'].min()} ~ {df['datetime'].max()})")
        return df

    # 兼容旧的日期后缀文件
    legacy = sorted(OUTPUT_DIR.glob(f"{SYMBOL}_1min_*.parquet"))
    if legacy:
        df = pd.read_parquet(legacy[-1])
        df["datetime"] = pd.to_datetime(df["datetime"])
        print(f"[本地] 从旧文件加载 {len(df):,} 条  ({legacy[-1].name})")
        return df

    print("[本地] 无历史数据，将执行全量下载")
    return pd.DataFrame()


def fetch_pytdx_1min(since: pd.Timestamp | None = None) -> pd.DataFrame:
    """
    从通达信获取 1 分钟 K 线

    如果 since 不为 None，获取到与已有数据重叠后即停止（增量模式）。
    如果 since 为 None，获取全部可用历史（全量模式）。
    """
    from pytdx.hq import TdxHq_API

    mode = "增量" if since else "全量"
    print(f"[pytdx] {mode}模式，正在连接...")

    api = TdxHq_API()
    connected = False
    for host, port in TDX_HOSTS:
        try:
            if api.connect(host, port):
                print(f"[pytdx] 已连接 {host}:{port}")
                connected = True
                break
        except Exception:
            continue

    if not connected:
        raise ConnectionError("[pytdx] 所有通达信服务器连接失败")

    all_frames: list[pd.DataFrame] = []
    offset = 0
    reached_overlap = False

    try:
        while True:
            data = api.get_security_bars(TDX_CAT_1MIN, TDX_MARKET, SYMBOL, offset, TDX_BATCH_SIZE)
            if not data:
                break

            batch = api.to_df(data)
            batch["datetime"] = pd.to_datetime(batch["datetime"])
            all_frames.append(batch)
            offset += TDX_BATCH_SIZE

            oldest_in_batch = batch["datetime"].min()

            if offset % (TDX_BATCH_SIZE * 10) == 0:
                print(f"[pytdx] 已获取 {offset} 条，最早 {oldest_in_batch}")

            if since is not None and oldest_in_batch <= since:
                reached_overlap = True
                break

            if len(batch) < TDX_BATCH_SIZE:
                break
    finally:
        api.disconnect()

    if not all_frames:
        print("[pytdx] 未获取到新数据")
        return pd.DataFrame()

    df = pd.concat(all_frames, ignore_index=True)
    df["datetime"] = pd.to_datetime(df["datetime"])
    df.rename(columns={"vol": "volume"}, inplace=True)
    df = df[["datetime", "open", "high", "low", "close", "volume", "amount"]].copy()

    if since is not None:
        new_only = df[df["datetime"] > since]
        print(f"[pytdx] 获取 {len(df)} 条，其中新增 {len(new_only)} 条")
        return new_only

    print(f"[pytdx] 全量获取 {len(df)} 条")
    return df


def merge_and_save(existing: pd.DataFrame, new_data: pd.DataFrame) -> pd.DataFrame:
    """合并新旧数据，去重排序后保存"""
    if existing.empty:
        merged = new_data.copy()
    elif new_data.empty:
        print("无新增数据，跳过保存")
        return existing
    else:
        merged = pd.concat([existing, new_data], ignore_index=True)

    if "source" in merged.columns:
        merged.drop(columns=["source"], inplace=True)

    merged.sort_values("datetime", inplace=True)
    merged.drop_duplicates(subset="datetime", keep="last", inplace=True)
    merged.reset_index(drop=True, inplace=True)

    OUTPUT_DIR.mkdir(parents=True, exist_ok=True)
    merged.to_parquet(PARQUET_PATH, index=False)
    merged.to_csv(CSV_PATH, index=False, encoding="utf-8-sig")

    pq_kb = PARQUET_PATH.stat().st_size / 1024
    csv_kb = CSV_PATH.stat().st_size / 1024
    print(f"\n[保存] {PARQUET_PATH}  ({pq_kb:.1f} KB)")
    print(f"[保存] {CSV_PATH}  ({csv_kb:.1f} KB)")
    return merged


def show_summary(df: pd.DataFrame) -> None:
    bar = "=" * 60
    print(f"\n{bar}")
    print(f"  ETF {SYMBOL} · 1 分钟 K 线 · 数据摘要")
    print(bar)
    print(f"  总记录数:   {len(df):,}")
    n_days = df["datetime"].dt.date.nunique()
    print(f"  交易日数:   {n_days}")
    print(f"  时间范围:   {df['datetime'].min()} ~ {df['datetime'].max()}")
    print(f"\n  最新 5 条:")
    print(df.tail(5).to_string(index=False))


def main() -> None:
    print(f"ETF {SYMBOL} · 1 分钟 K 线 · 增量采集")
    print(f"运行时间: {datetime.now().strftime('%Y-%m-%d %H:%M:%S')}")
    print()

    existing = load_existing()

    since = existing["datetime"].max() if not existing.empty else None
    new_data = fetch_pytdx_1min(since=since)

    merged = merge_and_save(existing, new_data)
    show_summary(merged)


if __name__ == "__main__":
    main()
