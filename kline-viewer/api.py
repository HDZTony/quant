"""
pywebview JS bridge API —— 为前端提供 K 线数据。

通过 window.pywebview.api 暴露给 JavaScript 调用。
"""

from pathlib import Path

import pandas as pd

CSV_PATH = Path(__file__).resolve().parent.parent / "data" / "159506_1min.csv"

VOLUME_NOISE_THRESHOLD = 1.0

UP_COLOR = "rgba(239,83,80,0.5)"
DOWN_COLOR = "rgba(38,166,154,0.5)"


class Api:
    """pywebview 暴露给前端的 API 类。

    所有 public 方法均可通过 window.pywebview.api.<method>() 调用。
    """

    def __init__(self, csv_path: Path = CSV_PATH):
        self._df = self._load_and_clean(csv_path)
        self._dates = sorted(self._df["date_str"].unique().tolist())

    @staticmethod
    def _load_and_clean(csv_path: Path) -> pd.DataFrame:
        df = pd.read_csv(csv_path, parse_dates=["datetime"])

        df.loc[df["volume"] < VOLUME_NOISE_THRESHOLD, ["volume", "amount"]] = 0

        df["date_str"] = df["datetime"].dt.strftime("%Y-%m-%d")

        # Lightweight Charts 需要秒级 Unix 时间戳。
        # 将北京时间"当作 UTC"处理，这样图表上 09:31 就显示 09:31。
        df["time"] = (
            df["datetime"].astype("int64") // 10**9
        )

        return df

    def get_trading_dates(self) -> list[str]:
        return self._dates

    def get_kline_data(self, date: str) -> dict:
        day = self._df[self._df["date_str"] == date]

        candles = []
        volume = []

        for row in day.itertuples(index=False):
            t = int(row.time)
            o, h, l, c = row.open, row.high, row.low, row.close
            v = row.volume

            candles.append({"time": t, "open": o, "high": h, "low": l, "close": c})
            color = UP_COLOR if c >= o else DOWN_COLOR
            volume.append({"time": t, "value": v, "color": color})

        return {"candles": candles, "volume": volume}
