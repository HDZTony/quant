interface KlineCandle {
  time: number
  open: number
  high: number
  low: number
  close: number
}

interface KlineVolume {
  time: number
  value: number
  color: string
}

interface KlineData {
  candles: KlineCandle[]
  volume: KlineVolume[]
}

interface PyWebViewApi {
  get_trading_dates(): Promise<string[]>
  get_kline_data(date: string): Promise<KlineData>
}

interface Window {
  pywebview: {
    api: PyWebViewApi
  }
}
