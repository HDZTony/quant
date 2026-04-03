interface BarData {
  time: number
  open: number
  high: number
  low: number
  close: number
  volume: number
  macd_dif: number | null
  macd_dea: number | null
  macd_histogram: number | null
  rsi: number | null
  kdj_k: number | null
  kdj_d: number | null
  kdj_j: number | null
  signal?: 'buy' | 'sell' | null
}

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

interface MacdPoint {
  time: number
  dif: number | null
  dea: number | null
  histogram: number | null
}

interface RsiPoint {
  time: number
  value: number | null
}

interface KdjPoint {
  time: number
  k: number | null
  d: number | null
  j: number | null
}

interface TradeSignal {
  time: number
  side: 'buy' | 'sell'
  price: number
  signal_type: string
}

interface BacktestMeta {
  ok: boolean
  error?: string | null
  signal_count: number
}

interface KlineResponse {
  candles: KlineCandle[]
  volume: KlineVolume[]
  macd: MacdPoint[]
  rsi: RsiPoint[]
  kdj: KdjPoint[]
  signals: TradeSignal[]
  /** POST /api/backtest 附加字段 */
  backtest_meta?: BacktestMeta
}

interface MacroSeriesMeta {
  series_id: string
  name: string
  unit: string
  frequency: string
  description: string
  transform: 'raw' | 'yoy_pct'
}

interface MacroDataPoint {
  time: number
  value: number | null
}

interface MacroSeries {
  meta: MacroSeriesMeta
  data: MacroDataPoint[]
  updated_at: string
}

interface MacroResponse {
  series: MacroSeries[]
}

interface EquityResponse {
  symbol: string
  period: string
  candles: KlineCandle[]
  volume: KlineVolume[]
  updated_at: string
}

type ViewMode = 'realtime' | 'backtest' | 'ml' | 'macro' | 'explore'

type TradingMode = 'off' | 'paper' | 'live'

interface EquityCurve {
  name: string
  data: { time: number; value: number }[]
}

interface StrategyMetrics {
  name: string
  total_return: number
  annual_return: number
  max_drawdown: number
  win_rate: number
  total_trades: number
  profit_factor: number
  sharpe_ratio: number
}

interface FeatureItem {
  feature: string
  importance: number
  rank: number
}

interface MLSignalMarker {
  time: number
  position: string
  color: string
  shape: string
  text: string
}

interface MLBacktestResponse {
  kline?: KlineResponse
  equity_curves: EquityCurve[]
  metrics: StrategyMetrics[]
  signals: MLSignalMarker[]
  feature_importance: FeatureItem[]
  error?: string
}
