<script setup lang="ts">
import { ref, onMounted, onUnmounted, watch, nextTick } from 'vue'
import {
  createChart,
  createSeriesMarkers,
  CandlestickSeries,
  HistogramSeries,
  LineSeries,
  type IChartApi,
  type ISeriesApi,
  type ISeriesMarkersPluginApi,
  type CandlestickData,
  type HistogramData,
  type LineData,
  type MouseEventParams,
  type Time,
  type LogicalRange,
  CrosshairMode,
} from 'lightweight-charts'

const props = defineProps<{
  candles: KlineCandle[]
  volume: KlineVolume[]
  macd: MacdPoint[]
  rsi: RsiPoint[]
  kdj: KdjPoint[]
  signals: TradeSignal[]
  mode: ViewMode
}>()

const emit = defineEmits<{ 'load-more': [] }>()

// --- DOM refs ---
const mainContainer = ref<HTMLDivElement | null>(null)
const macdContainer = ref<HTMLDivElement | null>(null)
const rsiContainer = ref<HTMLDivElement | null>(null)
const kdjContainer = ref<HTMLDivElement | null>(null)

// --- Chart instances ---
let mainChart: IChartApi | null = null
let macdChart: IChartApi | null = null
let rsiChart: IChartApi | null = null
let kdjChart: IChartApi | null = null

let candleSeries: ISeriesApi<'Candlestick'> | null = null
let volumeSeries: ISeriesApi<'Histogram'> | null = null
let macdDifSeries: ISeriesApi<'Line'> | null = null
let macdDeaSeries: ISeriesApi<'Line'> | null = null
let macdHistSeries: ISeriesApi<'Histogram'> | null = null
let rsiSeries: ISeriesApi<'Line'> | null = null
let kdjKSeries: ISeriesApi<'Line'> | null = null
let kdjDSeries: ISeriesApi<'Line'> | null = null
let kdjJSeries: ISeriesApi<'Line'> | null = null

let markersPlugin: ISeriesMarkersPluginApi<Time> | null = null
let resizeObserver: ResizeObserver | null = null
let syncingTimeScale = false
let syncingCrosshair = false
let isFirstLoad = true

type CrosshairSource = 'main' | 'macd' | 'rsi' | 'kdj'

const legend = ref({ o: 0, h: 0, l: 0, c: 0, v: 0, change: 0 })
const legendVisible = ref(false)
/** 与子图十字线同步的指标数值（左上角叠加层） */
const hoverMacd = ref({ dif: null as number | null, dea: null as number | null, hist: null as number | null })
const hoverRsi = ref({ value: null as number | null })
const hoverKdj = ref({ k: null as number | null, d: null as number | null, j: null as number | null })
const hoverTimeLabel = ref('')
/** 鼠标在图左半区域时，图例贴右上角，避免挡十字线；右半则贴左上角 */
const legendDockRight = ref(true)

/** 各图区高度（px），可拖拽分割条在相邻两格间调整；总高度随图表区视口变化 */
const PANE_MIN_H = 72
/** 首次分配主图 / MACD / RSI / KDJ 占比（总和为 1） */
const DEFAULT_FRAC = [0.52, 0.16, 0.16, 0.16]
const paneHeights = ref([320, 120, 120, 120])

const chartScrollRef = ref<HTMLDivElement | null>(null)
const chartStackRef = ref<HTMLDivElement | null>(null)
const klineRootRef = ref<HTMLDivElement | null>(null)
let chartAreaResizeObserver: ResizeObserver | null = null
let layoutAreaDone = false
/** 上一次由 layoutChartArea 写入后的各 pane 高度之和；用于区分「用户拖拽增高」与「仅视口变矮」 */
const lastLayoutPaneSum = ref(0)

const PANE_HEIGHTS_STORAGE_KEY = 'kline-viewer:pane-heights-v1'

function getViewportPaneDist(): number | null {
  const scroll = chartScrollRef.value
  const stack = chartStackRef.value
  if (!scroll || !stack) return null
  const available = scroll.clientHeight
  if (available < 1) return null
  const fixed = measureFixedOverhead(stack)
  const rawDist = Math.max(0, available - fixed)
  return Math.max(rawDist, PANE_MIN_H * 4)
}

function parseSavedPaneHeights(raw: string | null): number[] | null {
  if (raw == null || raw === '') return null
  try {
    const data = JSON.parse(raw) as unknown
    const arr = Array.isArray(data)
      ? data
      : (data && typeof data === 'object' && Array.isArray((data as { heights?: unknown }).heights)
        ? (data as { heights: unknown[] }).heights
        : null)
    if (!arr || arr.length !== 4) return null
    const nums = arr.map(x => Number(x))
    if (!nums.every(n => Number.isFinite(n) && n >= PANE_MIN_H)) return null
    return nums
  } catch {
    return null
  }
}

function persistPaneHeights() {
  try {
    localStorage.setItem(PANE_HEIGHTS_STORAGE_KEY, JSON.stringify({ v: 1, heights: paneHeights.value }))
  } catch {
    /* private mode / quota */
  }
}

let paneHeightsSaveTimer: ReturnType<typeof setTimeout> | null = null
function schedulePersistPaneHeights() {
  if (paneHeightsSaveTimer) clearTimeout(paneHeightsSaveTimer)
  paneHeightsSaveTimer = setTimeout(() => {
    paneHeightsSaveTimer = null
    persistPaneHeights()
  }, 400)
}

function restoreFullscreenLayout() {
  layoutChartArea(false)
  chartScrollRef.value?.scrollTo({ top: 0, behavior: 'smooth' })
  persistPaneHeights()
}

function onBeforeUnloadPersistPaneHeights() {
  persistPaneHeights()
}

type SplitterDragState = { index: number; startY: number; startHeights: number[] }
let splitterDrag: SplitterDragState | null = null

function onSplitterMouseDown(index: number, e: MouseEvent) {
  splitterDrag = { index, startY: e.clientY, startHeights: [...paneHeights.value] }
  window.addEventListener('mousemove', onSplitterMouseMove)
  window.addEventListener('mouseup', onSplitterMouseUp)
  e.preventDefault()
}

function onSplitterMouseMove(e: MouseEvent) {
  if (!splitterDrag) return
  const { index, startY, startHeights } = splitterDrag
  const dy = e.clientY - startY
  let a = startHeights[index]! + dy
  let b = startHeights[index + 1]! - dy
  // 一侧达到最小高度后仍继续拖拽：增加上下两格高度之和，外层 chart-scroll 纵向滚动，便于继续拉高指标区
  if (a < PANE_MIN_H) {
    b += PANE_MIN_H - a
    a = PANE_MIN_H
  }
  if (b < PANE_MIN_H) {
    a += PANE_MIN_H - b
    b = PANE_MIN_H
  }
  const next = [...startHeights]
  next[index] = a
  next[index + 1] = b
  paneHeights.value = next
}

function onSplitterMouseUp() {
  splitterDrag = null
  window.removeEventListener('mousemove', onSplitterMouseMove)
  window.removeEventListener('mouseup', onSplitterMouseUp)
}

function sumPaneHeights(heights: number[]): number {
  return heights.reduce((a, b) => a + b, 0)
}

/** 分割条与 MACD/RSI/KDJ 标签等非 pane-wrap 区域占用高度 */
function measureFixedOverhead(stack: HTMLElement): number {
  let h = 0
  for (const child of Array.from(stack.children)) {
    const el = child as HTMLElement
    if (!el.classList.contains('pane-wrap')) {
      h += el.offsetHeight
    }
  }
  return h
}

/**
 * 按图表区可视高度分配各 pane；preserveRatio 时在窗口缩放等场景保持用户拖拽后的比例。
 */
function layoutChartArea(preserveRatio: boolean) {
  const scroll = chartScrollRef.value
  const stack = chartStackRef.value
  if (!scroll || !stack) return

  const available = scroll.clientHeight
  if (available < 1) return

  const fixed = measureFixedOverhead(stack)
  const rawDist = Math.max(0, available - fixed)
  const dist = Math.max(rawDist, PANE_MIN_H * 4)

  if (preserveRatio && layoutAreaDone && sumPaneHeights(paneHeights.value) > 0) {
    const oldSum = sumPaneHeights(paneHeights.value)
    // 用户已通过分割条把总高度拉高到超过当前视口可分配值时，不要按比例压回，
    // 否则纵向滚动条出现会触发 RO，拖拽「增高」会立刻被抵消。
    if (oldSum > dist && oldSum > lastLayoutPaneSum.value + 0.5) {
      return
    }
    const scale = dist / oldSum
    const next = paneHeights.value.map(h => Math.max(PANE_MIN_H, Math.round(h * scale)))
    next[0] += dist - sumPaneHeights(next)
    paneHeights.value = next
  } else {
    const fracSum = DEFAULT_FRAC.reduce((a, b) => a + b, 0)
    const next = DEFAULT_FRAC.map(f =>
      Math.max(PANE_MIN_H, Math.floor((dist * f) / fracSum)),
    )
    next[0] += dist - sumPaneHeights(next)
    paneHeights.value = next
  }
  layoutAreaDone = true
  lastLayoutPaneSum.value = sumPaneHeights(paneHeights.value)
}

function setupChartAreaResizeObserver() {
  chartAreaResizeObserver?.disconnect()
  const el = klineRootRef.value
  if (!el) return
  chartAreaResizeObserver = new ResizeObserver(() => {
    const h = el.clientHeight
    if (h < 1) return
    layoutChartArea(layoutAreaDone)
  })
  chartAreaResizeObserver.observe(el)
}

// --- Shared chart options ---
const DARK_BG = '#1e1e2e'
const GRID_COLOR = '#313244'
const TEXT_COLOR = '#cdd6f4'
const BORDER_COLOR = '#45475a'

function chartOpts(container: HTMLDivElement, height?: number) {
  return {
    layout: { background: { color: DARK_BG }, textColor: TEXT_COLOR },
    grid: { vertLines: { color: GRID_COLOR }, horzLines: { color: GRID_COLOR } },
    crosshair: { mode: CrosshairMode.Normal },
    rightPriceScale: { borderColor: BORDER_COLOR },
    timeScale: {
      borderColor: BORDER_COLOR,
      timeVisible: true,
      secondsVisible: false,
      rightOffset: 5,
      minBarSpacing: 3,
    },
    width: container.clientWidth,
    height: height ?? container.clientHeight,
  }
}

// --- Init ---
function initCharts() {
  if (!mainContainer.value || !macdContainer.value || !rsiContainer.value || !kdjContainer.value) return

  // Main chart
  mainChart = createChart(mainContainer.value, chartOpts(mainContainer.value))
  candleSeries = mainChart.addSeries(CandlestickSeries, {
    upColor: '#ef5350', downColor: '#26a69a',
    borderVisible: false,
    wickUpColor: '#ef5350', wickDownColor: '#26a69a',
  })
  volumeSeries = mainChart.addSeries(HistogramSeries, {
    priceFormat: { type: 'volume' },
    priceScaleId: 'vol',
  })
  volumeSeries.priceScale().applyOptions({ scaleMargins: { top: 0.8, bottom: 0 } })
  mainChart.subscribeCrosshairMove((param) => onAnyCrosshairMove('main', param))

  // MACD sub-chart
  macdChart = createChart(macdContainer.value, chartOpts(macdContainer.value))
  macdDifSeries = macdChart.addSeries(LineSeries, {
    color: '#89b4fa', lineWidth: 1, priceScaleId: 'macd',
    priceFormat: { type: 'price', precision: 4, minMove: 0.0001 },
  })
  macdDeaSeries = macdChart.addSeries(LineSeries, {
    color: '#fab387', lineWidth: 1, priceScaleId: 'macd',
    priceFormat: { type: 'price', precision: 4, minMove: 0.0001 },
  })
  macdHistSeries = macdChart.addSeries(HistogramSeries, {
    priceScaleId: 'macd',
    priceFormat: { type: 'price', precision: 4, minMove: 0.0001 },
  })

  // RSI sub-chart
  rsiChart = createChart(rsiContainer.value, chartOpts(rsiContainer.value))
  rsiSeries = rsiChart.addSeries(LineSeries, {
    color: '#cba6f7', lineWidth: 1,
    priceFormat: { type: 'price', precision: 1, minMove: 0.1 },
  })
  rsiSeries.createPriceLine({ price: 70, color: '#f38ba866', lineWidth: 1, lineStyle: 2, axisLabelVisible: true, title: '70' })
  rsiSeries.createPriceLine({ price: 30, color: '#a6e3a166', lineWidth: 1, lineStyle: 2, axisLabelVisible: true, title: '30' })

  // KDJ sub-chart
  kdjChart = createChart(kdjContainer.value, chartOpts(kdjContainer.value))
  kdjKSeries = kdjChart.addSeries(LineSeries, {
    color: '#f9e2af', lineWidth: 1, priceScaleId: 'kdj',
    priceFormat: { type: 'price', precision: 1, minMove: 0.1 },
  })
  kdjDSeries = kdjChart.addSeries(LineSeries, {
    color: '#89b4fa', lineWidth: 1, priceScaleId: 'kdj',
    priceFormat: { type: 'price', precision: 1, minMove: 0.1 },
  })
  kdjJSeries = kdjChart.addSeries(LineSeries, {
    color: '#cba6f7', lineWidth: 1, priceScaleId: 'kdj',
    priceFormat: { type: 'price', precision: 1, minMove: 0.1 },
  })

  macdChart.subscribeCrosshairMove((param) => onAnyCrosshairMove('macd', param))
  rsiChart.subscribeCrosshairMove((param) => onAnyCrosshairMove('rsi', param))
  kdjChart.subscribeCrosshairMove((param) => onAnyCrosshairMove('kdj', param))

  setupTimeSync()
  setupResize()
}

// --- Time scale sync + infinite scroll ---
let loadMoreCooldown = false

function setupTimeSync() {
  const charts = [mainChart, macdChart, rsiChart, kdjChart]
  for (const src of charts) {
    if (!src) continue
    src.timeScale().subscribeVisibleLogicalRangeChange((range: LogicalRange | null) => {
      if (syncingTimeScale || !range) return
      syncingTimeScale = true
      for (const dst of charts) {
        if (dst && dst !== src) {
          dst.timeScale().setVisibleLogicalRange(range)
        }
      }
      syncingTimeScale = false

      if (range.from < 10 && !loadMoreCooldown) {
        loadMoreCooldown = true
        emit('load-more')
        setTimeout(() => { loadMoreCooldown = false }, 2000)
      }
    })
  }
}

// --- Resize ---
function setupResize() {
  resizeObserver = new ResizeObserver(() => {
    const pairs: [IChartApi | null, HTMLDivElement | null][] = [
      [mainChart, mainContainer.value],
      [macdChart, macdContainer.value],
      [rsiChart, rsiContainer.value],
      [kdjChart, kdjContainer.value],
    ]
    for (const [chart, el] of pairs) {
      if (chart && el) chart.applyOptions({ width: el.clientWidth, height: el.clientHeight })
    }
  })
  for (const el of [mainContainer.value, macdContainer.value, rsiContainer.value, kdjContainer.value]) {
    if (el) resizeObserver.observe(el)
  }
}

// --- Legend ---
function formatPrice(n: number): string { return n.toFixed(3) }
function formatVolume(n: number): string {
  if (n >= 1_000_000) return (n / 1_000_000).toFixed(2) + 'M'
  if (n >= 1_000) return (n / 1_000).toFixed(1) + 'K'
  return String(n)
}

function formatNum(n: number | null | undefined, digits = 4): string {
  if (n == null || Number.isNaN(n)) return '--'
  return n.toFixed(digits)
}

/**
 * 与 lightweight-charts 时间轴一致：K 线 `time` 为 UTC 秒，轴标签按 UTC 显示时刻。
 * 若用本地 toLocaleString，在中国会与轴相差 8 小时（例如轴 09:49、图例误为 17:49）。
 */
function formatCrosshairTime(tUnix: number): string {
  const d = new Date(tUnix * 1000)
  const mm = String(d.getUTCMonth() + 1).padStart(2, '0')
  const dd = String(d.getUTCDate()).padStart(2, '0')
  const hh = String(d.getUTCHours()).padStart(2, '0')
  const mi = String(d.getUTCMinutes()).padStart(2, '0')
  return `${mm}/${dd} ${hh}:${mi}`
}

function timeToUnix(t: Time): number {
  if (typeof t === 'number') return Math.trunc(t)
  if (typeof t === 'string') return Math.trunc(Date.parse(t) / 1000)
  return Math.trunc(Date.UTC(t.year, t.month - 1, t.day) / 1000)
}

function lowerBoundByTime<T extends { time: number }>(arr: T[], tUnix: number): T | null {
  if (arr.length === 0) return null
  let lo = 0
  let hi = arr.length - 1
  let ans = -1
  while (lo <= hi) {
    const mid = (lo + hi) >> 1
    if (arr[mid].time <= tUnix) {
      ans = mid
      lo = mid + 1
    } else {
      hi = mid - 1
    }
  }
  return ans >= 0 ? arr[ans]! : null
}

function clearAllCrosshairs(): void {
  mainChart?.clearCrosshairPosition()
  macdChart?.clearCrosshairPosition()
  rsiChart?.clearCrosshairPosition()
  kdjChart?.clearCrosshairPosition()
}

function containerForCrosshairSource(source: CrosshairSource): HTMLDivElement | null {
  switch (source) {
    case 'main':
      return mainContainer.value
    case 'macd':
      return macdContainer.value
    case 'rsi':
      return rsiContainer.value
    case 'kdj':
      return kdjContainer.value
    default:
      return null
  }
}

function onAnyCrosshairMove(source: CrosshairSource, param: MouseEventParams<Time>) {
  if (syncingCrosshair) return
  const point = param.point
  if (!point || point.x < 0 || point.y < 0 || !param.time || !candleSeries) {
    clearAllCrosshairs()
    legendVisible.value = false
    hoverTimeLabel.value = ''
    hoverMacd.value = { dif: null, dea: null, hist: null }
    hoverRsi.value = { value: null }
    hoverKdj.value = { k: null, d: null, j: null }
    return
  }

  const t = param.time
  const tUnix = timeToUnix(t)
  hoverTimeLabel.value = formatCrosshairTime(tUnix)

  const srcEl = containerForCrosshairSource(source)
  if (srcEl && srcEl.clientWidth > 0) {
    legendDockRight.value = point.x < srcEl.clientWidth * 0.5
  }

  let candle: KlineCandle | null = null
  let volVal = 0

  if (source === 'main') {
    const c = param.seriesData.get(candleSeries) as CandlestickData<Time> | undefined
    const vol = volumeSeries ? param.seriesData.get(volumeSeries) as HistogramData<Time> | undefined : undefined
    if (c) {
      candle = {
        time: tUnix,
        open: c.open,
        high: c.high,
        low: c.low,
        close: c.close,
      }
      const vBar = lowerBoundByTime(props.volume, candle.time)
      volVal = vol?.value ?? vBar?.value ?? 0
    }
  }

  if (!candle) {
    candle = lowerBoundByTime(props.candles, tUnix)
    if (candle) {
      const vBar = lowerBoundByTime(props.volume, candle.time)
      volVal = vBar?.value ?? 0
    }
  }

  const mRow = lowerBoundByTime(props.macd, tUnix)
  hoverMacd.value = {
    dif: mRow?.dif ?? null,
    dea: mRow?.dea ?? null,
    hist: mRow?.histogram ?? null,
  }
  const rRow = lowerBoundByTime(props.rsi, tUnix)
  hoverRsi.value = { value: rRow?.value ?? null }
  const kRow = lowerBoundByTime(props.kdj, tUnix)
  hoverKdj.value = {
    k: kRow?.k ?? null,
    d: kRow?.d ?? null,
    j: kRow?.j ?? null,
  }

  if (candle) {
    const change = candle.open !== 0 ? ((candle.close - candle.open) / candle.open) * 100 : 0
    legend.value = { o: candle.open, h: candle.high, l: candle.low, c: candle.close, v: volVal, change }
    legendVisible.value = true
  } else {
    legendVisible.value = false
  }

  syncingCrosshair = true
  try {
    if (source !== 'main' && mainChart && candle) {
      mainChart.setCrosshairPosition(candle.close, t, candleSeries)
    }

    if (source !== 'macd' && macdChart && macdDifSeries) {
      const m = lowerBoundByTime(props.macd, tUnix)
      if (m && m.dif != null) {
        macdChart.setCrosshairPosition(m.dif, t, macdDifSeries)
      } else {
        macdChart.clearCrosshairPosition()
      }
    }

    if (source !== 'rsi' && rsiChart && rsiSeries) {
      const r = lowerBoundByTime(props.rsi, tUnix)
      if (r && r.value != null) {
        rsiChart.setCrosshairPosition(r.value, t, rsiSeries)
      } else {
        rsiChart.clearCrosshairPosition()
      }
    }

    if (source !== 'kdj' && kdjChart && kdjKSeries) {
      const k = lowerBoundByTime(props.kdj, tUnix)
      if (k && k.k != null) {
        kdjChart.setCrosshairPosition(k.k, t, kdjKSeries)
      } else {
        kdjChart.clearCrosshairPosition()
      }
    }
  } finally {
    syncingCrosshair = false
  }
}

// --- Set data ---
function setAllData() {
  if (!candleSeries) return

  candleSeries.setData(props.candles as CandlestickData<Time>[])
  volumeSeries?.setData(props.volume as HistogramData<Time>[])

  // MACD
  const difData: LineData<Time>[] = []
  const deaData: LineData<Time>[] = []
  const histData: HistogramData<Time>[] = []
  for (const p of props.macd) {
    const t = p.time as Time
    if (p.dif != null) difData.push({ time: t, value: p.dif })
    if (p.dea != null) deaData.push({ time: t, value: p.dea })
    if (p.histogram != null) {
      histData.push({ time: t, value: p.histogram, color: p.histogram >= 0 ? '#ef535099' : '#26a69a99' })
    }
  }
  macdDifSeries?.setData(difData)
  macdDeaSeries?.setData(deaData)
  macdHistSeries?.setData(histData)

  // RSI
  const rsiData: LineData<Time>[] = []
  for (const p of props.rsi) {
    if (p.value != null) rsiData.push({ time: p.time as Time, value: p.value })
  }
  rsiSeries?.setData(rsiData)

  // KDJ
  const kData: LineData<Time>[] = []
  const dData: LineData<Time>[] = []
  const jData: LineData<Time>[] = []
  for (const p of props.kdj) {
    const t = p.time as Time
    if (p.k != null) kData.push({ time: t, value: p.k })
    if (p.d != null) dData.push({ time: t, value: p.d })
    if (p.j != null) jData.push({ time: t, value: p.j })
  }
  kdjKSeries?.setData(kData)
  kdjDSeries?.setData(dData)
  kdjJSeries?.setData(jData)

  // Buy/sell markers (v5 API: createSeriesMarkers)
  if (candleSeries) {
    if (markersPlugin) { markersPlugin.setMarkers([]) }
    if (props.signals.length > 0) {
      const markers = props.signals.map(s => ({
        time: s.time as Time,
        position: s.side === 'buy' ? 'belowBar' as const : 'aboveBar' as const,
        color: s.side === 'buy' ? '#a6e3a1' : '#f38ba8',
        shape: s.side === 'buy' ? 'arrowUp' as const : 'arrowDown' as const,
        text: s.side === 'buy' ? 'B' : 'S',
      }))
      if (!markersPlugin) {
        markersPlugin = createSeriesMarkers(candleSeries, markers)
      } else {
        markersPlugin.setMarkers(markers)
      }
    }
  }

  if (isFirstLoad && mainChart) {
    const total = props.candles.length
    if (total > 300) {
      const from = total - 250
      mainChart.timeScale().setVisibleLogicalRange({ from, to: total + 5 })
    } else {
      mainChart.timeScale().fitContent()
    }
    isFirstLoad = false
  } else if (prependedCount > 0 && mainChart) {
    const range = mainChart.timeScale().getVisibleLogicalRange()
    if (range) {
      mainChart.timeScale().setVisibleLogicalRange({
        from: range.from + prependedCount,
        to: range.to + prependedCount,
      })
    }
    prependedCount = 0
  }
}

// --- Lifecycle ---
onMounted(async () => {
  await nextTick()
  const raw = typeof localStorage !== 'undefined' ? localStorage.getItem(PANE_HEIGHTS_STORAGE_KEY) : null
  const saved = parseSavedPaneHeights(raw)
  if (saved) {
    paneHeights.value = saved
    layoutAreaDone = true
    await nextTick()
    const dist = getViewportPaneDist()
    lastLayoutPaneSum.value = dist ?? sumPaneHeights(saved)
  } else {
    layoutChartArea(false)
  }
  await nextTick()
  initCharts()
  setupChartAreaResizeObserver()
  window.addEventListener('beforeunload', onBeforeUnloadPersistPaneHeights)
  if (props.candles.length > 0) setAllData()
})

onUnmounted(() => {
  window.removeEventListener('beforeunload', onBeforeUnloadPersistPaneHeights)
  if (paneHeightsSaveTimer) {
    clearTimeout(paneHeightsSaveTimer)
    paneHeightsSaveTimer = null
  }
  persistPaneHeights()
  onSplitterMouseUp()
  chartAreaResizeObserver?.disconnect()
  chartAreaResizeObserver = null
  resizeObserver?.disconnect()
  mainChart?.remove()
  macdChart?.remove()
  rsiChart?.remove()
  kdjChart?.remove()
  mainChart = macdChart = rsiChart = kdjChart = null
})

let prevCandleCount = 0
let prependedCount = 0

watch(() => [props.candles, props.macd, props.rsi, props.kdj, props.signals], () => {
  const newCount = props.candles.length
  const delta = newCount - prevCandleCount

  if (prevCandleCount === 0 || delta < -10) {
    isFirstLoad = true
    prependedCount = 0
  } else if (delta > 10 && prevCandleCount > 0) {
    const oldFirst = prevFirstTime
    const newFirst = props.candles[0]?.time ?? 0
    if (newFirst < oldFirst) {
      prependedCount = delta
    } else {
      prependedCount = 0
    }
  } else {
    prependedCount = 0
  }

  prevCandleCount = newCount
  prevFirstTime = props.candles[0]?.time ?? 0
  nextTick(setAllData)
}, { deep: true })

let prevFirstTime: number = 0

watch(paneHeights, () => { schedulePersistPaneHeights() }, { deep: true })
</script>

<template>
  <div ref="klineRootRef" class="kline-root">
    <button
      type="button"
      class="btn-restore-pane-layout"
      title="主图与 MACD/RSI/KDJ 恢复为默认比例铺满当前图表区，并写入本地保存"
      @click="restoreFullscreenLayout"
    >
      恢复全屏布局
    </button>
    <div ref="chartScrollRef" class="chart-scroll">
      <div ref="chartStackRef" class="chart-stack">
    <div class="pane-wrap pane-wrap--main" :style="{ height: `${paneHeights[0]}px` }">
      <div ref="mainContainer" class="pane main-pane" />
      <div
        v-show="hoverTimeLabel"
        :class="['pane-legend', 'pane-legend--main', legendDockRight ? 'pane-legend--dock-right' : 'pane-legend--dock-left']"
      >
        <div class="pane-legend-time">{{ hoverTimeLabel }}</div>
        <div v-if="legendVisible" class="pane-legend-row">
          <span>O <b>{{ formatPrice(legend.o) }}</b></span>
          <span>H <b>{{ formatPrice(legend.h) }}</b></span>
          <span>L <b>{{ formatPrice(legend.l) }}</b></span>
          <span>C <b :class="legend.change >= 0 ? 'up' : 'down'">{{ formatPrice(legend.c) }}</b></span>
          <span :class="legend.change >= 0 ? 'up' : 'down'">
            {{ legend.change >= 0 ? '+' : '' }}{{ legend.change.toFixed(2) }}%
          </span>
          <span class="vol">Vol <b>{{ formatVolume(legend.v) }}</b></span>
        </div>
      </div>
    </div>

    <div
      class="pane-splitter"
      title="拖拽调整主图与 MACD 高度；主图压到最低后继续拖可整体增高（右侧滚动）"
      @mousedown="onSplitterMouseDown(0, $event)"
    />

    <div class="pane-label">MACD</div>
    <div class="pane-wrap pane-wrap--sub" :style="{ height: `${paneHeights[1]}px` }">
      <div ref="macdContainer" class="pane sub-pane" />
      <div
        v-show="hoverTimeLabel"
        :class="['pane-legend', 'pane-legend--sub', legendDockRight ? 'pane-legend--dock-right' : 'pane-legend--dock-left']"
      >
        <div class="pane-legend-time">{{ hoverTimeLabel }}</div>
        <div class="pane-legend-metrics">
          <span class="macd-dif">DIF {{ formatNum(hoverMacd.dif) }}</span>
          <span class="macd-dea">DEA {{ formatNum(hoverMacd.dea) }}</span>
          <span class="macd-hist">柱 {{ formatNum(hoverMacd.hist) }}</span>
        </div>
      </div>
    </div>

    <div
      class="pane-splitter"
      title="拖拽调整 MACD 与 RSI 高度；一侧压到最低后继续拖可整体增高（滚动）"
      @mousedown="onSplitterMouseDown(1, $event)"
    />

    <div class="pane-label">RSI</div>
    <div class="pane-wrap pane-wrap--sub" :style="{ height: `${paneHeights[2]}px` }">
      <div ref="rsiContainer" class="pane sub-pane" />
      <div
        v-show="hoverTimeLabel"
        :class="['pane-legend', 'pane-legend--sub', legendDockRight ? 'pane-legend--dock-right' : 'pane-legend--dock-left']"
      >
        <div class="pane-legend-time">{{ hoverTimeLabel }}</div>
        <div class="pane-legend-metrics">
          <span>RSI <b>{{ formatNum(hoverRsi.value, 1) }}</b></span>
        </div>
      </div>
    </div>

    <div
      class="pane-splitter"
      title="拖拽调整 RSI 与 KDJ 高度；一侧压到最低后继续拖可整体增高（滚动）"
      @mousedown="onSplitterMouseDown(2, $event)"
    />

    <div class="pane-label">KDJ</div>
    <div class="pane-wrap pane-wrap--sub" :style="{ height: `${paneHeights[3]}px` }">
      <div ref="kdjContainer" class="pane sub-pane" />
      <div
        v-show="hoverTimeLabel"
        :class="['pane-legend', 'pane-legend--sub', legendDockRight ? 'pane-legend--dock-right' : 'pane-legend--dock-left']"
      >
        <div class="pane-legend-time">{{ hoverTimeLabel }}</div>
        <div class="pane-legend-metrics">
          <span class="kdj-k">K {{ formatNum(hoverKdj.k, 1) }}</span>
          <span class="kdj-d">D {{ formatNum(hoverKdj.d, 1) }}</span>
          <span class="kdj-j">J {{ formatNum(hoverKdj.j, 1) }}</span>
        </div>
      </div>
    </div>
      </div>
    </div>
  </div>
</template>

<style scoped>
.kline-root {
  display: flex;
  flex-direction: column;
  width: 100%;
  height: 100%;
  min-height: 0;
  position: relative;
}

.btn-restore-pane-layout {
  position: absolute;
  top: 8px;
  right: 56px;
  z-index: 45;
  padding: 4px 10px;
  border-radius: 4px;
  border: 1px solid #45475a;
  background: rgba(24, 24, 37, 0.92);
  color: #a6adc8;
  font-size: 12px;
  cursor: pointer;
  transition: background 0.15s, color 0.15s, border-color 0.15s;
}

.btn-restore-pane-layout:hover {
  border-color: #89b4fa;
  color: #cdd6f4;
  background: #313244;
}

.chart-scroll {
  flex: 1;
  min-height: 0;
  overflow-x: hidden;
  overflow-y: auto;
}

.chart-stack {
  display: flex;
  flex-direction: column;
  width: 100%;
  flex-shrink: 0;
}

.pane-splitter {
  flex-shrink: 0;
  height: 6px;
  margin: 0;
  cursor: ns-resize;
  background: #313244;
  border-top: 1px solid #1e1e2e;
  border-bottom: 1px solid #1e1e2e;
}

.pane-splitter:hover {
  background: #45475a;
}

.pane-wrap {
  position: relative;
  width: 100%;
  min-height: 0;
  flex-shrink: 0;
  display: flex;
  flex-direction: column;
}

.pane-wrap--main,
.pane-wrap--sub {
  min-height: 72px;
}

.pane {
  width: 100%;
  flex: 1;
  min-height: 0;
}

.main-pane {
  min-height: 0;
}

.sub-pane {
  border-top: 1px solid #313244;
}

.pane-legend {
  position: absolute;
  top: 6px;
  z-index: 30;
  pointer-events: none;
  padding: 6px 8px;
  border-radius: 6px;
  background: rgba(24, 24, 37, 0.92);
  border: 1px solid #45475a;
  font-size: 12px;
  color: #cdd6f4;
  line-height: 1.35;
}

/* 左上角：给右侧价轴留空 */
.pane-legend--dock-left {
  left: 8px;
  right: auto;
  max-width: calc(100% - 56px);
  text-align: left;
}

/* 右上角：避开右侧价格刻度 */
.pane-legend--dock-right {
  left: auto;
  right: 52px;
  max-width: calc(100% - 60px);
  text-align: right;
}

.pane-legend-time {
  font-size: 11px;
  color: #a6adc8;
  margin-bottom: 4px;
}

.pane-legend--dock-right .pane-legend-time {
  text-align: right;
}

.pane-legend--main .pane-legend-row {
  display: flex;
  flex-wrap: wrap;
  gap: 8px 12px;
  align-items: center;
}

.pane-legend--dock-right.pane-legend--main .pane-legend-row {
  justify-content: flex-end;
}

.pane-legend-metrics {
  display: flex;
  flex-wrap: wrap;
  gap: 8px 12px;
  align-items: center;
}

.pane-legend--dock-right .pane-legend-metrics {
  justify-content: flex-end;
}

.pane-legend .macd-dif { color: #89b4fa; }
.pane-legend .macd-dea { color: #fab387; }
.pane-legend .macd-hist { color: #a6adc8; }
.pane-legend .kdj-k { color: #f9e2af; }
.pane-legend .kdj-d { color: #89b4fa; }
.pane-legend .kdj-j { color: #cba6f7; }

.pane-legend b { font-weight: 600; }
.pane-legend .up { color: #ef5350; }
.pane-legend .down { color: #26a69a; }
.pane-legend .vol { color: #a6adc8; }

.pane-label {
  position: relative;
  padding: 1px 8px;
  font-size: 11px;
  color: #6c7086;
  background: #181825;
  flex-shrink: 0;
}
</style>
