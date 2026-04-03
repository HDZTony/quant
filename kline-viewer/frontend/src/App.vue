<script setup lang="ts">
import { ref, onMounted, onUnmounted, watch } from 'vue'
import KlineChart from './components/KlineChart.vue'
import MacroDashboard from './components/MacroDashboard.vue'
import EquityExplorer from './components/EquityExplorer.vue'
import MLDashboardVue from './components/MLDashboard.vue'

const API_BASE = import.meta.env.DEV ? 'http://localhost:9090' : ''

const mode = ref<ViewMode>('realtime')
const dates = ref<string[]>([])
const selectedDate = ref('')
const klineData = ref<KlineResponse | null>(null)
const macroData = ref<MacroSeries[]>([])
const macroLoaded = ref(false)
const loading = ref(false)
const error = ref('')
const backtestInfo = ref('')
const wsConnected = ref(false)

const liveTrading = ref({ running: false, scheduled: false, pid: null as number | null })
const liveToggling = ref(false)
let liveStatusTimer: ReturnType<typeof setInterval> | null = null

const mlBacktestData = ref<MLBacktestResponse>({ equity_curves: [], metrics: [], signals: [], feature_importance: [] })
const mlLoading = ref(false)

const paperTrading = ref({ running: false, pid: null as number | null })
const tradingMode = ref<TradingMode>('off')

let ws: WebSocket | null = null
let realtimeRefreshTimer: ReturnType<typeof setInterval> | null = null

async function loadDates() {
  try {
    const res = await fetch(`${API_BASE}/api/dates`)
    dates.value = await res.json()
    if (dates.value.length > 0) {
      selectedDate.value = dates.value[dates.value.length - 1]
      if (mode.value === 'realtime') {
        await syncKlineViewForSelection()
      } else {
        await loadKline()
      }
    }
  } catch (e) {
    error.value = String(e)
  }
}

const HISTORY_DAYS = 5
const LOAD_MORE_DAYS = 5
let loadingMore = false
let noMoreData = false

async function loadMore() {
  if (loadingMore || noMoreData || !klineData.value || !klineData.value.candles.length) return
  loadingMore = true
  try {
    const earliestTime = klineData.value.candles[0].time
    const earliestDate = new Date(earliestTime * 1000)
    const yyyy = earliestDate.getFullYear()
    const mm = String(earliestDate.getMonth() + 1).padStart(2, '0')
    const dd = String(earliestDate.getDate()).padStart(2, '0')
    const dateStr = `${yyyy}-${mm}-${dd}`

    const idx = dates.value.indexOf(dateStr)
    if (idx <= 0) {
      noMoreData = true
      return
    }

    const endIdx = idx - 1
    const startIdx = Math.max(0, endIdx - LOAD_MORE_DAYS + 1)
    const endDate = dates.value[endIdx]
    const count = endIdx - startIdx + 1

    const res = await fetch(`${API_BASE}/api/kline/range?end=${endDate}&days=${count}`)
    const older: KlineResponse = await res.json()
    if (!older.candles || older.candles.length === 0) {
      noMoreData = true
      return
    }

    const d = klineData.value
    klineData.value = {
      candles: [...older.candles, ...d.candles],
      volume: [...older.volume, ...d.volume],
      macd: [...older.macd, ...d.macd],
      rsi: [...older.rsi, ...d.rsi],
      kdj: [...older.kdj, ...d.kdj],
      signals: d.signals,
    }

    if (startIdx === 0) noMoreData = true
  } catch { /* silent */ } finally {
    loadingMore = false
  }
}

async function loadKline() {
  if (!selectedDate.value) return
  loading.value = true
  error.value = ''
  noMoreData = false
  try {
    const res = await fetch(`${API_BASE}/api/kline/range?end=${selectedDate.value}&days=${HISTORY_DAYS}`)
    klineData.value = await res.json()
  } catch (e) {
    error.value = String(e)
  } finally {
    loading.value = false
  }
}

async function runBacktest() {
  if (!selectedDate.value) return
  loading.value = true
  error.value = ''
  backtestInfo.value = ''
  try {
    const [rangeRes, btRes] = await Promise.all([
      fetch(`${API_BASE}/api/kline/range?end=${selectedDate.value}&days=${HISTORY_DAYS}`),
      fetch(`${API_BASE}/api/backtest`, {
        method: 'POST',
        headers: { 'Content-Type': 'application/json' },
        body: JSON.stringify({ date: selectedDate.value }),
      }),
    ])
    const rangeData: KlineResponse = await rangeRes.json()
    const btData: KlineResponse = await btRes.json()

    if (!btRes.ok) {
      error.value = `回测接口 HTTP ${btRes.status}`
      return
    }

    const bm = btData.backtest_meta
    if (bm?.error) {
      error.value = `回测失败: ${bm.error}`
    } else if (bm?.ok) {
      backtestInfo.value = `回测完成，载入 ${bm.signal_count} 个买卖点标记`
    }

    const hasBtCandles = Array.isArray(btData.candles) && btData.candles.length > 0
    if (rangeData.candles && rangeData.candles.length > 0 && hasBtCandles) {
      const todayFirstTime = btData.candles![0]?.time ?? Infinity
      const prevCandles = rangeData.candles.filter((c: KlineCandle) => c.time < todayFirstTime)
      const prevVolume = rangeData.volume.filter((v: KlineVolume) => v.time < todayFirstTime)
      const prevMacd = rangeData.macd.filter((m: MacdPoint) => m.time < todayFirstTime)
      const prevRsi = rangeData.rsi.filter((r: RsiPoint) => r.time < todayFirstTime)
      const prevKdj = rangeData.kdj.filter((k: KdjPoint) => k.time < todayFirstTime)

      btData.candles = [...prevCandles, ...btData.candles!]
      btData.volume = [...prevVolume, ...btData.volume]
      btData.macd = [...prevMacd, ...btData.macd]
      btData.rsi = [...prevRsi, ...btData.rsi]
      btData.kdj = [...prevKdj, ...btData.kdj]
    }
    klineData.value = { ...btData }
  } catch (e) {
    error.value = String(e)
  } finally {
    loading.value = false
  }
}

async function loadMacroData() {
  if (macroLoaded.value) return
  loading.value = true
  error.value = ''
  try {
    const res = await fetch(`${API_BASE}/api/macro/data`)
    const payload: MacroResponse = await res.json()
    macroData.value = payload.series ?? []
    macroLoaded.value = true
  } catch (e) {
    error.value = String(e)
  } finally {
    loading.value = false
  }
}

function connectWs() {
  if (ws) ws.close()
  const wsUrl = import.meta.env.DEV
    ? 'ws://localhost:9090/ws/realtime'
    : `ws://${location.host}/ws/realtime`
  ws = new WebSocket(wsUrl)
  ws.onopen = () => { wsConnected.value = true }
  ws.onclose = () => {
    wsConnected.value = false
    if (mode.value === 'realtime') {
      setTimeout(connectWs, 3000)
    }
  }
  ws.onmessage = (event) => {
    try {
      const payload = JSON.parse(event.data)
      if (!isBarPayload(payload)) return
      const bar: BarData = payload
      appendRealtimeBar(bar)
    } catch { /* ignore non-json */ }
  }
}

function isBarPayload(payload: unknown): payload is BarData {
  if (!payload || typeof payload !== 'object') return false
  const p = payload as Record<string, unknown>
  return (
    typeof p.time === 'number'
    && typeof p.open === 'number'
    && typeof p.high === 'number'
    && typeof p.low === 'number'
    && typeof p.close === 'number'
    && typeof p.volume === 'number'
  )
}

function disconnectWs() {
  if (ws) { ws.close(); ws = null }
  wsConnected.value = false
}

function appendRealtimeBar(bar: BarData) {
  if (!klineData.value) {
    klineData.value = { candles: [], volume: [], macd: [], rsi: [], kdj: [], signals: [] }
  }
  const d = klineData.value
  const t = bar.time
  const o = bar.open, h = bar.high, l = bar.low, c = bar.close
  if (!Number.isFinite(t) || !Number.isFinite(o) || !Number.isFinite(h) || !Number.isFinite(l) || !Number.isFinite(c)) {
    return
  }
  const color = c >= o ? 'rgba(239,83,80,0.5)' : 'rgba(38,166,154,0.5)'

  // 同一时间戳到来时覆盖最后一根，避免重复bar
  const lastCandle = d.candles[d.candles.length - 1]
  if (lastCandle && lastCandle.time === t) {
    d.candles[d.candles.length - 1] = { time: t, open: o, high: h, low: l, close: c }
    d.volume[d.volume.length - 1] = { time: t, value: bar.volume, color }
    d.macd[d.macd.length - 1] = { time: t, dif: bar.macd_dif, dea: bar.macd_dea, histogram: bar.macd_histogram }
    d.rsi[d.rsi.length - 1] = { time: t, value: bar.rsi }
    d.kdj[d.kdj.length - 1] = { time: t, k: bar.kdj_k, d: bar.kdj_d, j: bar.kdj_j }
    if (bar.signal) {
      d.signals.push({ time: t, side: bar.signal, price: c, signal_type: 'realtime' })
    }
    klineData.value = { ...d }
    return
  }

  d.candles.push({ time: t, open: o, high: h, low: l, close: c })
  d.volume.push({ time: t, value: bar.volume, color })
  d.macd.push({ time: t, dif: bar.macd_dif, dea: bar.macd_dea, histogram: bar.macd_histogram })
  d.rsi.push({ time: t, value: bar.rsi })
  d.kdj.push({ time: t, k: bar.kdj_k, d: bar.kdj_d, j: bar.kdj_j })
  if (bar.signal) {
    d.signals.push({ time: t, side: bar.signal, price: c, signal_type: 'realtime' })
  }

  klineData.value = { ...d }
}

async function loadTodayKline() {
  try {
    const res = await fetch(`${API_BASE}/api/kline/today?history_days=${HISTORY_DAYS}`)
    const data = await res.json()
    if (data.candles && data.candles.length > 0) {
      klineData.value = data
    }
  } catch { /* silent */ }
}

/** 选中最后一个交易日（列表最后一项）时走当日接口 + WebSocket；否则仅历史 K 线 */
function isLatestTradingDaySelected(): boolean {
  if (!dates.value.length || !selectedDate.value) return true
  return selectedDate.value === dates.value[dates.value.length - 1]
}

function exitRealtimePipeline() {
  disconnectWs()
  if (realtimeRefreshTimer) {
    clearInterval(realtimeRefreshTimer)
    realtimeRefreshTimer = null
  }
}

/** 实时页：按当前日期切换「当日+推送」或「仅历史」 */
async function syncKlineViewForSelection() {
  if (mode.value !== 'realtime') return
  exitRealtimePipeline()
  loading.value = true
  error.value = ''
  noMoreData = false
  try {
    if (!selectedDate.value) return
    if (isLatestTradingDaySelected()) {
      await loadTodayKline()
      connectWs()
      realtimeRefreshTimer = setInterval(loadTodayKline, 30_000)
    } else {
      await loadKline()
    }
  } finally {
    loading.value = false
  }
}

function onModeChange(newMode: ViewMode) {
  if (mode.value === 'realtime') exitRealtimePipeline()
  mode.value = newMode
  if (newMode === 'realtime') {
    void syncKlineViewForSelection()
  } else if (newMode === 'macro') {
    loadMacroData()
  } else if (newMode === 'explore') {
    /* nothing */
  } else if (newMode === 'backtest') {
    if (selectedDate.value) loadKline()
  } else if (newMode === 'ml') {
    /* ML tab is loaded on-demand via button */
  }
}

function onDateChange(e: Event) {
  selectedDate.value = (e.target as HTMLSelectElement).value
  if (mode.value === 'realtime') {
    void syncKlineViewForSelection()
  } else if (mode.value === 'backtest') {
    loadKline()
  }
}

watch(mode, (newMode) => {
  if (newMode === 'backtest' && selectedDate.value) {
    runBacktest()
  }
})

async function fetchLiveStatus() {
  try {
    const res = await fetch(`${API_BASE}/api/live-trading/status`)
    liveTrading.value = await res.json()
    if (liveTrading.value.running && tradingMode.value === 'off') {
      tradingMode.value = 'live'
    }
  } catch { /* silent */ }
}

function startLiveStatusPolling() {
  fetchLiveStatus()
  fetchPaperStatus()
  liveStatusTimer = setInterval(() => { fetchLiveStatus(); fetchPaperStatus() }, 10_000)
}

function stopLiveStatusPolling() {
  if (liveStatusTimer) { clearInterval(liveStatusTimer); liveStatusTimer = null }
}

async function runMLBacktest() {
  mlLoading.value = true
  error.value = ''
  try {
    const res = await fetch(`${API_BASE}/api/ml-backtest`, {
      method: 'POST',
      headers: { 'Content-Type': 'application/json' },
      body: JSON.stringify({ date: selectedDate.value || '' }),
    })
    mlBacktestData.value = await res.json()
  } catch (e) {
    error.value = String(e)
  } finally {
    mlLoading.value = false
  }
}

async function fetchPaperStatus() {
  try {
    const res = await fetch(`${API_BASE}/api/paper-trading/status`)
    paperTrading.value = await res.json()
    if (paperTrading.value.running && tradingMode.value === 'off') {
      tradingMode.value = 'paper'
    }
  } catch { /* silent */ }
}

async function switchTradingMode(target: TradingMode) {
  if (target === tradingMode.value) return
  liveToggling.value = true
  error.value = ''
  try {
    if (tradingMode.value === 'live') {
      await fetch(`${API_BASE}/api/live-trading/stop`, { method: 'POST' })
    } else if (tradingMode.value === 'paper') {
      await fetch(`${API_BASE}/api/paper-trading/stop`, { method: 'POST' })
    }

    if (target === 'live') {
      const res = await fetch(`${API_BASE}/api/live-trading/start`, { method: 'POST' })
      const data = await res.json()
      if (data.detail) { error.value = data.detail; return }
    } else if (target === 'paper') {
      const res = await fetch(`${API_BASE}/api/paper-trading/start`, { method: 'POST' })
      const data = await res.json()
      if (data.detail) { error.value = data.detail; return }
    }

    tradingMode.value = target
    await fetchLiveStatus()
    await fetchPaperStatus()
  } catch (e) {
    error.value = String(e)
  } finally {
    liveToggling.value = false
  }
}

onMounted(() => {
  loadDates()
  startLiveStatusPolling()
})
onUnmounted(() => {
  exitRealtimePipeline()
  stopLiveStatusPolling()
})
</script>

<template>
  <div class="app">
    <header class="toolbar">
      <span class="title">159506 ETF</span>

      <div class="mode-tabs">
        <button
          v-for="m in (['realtime', 'backtest', 'ml', 'macro', 'explore'] as ViewMode[])"
          :key="m"
          :class="['tab', { active: mode === m }]"
          @click="onModeChange(m)"
        >
          {{ { realtime: '实时', backtest: '回测', ml: 'ML回测', macro: '宏观', explore: '探索' }[m] }}
        </button>
      </div>

      <select
        v-if="mode === 'realtime' || mode === 'backtest' || mode === 'ml'"
        :value="selectedDate"
        class="date-select"
        @change="onDateChange"
      >
        <option v-for="d in dates" :key="d" :value="d">{{ d }}</option>
      </select>

      <button
        v-if="mode === 'backtest'"
        class="btn-run"
        :disabled="loading"
        @click="runBacktest"
      >
        {{ loading ? '回测中…' : '运行回测' }}
      </button>

      <button
        v-if="mode === 'ml'"
        class="btn-run ml-btn"
        :disabled="mlLoading"
        @click="runMLBacktest"
      >
        {{ mlLoading ? 'ML 回测中...' : '运行 ML 回测' }}
      </button>

      <span v-if="loading" class="status loading-hint">
        {{ mode === 'backtest' ? 'Nautilus 回测运行中，可能需要 1～5 分钟，请稍候…' : '加载中…' }}
      </span>
      <span v-if="mode === 'realtime'" :class="['status', wsConnected ? 'connected' : 'disconnected']">
        {{ wsConnected ? (klineData && klineData.candles.length > 0 ? '行情接收中' : '等待行情') : '未连接' }}
      </span>
      <span v-if="mode === 'macro' && macroData.length > 0 && !loading" class="status bar-count">
        {{ macroData.length }} 个指标
      </span>
      <span v-if="klineData && klineData.candles.length > 0 && !loading" class="status bar-count">
        {{ klineData.candles.length }} 根K线
      </span>

      <div class="trading-mode-switch">
        <button
          v-for="tm in (['off', 'paper', 'live'] as TradingMode[])"
          :key="tm"
          :class="['tm-btn', { active: tradingMode === tm }, tm]"
          :disabled="liveToggling"
          @click="switchTradingMode(tm)"
        >
          {{ { off: '关闭', paper: 'ML模拟', live: '实盘' }[tm] }}
        </button>
        <span v-if="liveTrading.scheduled" class="scheduled-badge">自动</span>
      </div>
    </header>

    <div v-if="error" class="error">{{ error }}</div>
    <div v-else-if="backtestInfo && mode === 'backtest'" class="info-banner">{{ backtestInfo }}</div>

    <main class="chart-area">
      <EquityExplorer v-if="mode === 'explore'" />
      <MacroDashboard
        v-else-if="mode === 'macro'"
        :series="macroData"
      />
      <MLDashboardVue
        v-else-if="mode === 'ml'"
        :data="mlBacktestData"
      />
      <div
        v-else-if="mode === 'realtime' && (!klineData || klineData.candles.length === 0)"
        class="realtime-empty"
      >
        <div class="empty-icon">&#8987;</div>
        <p class="empty-title">等待行情数据</p>
        <p class="empty-hint">
          当前非交易时段或暂无当天数据，交易时段将自动推送
        </p>
      </div>
      <KlineChart
        v-else-if="klineData"
        :candles="klineData.candles"
        :volume="klineData.volume"
        :macd="klineData.macd"
        :rsi="klineData.rsi"
        :kdj="klineData.kdj"
        :signals="klineData.signals"
        :mode="mode"
        @load-more="loadMore"
      />
    </main>
  </div>
</template>

<style scoped>
.app {
  display: flex;
  flex-direction: column;
  height: 100vh;
  background: #1e1e2e;
  color: #cdd6f4;
}

.toolbar {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 6px 16px;
  background: #181825;
  border-bottom: 1px solid #313244;
  flex-shrink: 0;
}

.title {
  font-weight: 700;
  font-size: 15px;
  color: #cba6f7;
}

.mode-tabs {
  display: flex;
  gap: 2px;
  background: #313244;
  border-radius: 6px;
  padding: 2px;
}

.tab {
  padding: 4px 14px;
  border: none;
  border-radius: 4px;
  background: transparent;
  color: #a6adc8;
  font-size: 13px;
  cursor: pointer;
  transition: all 0.15s;
}

.tab:hover { color: #cdd6f4; }
.tab.active { background: #45475a; color: #cdd6f4; }

.date-select {
  padding: 4px 8px;
  border-radius: 4px;
  border: 1px solid #45475a;
  background: #313244;
  color: #cdd6f4;
  font-size: 13px;
  cursor: pointer;
  outline: none;
}

.date-select:focus { border-color: #cba6f7; }

.btn-run {
  padding: 4px 14px;
  border: 1px solid #cba6f7;
  border-radius: 4px;
  background: transparent;
  color: #cba6f7;
  font-size: 13px;
  cursor: pointer;
  transition: all 0.15s;
}

.btn-run:hover { background: #cba6f733; }
.btn-run:disabled { opacity: 0.4; cursor: not-allowed; }

.status { font-size: 13px; color: #a6adc8; }
.bar-count { margin-left: auto; }
.connected { color: #a6e3a1; }
.disconnected { color: #f38ba8; }

.trading-mode-switch {
  display: flex;
  align-items: center;
  gap: 2px;
  margin-left: auto;
  background: #313244;
  border-radius: 6px;
  padding: 2px;
}

.tm-btn {
  padding: 4px 12px;
  border: none;
  border-radius: 4px;
  background: transparent;
  color: #a6adc8;
  font-size: 12px;
  cursor: pointer;
  transition: all 0.15s;
  white-space: nowrap;
}

.tm-btn:hover { color: #cdd6f4; }
.tm-btn:disabled { opacity: 0.5; cursor: wait; }
.tm-btn.active.off { background: #45475a; color: #cdd6f4; }
.tm-btn.active.paper { background: #a6e3a133; color: #a6e3a1; border: 1px dashed #a6e3a1; }
.tm-btn.active.live { background: #f38ba833; color: #f38ba8; border: 1px solid #f38ba8; }

.ml-btn {
  border-color: #89b4fa;
  color: #89b4fa;
}
.ml-btn:hover { background: #89b4fa33; }

.scheduled-badge {
  display: inline-block;
  font-size: 10px;
  padding: 1px 5px;
  border-radius: 3px;
  background: #cba6f733;
  color: #cba6f7;
  margin-left: 4px;
}

.error {
  padding: 8px 16px;
  background: #45272e;
  color: #f38ba8;
  font-size: 13px;
}

.info-banner {
  padding: 8px 16px;
  background: #1e3a2f;
  color: #a6e3a1;
  font-size: 13px;
}

.loading-hint {
  max-width: 420px;
  line-height: 1.35;
}

.chart-area {
  flex: 1;
  min-height: 0;
}

.realtime-empty {
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  height: 100%;
  gap: 8px;
  color: #a6adc8;
}

.empty-icon { font-size: 48px; opacity: 0.4; }
.empty-title { font-size: 18px; font-weight: 600; margin: 0; color: #cdd6f4; }
.empty-hint { font-size: 13px; margin: 0; color: #7f849c; }
</style>
