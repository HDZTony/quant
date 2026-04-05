<script setup lang="ts">
import { ref, onMounted, onBeforeUnmount, watch, nextTick } from 'vue'
import {
  createChart,
  CandlestickSeries,
  HistogramSeries,
  type IChartApi,
  type ISeriesApi,
  type CandlestickData,
  type HistogramData,
  type Time,
  CrosshairMode,
} from 'lightweight-charts'

const API_BASE = ''

const symbol = ref('AAPL')
const period = ref('1y')
const loading = ref(false)
const error = ref('')
const equityData = ref<EquityResponse | null>(null)

const periods = [
  { value: '1m', label: '1月' },
  { value: '3m', label: '3月' },
  { value: '6m', label: '6月' },
  { value: '1y', label: '1年' },
  { value: '2y', label: '2年' },
  { value: '5y', label: '5年' },
]

const quickSymbols = [
  'AAPL', 'MSFT', 'GOOGL', 'AMZN', 'NVDA', 'TSLA', 'META',
  'SPY', 'QQQ', 'IWM', 'DIA', 'VIX',
]

const mainContainer = ref<HTMLDivElement | null>(null)
let mainChart: IChartApi | null = null
let candleSeries: ISeriesApi<'Candlestick'> | null = null
let volumeSeries: ISeriesApi<'Histogram'> | null = null

async function fetchEquity() {
  const sym = symbol.value.trim().toUpperCase()
  if (!sym) return
  loading.value = true
  error.value = ''
  try {
    const res = await fetch(`${API_BASE}/api/equity/price/${encodeURIComponent(sym)}?period=${period.value}`)
    if (!res.ok) {
      const detail = await res.json().catch(() => ({ detail: res.statusText }))
      throw new Error(detail.detail || res.statusText)
    }
    equityData.value = await res.json()
    await renderChart()
  } catch (e) {
    error.value = String(e)
  } finally {
    loading.value = false
  }
}

function selectSymbol(sym: string) {
  symbol.value = sym
  fetchEquity()
}

function onSearchSubmit() {
  fetchEquity()
}

function cleanupChart() {
  if (mainChart) {
    mainChart.remove()
    mainChart = null
    candleSeries = null
    volumeSeries = null
  }
}

async function renderChart() {
  await nextTick()
  cleanupChart()
  if (!mainContainer.value || !equityData.value || equityData.value.candles.length === 0) return

  const container = mainContainer.value
  mainChart = createChart(container, {
    width: container.clientWidth,
    height: container.clientHeight,
    layout: { background: { color: '#1e1e2e' }, textColor: '#a6adc8' },
    grid: { vertLines: { color: '#313244' }, horzLines: { color: '#313244' } },
    rightPriceScale: { borderColor: '#313244' },
    timeScale: { borderColor: '#313244', timeVisible: false, rightOffset: 5, minBarSpacing: 3 },
    crosshair: { mode: CrosshairMode.Normal },
  })

  candleSeries = mainChart.addSeries(CandlestickSeries, {
    upColor: '#a6e3a1',
    downColor: '#f38ba8',
    borderUpColor: '#a6e3a1',
    borderDownColor: '#f38ba8',
    wickUpColor: '#a6e3a1',
    wickDownColor: '#f38ba8',
  })

  const candles: CandlestickData<Time>[] = equityData.value.candles.map((c) => ({
    time: c.time as Time,
    open: c.open,
    high: c.high,
    low: c.low,
    close: c.close,
  }))
  candleSeries.setData(candles)

  volumeSeries = mainChart.addSeries(HistogramSeries, {
    priceFormat: { type: 'volume' },
    priceScaleId: 'volume',
  })
  mainChart.priceScale('volume').applyOptions({
    scaleMargins: { top: 0.8, bottom: 0 },
  })

  const volumes: HistogramData<Time>[] = equityData.value.volume.map((v) => ({
    time: v.time as Time,
    value: v.value,
    color: v.color,
  }))
  volumeSeries.setData(volumes)

  mainChart.timeScale().fitContent()
}

function handleResize() {
  if (mainChart && mainContainer.value) {
    mainChart.applyOptions({
      width: mainContainer.value.clientWidth,
      height: mainContainer.value.clientHeight,
    })
  }
}

watch(period, () => { fetchEquity() })

onMounted(() => {
  fetchEquity()
  window.addEventListener('resize', handleResize)
})

onBeforeUnmount(() => {
  window.removeEventListener('resize', handleResize)
  cleanupChart()
})
</script>

<template>
  <div class="equity-explorer">
    <div class="explorer-toolbar">
      <form class="search-form" @submit.prevent="onSearchSubmit">
        <input
          v-model="symbol"
          class="search-input"
          placeholder="输入代码 (如 AAPL, TSLA, SPY)"
          @keyup.enter="onSearchSubmit"
        />
        <button class="search-btn" type="submit" :disabled="loading">查询</button>
      </form>

      <div class="period-tabs">
        <button
          v-for="p in periods"
          :key="p.value"
          :class="['period-btn', { active: period === p.value }]"
          @click="period = p.value"
        >
          {{ p.label }}
        </button>
      </div>

      <span v-if="equityData" class="symbol-label">
        {{ equityData.symbol }}
        <span v-if="equityData.candles.length > 0" class="bar-info">
          {{ equityData.candles.length }} 根K线
        </span>
      </span>
      <span v-if="loading" class="loading-text">加载中...</span>
    </div>

    <div class="quick-symbols">
      <button
        v-for="s in quickSymbols"
        :key="s"
        :class="['quick-btn', { active: symbol.toUpperCase() === s }]"
        @click="selectSymbol(s)"
      >
        {{ s }}
      </button>
    </div>

    <div v-if="error" class="error-bar">{{ error }}</div>

    <div ref="mainContainer" class="chart-container" />
  </div>
</template>

<style scoped>
.equity-explorer {
  height: 100%;
  display: flex;
  flex-direction: column;
  background: #1e1e2e;
  color: #cdd6f4;
}

.explorer-toolbar {
  display: flex;
  align-items: center;
  gap: 12px;
  padding: 8px 16px;
  background: #181825;
  border-bottom: 1px solid #313244;
  flex-shrink: 0;
}

.search-form {
  display: flex;
  gap: 6px;
}

.search-input {
  padding: 5px 10px;
  border-radius: 4px;
  border: 1px solid #45475a;
  background: #313244;
  color: #cdd6f4;
  font-size: 13px;
  width: 220px;
  outline: none;
}

.search-input:focus { border-color: #89b4fa; }

.search-btn {
  padding: 5px 14px;
  border: 1px solid #89b4fa;
  border-radius: 4px;
  background: transparent;
  color: #89b4fa;
  font-size: 13px;
  cursor: pointer;
  transition: all 0.15s;
}

.search-btn:hover { background: #89b4fa33; }
.search-btn:disabled { opacity: 0.4; cursor: wait; }

.period-tabs {
  display: flex;
  gap: 2px;
  background: #313244;
  border-radius: 6px;
  padding: 2px;
}

.period-btn {
  padding: 4px 10px;
  border: none;
  border-radius: 4px;
  background: transparent;
  color: #a6adc8;
  font-size: 12px;
  cursor: pointer;
  transition: all 0.15s;
}

.period-btn:hover { color: #cdd6f4; }
.period-btn.active { background: #45475a; color: #cdd6f4; }

.symbol-label {
  font-size: 14px;
  font-weight: 700;
  color: #89b4fa;
}

.bar-info {
  font-weight: 400;
  font-size: 12px;
  color: #a6adc8;
  margin-left: 6px;
}

.loading-text {
  font-size: 13px;
  color: #a6adc8;
}

.quick-symbols {
  display: flex;
  gap: 4px;
  padding: 6px 16px;
  background: #181825;
  border-bottom: 1px solid #313244;
  flex-wrap: wrap;
  flex-shrink: 0;
}

.quick-btn {
  padding: 3px 10px;
  border: 1px solid #45475a;
  border-radius: 4px;
  background: transparent;
  color: #a6adc8;
  font-size: 12px;
  cursor: pointer;
  transition: all 0.15s;
}

.quick-btn:hover { border-color: #89b4fa; color: #89b4fa; }
.quick-btn.active { border-color: #89b4fa; background: #89b4fa22; color: #89b4fa; }

.error-bar {
  padding: 6px 16px;
  background: #45272e;
  color: #f38ba8;
  font-size: 13px;
  flex-shrink: 0;
}

.chart-container {
  flex: 1;
  min-height: 0;
}
</style>
