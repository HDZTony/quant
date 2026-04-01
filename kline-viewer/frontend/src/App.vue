<script setup lang="ts">
import { ref, onMounted } from 'vue'
import type { CandlestickData, HistogramData, Time } from 'lightweight-charts'
import KlineChart from './components/KlineChart.vue'

const dates = ref<string[]>([])
const selectedDate = ref('')
const candles = ref<CandlestickData<Time>[]>([])
const volume = ref<HistogramData<Time>[]>([])
const loading = ref(false)
const error = ref('')

async function waitForApi(retries = 50, interval = 100): Promise<PyWebViewApi> {
  for (let i = 0; i < retries; i++) {
    if (window.pywebview?.api) return window.pywebview.api
    await new Promise(r => setTimeout(r, interval))
  }
  throw new Error('pywebview API 不可用，请通过 app.py 启动')
}

async function loadDates() {
  try {
    const api = await waitForApi()
    dates.value = await api.get_trading_dates()
    if (dates.value.length > 0) {
      selectedDate.value = dates.value[dates.value.length - 1]
      await loadKline()
    }
  } catch (e) {
    error.value = String(e)
  }
}

async function loadKline() {
  if (!selectedDate.value) return
  loading.value = true
  error.value = ''
  try {
    const api = await waitForApi()
    const data = await api.get_kline_data(selectedDate.value)
    candles.value = data.candles as CandlestickData<Time>[]
    volume.value = data.volume as HistogramData<Time>[]
  } catch (e) {
    error.value = String(e)
  } finally {
    loading.value = false
  }
}

function onDateChange(e: Event) {
  selectedDate.value = (e.target as HTMLSelectElement).value
  loadKline()
}

onMounted(loadDates)
</script>

<template>
  <div class="app">
    <header class="toolbar">
      <span class="title">159506 ETF</span>
      <select
        :value="selectedDate"
        class="date-select"
        @change="onDateChange"
      >
        <option v-for="d in dates" :key="d" :value="d">{{ d }}</option>
      </select>
      <span v-if="loading" class="status">加载中...</span>
      <span v-if="candles.length > 0 && !loading" class="status bar-count">
        {{ candles.length }} 根K线
      </span>
    </header>

    <div v-if="error" class="error">{{ error }}</div>

    <main class="chart-area">
      <KlineChart :candles="candles" :volume="volume" />
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
  gap: 16px;
  padding: 8px 16px;
  background: #181825;
  border-bottom: 1px solid #313244;
  flex-shrink: 0;
}

.title {
  font-weight: 700;
  font-size: 15px;
  color: #cba6f7;
}

.date-select {
  padding: 4px 8px;
  border-radius: 4px;
  border: 1px solid #45475a;
  background: #313244;
  color: #cdd6f4;
  font-size: 14px;
  cursor: pointer;
  outline: none;
}

.date-select:focus {
  border-color: #cba6f7;
}

.status {
  font-size: 13px;
  color: #a6adc8;
}

.bar-count {
  margin-left: auto;
}

.error {
  padding: 8px 16px;
  background: #45272e;
  color: #f38ba8;
  font-size: 13px;
}

.chart-area {
  flex: 1;
  min-height: 0;
}
</style>
