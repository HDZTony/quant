<script setup lang="ts">
import { ref, onMounted, onUnmounted, watch, nextTick } from 'vue'
import {
  createChart,
  LineSeries,
  type IChartApi,
  type ISeriesApi,
  type LineData,
  type Time,
  CrosshairMode,
} from 'lightweight-charts'

const props = defineProps<{
  data: MLBacktestResponse
}>()

const CHART_BG = '#1e1e2e'
const GRID_COLOR = '#31324422'
const TEXT_COLOR = '#a6adc8'
const CURVE_COLORS = ['#cba6f7', '#a6e3a1', '#89b4fa']

const equityRef = ref<HTMLDivElement | null>(null)
const featureRef = ref<HTMLDivElement | null>(null)

let equityChart: IChartApi | null = null
let featureChart: IChartApi | null = null
let equitySeries: ISeriesApi<'Line'>[] = []

function chartOptions() {
  return {
    layout: { background: { color: CHART_BG }, textColor: TEXT_COLOR, fontSize: 11 },
    grid: { vertLines: { color: GRID_COLOR }, horzLines: { color: GRID_COLOR } },
    crosshair: { mode: CrosshairMode.Normal },
    rightPriceScale: { borderColor: '#313244' },
    timeScale: { borderColor: '#313244', timeVisible: true, secondsVisible: false },
  }
}

function renderEquityCurves() {
  if (!equityRef.value || !props.data.equity_curves.length) return

  if (equityChart) {
    equityChart.remove()
    equityChart = null
    equitySeries = []
  }

  equityChart = createChart(equityRef.value, {
    ...chartOptions(),
    height: equityRef.value.clientHeight || 280,
    width: equityRef.value.clientWidth || 400,
  })

  props.data.equity_curves.forEach((curve, i) => {
    const series = equityChart!.addSeries(LineSeries, {
      color: CURVE_COLORS[i % CURVE_COLORS.length],
      lineWidth: 2,
      title: curve.name,
    })
    const lineData: LineData<Time>[] = curve.data.map(p => ({
      time: p.time as Time,
      value: p.value,
    }))
    series.setData(lineData)
    equitySeries.push(series)
  })

  equityChart.timeScale().fitContent()
}

function renderFeatureImportance() {
  if (!featureRef.value || !props.data.feature_importance.length) return

  if (featureChart) {
    featureChart.remove()
    featureChart = null
  }

  featureChart = createChart(featureRef.value, {
    ...chartOptions(),
    height: featureRef.value.clientHeight || 280,
    width: featureRef.value.clientWidth || 400,
  })

  const sorted = [...props.data.feature_importance]
    .sort((a, b) => a.importance - b.importance)
    .slice(-20)

  const series = featureChart.addSeries(LineSeries, {
    color: '#f9e2af',
    lineWidth: 2,
    title: '特征重要性',
  })

  const data: LineData<Time>[] = sorted.map((item, i) => ({
    time: (i + 1) as unknown as Time,
    value: item.importance,
  }))
  series.setData(data)
  featureChart.timeScale().fitContent()
}

function resizeCharts() {
  if (equityChart && equityRef.value) {
    equityChart.resize(equityRef.value.clientWidth, equityRef.value.clientHeight)
  }
  if (featureChart && featureRef.value) {
    featureChart.resize(featureRef.value.clientWidth, featureRef.value.clientHeight)
  }
}

function fmtPct(v: number): string {
  return (v * 100).toFixed(2) + '%'
}

function fmtNum(v: number, digits = 2): string {
  return v.toFixed(digits)
}

watch(() => props.data, () => {
  nextTick(() => {
    renderEquityCurves()
    renderFeatureImportance()
  })
}, { deep: true })

let ro: ResizeObserver | null = null

onMounted(() => {
  nextTick(() => {
    renderEquityCurves()
    renderFeatureImportance()
  })
  ro = new ResizeObserver(() => resizeCharts())
  if (equityRef.value) ro.observe(equityRef.value)
  if (featureRef.value) ro.observe(featureRef.value)
})

onUnmounted(() => {
  equityChart?.remove()
  featureChart?.remove()
  ro?.disconnect()
})
</script>

<template>
  <div class="ml-dashboard">
    <!-- 上排：指标卡片 -->
    <section class="metrics-row" v-if="data.metrics.length">
      <div
        v-for="m in data.metrics"
        :key="m.name"
        class="metric-card"
      >
        <div class="metric-name">{{ m.name }}</div>
        <div class="metric-grid">
          <div class="metric-item">
            <span class="label">总收益</span>
            <span :class="['value', m.total_return >= 0 ? 'up' : 'down']">{{ fmtPct(m.total_return) }}</span>
          </div>
          <div class="metric-item">
            <span class="label">年化</span>
            <span :class="['value', m.annual_return >= 0 ? 'up' : 'down']">{{ fmtPct(m.annual_return) }}</span>
          </div>
          <div class="metric-item">
            <span class="label">最大回撤</span>
            <span class="value down">{{ fmtPct(m.max_drawdown) }}</span>
          </div>
          <div class="metric-item">
            <span class="label">胜率</span>
            <span class="value">{{ fmtPct(m.win_rate) }}</span>
          </div>
          <div class="metric-item">
            <span class="label">交易次数</span>
            <span class="value">{{ m.total_trades }}</span>
          </div>
          <div class="metric-item">
            <span class="label">盈亏比</span>
            <span class="value">{{ fmtNum(m.profit_factor) }}</span>
          </div>
          <div class="metric-item">
            <span class="label">Sharpe</span>
            <span class="value">{{ fmtNum(m.sharpe_ratio) }}</span>
          </div>
        </div>
      </div>
    </section>

    <!-- 下排：两个图表 -->
    <div class="charts-row">
      <div class="chart-panel">
        <div class="panel-title">权益曲线对比（归一化）</div>
        <div ref="equityRef" class="chart-container" />
      </div>
      <div class="chart-panel">
        <div class="panel-title">特征重要性 Top 20</div>
        <div class="feature-list" v-if="data.feature_importance.length">
          <div
            v-for="item in data.feature_importance.slice(0, 20)"
            :key="item.feature"
            class="feature-bar"
          >
            <span class="f-name">{{ item.feature }}</span>
            <div class="f-bar-track">
              <div
                class="f-bar-fill"
                :style="{ width: (item.importance / data.feature_importance[0].importance * 100) + '%' }"
              />
            </div>
            <span class="f-value">{{ (item.importance * 100).toFixed(1) }}%</span>
          </div>
        </div>
        <div v-else class="empty-hint">暂无特征重要性数据</div>
      </div>
    </div>

    <div v-if="data.error" class="ml-error">{{ data.error }}</div>
  </div>
</template>

<style scoped>
.ml-dashboard {
  display: flex;
  flex-direction: column;
  height: 100%;
  gap: 8px;
  padding: 8px 12px;
  overflow-y: auto;
}

.metrics-row {
  display: flex;
  gap: 8px;
  flex-shrink: 0;
}

.metric-card {
  flex: 1;
  background: #181825;
  border-radius: 8px;
  padding: 10px 14px;
  border: 1px solid #313244;
}

.metric-name {
  font-size: 14px;
  font-weight: 600;
  color: #cba6f7;
  margin-bottom: 8px;
}

.metric-grid {
  display: grid;
  grid-template-columns: repeat(4, 1fr);
  gap: 6px 12px;
}

.metric-item {
  display: flex;
  flex-direction: column;
  gap: 2px;
}

.metric-item .label {
  font-size: 11px;
  color: #7f849c;
}

.metric-item .value {
  font-size: 13px;
  font-weight: 600;
  color: #cdd6f4;
}

.metric-item .value.up { color: #f38ba8; }
.metric-item .value.down { color: #a6e3a1; }

.charts-row {
  display: flex;
  gap: 8px;
  flex: 1;
  min-height: 0;
}

.chart-panel {
  flex: 1;
  display: flex;
  flex-direction: column;
  background: #181825;
  border-radius: 8px;
  border: 1px solid #313244;
  overflow: hidden;
}

.panel-title {
  padding: 8px 12px;
  font-size: 13px;
  font-weight: 600;
  color: #cba6f7;
  border-bottom: 1px solid #313244;
  flex-shrink: 0;
}

.chart-container {
  flex: 1;
  min-height: 200px;
}

.feature-list {
  flex: 1;
  overflow-y: auto;
  padding: 8px 12px;
  display: flex;
  flex-direction: column;
  gap: 4px;
}

.feature-bar {
  display: flex;
  align-items: center;
  gap: 8px;
}

.f-name {
  width: 160px;
  font-size: 11px;
  color: #a6adc8;
  text-align: right;
  flex-shrink: 0;
  overflow: hidden;
  text-overflow: ellipsis;
  white-space: nowrap;
}

.f-bar-track {
  flex: 1;
  height: 14px;
  background: #31324488;
  border-radius: 3px;
  overflow: hidden;
}

.f-bar-fill {
  height: 100%;
  background: linear-gradient(90deg, #f9e2af, #fab387);
  border-radius: 3px;
  transition: width 0.3s;
}

.f-value {
  width: 50px;
  font-size: 11px;
  color: #a6adc8;
  flex-shrink: 0;
}

.empty-hint {
  display: flex;
  align-items: center;
  justify-content: center;
  height: 100%;
  color: #7f849c;
  font-size: 13px;
}

.ml-error {
  padding: 8px 12px;
  background: #45272e;
  color: #f38ba8;
  font-size: 12px;
  border-radius: 6px;
}
</style>
