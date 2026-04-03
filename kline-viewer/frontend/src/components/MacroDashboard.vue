<script setup lang="ts">
import { computed, nextTick, onBeforeUnmount, onMounted, ref, watch, type ComponentPublicInstance } from 'vue'
import { createChart, LineSeries, type IChartApi, type ISeriesApi, type LineData, type Time } from 'lightweight-charts'

const props = defineProps<{
  series: MacroSeries[]
}>()

type ChartState = {
  chart: IChartApi
  line: ISeriesApi<'Line'>
}

type LayoutMode = 'single' | 'triple'

const containerMap = new Map<string, HTMLDivElement>()
const chartMap = new Map<string, ChartState>()
const layoutMode = ref<LayoutMode>('triple')
const hoveredTime = ref<number | null>(null)
let syncingCrosshair = false
const chartResizeObservers = new Map<string, ResizeObserver>()
const DAY_SECONDS = 24 * 60 * 60

const layoutClass = computed(() => (layoutMode.value === 'single' ? 'layout-single' : 'layout-triple'))

function frequencyLabel(): string {
  return '日频'
}
const hoveredTimeLabel = computed(() => {
  if (hoveredTime.value == null) return ''
  return new Date(hoveredTime.value * 1000).toLocaleDateString('zh-CN')
})

function normalizeDayTs(ts: number): number {
  return Math.floor(ts / DAY_SECONDS) * DAY_SECONDS
}

function resampleToDaily(data: MacroDataPoint[]): MacroDataPoint[] {
  if (data.length === 0) return []
  const sorted = [...data].sort((a, b) => a.time - b.time)
  const dayToValue = new Map<number, number>()
  for (const point of sorted) {
    if (point.value == null) continue
    dayToValue.set(normalizeDayTs(point.time), Number(point.value))
  }
  if (dayToValue.size === 0) return []

  const dayKeys = [...dayToValue.keys()].sort((a, b) => a - b)
  const start = dayKeys[0]
  const end = dayKeys[dayKeys.length - 1]
  const out: MacroDataPoint[] = []
  let lastValue: number | null = null

  for (let day = start; day <= end; day += DAY_SECONDS) {
    if (dayToValue.has(day)) lastValue = dayToValue.get(day) ?? null
    if (lastValue != null) out.push({ time: day, value: lastValue })
  }
  return out
}

const normalizedSeries = computed<MacroSeries[]>(() => {
  return props.series
    .filter((item) => item.meta.series_id !== 'NAPM')
    .map((item) => ({
      ...item,
      meta: { ...item.meta, frequency: 'daily' },
      data: resampleToDaily(item.data),
    }))
    .filter((item) => item.data.length > 0)
})

function setChartRef(seriesId: string) {
  return (el: Element | ComponentPublicInstance | null) => {
    const target = el instanceof HTMLDivElement
      ? el
      : (el && '$el' in el && el.$el instanceof HTMLDivElement ? el.$el : null)

    if (target) {
      containerMap.set(seriesId, target)
    } else {
      containerMap.delete(seriesId)
    }
  }
}

function setLayout(mode: LayoutMode): void {
  layoutMode.value = mode
}

function normalizeChartTime(time: Time | undefined): number | null {
  if (time == null) return null
  if (typeof time === 'number') return Math.trunc(time)
  if (typeof time === 'string') {
    const ts = Date.parse(`${time}T00:00:00Z`)
    return Number.isNaN(ts) ? null : Math.trunc(ts / 1000)
  }
  const ts = Date.UTC(time.year, time.month - 1, time.day) / 1000
  return Math.trunc(ts)
}

function lastValidPoint(data: MacroDataPoint[]): MacroDataPoint | null {
  for (let idx = data.length - 1; idx >= 0; idx -= 1) {
    const point = data[idx]
    if (point.value != null) return point
  }
  return null
}

function pointAtOrBefore(data: MacroDataPoint[], targetTime: number): MacroDataPoint | null {
  let left = 0
  let right = data.length - 1
  let answer: MacroDataPoint | null = null

  while (left <= right) {
    const mid = Math.floor((left + right) / 2)
    const point = data[mid]
    if (point.time <= targetTime) {
      if (point.value != null) answer = point
      left = mid + 1
    } else {
      right = mid - 1
    }
  }

  return answer
}

function displayPoint(item: MacroSeries): MacroDataPoint | null {
  if (hoveredTime.value == null) return lastValidPoint(item.data)
  return pointAtOrBefore(item.data, hoveredTime.value) ?? lastValidPoint(item.data)
}

function syncCrosshair(sourceSeriesId: string, targetTime: number | null): void {
  if (syncingCrosshair) return
  syncingCrosshair = true

  try {
    for (const item of normalizedSeries.value) {
      if (item.meta.series_id === sourceSeriesId) continue

      const state = chartMap.get(item.meta.series_id)
      if (!state) continue

      if (targetTime == null) {
        state.chart.clearCrosshairPosition()
        continue
      }

      const point = pointAtOrBefore(item.data, targetTime)
      if (!point || point.value == null) {
        state.chart.clearCrosshairPosition()
        continue
      }

      state.chart.setCrosshairPosition(point.value, targetTime as Time, state.line)
    }
  } finally {
    syncingCrosshair = false
  }
}

function latestValue(item: MacroSeries): number | null {
  const point = lastValidPoint(item.data)
  return point?.value ?? null
}

function previousValue(item: MacroSeries): number | null {
  let found = 0
  for (let idx = item.data.length - 1; idx >= 0; idx -= 1) {
    const point = item.data[idx]
    if (point.value == null) continue
    found += 1
    if (found === 2) return point.value
  }
  return null
}

function trendClass(item: MacroSeries): 'trend-up' | 'trend-down' | 'trend-flat' {
  const current = latestValue(item)
  const prev = previousValue(item)
  if (current == null || prev == null) return 'trend-flat'
  if (current > prev) return 'trend-up'
  if (current < prev) return 'trend-down'
  return 'trend-flat'
}

function formatValue(item: MacroSeries): string {
  const val = displayPoint(item)?.value ?? null
  if (val == null) return '--'
  return `${val.toFixed(2)} ${item.meta.unit}`
}

/** 单列：图表区高度优先用 DOM，否则用视口比例，避免首帧 clientHeight 为 0 */
function chartHeightPx(container: HTMLDivElement): number {
  if (layoutMode.value !== 'single') return 140
  const fromDom = container.clientHeight
  if (fromDom > 1) return fromDom
  return Math.max(Math.round(window.innerHeight * 0.28), 220)
}

function cleanupCharts() {
  for (const observer of chartResizeObservers.values()) {
    observer.disconnect()
  }
  chartResizeObservers.clear()
  for (const state of chartMap.values()) {
    state.chart.remove()
  }
  chartMap.clear()
}

async function renderCharts() {
  await nextTick()
  cleanupCharts()

  for (const item of normalizedSeries.value) {
    const container = containerMap.get(item.meta.series_id)
    if (!container) continue

    const points = item.data
      .filter((d) => d.value != null)
      .map((d) => ({ time: d.time as Time, value: Number(d.value) }) satisfies LineData<Time>)

    const chart = createChart(container, {
      width: Math.max(container.clientWidth, 1),
      height: chartHeightPx(container),
      layout: {
        background: { color: '#181825' },
        textColor: '#a6adc8',
      },
      grid: {
        vertLines: { color: '#313244' },
        horzLines: { color: '#313244' },
      },
      rightPriceScale: {
        borderColor: '#313244',
      },
      timeScale: {
        borderColor: '#313244',
        timeVisible: true,
        secondsVisible: false,
        rightOffset: 0,
        fixLeftEdge: true,
        fixRightEdge: true,
      },
      crosshair: {
        vertLine: { color: '#89b4fa' },
        horzLine: { color: '#89b4fa' },
      },
    })

    const line = chart.addSeries(LineSeries, {
      color: '#89b4fa',
      lineWidth: 2,
      lastValueVisible: false,
      priceLineVisible: false,
    })

    line.setData(points)
    chart.subscribeCrosshairMove((param) => {
      if (syncingCrosshair) return
      const point = param.point
      if (!point || point.x < 0 || point.y < 0 || !param.time) {
        hoveredTime.value = null
        syncCrosshair(item.meta.series_id, null)
        return
      }
      const normalizedTime = normalizeChartTime(param.time)
      hoveredTime.value = normalizedTime
      syncCrosshair(item.meta.series_id, normalizedTime)
    })
    chart.timeScale().fitContent()
    if (points.length > 1) {
      chart.timeScale().setVisibleRange({
        from: points[0].time,
        to: points[points.length - 1].time,
      })
    }
    chartMap.set(item.meta.series_id, { chart, line })

    const prevObs = chartResizeObservers.get(item.meta.series_id)
    if (prevObs) prevObs.disconnect()
    const ro = new ResizeObserver(() => {
      const el = containerMap.get(item.meta.series_id)
      const st = chartMap.get(item.meta.series_id)
      if (!el || !st) return
      const w = Math.max(el.clientWidth, 1)
      const h = chartHeightPx(el)
      st.chart.applyOptions({ width: w, height: h })
    })
    ro.observe(container)
    chartResizeObservers.set(item.meta.series_id, ro)
  }
}

function handleResize() {
  for (const [seriesId, state] of chartMap.entries()) {
    const container = containerMap.get(seriesId)
    if (!container) continue
    const w = Math.max(container.clientWidth, 1)
    const h = chartHeightPx(container)
    state.chart.applyOptions({ width: w, height: h })
  }
}

watch(
  () => props.series,
  async () => {
    await renderCharts()
  },
  { deep: true },
)

watch(layoutMode, async () => {
  await nextTick()
  handleResize()
})

onMounted(async () => {
  await renderCharts()
  window.addEventListener('resize', handleResize)
})

onBeforeUnmount(() => {
  window.removeEventListener('resize', handleResize)
  cleanupCharts()
})
</script>

<template>
  <div class="macro-panel">
    <div class="macro-toolbar">
      <div class="layout-switch">
        <button
          type="button"
          :class="['layout-btn', { active: layoutMode === 'single' }]"
          @click="setLayout('single')"
        >
          单列
        </button>
        <button
          type="button"
          :class="['layout-btn', { active: layoutMode === 'triple' }]"
          @click="setLayout('triple')"
        >
          三列
        </button>
      </div>
      <div class="hover-time">
        {{ hoveredTimeLabel ? `对齐时点: ${hoveredTimeLabel}` : '对齐时点: 最新值' }}
      </div>
    </div>
    <div :class="['macro-dashboard', layoutClass]">
    <article
      v-for="item in normalizedSeries"
      :key="item.meta.series_id"
      :class="['macro-card', { 'macro-card--single': layoutMode === 'single' }]"
    >
      <div class="macro-header">
        <h3 class="macro-title">{{ item.meta.name }}</h3>
        <span class="macro-frequency">{{ frequencyLabel() }}</span>
      </div>
      <p :class="['macro-value', trendClass(item)]">{{ formatValue(item) }}</p>
      <p class="macro-desc">{{ item.meta.description }}</p>
      <div :ref="setChartRef(item.meta.series_id)" class="macro-chart" />
    </article>
    </div>
  </div>
</template>

<style scoped>
.macro-panel {
  height: 100%;
  padding: 12px;
  overflow: hidden;
  display: flex;
  flex-direction: column;
  gap: 12px;
  background: #1e1e2e;
}

.macro-toolbar {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 12px;
  flex-shrink: 0;
}

.layout-switch {
  display: inline-flex;
  gap: 8px;
}

.layout-btn {
  border: 1px solid #45475a;
  border-radius: 8px;
  background: #181825;
  color: #cdd6f4;
  font-size: 12px;
  line-height: 1;
  padding: 8px 12px;
  cursor: pointer;
}

.layout-btn.active {
  border-color: #89b4fa;
  color: #89b4fa;
}

.hover-time {
  font-size: 12px;
  color: #a6adc8;
}

.macro-dashboard {
  display: grid;
  gap: 12px;
  flex: 1;
  min-height: 0;
  overflow: auto;
  align-content: start;
  padding-right: 2px;
}

.macro-dashboard.layout-single {
  grid-template-columns: minmax(0, 1fr);
  width: 100%;
}

.macro-dashboard.layout-triple {
  grid-template-columns: repeat(3, minmax(0, 1fr));
}

.macro-card {
  background: #11111b;
  border: 1px solid #313244;
  border-radius: 10px;
  padding: 12px;
  display: flex;
  flex-direction: column;
  gap: 6px;
  min-height: 250px;
}

/* 单列：文案在上，折线图整块全宽撑满容器（与视口同宽，仅受面板 padding 限制） */
.macro-card--single {
  display: flex;
  flex-direction: column;
  gap: 8px;
  width: 100%;
  max-width: 100%;
  min-width: 0;
  box-sizing: border-box;
}

.macro-card--single .macro-chart {
  margin-top: 0;
  width: 100%;
  min-width: 0;
  /* 竖向随视口变化，折线在横向始终 100% */
  height: clamp(220px, 30vh, 520px);
  min-height: 220px;
}

.macro-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 8px;
}

.macro-title {
  margin: 0;
  font-size: 14px;
  color: #cdd6f4;
}

.macro-frequency {
  font-size: 12px;
  color: #a6adc8;
}

.macro-value {
  margin: 0;
  font-size: 20px;
  font-weight: 700;
}

.trend-up { color: #a6e3a1; }
.trend-down { color: #f38ba8; }
.trend-flat { color: #cdd6f4; }

.macro-desc {
  margin: 0;
  font-size: 12px;
  color: #7f849c;
  min-height: 32px;
}

.macro-chart {
  margin-top: auto;
  width: 100%;
  height: 140px;
}

@media (max-width: 1600px) {
  .macro-dashboard.layout-triple {
    grid-template-columns: repeat(3, minmax(0, 1fr));
  }
}

@media (max-width: 1200px) {
  .macro-dashboard.layout-triple {
    grid-template-columns: repeat(2, minmax(0, 1fr));
  }
}

@media (max-width: 800px) {
  .macro-dashboard.layout-triple {
    grid-template-columns: 1fr;
  }
}
</style>
