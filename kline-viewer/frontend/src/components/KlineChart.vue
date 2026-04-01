<script setup lang="ts">
import { ref, onMounted, onUnmounted, watch, nextTick } from 'vue'
import {
  createChart,
  CandlestickSeries,
  HistogramSeries,
  type IChartApi,
  type ISeriesApi,
  type CandlestickData,
  type HistogramData,
  type MouseEventParams,
  type Time,
  CrosshairMode,
} from 'lightweight-charts'

const props = defineProps<{
  candles: CandlestickData<Time>[]
  volume: HistogramData<Time>[]
}>()

const chartContainer = ref<HTMLDivElement | null>(null)

let chart: IChartApi | null = null
let candleSeries: ISeriesApi<'Candlestick'> | null = null
let volumeSeries: ISeriesApi<'Histogram'> | null = null
let resizeObserver: ResizeObserver | null = null

const legend = ref({ o: 0, h: 0, l: 0, c: 0, v: 0, change: 0 })
const legendVisible = ref(false)

function formatPrice(n: number): string {
  return n.toFixed(3)
}

function formatVolume(n: number): string {
  if (n >= 1_000_000) return (n / 1_000_000).toFixed(2) + 'M'
  if (n >= 1_000) return (n / 1_000).toFixed(1) + 'K'
  return String(n)
}

function initChart() {
  if (!chartContainer.value) return

  chart = createChart(chartContainer.value, {
    layout: {
      background: { color: '#1e1e2e' },
      textColor: '#cdd6f4',
    },
    grid: {
      vertLines: { color: '#313244' },
      horzLines: { color: '#313244' },
    },
    crosshair: {
      mode: CrosshairMode.Normal,
    },
    rightPriceScale: {
      borderColor: '#45475a',
    },
    timeScale: {
      borderColor: '#45475a',
      timeVisible: true,
      secondsVisible: false,
    },
  })

  candleSeries = chart.addSeries(CandlestickSeries, {
    upColor: '#ef5350',
    downColor: '#26a69a',
    borderVisible: false,
    wickUpColor: '#ef5350',
    wickDownColor: '#26a69a',
  })

  volumeSeries = chart.addSeries(HistogramSeries, {
    priceFormat: { type: 'volume' },
    priceScaleId: '',
  })

  volumeSeries.priceScale().applyOptions({
    scaleMargins: { top: 0.8, bottom: 0 },
  })

  chart.subscribeCrosshairMove(onCrosshairMove)

  resizeObserver = new ResizeObserver(() => {
    if (chart && chartContainer.value) {
      chart.applyOptions({
        width: chartContainer.value.clientWidth,
        height: chartContainer.value.clientHeight,
      })
    }
  })
  resizeObserver.observe(chartContainer.value)
}

function onCrosshairMove(param: MouseEventParams<Time>) {
  if (!param.point || !param.time || !candleSeries) {
    legendVisible.value = false
    return
  }

  const candle = param.seriesData.get(candleSeries) as CandlestickData<Time> | undefined
  const vol = param.seriesData.get(volumeSeries!) as HistogramData<Time> | undefined

  if (!candle) {
    legendVisible.value = false
    return
  }

  const change = candle.open !== 0
    ? ((candle.close - candle.open) / candle.open) * 100
    : 0

  legend.value = {
    o: candle.open,
    h: candle.high,
    l: candle.low,
    c: candle.close,
    v: vol?.value ?? 0,
    change,
  }
  legendVisible.value = true
}

function setData() {
  if (!candleSeries || !volumeSeries || !chart) return
  candleSeries.setData(props.candles)
  volumeSeries.setData(props.volume)
  chart.timeScale().fitContent()
}

onMounted(() => {
  initChart()
  if (props.candles.length > 0) setData()
})

onUnmounted(() => {
  resizeObserver?.disconnect()
  chart?.remove()
  chart = null
  candleSeries = null
  volumeSeries = null
})

watch(() => props.candles, () => {
  nextTick(setData)
})
</script>

<template>
  <div class="chart-wrapper">
    <div
      v-show="legendVisible"
      class="legend"
    >
      <span>O <b>{{ formatPrice(legend.o) }}</b></span>
      <span>H <b>{{ formatPrice(legend.h) }}</b></span>
      <span>L <b>{{ formatPrice(legend.l) }}</b></span>
      <span>C <b :class="legend.change >= 0 ? 'up' : 'down'">{{ formatPrice(legend.c) }}</b></span>
      <span :class="legend.change >= 0 ? 'up' : 'down'">
        {{ legend.change >= 0 ? '+' : '' }}{{ legend.change.toFixed(2) }}%
      </span>
      <span class="vol">Vol <b>{{ formatVolume(legend.v) }}</b></span>
    </div>
    <div ref="chartContainer" class="chart-container" />
  </div>
</template>

<style scoped>
.chart-wrapper {
  position: relative;
  width: 100%;
  height: 100%;
}

.chart-container {
  width: 100%;
  height: 100%;
}

.legend {
  position: absolute;
  top: 8px;
  left: 12px;
  z-index: 10;
  display: flex;
  gap: 12px;
  font-size: 13px;
  color: #cdd6f4;
  pointer-events: none;
}

.legend b {
  font-weight: 600;
}

.legend .up {
  color: #ef5350;
}

.legend .down {
  color: #26a69a;
}

.legend .vol {
  color: #a6adc8;
}
</style>
