import fs from 'fs'
const p = 'src/components/KlineChart.vue'
let t = fs.readFileSync(p, 'utf8')

const oldM = `onMounted(async () => {
  await nextTick()
  layoutChartArea(false)
  await nextTick()
  initCharts()
  setupChartAreaResizeObserver()
  if (props.candles.length > 0) setAllData()
})`
const newM = `onMounted(async () => {
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
})`
if (!t.includes(oldM)) throw new Error('onMounted not found')
t = t.replace(oldM, newM)

const oldU = `onUnmounted(() => {
  onSplitterMouseUp()
  chartAreaResizeObserver?.disconnect()
  chartAreaResizeObserver = null
  resizeObserver?.disconnect()
  mainChart?.remove()
  macdChart?.remove()
  rsiChart?.remove()
  kdjChart?.remove()
  mainChart = macdChart = rsiChart = kdjChart = null
})`
const newU = `onUnmounted(() => {
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
})`
if (!t.includes(oldU)) throw new Error('onUnmounted not found')
t = t.replace(oldU, newU)

t = t.replace(
  'let prevFirstTime: number = 0\n</script>',
  'let prevFirstTime: number = 0\n\nwatch(paneHeights, () => { schedulePersistPaneHeights() }, { deep: true })\n</script>',
)

const oldTpl = `  <div ref="klineRootRef" class="kline-root">
    <div ref="chartScrollRef" class="chart-scroll">`
const newTpl = `  <div ref="klineRootRef" class="kline-root">
    <button
      type="button"
      class="btn-restore-pane-layout"
      title="主图与 MACD/RSI/KDJ 恢复为默认比例铺满当前图表区，并写入本地保存"
      @click="restoreFullscreenLayout"
    >
      恢复全屏布局
    </button>
    <div ref="chartScrollRef" class="chart-scroll">`
if (!t.includes(oldTpl)) throw new Error('template not found')
t = t.replace(oldTpl, newTpl)

const oldCss = `.kline-root {
  display: flex;
  flex-direction: column;
  width: 100%;
  height: 100%;
  min-height: 0;
  position: relative;
}

.chart-scroll {`
const newCss = `.kline-root {
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

.chart-scroll {`
if (!t.includes(oldCss)) throw new Error('css not found')
t = t.replace(oldCss, newCss)

fs.writeFileSync(p, t)
console.log('OK')
