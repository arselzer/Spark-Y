<template>
  <div class="batch-view">
    <div class="page-header">
      <div class="title-row">
        <div>
          <h2>Batch Run</h2>
          <p class="subtitle">
            Runs the queries you selected as one sweep and compares reference vs
            optimised at a glance. Click any row for the full breakdown.
          </p>
        </div>
        <div class="header-actions">
          <span v-if="viewingSavedName" class="viewing-badge">📁 {{ viewingSavedName }}</span>
          <button v-if="!viewingSavedName && queriesToRun.length" type="button" class="link-btn" @click="editSelection">← Edit selection</button>
          <router-link class="link-btn" :to="{ name: 'batch' }">Saved batches</router-link>
        </div>
      </div>
    </div>

    <div v-if="loading" class="empty-state">Loading…</div>
    <div v-else-if="loadError" class="empty-state error">{{ loadError }}</div>

    <div v-else-if="!queriesToRun.length" class="landing">
      <div class="empty-state">
        <p>No queries selected.</p>
        <p class="hint">
          Open the <router-link to="/queries">Queries</router-link> page, tick the queries you want,
          and press <strong>Run batch</strong>.
        </p>
      </div>

      <div v-if="savedBatches.length" class="saved-batches">
        <h3>Saved batches</h3>
        <ul>
          <li v-for="b in savedBatches" :key="b.id" class="saved-row">
            <button type="button" class="saved-open" @click="openSaved(b.id)">
              <span class="saved-name">{{ b.name }}</span>
              <span class="saved-meta">
                {{ b.query_count }} {{ b.query_count === 1 ? 'query' : 'queries' }}
                <template v-if="b.overall_speedup"> · {{ b.overall_speedup.toFixed(1) }}× overall</template>
                <template v-if="b.total_avoided_intermediate_rows"> · {{ formatNumber(b.total_avoided_intermediate_rows) }} avoided</template>
                <template v-if="b.timeout_count"> · {{ b.timeout_count }} ref timeout</template>
                <span class="saved-date">{{ b.saved_at.replace('T', ' ') }}</span>
              </span>
            </button>
            <button type="button" class="saved-del" title="Delete saved batch" @click="removeSaved(b.id)">✕</button>
          </li>
        </ul>
      </div>
    </div>

    <template v-else>
      <div class="run-bar">
        <button v-if="!running" class="btn btn-primary" @click="runBatch">
          {{ rows.length ? '↻ Re-run' : '▶ Run' }} {{ queriesToRun.length }} {{ queriesToRun.length === 1 ? 'query' : 'queries' }}
        </button>
        <button v-else class="btn btn-danger" @click="cancelBatch">■ Cancel</button>

        <div v-if="rows.length" class="progress-wrap">
          <div class="progress-track">
            <div class="progress-fill" :style="{ width: progressPct + '%' }"></div>
          </div>
          <div class="progress-text">
            <span>{{ processedCount }}/{{ rows.length }}</span>
            <span v-if="running && currentRow" class="current">running {{ currentRow.query.name }}…</span>
            <span class="elapsed">{{ formatDuration(elapsedMs) }}</span>
          </div>
        </div>

        <div v-if="completedRows.length && !running && !viewingSavedName" class="save-area">
          <button type="button" class="btn btn-secondary" :disabled="saving" @click="saveBatch">
            {{ saving ? 'Saving…' : '💾 Save batch' }}
          </button>
          <span v-if="savedMsg" class="saved-msg">{{ savedMsg }}</span>
        </div>
      </div>

      <!-- Aggregate summary -->
      <div v-if="completedRows.length" class="aggregates">
        <div class="agg-card">
          <div class="agg-label">Queries</div>
          <div class="agg-value">{{ doneCount }}<span v-if="failedCount" class="agg-sub"> · {{ failedCount }} failed</span></div>
        </div>
        <div class="agg-card">
          <div class="agg-label">Total reference</div>
          <div class="agg-value">{{ formatDuration(totalRefMs) }}</div>
        </div>
        <div class="agg-card">
          <div class="agg-label">Total optimised</div>
          <div class="agg-value good">{{ formatDuration(totalOptMs) }}</div>
        </div>
        <div class="agg-card highlight">
          <div class="agg-label">Overall speedup</div>
          <div class="agg-value">{{ overallSpeedup >= 1 ? overallSpeedup.toFixed(1) + '×' : '—' }}</div>
        </div>
        <div class="agg-card highlight">
          <div class="agg-label">Intermediate rows avoided</div>
          <div class="agg-value">{{ formatNumber(totalAvoided) }}</div>
        </div>
      </div>
      <p v-if="timeoutCount" class="totals-note">
        {{ timeoutCount }} reference run{{ timeoutCount === 1 ? '' : 's' }} hit the time budget —
        totals are lower bounds (reference would run longer).
      </p>

      <!-- Results table -->
      <div v-if="rows.length" class="table-wrap">
        <table class="results-table">
          <thead>
            <tr>
              <th class="col-status">Status</th>
              <th>Query</th>
              <th class="num sortable" @click="setSort('refMs')">Reference {{ sortArrow('refMs') }}</th>
              <th class="num sortable" @click="setSort('optMs')">Optimised {{ sortArrow('optMs') }}</th>
              <th class="num sortable" @click="setSort('speedup')">Speedup {{ sortArrow('speedup') }}</th>
              <th class="num sortable" @click="setSort('peakRows')">Peak intermediate {{ sortArrow('peakRows') }}</th>
              <th class="num sortable" @click="setSort('resultRows')">Result rows {{ sortArrow('resultRows') }}</th>
              <th class="num sortable" @click="setSort('avoided')">Avoided {{ sortArrow('avoided') }}</th>
            </tr>
          </thead>
          <tbody>
            <tr
              v-for="row in sortedRows"
              :key="row.query.query_id"
              :class="{ clickable: canExpand(row), selected: selectedRowKey === row.query.query_id }"
              @click="selectRow(row)"
            >
              <td class="col-status">
                <span :class="['status-chip', 'st-' + row.status]">
                  <span v-if="row.status === 'running'" class="mini-spinner"></span>
                  {{ statusLabel(row.status) }}
                </span>
              </td>
              <td class="q-cell">
                <span v-if="featuredRank.has(row.query.query_id)" class="star">★</span>
                {{ row.query.name }}
                <span v-if="canExpand(row)" class="expand-hint">{{ selectedRowKey === row.query.query_id ? '▾' : '▸' }}</span>
                <span v-if="row.error" class="row-error" :title="row.error">⚠ {{ row.error }}</span>
              </td>
              <td class="num">
                <span v-if="row.refTimedOut" class="timeout-val" title="Reference exceeded the time budget">≥{{ formatDuration(row.refMs) }}</span>
                <template v-else>{{ row.refMs != null ? formatDuration(row.refMs) : '—' }}</template>
              </td>
              <td class="num good">{{ row.optMs != null ? formatDuration(row.optMs) : '—' }}</td>
              <td class="num">
                <span v-if="row.speedup != null" class="speedup-badge" :class="{ big: (row.speedup || 0) >= 5 }">{{ row.speedup.toFixed(1) }}×</span>
                <template v-else>—</template>
              </td>
              <td class="num">{{ row.peakRows != null ? formatNumber(row.peakRows) : '—' }}</td>
              <td class="num">{{ row.resultRows != null ? formatNumber(row.resultRows) : '—' }}</td>
              <td class="num strong">{{ row.avoided != null ? formatNumber(row.avoided) : '—' }}</td>
            </tr>
          </tbody>
        </table>
      </div>

      <!-- Detail drill-down -->
      <div v-if="selectedRow && selectedRow.result" class="detail-panel">
        <div class="detail-head">
          <h3>
            <span v-if="featuredRank.has(selectedRow.query.query_id)" class="star">★</span>
            {{ selectedRow.query.name }}
          </h3>
          <button class="link-btn" @click="selectedRowKey = null">✕ Close</button>
        </div>

        <div v-if="currentHg" class="detail-chips">
          <span :class="['chip', currentHg.is_acyclic ? 'chip-ok' : 'chip-warn']">
            {{ currentHg.is_acyclic ? '✓ Acyclic' : '⚠ Cyclic' }}
          </span>
          <span class="chip">{{ guardednessLabel(currentHg.guardedness_type) }}</span>
        </div>

        <div class="detail-grid">
          <div class="sql-col">
            <div class="col-label">Query</div>
            <pre class="sql-block">{{ selectedRow.result.sql }}</pre>
          </div>
          <div class="hg-col">
            <div class="col-label">Hypergraph</div>
            <div class="hg-box">
              <HypergraphViewer v-if="currentViz" :key="selectedRowKey || 'hg'" :visualization-data="currentViz" />
              <div v-else-if="hgLoading" class="hg-msg">Building hypergraph…</div>
              <div v-else-if="hgError" class="hg-msg error">{{ hgError }}</div>
            </div>
          </div>
        </div>

        <PerformanceComparison
          v-if="selectedRow.result.optimized_metrics"
          :key="selectedRow.query.query_id"
          :reference-metrics="selectedRow.result.original_metrics"
          :optimized-metrics="selectedRow.result.optimized_metrics"
          :reference-plan="selectedRow.result.original_plan"
          :optimized-plan="selectedRow.result.optimized_plan"
        />
      </div>
    </template>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted, watch } from 'vue'
import { useRouter, useRoute } from 'vue-router'
import { queryApi, executionApi, hypergraphApi } from '@/services/api'
import type {
  QueryMetadata, ExecutionResult, Hypergraph, VisualizationData,
  SavedBatchItem, SavedBatchSummary,
} from '@/types'
import { FEATURED_QUERIES } from '@/types/benchmarks'
import { formatNumber, formatDuration } from '@/utils/format'
import { loadSparkConfigs } from '@/utils/sparkConfig'
import PerformanceComparison from '@/components/PerformanceComparison.vue'
import HypergraphViewer from '@/components/HypergraphViewer.vue'

type RowStatus = 'queued' | 'running' | 'done' | 'timeout' | 'failed' | 'cancelled'

interface BatchRow {
  query: QueryMetadata
  status: RowStatus
  result?: ExecutionResult
  error?: string
  refMs?: number
  optMs?: number
  speedup?: number
  peakRows?: number
  resultRows?: number
  avoided?: number
  refTimedOut?: boolean
}

type SortKey = 'order' | 'refMs' | 'optMs' | 'speedup' | 'peakRows' | 'resultRows' | 'avoided'

const router = useRouter()
const route = useRoute()

// Highlights demo picks with a ★ in the results table.
const featuredRank = new Map(FEATURED_QUERIES.map((q, i) => [q.queryId, i]))

const queriesToRun = ref<QueryMetadata[]>([])
const loading = ref(true)
const loadError = ref('')

const rows = ref<BatchRow[]>([])
const running = ref(false)
const currentIndex = ref(-1)
const elapsedMs = ref(0)
let cancelled = false
let abort: AbortController | null = null
let ticker: number | null = null

const selectedRowKey = ref<string | null>(null)
const sortKey = ref<SortKey>('order')
const sortDir = ref<'asc' | 'desc'>('desc')

// Saved-batch state: name when viewing a saved batch, the library list, and
// the save-action status.
const viewingSavedName = ref<string | null>(null)
const savedBatches = ref<SavedBatchSummary[]>([])
const saving = ref(false)
const savedMsg = ref('')

// Hypergraph for the expanded row, fetched lazily and cached per query.
const hgCache = ref<Record<string, { hg: Hypergraph; viz: VisualizationData }>>({})
const hgLoading = ref(false)
const hgError = ref('')

// Resolve the ?ids=… selection against the full catalogue (we need each
// query's SQL), preserving the order they were selected in, then auto-run —
// the user already pressed "Run batch" to get here.
// The view has three modes driven by the URL: run (?ids=…), view a saved batch
// (?batch=…), or landing (neither → show the saved-batch library). Re-run on
// query change so the in-app "Saved batches" / "Edit selection" links work even
// though Vue reuses this component instance across same-route navigations.
async function initFromRoute() {
  resetState()
  const batchId = route.query.batch
  if (batchId) {
    await openSavedBatchData(String(batchId))
    return
  }
  const ids = route.query.ids
  const idList = ids ? String(ids).split(',').filter(Boolean) : []
  if (!idList.length) {
    loading.value = false
    await loadSavedBatches()
    return
  }
  loading.value = true
  try {
    const all = await queryApi.listQueries({ limit: 1000 })
    const byId = new Map(all.map((q) => [q.query_id, q]))
    queriesToRun.value = idList.map((id) => byId.get(id)).filter(Boolean) as QueryMetadata[]
  } catch (e: any) {
    loadError.value = e?.message || 'Failed to load queries'
  } finally {
    loading.value = false
  }
  if (queriesToRun.value.length) runBatch()
}

function resetState() {
  if (abort) abort.abort()
  if (ticker) {
    clearInterval(ticker)
    ticker = null
  }
  rows.value = []
  queriesToRun.value = []
  selectedRowKey.value = null
  viewingSavedName.value = null
  loadError.value = ''
  savedMsg.value = ''
  running.value = false
  currentIndex.value = -1
}

onMounted(initFromRoute)
watch(() => route.query, initFromRoute)

onUnmounted(() => {
  if (abort) abort.abort()
  if (ticker) clearInterval(ticker)
})

function editSelection() {
  const ids = queriesToRun.value.map((q) => q.query_id)
  router.push({ name: 'queries', query: ids.length ? { ids: ids.join(',') } : {} })
}

// ── Saved batches ──────────────────────────────────────────────────────────
async function loadSavedBatches() {
  try {
    savedBatches.value = await executionApi.listSavedBatches()
  } catch {
    /* non-fatal */
  }
}

function rowFromSaved(item: SavedBatchItem): BatchRow {
  const r = item.result
  const row: BatchRow = {
    query: {
      query_id: item.query_id || item.name,
      name: item.name,
      category: 'custom',
      sql: r.sql,
      tables: [],
      num_joins: 0,
      num_aggregates: 0,
    } as QueryMetadata,
    status: 'queued',
  }
  applyResult(row, r)
  return row
}

async function openSavedBatchData(id: string) {
  loading.value = true
  try {
    const batch = await executionApi.getSavedBatch(id)
    rows.value = batch.items.map(rowFromSaved)
    queriesToRun.value = rows.value.map((r) => r.query)
    viewingSavedName.value = batch.name
  } catch (e: any) {
    loadError.value = e?.message || 'Failed to load saved batch'
  } finally {
    loading.value = false
  }
}

async function saveBatch() {
  const items: SavedBatchItem[] = rows.value
    .filter((r) => r.result)
    .map((r) => ({ query_id: r.query.query_id, name: r.query.name, result: r.result as ExecutionResult }))
  if (!items.length) return
  saving.value = true
  savedMsg.value = ''
  try {
    const s = await executionApi.saveBatch(items)
    savedMsg.value = `Saved “${s.name}”`
  } catch (e: any) {
    savedMsg.value = `Save failed: ${e?.response?.data?.detail || e?.message || 'error'}`
  } finally {
    saving.value = false
  }
}

function openSaved(id: string) {
  router.push({ name: 'batch', query: { batch: id } })
}
async function removeSaved(id: string) {
  try {
    await executionApi.deleteSavedBatch(id)
    await loadSavedBatches()
  } catch {
    /* non-fatal */
  }
}

// ── Execution ──────────────────────────────────────────────────────────
async function runBatch() {
  if (!queriesToRun.value.length || running.value) return

  rows.value = queriesToRun.value.map((q) => ({ query: q, status: 'queued' as RowStatus }))
  selectedRowKey.value = null
  sortKey.value = 'order'
  running.value = true
  cancelled = false
  abort = new AbortController()
  const { referenceConfig, optimizedConfig } = loadSparkConfigs()

  const startedAt = performance.now()
  elapsedMs.value = 0
  ticker = window.setInterval(() => {
    elapsedMs.value = performance.now() - startedAt
  }, 250)

  for (let i = 0; i < rows.value.length; i++) {
    if (cancelled) {
      rows.value[i].status = 'cancelled'
      continue
    }
    currentIndex.value = i
    rows.value[i].status = 'running'
    const q = rows.value[i].query
    try {
      const res = await executionApi.executeQuery(
        {
          query_id: q.query_id,
          sql: q.sql,
          use_optimization: true,
          collect_metrics: true,
          reference_config: referenceConfig,
          optimized_config: optimizedConfig,
        },
        abort.signal,
      )
      applyResult(rows.value[i], res)
    } catch (e: any) {
      if (abort?.signal.aborted) {
        rows.value[i].status = 'cancelled'
        cancelled = true
      } else {
        rows.value[i].status = 'failed'
        rows.value[i].error = e?.response?.data?.detail || e?.message || 'Execution error'
      }
    }
  }
  finishBatch()
}

function applyResult(row: BatchRow, res: ExecutionResult) {
  row.result = res
  if (!res.success) {
    row.status = 'failed'
    row.error = res.error || 'Query failed'
    return
  }
  const om = res.original_metrics
  const pm = res.optimized_metrics
  row.refMs = om?.execution_time_ms
  row.optMs = pm?.execution_time_ms
  row.speedup = res.speedup
  row.peakRows = om?.peak_intermediate_rows
  row.resultRows = pm?.total_output_rows ?? om?.total_output_rows
  row.avoided = res.avoided_intermediate_rows
  row.refTimedOut = om?.timed_out
  row.status = om?.timed_out ? 'timeout' : 'done'
}

function finishBatch() {
  running.value = false
  currentIndex.value = -1
  if (ticker) {
    clearInterval(ticker)
    ticker = null
  }
}

function cancelBatch() {
  cancelled = true
  if (abort) abort.abort()
}

const currentRow = computed(() => (currentIndex.value >= 0 ? rows.value[currentIndex.value] : null))
const processedCount = computed(
  () => rows.value.filter((r) => r.status !== 'queued' && r.status !== 'running').length,
)
const progressPct = computed(() =>
  rows.value.length ? Math.round((processedCount.value / rows.value.length) * 100) : 0,
)

// ── Aggregates ─────────────────────────────────────────────────────────
const completedRows = computed(() => rows.value.filter((r) => r.result && r.result.success))
const totalRefMs = computed(() => completedRows.value.reduce((s, r) => s + (r.refMs || 0), 0))
const totalOptMs = computed(() => completedRows.value.reduce((s, r) => s + (r.optMs || 0), 0))
const overallSpeedup = computed(() => (totalOptMs.value > 0 ? totalRefMs.value / totalOptMs.value : 0))
const totalAvoided = computed(() => completedRows.value.reduce((s, r) => s + (r.avoided || 0), 0))
const doneCount = computed(() => rows.value.filter((r) => r.status === 'done' || r.status === 'timeout').length)
const timeoutCount = computed(() => rows.value.filter((r) => r.status === 'timeout').length)
const failedCount = computed(() => rows.value.filter((r) => r.status === 'failed').length)

// ── Table sort + detail ────────────────────────────────────────────────
const sortedRows = computed(() => {
  if (sortKey.value === 'order') return rows.value
  const dir = sortDir.value === 'asc' ? 1 : -1
  const val = (r: BatchRow): number => {
    switch (sortKey.value) {
      case 'refMs':
        return r.refMs || 0
      case 'optMs':
        return r.optMs || 0
      case 'speedup':
        return r.speedup || 0
      case 'peakRows':
        return r.peakRows || 0
      case 'resultRows':
        return r.resultRows || 0
      case 'avoided':
        return r.avoided || 0
      default:
        return 0
    }
  }
  return [...rows.value].sort((a, b) => (val(a) - val(b)) * dir)
})

function setSort(key: SortKey) {
  if (sortKey.value === key) {
    sortDir.value = sortDir.value === 'asc' ? 'desc' : 'asc'
  } else {
    sortKey.value = key
    sortDir.value = 'desc'
  }
}
function sortArrow(key: SortKey) {
  if (sortKey.value !== key) return ''
  return sortDir.value === 'asc' ? '▲' : '▼'
}

function canExpand(row: BatchRow) {
  return !!(row.result && row.result.success && row.result.optimized_metrics)
}
async function selectRow(row: BatchRow) {
  if (!canExpand(row)) return
  const key = row.query.query_id
  selectedRowKey.value = selectedRowKey.value === key ? null : key
  if (selectedRowKey.value && !hgCache.value[key]) {
    hgError.value = ''
    hgLoading.value = true
    try {
      const hg = await hypergraphApi.extractHypergraph(row.result!.sql, key, true)
      const viz = await hypergraphApi.generateVisualization(hg)
      hgCache.value = { ...hgCache.value, [key]: { hg, viz } }
    } catch (e: any) {
      hgError.value = e?.response?.data?.detail || e?.message || 'Failed to build hypergraph'
    } finally {
      hgLoading.value = false
    }
  }
}
const selectedRow = computed(
  () => rows.value.find((r) => r.query.query_id === selectedRowKey.value) || null,
)
const currentHg = computed(() =>
  selectedRowKey.value ? hgCache.value[selectedRowKey.value]?.hg ?? null : null,
)
const currentViz = computed(() =>
  selectedRowKey.value ? hgCache.value[selectedRowKey.value]?.viz ?? null : null,
)

function guardednessLabel(t?: string): string {
  const map: Record<string, string> = {
    guarded: 'Guarded',
    piecewise_guarded: 'Piecewise-guarded',
    unguarded: 'Unguarded',
  }
  return map[t || ''] || 'Guardedness unknown'
}

function statusLabel(s: RowStatus): string {
  return {
    queued: 'Queued',
    running: 'Running',
    done: 'Done',
    timeout: 'Ref timeout',
    failed: 'Failed',
    cancelled: 'Cancelled',
  }[s]
}
</script>

<style scoped>
.batch-view {
  max-width: min(1800px, 96vw);
  margin: 0 auto;
  padding: 0 1rem;
}

.title-row {
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  gap: 1rem;
}
.page-header h2 {
  margin: 0;
  font-size: 1.6rem;
  font-weight: 700;
}
.page-header .subtitle {
  margin: 0.35rem 0 1.25rem;
  color: var(--color-text-secondary);
  max-width: 70ch;
}
.header-actions {
  flex-shrink: 0;
}
.link-btn {
  background: none;
  border: none;
  color: var(--color-primary);
  font-size: 0.875rem;
  font-weight: 600;
  cursor: pointer;
  padding: 0.15rem 0.25rem;
}
.link-btn:hover {
  text-decoration: underline;
}

.run-bar {
  display: flex;
  align-items: center;
  gap: 1rem;
  margin-bottom: 1rem;
}
.btn {
  border: none;
  border-radius: 0.4rem;
  padding: 0.6rem 1.1rem;
  font-weight: 600;
  cursor: pointer;
  font-size: 0.95rem;
}
.btn-primary {
  background: var(--color-primary);
  color: #fff;
}
.btn-danger {
  background: var(--color-danger);
  color: #fff;
}
.btn-secondary {
  background: var(--color-background-mute);
  color: var(--color-text);
  border: 1px solid var(--color-border);
}
.btn-secondary:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}
.progress-wrap {
  flex: 1;
}
.save-area {
  display: flex;
  align-items: center;
  gap: 0.6rem;
  flex-shrink: 0;
}
.saved-msg {
  font-size: 0.85rem;
  color: var(--color-secondary, #10b981);
  font-weight: 600;
}
.viewing-badge {
  font-size: 0.85rem;
  font-weight: 600;
  color: var(--color-text-secondary);
  margin-right: 0.75rem;
}

/* Saved-batch library (landing) */
.saved-batches {
  margin-top: 1.5rem;
}
.saved-batches h3 {
  font-size: 1.1rem;
  margin: 0 0 0.5rem;
}
.saved-batches ul {
  list-style: none;
  margin: 0;
  padding: 0;
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
}
.saved-row {
  display: flex;
  align-items: stretch;
  gap: 0.5rem;
}
.saved-open {
  flex: 1;
  text-align: left;
  background: var(--color-surface);
  border: 1px solid var(--color-border);
  border-radius: 0.5rem;
  padding: 0.75rem 1rem;
  cursor: pointer;
  display: flex;
  flex-direction: column;
  gap: 0.2rem;
}
.saved-open:hover {
  border-color: var(--color-primary);
}
.saved-name {
  font-weight: 600;
}
.saved-meta {
  font-size: 0.8rem;
  color: var(--color-text-secondary);
}
.saved-date {
  margin-left: 0.5rem;
  opacity: 0.8;
}
.saved-del {
  background: none;
  border: 1px solid var(--color-border);
  border-radius: 0.5rem;
  color: var(--color-text-secondary);
  cursor: pointer;
  padding: 0 0.75rem;
}
.saved-del:hover {
  color: var(--color-danger);
  border-color: var(--color-danger);
}
.progress-track {
  height: 8px;
  border-radius: 4px;
  background: var(--color-background-mute);
  overflow: hidden;
}
.progress-fill {
  height: 100%;
  background: var(--color-primary);
  transition: width 0.3s ease;
}
.progress-text {
  display: flex;
  gap: 0.75rem;
  margin-top: 0.3rem;
  font-size: 0.8rem;
  color: var(--color-text-secondary);
}
.progress-text .current {
  color: var(--color-primary);
  font-weight: 600;
}
.progress-text .elapsed {
  margin-left: auto;
  font-variant-numeric: tabular-nums;
}

/* Aggregates */
.aggregates {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(150px, 1fr));
  gap: 0.75rem;
  margin-bottom: 0.75rem;
}
.agg-card {
  background: var(--color-surface);
  border: 1px solid var(--color-border);
  border-radius: 0.5rem;
  padding: 0.75rem 1rem;
}
.agg-card.highlight {
  border-color: var(--color-primary);
}
.agg-label {
  font-size: 0.75rem;
  color: var(--color-text-secondary);
  text-transform: uppercase;
  letter-spacing: 0.03em;
}
.agg-value {
  font-size: 1.4rem;
  font-weight: 700;
  margin-top: 0.2rem;
  font-variant-numeric: tabular-nums;
}
.agg-value.good {
  color: var(--color-secondary, #10b981);
}
.agg-sub {
  font-size: 0.8rem;
  font-weight: 500;
  color: var(--color-danger);
}
.totals-note {
  font-size: 0.8rem;
  color: var(--color-text-secondary);
  margin: 0 0 1rem;
}

/* Table */
.table-wrap {
  overflow-x: auto;
  background: var(--color-surface);
  border: 1px solid var(--color-border);
  border-radius: 0.5rem;
}
.results-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 0.88rem;
}
.results-table th,
.results-table td {
  padding: 0.55rem 0.75rem;
  text-align: left;
  border-bottom: 1px solid var(--color-border);
  white-space: nowrap;
}
.results-table th {
  font-size: 0.75rem;
  text-transform: uppercase;
  letter-spacing: 0.03em;
  color: var(--color-text-secondary);
  background: var(--color-background-soft);
}
.results-table th.num,
.results-table td.num {
  text-align: right;
  font-variant-numeric: tabular-nums;
}
.results-table th.sortable {
  cursor: pointer;
  user-select: none;
}
.results-table th.sortable:hover {
  color: var(--color-text);
}
.results-table tbody tr.clickable {
  cursor: pointer;
}
.results-table tbody tr.clickable:hover {
  background: var(--color-background-soft);
}
.results-table tbody tr.selected {
  background: var(--color-background-mute);
}
.q-cell .expand-hint {
  color: var(--color-text-secondary);
  margin-left: 0.35rem;
}
.row-error {
  color: var(--color-danger);
  font-size: 0.78rem;
  margin-left: 0.5rem;
}
.num.good {
  color: var(--color-secondary, #10b981);
  font-weight: 600;
}
.num.strong {
  font-weight: 700;
}
.timeout-val {
  color: #d97706;
  font-weight: 600;
}
.speedup-badge {
  font-weight: 600;
}
.speedup-badge.big {
  background: rgba(16, 185, 129, 0.15);
  color: var(--color-secondary, #10b981);
  padding: 0.1rem 0.4rem;
  border-radius: 0.3rem;
}

/* Status chips */
.status-chip {
  display: inline-flex;
  align-items: center;
  gap: 0.35rem;
  padding: 0.15rem 0.5rem;
  border-radius: 999px;
  font-size: 0.75rem;
  font-weight: 600;
}
.st-queued {
  background: var(--color-background-mute);
  color: var(--color-text-secondary);
}
.st-running {
  background: rgba(59, 130, 246, 0.15);
  color: #3b82f6;
}
.st-done {
  background: rgba(16, 185, 129, 0.15);
  color: #10b981;
}
.st-timeout {
  background: rgba(217, 119, 6, 0.15);
  color: #d97706;
}
.st-failed {
  background: rgba(239, 68, 68, 0.15);
  color: #ef4444;
}
.st-cancelled {
  background: var(--color-background-mute);
  color: var(--color-text-secondary);
}
.mini-spinner {
  width: 10px;
  height: 10px;
  border: 2px solid currentColor;
  border-right-color: transparent;
  border-radius: 50%;
  animation: spin 0.7s linear infinite;
}
@keyframes spin {
  to {
    transform: rotate(360deg);
  }
}

/* Empty / loading state */
.empty-state {
  background: var(--color-surface);
  border: 1px dashed var(--color-border);
  border-radius: 0.5rem;
  padding: 3rem 2rem;
  text-align: center;
  color: var(--color-text-secondary);
}
.empty-state.error {
  color: var(--color-danger);
  border-color: var(--color-danger);
}
.empty-state .hint {
  font-size: 0.9rem;
}

/* Detail */
.detail-panel {
  margin-top: 1.5rem;
  background: var(--color-surface);
  border: 1px solid var(--color-border);
  border-radius: 0.5rem;
  padding: 1rem 1.25rem;
}
.detail-head {
  display: flex;
  align-items: center;
  justify-content: space-between;
}
.detail-head h3 {
  margin: 0;
  font-size: 1.2rem;
}
.detail-chips {
  display: flex;
  gap: 0.5rem;
  margin: 0.75rem 0;
}
.chip {
  font-size: 0.75rem;
  font-weight: 600;
  padding: 0.15rem 0.6rem;
  border-radius: 999px;
  background: var(--color-background-mute);
  color: var(--color-text-secondary);
}
.chip-ok {
  background: rgba(16, 185, 129, 0.15);
  color: #10b981;
}
.chip-warn {
  background: rgba(217, 119, 6, 0.15);
  color: #d97706;
}

.detail-grid {
  display: grid;
  grid-template-columns: minmax(0, 360px) 1fr;
  gap: 1rem;
  align-items: start;
  margin-bottom: 1rem;
}
.col-label {
  font-size: 0.75rem;
  text-transform: uppercase;
  letter-spacing: 0.03em;
  color: var(--color-text-secondary);
  margin-bottom: 0.35rem;
}
.sql-block {
  background: var(--color-background-soft);
  border: 1px solid var(--color-border);
  border-radius: 0.4rem;
  padding: 0.75rem 1rem;
  font-size: 0.82rem;
  overflow: auto;
  white-space: pre-wrap;
  margin: 0;
  max-height: clamp(440px, 72vh, 880px);
}
.hg-box {
  min-height: 200px;
}
.hg-msg {
  padding: 2rem;
  text-align: center;
  color: var(--color-text-secondary);
  border: 1px dashed var(--color-border);
  border-radius: 0.5rem;
}
.hg-msg.error {
  color: var(--color-danger);
}
@media (max-width: 1000px) {
  .detail-grid {
    grid-template-columns: 1fr;
  }
}
.star {
  color: #f59e0b;
}
</style>
