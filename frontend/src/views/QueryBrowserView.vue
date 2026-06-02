<template>
  <div class="container">
    <div class="query-browser">
      <h2>Query Browser</h2>
      <p class="description">Browse and search through hundreds of benchmark queries</p>

      <div class="search-controls">
        <div class="search-input-wrap">
          <input
            v-model="searchTerm"
            type="text"
            placeholder="Search by name, ID, or SQL fragment…"
            class="input search-input"
            @input="debounceSearch"
          />
          <button
            v-if="searchTerm"
            class="clear-search"
            type="button"
            aria-label="Clear search"
            @click="clearSearch"
          >×</button>
        </div>

        <select v-model="selectedCategory" class="input category-select" @change="loadQueries">
          <option value="">All Categories ({{ totalAcrossCategories }})</option>
          <option v-for="(cat, key) in categories" :key="key" :value="key">
            {{ benchmarkLabel(String(key)) }} ({{ cat.count }})
          </option>
        </select>
      </div>

      <div class="selection-toolbar">
        <span class="sel-hint">Tick queries to run them together as a batch.</span>
        <button type="button" class="link-btn" @click="selectDemoPicks">★ Select demo picks</button>
        <button v-if="selectedCount" type="button" class="link-btn" @click="clearSelection">Clear selection</button>
      </div>

      <div v-if="loading" class="loading-section">
        <div class="spinner"></div>
        <p>Loading queries...</p>
      </div>

      <div v-else-if="error" class="error-message">
        {{ error }}
      </div>

      <div v-else class="queries-groups">
        <div v-if="queries.length === 0" class="no-results">
          No queries found matching your criteria
        </div>

        <section v-for="group in groups" :key="group.key" class="query-group">
          <header class="group-header" @click="toggleGroup(group.key)">
            <span class="group-toggle">{{ isExpanded(group.key) ? '▼' : '▶' }}</span>
            <span :class="['group-title', { featured: group.key === 'featured' }]">{{ group.label }}</span>
            <label class="group-select" @click.stop :title="`Select all ${group.queries.length} queries`">
              <input
                type="checkbox"
                :checked="groupAllSelected(group)"
                @change="toggleGroupSelect(group)"
              />
              <span>all</span>
            </label>
            <span class="group-count">{{ group.queries.length }}</span>
          </header>

          <div v-show="isExpanded(group.key)" class="group-cards">
            <div
              v-for="query in group.queries"
              :key="`${group.key}-${query.query_id}`"
              class="query-card"
              :class="{
                'query-card-featured': FEATURED_QUERY_IDS.has(query.query_id),
                'query-card-selected': selected.has(query.query_id)
              }"
              @click="viewQuery(query.query_id)"
            >
              <div class="query-header">
                <div class="header-left">
                  <input
                    type="checkbox"
                    class="card-select"
                    :checked="selected.has(query.query_id)"
                    :aria-label="`Select ${query.name} for batch`"
                    @click.stop
                    @change="toggleSelect(query.query_id)"
                  />
                  <h3>
                    <span v-if="FEATURED_QUERY_IDS.has(query.query_id)" class="featured-star" title="Demo pick — large measured speedup">★</span>
                    {{ query.name }}
                  </h3>
                </div>
                <span :class="['badge', benchmarkInfo(query.category).badgeClass]">
                  {{ benchmarkLabel(query.category) }}
                </span>
              </div>

              <div class="query-meta">
                <span class="meta-item"><strong>{{ query.num_joins }}</strong> joins</span>
                <span class="meta-item"><strong>{{ query.num_aggregates }}</strong> aggregates</span>
                <span class="meta-item"><strong>{{ query.tables.length }}</strong> tables</span>
              </div>

              <div class="query-preview">
                <code>{{ truncateSQL(query.sql) }}</code>
              </div>

              <div class="query-actions">
                <button @click.stop="executeQuery(query)" class="btn btn-primary btn-sm">Execute</button>
                <button @click.stop="viewQuery(query.query_id)" class="btn btn-secondary btn-sm">View Details</button>
              </div>
            </div>
          </div>
        </section>
      </div>

      <transition name="slideup">
        <div v-if="selectedCount" class="batch-bar">
          <span class="batch-bar-count">{{ selectedCount }} {{ selectedCount === 1 ? 'query' : 'queries' }} selected</span>
          <button type="button" class="btn btn-primary" @click="runBatch">▶ Run batch</button>
          <button type="button" class="batch-bar-clear" @click="clearSelection">Clear</button>
        </div>
      </transition>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { useRouter, useRoute } from 'vue-router'
import { queryApi } from '@/services/api'
import type { QueryMetadata } from '@/types'
import { benchmarkLabel, benchmarkInfo, FEATURED_QUERY_IDS, FEATURED_QUERIES } from '@/types/benchmarks'
import type { QueryCategory } from '@/types'

const router = useRouter()
const route = useRoute()

// Queries ticked for a batch run. Persisted into the URL when launching a
// batch so "Edit selection" from the Batch page restores the ticks here.
const selected = ref<Set<string>>(new Set())
const selectedCount = computed(() => selected.value.size)

const queries = ref<QueryMetadata[]>([])
const categories = ref<Record<string, { count: number; name: string }>>({})
const searchTerm = ref('')
const selectedCategory = ref('')
const loading = ref(false)
const error = ref('')
// 1000 = backend hard cap; total catalogue across JOB/TPC-H/STATS-CEB/SNAP
// is well under that, so a single fetch loads everything for the demo.
const limit = ref(1000)
const offset = ref(0)

let searchTimeout: number | null = null

const totalAcrossCategories = computed(() =>
  Object.values(categories.value).reduce((acc, c) => acc + (c?.count ?? 0), 0)
)

// Rank of each featured query (0 = biggest speedup), used to order the
// ★ demo picks and to float featured queries to the top of their benchmark.
const featuredRank = new Map(FEATURED_QUERIES.map((q, i) => [q.queryId, i]))

// Fixed benchmark display order for the grouped sections.
const CATEGORY_ORDER: QueryCategory[] = ['job', 'tpch', 'stats-ceb', 'snap', 'tpcds', 'custom']

// Which group sections are expanded. Demo Picks open by default; benchmark
// sections start collapsed so the page is short and scannable.
const expandedGroups = ref<Set<string>>(new Set(['featured']))

function toggleGroup(key: string) {
  const next = new Set(expandedGroups.value)
  next.has(key) ? next.delete(key) : next.add(key)
  expandedGroups.value = next
}

// Force everything open while searching or when a single category is
// selected (so matches aren't hidden behind a collapsed header).
function isExpanded(key: string): boolean {
  if (searchTerm.value || selectedCategory.value) return true
  return expandedGroups.value.has(key)
}

function featuredFirst(list: QueryMetadata[]): QueryMetadata[] {
  return [...list].sort((a, b) => {
    const ra = featuredRank.has(a.query_id) ? featuredRank.get(a.query_id)! : Infinity
    const rb = featuredRank.has(b.query_id) ? featuredRank.get(b.query_id)! : Infinity
    return ra - rb
  })
}

interface QueryGroup { key: string; label: string; queries: QueryMetadata[] }

// Build the grouped view: a "Demo Picks" section (featured queries, by
// speedup) followed by one collapsible section per benchmark present.
const groups = computed<QueryGroup[]>(() => {
  const base = queries.value
  const out: QueryGroup[] = []

  // Demo Picks — only when not filtered to a single category.
  if (!selectedCategory.value) {
    const featured = featuredFirst(base.filter(q => FEATURED_QUERY_IDS.has(q.query_id)))
    if (featured.length) out.push({ key: 'featured', label: '★ Demo Picks', queries: featured })
  }

  for (const cat of CATEGORY_ORDER) {
    const inCat = featuredFirst(base.filter(q => q.category === cat))
    if (inCat.length) {
      out.push({ key: cat, label: `${benchmarkInfo(cat).fullName} (${benchmarkLabel(cat)})`, queries: inCat })
    }
  }
  return out
})

function clearSearch() {
  searchTerm.value = ''
  offset.value = 0
  loadQueries()
}

onMounted(async () => {
  const ids = route.query.ids
  if (ids) selected.value = new Set(String(ids).split(',').filter(Boolean))
  await Promise.all([loadCategories(), loadQueries()])
})

async function loadCategories() {
  try {
    categories.value = await queryApi.getCategories()
  } catch (err: any) {
    console.error('Failed to load categories:', err)
  }
}

async function loadQueries() {
  loading.value = true
  error.value = ''

  try {
    const result = await queryApi.listQueries({
      category: selectedCategory.value || undefined,
      search: searchTerm.value || undefined,
      limit: limit.value,
      offset: offset.value
    })
    queries.value = result
  } catch (err: any) {
    error.value = err.response?.data?.detail || err.message || 'Failed to load queries'
  } finally {
    loading.value = false
  }
}

function debounceSearch() {
  if (searchTimeout) clearTimeout(searchTimeout)
  searchTimeout = setTimeout(() => {
    offset.value = 0
    loadQueries()
  }, 300) as unknown as number
}

function loadMore() {
  offset.value += limit.value
  loadQueries()
}

function truncateSQL(sql: string, maxLength: number = 150): string {
  const cleaned = sql.replace(/\s+/g, ' ').trim()
  if (cleaned.length <= maxLength) return cleaned
  return cleaned.substring(0, maxLength) + '...'
}

function viewQuery(queryId: string) {
  router.push(`/query/${queryId}`)
}

function executeQuery(query: QueryMetadata) {
  router.push({
    name: 'execute',
    state: { sql: query.sql, queryId: query.query_id }
  })
}

// ── Batch selection ──────────────────────────────────────────────────────
function toggleSelect(id: string) {
  const next = new Set(selected.value)
  next.has(id) ? next.delete(id) : next.add(id)
  selected.value = next
}
function groupAllSelected(group: QueryGroup): boolean {
  return group.queries.length > 0 && group.queries.every(q => selected.value.has(q.query_id))
}
function toggleGroupSelect(group: QueryGroup) {
  const all = groupAllSelected(group)
  const next = new Set(selected.value)
  group.queries.forEach(q => (all ? next.delete(q.query_id) : next.add(q.query_id)))
  selected.value = next
}
function selectDemoPicks() {
  selected.value = new Set(FEATURED_QUERY_IDS)
}
function clearSelection() {
  selected.value = new Set()
}
function runBatch() {
  if (!selected.value.size) return
  router.push({ name: 'batch', query: { ids: [...selected.value].join(',') } })
}
</script>

<style scoped>
.query-browser {
  max-width: 1200px;
  margin: 0 auto;
}

.query-browser h2 {
  font-size: 2rem;
  margin-bottom: 0.5rem;
}

.description {
  color: var(--color-text-secondary);
  margin-bottom: 2rem;
}

.search-controls {
  display: grid;
  grid-template-columns: 1fr auto;
  gap: 1rem;
  margin-bottom: 2rem;
}

.search-input-wrap {
  position: relative;
}

.search-input {
  font-size: 1rem;
  width: 100%;
  padding-right: 2.25rem;
}

.clear-search {
  position: absolute;
  right: 0.5rem;
  top: 50%;
  transform: translateY(-50%);
  background: transparent;
  border: none;
  color: var(--color-text-secondary);
  cursor: pointer;
  font-size: 1.5rem;
  line-height: 1;
  padding: 0 0.25rem;
}

.clear-search:hover {
  color: var(--color-text);
}

.category-select {
  min-width: 200px;
}

.selection-toolbar {
  display: flex;
  align-items: center;
  gap: 1rem;
  margin: -1rem 0 1.5rem;
  font-size: 0.875rem;
}
.sel-hint {
  color: var(--color-text-secondary);
}
.link-btn {
  background: none;
  border: none;
  color: var(--color-primary);
  font-weight: 600;
  cursor: pointer;
  padding: 0.15rem 0.25rem;
  font-size: 0.875rem;
}
.link-btn:hover {
  text-decoration: underline;
}

.header-left {
  display: flex;
  align-items: center;
  gap: 0.6rem;
  min-width: 0;
}
.card-select {
  width: 1.05rem;
  height: 1.05rem;
  flex-shrink: 0;
  cursor: pointer;
}
.query-card-selected {
  border-left-color: var(--color-primary);
  box-shadow: 0 0 0 2px var(--color-primary) inset;
}

.group-select {
  display: flex;
  align-items: center;
  gap: 0.3rem;
  font-size: 0.78rem;
  color: var(--color-text-secondary);
  cursor: pointer;
  margin-left: auto;
}
.group-select + .group-count {
  margin-left: 0.75rem;
}

/* Sticky batch action bar */
.batch-bar {
  position: fixed;
  bottom: 1.25rem;
  left: 50%;
  transform: translateX(-50%);
  z-index: 50;
  display: flex;
  align-items: center;
  gap: 1rem;
  padding: 0.75rem 1.25rem;
  background: var(--color-surface);
  border: 1px solid var(--color-border);
  border-radius: 0.6rem;
  box-shadow: 0 8px 24px rgba(0, 0, 0, 0.18);
}
.batch-bar-count {
  font-weight: 600;
}
.batch-bar-clear {
  background: none;
  border: none;
  color: var(--color-text-secondary);
  cursor: pointer;
  font-size: 0.875rem;
}
.batch-bar-clear:hover {
  color: var(--color-text);
  text-decoration: underline;
}
.slideup-enter-active,
.slideup-leave-active {
  transition: transform 0.2s ease, opacity 0.2s ease;
}
.slideup-enter-from,
.slideup-leave-to {
  transform: translateX(-50%) translateY(1rem);
  opacity: 0;
}

.queries-groups {
  display: flex;
  flex-direction: column;
  gap: 1rem;
}

.query-group {
  border: 1px solid var(--color-border);
  border-radius: 0.5rem;
  overflow: hidden;
  background: var(--color-surface);
}

.group-header {
  display: flex;
  align-items: center;
  gap: 0.75rem;
  padding: 0.85rem 1rem;
  cursor: pointer;
  user-select: none;
  background: var(--color-background-soft);
}

.group-header:hover {
  background: var(--color-background-mute, var(--color-background-soft));
}

.group-toggle {
  color: var(--color-text-secondary);
  font-size: 0.8rem;
  width: 1rem;
}

.group-title {
  font-weight: 600;
  font-size: 1.05rem;
}

.group-title.featured {
  color: var(--color-warning, #c2410c);
}

.group-count {
  margin-left: auto;
  font-size: 0.85rem;
  color: var(--color-text-secondary);
  background: var(--color-surface);
  border: 1px solid var(--color-border);
  border-radius: 9999px;
  padding: 0.1rem 0.6rem;
}

.group-cards {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(340px, 1fr));
  gap: 1rem;
  padding: 1rem;
}

.query-card {
  background: var(--color-surface);
  color: var(--color-text);
  border-radius: 0.5rem;
  padding: 1.5rem;
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
  cursor: pointer;
  transition: all 0.2s;
  border-left: 3px solid transparent;
}

.query-card-featured {
  border-left-color: var(--color-warning, #c2410c);
}

.query-card:hover {
  box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
  transform: translateY(-2px);
}

.query-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 1rem;
}

.query-header h3 {
  margin: 0;
  font-size: 1.125rem;
  font-weight: 600;
}

.featured-star {
  color: var(--color-warning, #c2410c);
  margin-right: 0.25rem;
  font-size: 1rem;
}

.query-meta {
  display: flex;
  gap: 1.5rem;
  margin-bottom: 1rem;
  font-size: 0.875rem;
  color: var(--color-text-secondary);
}

.query-preview {
  background: var(--color-background-soft);
  padding: 1rem;
  border-radius: 0.375rem;
  margin-bottom: 1rem;
  font-size: 0.875rem;
}

.query-preview code {
  font-family: 'Monaco', 'Menlo', monospace;
  color: var(--color-text);
}

.query-actions {
  display: flex;
  gap: 0.5rem;
}

.btn-sm {
  padding: 0.375rem 0.75rem;
  font-size: 0.875rem;
}

.loading-section {
  text-align: center;
  padding: 3rem;
}

.no-results {
  text-align: center;
  padding: 3rem;
  color: var(--color-text-secondary);
  font-size: 1.125rem;
}

.load-more {
  text-align: center;
  margin-top: 2rem;
}

.error-message {
  background: #FEE2E2;
  border: 1px solid #FCA5A5;
  color: #991B1B;
  padding: 1rem;
  border-radius: 0.5rem;
}
</style>
