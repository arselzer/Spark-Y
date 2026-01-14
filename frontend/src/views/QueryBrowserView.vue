<template>
  <div class="container">
    <div class="query-browser">
      <h2>Query Browser</h2>
      <p class="description">Browse and search through hundreds of benchmark queries</p>

      <div class="search-controls">
        <input
          v-model="searchTerm"
          type="text"
          placeholder="Search queries..."
          class="input search-input"
          @input="debounceSearch"
        />

        <select v-model="selectedCategory" class="input category-select" @change="loadQueries">
          <option value="">All Categories</option>
          <option v-for="(cat, key) in categories" :key="key" :value="key">
            {{ cat.name }} ({{ cat.count }})
          </option>
        </select>
      </div>

      <div v-if="loading" class="loading-section">
        <div class="spinner"></div>
        <p>Loading queries...</p>
      </div>

      <div v-else-if="error" class="error-message">
        {{ error }}
      </div>

      <div v-else class="queries-list">
        <div v-if="queries.length === 0" class="no-results">
          No queries found matching your criteria
        </div>

        <div
          v-for="query in queries"
          :key="query.query_id"
          class="query-card"
          @click="viewQuery(query.query_id)"
        >
          <div class="query-header">
            <h3>{{ query.name }}</h3>
            <span class="badge badge-primary">{{ query.category.toUpperCase() }}</span>
          </div>

          <div class="query-meta">
            <span class="meta-item">
              <strong>{{ query.num_joins }}</strong> joins
            </span>
            <span class="meta-item">
              <strong>{{ query.num_aggregates }}</strong> aggregates
            </span>
            <span class="meta-item">
              <strong>{{ query.tables.length }}</strong> tables
            </span>
          </div>

          <div class="query-preview">
            <code>{{ truncateSQL(query.sql) }}</code>
          </div>

          <div class="query-actions">
            <button @click.stop="executeQuery(query)" class="btn btn-primary btn-sm">
              Execute
            </button>
            <button @click.stop="viewQuery(query.query_id)" class="btn btn-secondary btn-sm">
              View Details
            </button>
          </div>
        </div>
      </div>

      <div v-if="queries.length > 0 && queries.length >= limit" class="load-more">
        <button @click="loadMore" class="btn btn-secondary">Load More</button>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { useRouter } from 'vue-router'
import { queryApi } from '@/services/api'
import type { QueryMetadata } from '@/types'

const router = useRouter()

const queries = ref<QueryMetadata[]>([])
const categories = ref<Record<string, { count: number; name: string }>>({})
const searchTerm = ref('')
const selectedCategory = ref('')
const loading = ref(false)
const error = ref('')
const limit = ref(50)
const offset = ref(0)

let searchTimeout: number | null = null

onMounted(async () => {
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

.search-input {
  font-size: 1rem;
}

.category-select {
  min-width: 200px;
}

.queries-list {
  display: grid;
  gap: 1rem;
}

.query-card {
  background: white;
  border-radius: 0.5rem;
  padding: 1.5rem;
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
  cursor: pointer;
  transition: all 0.2s;
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

.query-meta {
  display: flex;
  gap: 1.5rem;
  margin-bottom: 1rem;
  font-size: 0.875rem;
  color: var(--color-text-secondary);
}

.query-preview {
  background: #F9FAFB;
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
