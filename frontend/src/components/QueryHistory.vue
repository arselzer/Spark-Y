<template>
  <div class="query-history">
    <div class="history-header">
      <h3>{{ showFavoritesOnly ? 'Favorite Queries' : 'Query History' }}</h3>
      <div class="header-actions">
        <button
          @click="showFavoritesOnly = !showFavoritesOnly"
          class="btn-icon"
          :title="showFavoritesOnly ? 'Show all history' : 'Show favorites only'"
        >
          <span v-if="showFavoritesOnly">📚</span>
          <span v-else>⭐</span>
        </button>
        <button @click="showExportImport = !showExportImport" class="btn-icon" title="Export/Import">
          📤
        </button>
        <button v-if="!showFavoritesOnly" @click="confirmClearHistory" class="btn-icon" title="Clear history">
          🗑️
        </button>
      </div>
    </div>

    <!-- Export/Import Section -->
    <div v-if="showExportImport" class="export-import-section">
      <div class="export-import-actions">
        <button @click="exportToFile" class="btn btn-secondary btn-sm">
          Export to File
        </button>
        <label class="btn btn-secondary btn-sm">
          Import from File
          <input
            type="file"
            accept=".json"
            @change="importFromFile"
            style="display: none"
          />
        </label>
      </div>
      <div v-if="importMessage" :class="['message', importMessage.type]">
        {{ importMessage.text }}
      </div>
    </div>

    <!-- Search -->
    <div class="search-box">
      <input
        v-model="searchQuery"
        type="text"
        placeholder="Search queries..."
        class="search-input"
      />
    </div>

    <!-- Query List -->
    <div class="query-list">
      <div v-if="filteredQueries.length === 0" class="empty-state">
        <p v-if="showFavoritesOnly">No favorite queries yet. Star a query to save it!</p>
        <p v-else-if="searchQuery">No queries match your search.</p>
        <p v-else>No queries in history. Execute a query to get started!</p>
      </div>

      <div
        v-for="item in filteredQueries"
        :key="item.id"
        class="query-item"
        @click="$emit('select', item.query)"
      >
        <div class="query-item-header">
          <button
            @click.stop="toggleFavorite(item.id)"
            class="favorite-btn"
            :class="{ active: item.favorite }"
            :title="item.favorite ? 'Remove from favorites' : 'Add to favorites'"
          >
            {{ item.favorite ? '⭐' : '☆' }}
          </button>
          <div class="query-info">
            <div v-if="item.title" class="query-title">{{ item.title }}</div>
            <div class="query-meta">
              <span class="timestamp">{{ formatTimestamp(item.timestamp) }}</span>
              <span v-if="item.acyclic !== undefined" class="badge" :class="item.acyclic ? 'badge-success' : 'badge-warning'">
                {{ item.acyclic ? 'Acyclic' : 'Cyclic' }}
              </span>
              <span v-if="item.numTables" class="meta-item">{{ item.numTables }} tables</span>
              <span v-if="item.numJoins" class="meta-item">{{ item.numJoins }} joins</span>
            </div>
          </div>
          <button
            @click.stop="removeQuery(item.id)"
            class="remove-btn"
            title="Remove from history"
          >
            ×
          </button>
        </div>
        <div class="query-preview">
          <code>{{ truncateQuery(item.query) }}</code>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed } from 'vue'
import { useQueryHistory } from '@/composables/useQueryHistory'

interface Emits {
  (e: 'select', query: string): void
}

const emit = defineEmits<Emits>()

const {
  recentQueries,
  favorites,
  toggleFavorite,
  removeQuery,
  clearHistory,
  exportHistory,
  importHistory
} = useQueryHistory()

const showFavoritesOnly = ref(false)
const searchQuery = ref('')
const showExportImport = ref(false)
const importMessage = ref<{ text: string; type: 'success' | 'error' } | null>(null)

const displayedQueries = computed(() =>
  showFavoritesOnly.value ? favorites.value : recentQueries.value
)

const filteredQueries = computed(() => {
  if (!searchQuery.value) return displayedQueries.value

  const query = searchQuery.value.toLowerCase()
  return displayedQueries.value.filter(item =>
    item.query.toLowerCase().includes(query) ||
    item.title?.toLowerCase().includes(query)
  )
})

function formatTimestamp(timestamp: number): string {
  const date = new Date(timestamp)
  const now = new Date()
  const diff = now.getTime() - date.getTime()

  const minutes = Math.floor(diff / 60000)
  const hours = Math.floor(diff / 3600000)
  const days = Math.floor(diff / 86400000)

  if (minutes < 1) return 'Just now'
  if (minutes < 60) return `${minutes}m ago`
  if (hours < 24) return `${hours}h ago`
  if (days < 7) return `${days}d ago`

  return date.toLocaleDateString()
}

function truncateQuery(query: string, maxLength = 100): string {
  const cleaned = query.replace(/\s+/g, ' ').trim()
  return cleaned.length > maxLength
    ? cleaned.substring(0, maxLength) + '...'
    : cleaned
}

function confirmClearHistory() {
  if (confirm('Clear all query history (favorites will be kept)?')) {
    clearHistory()
  }
}

function exportToFile() {
  const json = exportHistory()
  const blob = new Blob([json], { type: 'application/json' })
  const url = URL.createObjectURL(blob)
  const a = document.createElement('a')
  a.href = url
  a.download = `query-history-${new Date().toISOString().split('T')[0]}.json`
  a.click()
  URL.revokeObjectURL(url)

  importMessage.value = { text: 'History exported successfully!', type: 'success' }
  setTimeout(() => { importMessage.value = null }, 3000)
}

function importFromFile(event: Event) {
  const target = event.target as HTMLInputElement
  const file = target.files?.[0]
  if (!file) return

  const reader = new FileReader()
  reader.onload = (e) => {
    const content = e.target?.result as string
    const result = importHistory(content)

    importMessage.value = {
      text: result.message,
      type: result.success ? 'success' : 'error'
    }

    setTimeout(() => { importMessage.value = null }, 5000)
  }
  reader.readAsText(file)

  // Reset input
  target.value = ''
}
</script>

<style scoped>
.query-history {
  display: flex;
  flex-direction: column;
  height: 100%;
  background: var(--color-background);
  border-radius: 8px;
  border: 1px solid var(--color-border);
  overflow: hidden;
}

.history-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 1rem;
  border-bottom: 1px solid var(--color-border);
  background: var(--color-background-soft);
}

.history-header h3 {
  margin: 0;
  font-size: 1.125rem;
  font-weight: 600;
}

.header-actions {
  display: flex;
  gap: 0.5rem;
}

.btn-icon {
  background: none;
  border: 1px solid var(--color-border);
  border-radius: 4px;
  padding: 0.25rem 0.5rem;
  cursor: pointer;
  font-size: 1.125rem;
  transition: all 0.2s;
}

.btn-icon:hover {
  background: var(--color-background-mute);
  transform: scale(1.05);
}

.export-import-section {
  padding: 0.75rem 1rem;
  background: var(--color-background-soft);
  border-bottom: 1px solid var(--color-border);
}

.export-import-actions {
  display: flex;
  gap: 0.5rem;
  flex-wrap: wrap;
}

.btn-sm {
  font-size: 0.875rem;
  padding: 0.375rem 0.75rem;
}

.message {
  margin-top: 0.5rem;
  padding: 0.5rem;
  border-radius: 4px;
  font-size: 0.875rem;
}

.message.success {
  background: rgba(72, 187, 120, 0.1);
  color: #48bb78;
  border: 1px solid rgba(72, 187, 120, 0.3);
}

.message.error {
  background: rgba(245, 101, 101, 0.1);
  color: #f56565;
  border: 1px solid rgba(245, 101, 101, 0.3);
}

.search-box {
  padding: 0.75rem 1rem;
  border-bottom: 1px solid var(--color-border);
}

.search-input {
  width: 100%;
  padding: 0.5rem;
  border: 1px solid var(--color-border);
  border-radius: 4px;
  font-size: 0.875rem;
  background: var(--color-background);
  color: var(--color-text);
}

.search-input:focus {
  outline: none;
  border-color: var(--color-primary);
}

.query-list {
  flex: 1;
  overflow-y: auto;
  padding: 0.5rem;
}

.empty-state {
  display: flex;
  align-items: center;
  justify-content: center;
  height: 100%;
  padding: 2rem;
  text-align: center;
  color: var(--color-text-secondary);
}

.query-item {
  background: var(--color-background-soft);
  border: 1px solid var(--color-border);
  border-radius: 6px;
  padding: 0.75rem;
  margin-bottom: 0.5rem;
  cursor: pointer;
  transition: all 0.2s;
}

.query-item:hover {
  background: var(--color-background-mute);
  border-color: var(--color-primary);
  transform: translateY(-1px);
  box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
}

.query-item-header {
  display: flex;
  align-items: flex-start;
  gap: 0.5rem;
  margin-bottom: 0.5rem;
}

.favorite-btn {
  background: none;
  border: none;
  font-size: 1.25rem;
  cursor: pointer;
  padding: 0;
  line-height: 1;
  opacity: 0.5;
  transition: all 0.2s;
}

.favorite-btn:hover,
.favorite-btn.active {
  opacity: 1;
  transform: scale(1.2);
}

.query-info {
  flex: 1;
  min-width: 0;
}

.query-title {
  font-weight: 600;
  font-size: 0.9375rem;
  margin-bottom: 0.25rem;
  color: var(--color-text);
}

.query-meta {
  display: flex;
  flex-wrap: wrap;
  gap: 0.5rem;
  align-items: center;
  font-size: 0.75rem;
  color: var(--color-text-secondary);
}

.timestamp {
  font-weight: 500;
}

.meta-item {
  padding: 0.125rem 0.375rem;
  background: var(--color-background-mute);
  border-radius: 3px;
}

.badge {
  font-size: 0.625rem;
  padding: 0.125rem 0.375rem;
  border-radius: 3px;
  font-weight: 600;
  text-transform: uppercase;
}

.badge-success {
  background: rgba(72, 187, 120, 0.2);
  color: #48bb78;
}

.badge-warning {
  background: rgba(251, 191, 36, 0.2);
  color: #f59e0b;
}

.remove-btn {
  background: none;
  border: none;
  font-size: 1.5rem;
  line-height: 1;
  cursor: pointer;
  color: var(--color-text-secondary);
  opacity: 0;
  transition: all 0.2s;
}

.query-item:hover .remove-btn {
  opacity: 0.5;
}

.remove-btn:hover {
  opacity: 1 !important;
  color: #f56565;
}

.query-preview {
  font-size: 0.8125rem;
  color: var(--color-text-secondary);
  overflow: hidden;
  text-overflow: ellipsis;
}

.query-preview code {
  font-family: 'Courier New', monospace;
  background: var(--color-background);
  padding: 0.25rem 0.5rem;
  border-radius: 3px;
  display: block;
}

/* Scrollbar styling */
.query-list::-webkit-scrollbar {
  width: 8px;
}

.query-list::-webkit-scrollbar-track {
  background: var(--color-background);
}

.query-list::-webkit-scrollbar-thumb {
  background: var(--color-border);
  border-radius: 4px;
}

.query-list::-webkit-scrollbar-thumb:hover {
  background: var(--color-text-secondary);
}
</style>
