import { ref, computed } from 'vue'

export interface QueryHistoryItem {
  id: string
  query: string
  timestamp: number
  favorite: boolean
  acyclic?: boolean
  numTables?: number
  numJoins?: number
  executionTime?: number
  title?: string
}

const STORAGE_KEY_HISTORY = 'yannasparkis_query_history'
const STORAGE_KEY_FAVORITES = 'yannasparkis_query_favorites'
const MAX_HISTORY_SIZE = 50

// Shared state across all instances
const historyItems = ref<QueryHistoryItem[]>([])
const favoriteIds = ref<Set<string>>(new Set())
const isLoaded = ref(false)

export function useQueryHistory() {
  // Load from localStorage on first use
  if (!isLoaded.value) {
    loadFromStorage()
    isLoaded.value = true
  }

  const recentQueries = computed(() =>
    historyItems.value
      .filter(item => !item.favorite)
      .sort((a, b) => b.timestamp - a.timestamp)
      .slice(0, 20)
  )

  const favorites = computed(() =>
    historyItems.value
      .filter(item => item.favorite)
      .sort((a, b) => b.timestamp - a.timestamp)
  )

  function loadFromStorage() {
    try {
      const historyJson = localStorage.getItem(STORAGE_KEY_HISTORY)
      const favoritesJson = localStorage.getItem(STORAGE_KEY_FAVORITES)

      if (historyJson) {
        historyItems.value = JSON.parse(historyJson)
      }

      if (favoritesJson) {
        favoriteIds.value = new Set(JSON.parse(favoritesJson))
      }

      // Sync favorite status
      historyItems.value.forEach(item => {
        item.favorite = favoriteIds.value.has(item.id)
      })
    } catch (error) {
      console.error('Failed to load query history:', error)
      historyItems.value = []
      favoriteIds.value = new Set()
    }
  }

  function saveToStorage() {
    try {
      localStorage.setItem(STORAGE_KEY_HISTORY, JSON.stringify(historyItems.value))
      localStorage.setItem(STORAGE_KEY_FAVORITES, JSON.stringify([...favoriteIds.value]))
    } catch (error) {
      console.error('Failed to save query history:', error)
    }
  }

  function addQuery(
    query: string,
    metadata?: Partial<Omit<QueryHistoryItem, 'id' | 'query' | 'timestamp' | 'favorite'>>
  ): QueryHistoryItem {
    // Check if query already exists (case-insensitive, trim whitespace)
    const normalizedQuery = query.trim().toLowerCase()
    const existingIndex = historyItems.value.findIndex(
      item => item.query.trim().toLowerCase() === normalizedQuery
    )

    let item: QueryHistoryItem

    if (existingIndex >= 0) {
      // Update existing query
      item = historyItems.value[existingIndex]
      item.timestamp = Date.now()
      if (metadata) {
        Object.assign(item, metadata)
      }
      // Move to end (will be sorted by timestamp)
    } else {
      // Create new query
      item = {
        id: generateId(),
        query: query.trim(),
        timestamp: Date.now(),
        favorite: false,
        ...metadata
      }
      historyItems.value.push(item)
    }

    // Limit history size (keep favorites + recent)
    const nonFavorites = historyItems.value.filter(i => !i.favorite)
    if (nonFavorites.length > MAX_HISTORY_SIZE) {
      const sorted = nonFavorites.sort((a, b) => b.timestamp - a.timestamp)
      const toRemove = sorted.slice(MAX_HISTORY_SIZE)
      historyItems.value = historyItems.value.filter(i => !toRemove.includes(i))
    }

    saveToStorage()
    return item
  }

  function toggleFavorite(id: string) {
    const item = historyItems.value.find(i => i.id === id)
    if (!item) return

    item.favorite = !item.favorite

    if (item.favorite) {
      favoriteIds.value.add(id)
    } else {
      favoriteIds.value.delete(id)
    }

    saveToStorage()
  }

  function removeQuery(id: string) {
    historyItems.value = historyItems.value.filter(i => i.id !== id)
    favoriteIds.value.delete(id)
    saveToStorage()
  }

  function clearHistory() {
    // Only clear non-favorites
    historyItems.value = historyItems.value.filter(i => i.favorite)
    saveToStorage()
  }

  function clearAll() {
    historyItems.value = []
    favoriteIds.value.clear()
    saveToStorage()
  }

  function exportHistory(): string {
    return JSON.stringify({
      version: 1,
      exportDate: new Date().toISOString(),
      queries: historyItems.value
    }, null, 2)
  }

  function importHistory(jsonString: string): { success: boolean; message: string } {
    try {
      const data = JSON.parse(jsonString)

      if (!data.queries || !Array.isArray(data.queries)) {
        return { success: false, message: 'Invalid format: missing queries array' }
      }

      // Merge with existing (avoid duplicates)
      const existingQueries = new Set(
        historyItems.value.map(i => i.query.trim().toLowerCase())
      )

      let imported = 0
      for (const item of data.queries) {
        if (!item.query) continue

        const normalized = item.query.trim().toLowerCase()
        if (!existingQueries.has(normalized)) {
          historyItems.value.push({
            id: item.id || generateId(),
            query: item.query,
            timestamp: item.timestamp || Date.now(),
            favorite: item.favorite || false,
            acyclic: item.acyclic,
            numTables: item.numTables,
            numJoins: item.numJoins,
            executionTime: item.executionTime,
            title: item.title
          })
          imported++
        }
      }

      // Update favorites set
      favoriteIds.value = new Set(
        historyItems.value.filter(i => i.favorite).map(i => i.id)
      )

      saveToStorage()
      return { success: true, message: `Imported ${imported} queries` }
    } catch (error: any) {
      return { success: false, message: error.message || 'Failed to parse JSON' }
    }
  }

  function generateId(): string {
    return `q_${Date.now()}_${Math.random().toString(36).substr(2, 9)}`
  }

  return {
    historyItems: computed(() => historyItems.value),
    recentQueries,
    favorites,
    addQuery,
    toggleFavorite,
    removeQuery,
    clearHistory,
    clearAll,
    exportHistory,
    importHistory
  }
}
