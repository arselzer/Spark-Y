<template>
  <div class="saved-runs">
    <div class="saved-runs-header">
      <h3>Saved Runs</h3>
      <button class="refresh-btn" title="Refresh" @click="load">↻</button>
    </div>

    <p v-if="error" class="saved-error">{{ error }}</p>

    <p v-else-if="!loading && runs.length === 0" class="saved-empty">
      No saved runs yet. Run a query and click “Save run” to keep it for instant replay.
    </p>

    <ul v-else class="saved-list">
      <li
        v-for="run in runs"
        :key="run.id"
        class="saved-item"
        @click="$emit('replay', run.id)"
      >
        <div class="saved-item-main">
          <span class="saved-name">{{ run.name }}</span>
          <button
            class="delete-btn"
            title="Delete saved run"
            @click.stop="remove(run.id)"
          >×</button>
        </div>
        <div class="saved-meta">
          <span v-if="run.reference_timed_out" class="badge-timeout">ref timeout</span>
          <span v-else-if="run.speedup" class="badge-speedup">{{ run.speedup.toFixed(1) }}×</span>
          <span class="saved-date">{{ formatDate(run.saved_at) }}</span>
        </div>
      </li>
    </ul>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { executionApi } from '@/services/api'
import type { SavedRunSummary } from '@/types'

const emit = defineEmits<{ (e: 'replay', id: string): void }>()

const runs = ref<SavedRunSummary[]>([])
const loading = ref(false)
const error = ref('')

async function load() {
  loading.value = true
  error.value = ''
  try {
    runs.value = await executionApi.listSavedRuns()
  } catch (e: any) {
    error.value = e?.message || 'Failed to load saved runs'
  } finally {
    loading.value = false
  }
}

async function remove(id: string) {
  try {
    await executionApi.deleteSavedRun(id)
    runs.value = runs.value.filter(r => r.id !== id)
  } catch (e: any) {
    error.value = e?.message || 'Failed to delete saved run'
  }
}

function formatDate(iso: string): string {
  try {
    return new Date(iso).toLocaleString([], { month: 'short', day: 'numeric', hour: '2-digit', minute: '2-digit' })
  } catch {
    return iso
  }
}

// Expose load so the parent can refresh after a new save.
defineExpose({ load })

onMounted(load)
</script>

<style scoped>
.saved-runs {
  background: var(--color-surface);
  border: 1px solid var(--color-border);
  border-radius: 0.5rem;
  padding: 1rem;
}

.saved-runs-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 0.75rem;
}

.saved-runs-header h3 {
  margin: 0;
  font-size: 1rem;
  font-weight: 600;
}

.refresh-btn {
  background: transparent;
  border: none;
  color: var(--color-text-secondary);
  cursor: pointer;
  font-size: 1.1rem;
  line-height: 1;
}

.refresh-btn:hover { color: var(--color-text); }

.saved-empty,
.saved-error {
  font-size: 0.85rem;
  color: var(--color-text-secondary);
  line-height: 1.4;
}

.saved-error { color: var(--color-danger, #dc2626); }

.saved-list {
  list-style: none;
  margin: 0;
  padding: 0;
  display: flex;
  flex-direction: column;
  gap: 0.4rem;
  max-height: calc(100vh - 240px);
  overflow-y: auto;
}

.saved-item {
  padding: 0.5rem 0.6rem;
  border: 1px solid var(--color-border);
  border-radius: 0.375rem;
  cursor: pointer;
  transition: border-color 0.15s, background 0.15s;
}

.saved-item:hover {
  border-color: var(--color-primary, #3b82f6);
  background: var(--color-background-soft);
}

.saved-item-main {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 0.5rem;
}

.saved-name {
  font-family: 'Monaco', 'Menlo', monospace;
  font-size: 0.85rem;
  font-weight: 600;
  white-space: nowrap;
  overflow: hidden;
  text-overflow: ellipsis;
}

.delete-btn {
  background: transparent;
  border: none;
  color: var(--color-text-secondary);
  cursor: pointer;
  font-size: 1.1rem;
  line-height: 1;
  flex-shrink: 0;
}

.delete-btn:hover { color: var(--color-danger, #dc2626); }

.saved-meta {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  margin-top: 0.25rem;
  font-size: 0.75rem;
}

.badge-speedup {
  font-weight: 700;
  color: var(--color-primary, #0f766e);
}

.badge-timeout {
  font-weight: 600;
  color: var(--color-warning, #c2410c);
}

.saved-date {
  color: var(--color-text-secondary);
  margin-left: auto;
}
</style>
