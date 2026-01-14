<template>
  <div class="sql-display">
    <Codemirror
      :model-value="sqlContent"
      :style="{ height: height }"
      :disabled="true"
      :extensions="extensions"
    />
  </div>
</template>

<script setup lang="ts">
import { shallowRef, computed } from 'vue'
import { Codemirror } from 'vue-codemirror'
import { sql, PostgreSQL } from '@codemirror/lang-sql'
import { oneDark } from '@codemirror/theme-one-dark'
import { EditorView } from '@codemirror/view'
import { useSQLHighlighting } from '@/composables/useSQLHighlighting'

interface Props {
  sql: string
  height?: string
}

const props = withDefaults(defineProps<Props>(), {
  height: 'auto'
})

// Ensure we always pass a string to CodeMirror
const sqlContent = computed(() => {
  return props.sql || ''
})

// SQL highlighting coordination with hypergraph
const { highlightingExtensions } = useSQLHighlighting()

// CodeMirror extensions for read-only display
const extensions = shallowRef([
  sql({ dialect: PostgreSQL }),
  oneDark,
  EditorView.editable.of(false), // Make it read-only
  ...highlightingExtensions.value
])
</script>

<style scoped>
.sql-display {
  border-radius: 0.375rem;
  overflow: hidden;
}

/* CodeMirror styling overrides for read-only display */
.sql-display :deep(.cm-editor) {
  font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', 'Consolas', monospace;
  font-size: 14px;
}

.sql-display :deep(.cm-scroller) {
  overflow: auto;
}

.sql-display :deep(.cm-content) {
  padding: 0.5rem;
}

.sql-display :deep(.cm-focused) {
  outline: none;
}

/* SQL text highlighting for hypergraph coordination */
.sql-display :deep(.cm-sql-highlight) {
  background-color: #FED7AA;
  border-bottom: 2px solid #EA580C;
  font-weight: 600;
  border-radius: 2px;
  padding: 0 2px;
}

.dark-mode .sql-display :deep(.cm-sql-highlight) {
  background-color: #7C2D12;
  border-bottom: 2px solid #FB923C;
}

/* Secondary highlight for rest of equivalence class */
.sql-display :deep(.cm-sql-highlight-secondary) {
  background-color: #DBEAFE;
  border-bottom: 2px solid #3B82F6;
  font-weight: 500;
  border-radius: 2px;
  padding: 0 2px;
}

.dark-mode .sql-display :deep(.cm-sql-highlight-secondary) {
  background-color: #1E3A8A;
  border-bottom: 2px solid #60A5FA;
}
</style>
