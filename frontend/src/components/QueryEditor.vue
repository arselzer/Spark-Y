<template>
  <div class="query-editor">
    <div class="editor-header">
      <h3>SQL Query Editor</h3>
      <div class="editor-actions">
        <button @click="formatQuery" class="btn btn-secondary">Format</button>
        <button @click="$emit('execute')" class="btn btn-primary" :disabled="!modelValue">
          Execute Query
        </button>
      </div>
    </div>
    <div class="editor-container">
      <Codemirror
        v-model="code"
        :style="{ height: '900px' }"
        :autofocus="true"
        :indent-with-tab="true"
        :tab-size="2"
        :extensions="extensions"
      />
    </div>
    <div class="editor-footer">
      <span class="char-count">{{ modelValue?.length || 0 }} characters</span>
      <span v-if="syntaxValid" class="syntax-status valid">✓ Syntax OK</span>
      <span v-else-if="syntaxError" class="syntax-status invalid">⚠ {{ syntaxError }}</span>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, watch, shallowRef, onMounted } from 'vue'
import { Codemirror } from 'vue-codemirror'
import { sql, PostgreSQL } from '@codemirror/lang-sql'
import { oneDark } from '@codemirror/theme-one-dark'
import { lineNumbers } from '@codemirror/view'
import { useSQLHighlighting } from '@/composables/useSQLHighlighting'

interface Props {
  modelValue: string
}

interface Emits {
  (e: 'update:modelValue', value: string): void
  (e: 'execute'): void
}

const props = defineProps<Props>()
const emit = defineEmits<Emits>()

// Ensure code is always a string
const code = ref(String(props.modelValue || ''))
const syntaxValid = ref(false)
const syntaxError = ref('')

// SQL highlighting coordination with hypergraph
const { highlightingExtensions } = useSQLHighlighting()

// CodeMirror extensions
const extensions = shallowRef([
  sql({ dialect: PostgreSQL }),
  oneDark,
  lineNumbers(),
  ...highlightingExtensions.value
])

// Track if we're updating internally to prevent circular updates
let isInternalUpdate = false

function validateSyntax(sqlQuery: string) {
  if (!sqlQuery || !sqlQuery.trim()) {
    syntaxValid.value = false
    syntaxError.value = ''
    return
  }

  // Basic SQL validation
  const upperSQL = sqlQuery.trim().toUpperCase()
  if (upperSQL.startsWith('SELECT') || upperSQL.startsWith('WITH')) {
    syntaxValid.value = true
    syntaxError.value = ''
  } else {
    syntaxValid.value = false
    syntaxError.value = 'Query must start with SELECT or WITH'
  }
}

// Watch code changes from CodeMirror
watch(code, (newValue) => {
  if (isInternalUpdate) return

  isInternalUpdate = true
  emit('update:modelValue', newValue)
  validateSyntax(newValue)
  isInternalUpdate = false
})

function formatQuery() {
  if (!props.modelValue) return

  // Simple SQL formatting
  let formatted = props.modelValue
    .replace(/\s+/g, ' ')
    .replace(/\bSELECT\b/gi, '\nSELECT\n  ')
    .replace(/\bFROM\b/gi, '\nFROM\n  ')
    .replace(/\bWHERE\b/gi, '\nWHERE\n  ')
    .replace(/\b(INNER|LEFT|RIGHT|FULL|CROSS)?\s*JOIN\b/gi, '\n$1 JOIN\n  ')
    .replace(/\bAND\b/gi, '\n  AND ')
    .replace(/\bOR\b/gi, '\n  OR ')
    .replace(/\bGROUP BY\b/gi, '\nGROUP BY\n  ')
    .replace(/\bHAVING\b/gi, '\nHAVING\n  ')
    .replace(/\bORDER BY\b/gi, '\nORDER BY\n  ')
    .replace(/\bLIMIT\b/gi, '\nLIMIT ')
    .trim()

  isInternalUpdate = true
  code.value = formatted
  emit('update:modelValue', formatted)
  isInternalUpdate = false
}

// Watch for external changes to modelValue
watch(() => props.modelValue, (newVal) => {
  if (isInternalUpdate) return

  const newValStr = String(newVal || '')
  if (newValStr !== code.value) {
    code.value = newValStr
  }
  if (newValStr) {
    validateSyntax(newValStr)
  }
})

onMounted(() => {
  if (props.modelValue) {
    validateSyntax(props.modelValue)
  }
})
</script>

<style scoped>
.query-editor {
  background: white;
  border-radius: 0.5rem;
  overflow: hidden;
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
}

.editor-header {
  padding: 1rem;
  border-bottom: 1px solid var(--color-border);
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.editor-header h3 {
  margin: 0;
  font-size: 1.125rem;
  font-weight: 600;
}

.editor-actions {
  display: flex;
  gap: 0.5rem;
}

.editor-container {
  position: relative;
}

/* CodeMirror styling overrides */
.editor-container :deep(.cm-editor) {
  font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', 'Consolas', monospace;
  font-size: 14px;
}

.editor-container :deep(.cm-scroller) {
  overflow: auto;
}

.editor-container :deep(.cm-content) {
  padding: 0.5rem;
}

.editor-footer {
  padding: 0.75rem 1rem;
  border-top: 1px solid var(--color-border);
  display: flex;
  justify-content: space-between;
  align-items: center;
  font-size: 0.875rem;
}

.char-count {
  color: var(--color-text-secondary);
}

.syntax-status {
  font-weight: 500;
}

.syntax-status.valid {
  color: var(--color-secondary);
}

.syntax-status.invalid {
  color: var(--color-danger);
}

/* SQL text highlighting for hypergraph coordination */
.editor-container :deep(.cm-sql-highlight) {
  background-color: #FED7AA;
  border-bottom: 2px solid #EA580C;
  font-weight: 600;
  border-radius: 2px;
  padding: 0 2px;
}

.dark-mode .editor-container :deep(.cm-sql-highlight) {
  background-color: #7C2D12;
  border-bottom: 2px solid #FB923C;
}

/* Secondary highlight for rest of equivalence class */
.editor-container :deep(.cm-sql-highlight-secondary) {
  background-color: #DBEAFE;
  border-bottom: 2px solid #3B82F6;
  font-weight: 500;
  border-radius: 2px;
  padding: 0 2px;
}

.dark-mode .editor-container :deep(.cm-sql-highlight-secondary) {
  background-color: #1E3A8A;
  border-bottom: 2px solid #60A5FA;
}
</style>
