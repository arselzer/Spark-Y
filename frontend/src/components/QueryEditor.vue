<template>
  <div class="query-editor">
    <!-- Header (title + Format/Execute) lives in the parent ExecuteView so the
         editor isn't double-headed; formatQuery is exposed for the parent. -->
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
import { lineNumbers, EditorView } from '@codemirror/view'
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

// Soften oneDark: keep its syntax colours but lift the near-black background
// to a calmer slate so the editor is less harsh. Applied after oneDark so its
// background rules win.
const softerDark = EditorView.theme({
  '&': { backgroundColor: '#2b3440' },
  '.cm-gutters': { backgroundColor: '#2b3440', borderRight: '1px solid #3a4654' },
  '.cm-activeLine': { backgroundColor: 'rgba(255,255,255,0.04)' },
  '.cm-activeLineGutter': { backgroundColor: 'rgba(255,255,255,0.06)' }
}, { dark: true })

// CodeMirror extensions. lineWrapping guarantees long single-line queries
// (e.g. the one-line STATS-CEB queries) never overflow the editor horizontally,
// independent of the auto-formatting below.
const extensions = shallowRef([
  sql({ dialect: PostgreSQL }),
  oneDark,
  softerDark,
  lineNumbers(),
  EditorView.lineWrapping,
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

// Reflow one keyword-bearing segment (no string literals inside it).
function reflowKeywords(seg: string): string {
  return seg
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
}

// Pure SQL pretty-printer: breaks the query onto multiple lines at major
// clauses and boolean connectives. Single-quoted literals (odd-indexed
// segments of the split) are passed through untouched so a value such as a
// timestamp or a string containing "AND" is never reflowed.
function formatSQL(input: string): string {
  const parts = input.split(/('(?:[^']|'')*')/)
  return parts
    .map((seg, i) => (i % 2 === 1 ? seg : reflowKeywords(seg)))
    .join('')
    .trim()
}

// A query worth auto-formatting: a single long line (the unformatted benchmark
// queries). Leaves short or already multi-line queries (incl. user edits) alone.
function shouldAutoFormat(value: string): boolean {
  return !value.includes('\n') && value.trim().length > 100
}

function formatQuery() {
  if (!props.modelValue) return
  const formatted = formatSQL(props.modelValue)
  isInternalUpdate = true
  code.value = formatted
  emit('update:modelValue', formatted)
  isInternalUpdate = false
}

// Let the parent (ExecuteView) trigger formatting from its single header.
defineExpose({ formatQuery })

// Watch for external changes to modelValue (e.g. loading a benchmark query).
// Auto-format long single-line queries so the one-line STATS-CEB queries don't
// arrive as an unreadable, overflowing single line.
watch(() => props.modelValue, (newVal) => {
  if (isInternalUpdate) return

  let newValStr = String(newVal || '')
  if (shouldAutoFormat(newValStr)) {
    const formatted = formatSQL(newValStr)
    isInternalUpdate = true
    code.value = formatted
    emit('update:modelValue', formatted)
    isInternalUpdate = false
    validateSyntax(formatted)
    return
  }
  if (newValStr !== code.value) {
    code.value = newValStr
  }
  if (newValStr) {
    validateSyntax(newValStr)
  }
})

onMounted(() => {
  if (props.modelValue) {
    // Format an unformatted query that was already present at mount.
    if (shouldAutoFormat(String(props.modelValue))) {
      formatQuery()
    } else {
      validateSyntax(props.modelValue)
    }
  }
})
</script>

<style scoped>
.query-editor {
  background: var(--color-surface);
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
