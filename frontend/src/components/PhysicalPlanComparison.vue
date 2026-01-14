<template>
  <div class="physical-plan-comparison">
    <h4>Physical Plan Comparison</h4>
    <p class="description">Side-by-side comparison of execution plans. Lines highlighted show differences.</p>

    <div class="plans-container">
      <!-- Reference Plan -->
      <div class="plan-panel">
        <h5>Reference Plan</h5>
        <div class="plan-content">
          <pre><code v-html="highlightedReferencePlan"></code></pre>
        </div>
      </div>

      <!-- Optimized Plan -->
      <div class="plan-panel">
        <h5>Optimized Plan</h5>
        <div class="plan-content">
          <pre><code v-html="highlightedOptimizedPlan"></code></pre>
        </div>
      </div>
    </div>

    <!-- Legend -->
    <div class="legend">
      <div class="legend-item">
        <span class="legend-color added"></span>
        <span>Added/Modified in Optimized</span>
      </div>
      <div class="legend-item">
        <span class="legend-color removed"></span>
        <span>Removed from Reference</span>
      </div>
      <div class="legend-item">
        <span class="legend-color common"></span>
        <span>Common to Both</span>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'

interface ExecutionPlan {
  plan_string: string
  plan_tree?: any
  optimizations_applied?: string[]
}

interface Props {
  referencePlan: ExecutionPlan
  optimizedPlan: ExecutionPlan
}

const props = defineProps<Props>()

// Simple diff algorithm to highlight differences
const highlightedReferencePlan = computed(() => {
  const refLines = props.referencePlan.plan_string.split('\n')
  const optLines = props.optimizedPlan.plan_string.split('\n')

  return refLines.map((line, idx) => {
    const normalized = normalizeLine(line)
    const optHasSimilar = optLines.some(optLine => normalizeLine(optLine) === normalized)

    if (!optHasSimilar && normalized.trim()) {
      return `<span class="diff-removed">${escapeHtml(line)}</span>`
    }
    return escapeHtml(line)
  }).join('\n')
})

const highlightedOptimizedPlan = computed(() => {
  const refLines = props.referencePlan.plan_string.split('\n')
  const optLines = props.optimizedPlan.plan_string.split('\n')

  return optLines.map((line, idx) => {
    const normalized = normalizeLine(line)
    const refHasSimilar = refLines.some(refLine => normalizeLine(refLine) === normalized)

    if (!refHasSimilar && normalized.trim()) {
      return `<span class="diff-added">${escapeHtml(line)}</span>`
    }
    return escapeHtml(line)
  }).join('\n')
})

function normalizeLine(line: string): string {
  // Remove leading/trailing whitespace and common prefixes for comparison
  return line.trim()
    .replace(/^\+\-\s+/, '') // Remove tree characters
    .replace(/^\|\s+/, '')
    .replace(/^:\-\s+/, '')
    .replace(/\[.*?\]/, '') // Remove brackets with IDs
    .replace(/#\d+/g, '') // Remove hash IDs
}

function escapeHtml(text: string): string {
  const div = document.createElement('div')
  div.textContent = text
  return div.innerHTML
}
</script>

<style scoped>
.physical-plan-comparison {
  margin-top: 2rem;
  padding: 1.5rem;
  background: var(--color-surface);
  border-radius: 8px;
  border: 1px solid var(--color-border);
}

.physical-plan-comparison h4 {
  margin: 0 0 0.5rem 0;
  font-size: 1.125rem;
  font-weight: 600;
  color: var(--color-text);
}

.description {
  margin: 0 0 1.5rem 0;
  font-size: 0.875rem;
  color: var(--color-text-secondary);
}

.plans-container {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 1.5rem;
  margin-bottom: 1.5rem;
}

.plan-panel {
  background: var(--color-background);
  border-radius: 6px;
  border: 1px solid var(--color-border);
  overflow: hidden;
}

.plan-panel h5 {
  margin: 0;
  padding: 0.75rem 1rem;
  background: var(--color-background-soft);
  border-bottom: 1px solid var(--color-border);
  font-size: 0.9375rem;
  font-weight: 600;
  color: var(--color-text);
}

.plan-content {
  padding: 1rem;
  max-height: 500px;
  overflow: auto;
}

.plan-content pre {
  margin: 0;
  font-family: 'Monaco', 'Courier New', monospace;
  font-size: 0.8125rem;
  line-height: 1.5;
  color: var(--color-text);
}

.plan-content code {
  display: block;
}

/* Diff highlighting */
:deep(.diff-removed) {
  background-color: rgba(239, 68, 68, 0.15);
  color: #dc2626;
  display: block;
  padding-left: 0.5rem;
  border-left: 3px solid #dc2626;
}

:deep(.diff-added) {
  background-color: rgba(34, 197, 94, 0.15);
  color: #16a34a;
  display: block;
  padding-left: 0.5rem;
  border-left: 3px solid #16a34a;
}

.legend {
  display: flex;
  gap: 1.5rem;
  padding-top: 1rem;
  border-top: 1px solid var(--color-border);
  flex-wrap: wrap;
}

.legend-item {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  font-size: 0.875rem;
  color: var(--color-text-secondary);
}

.legend-color {
  width: 20px;
  height: 20px;
  border-radius: 3px;
  border: 1px solid var(--color-border);
}

.legend-color.removed {
  background-color: rgba(239, 68, 68, 0.15);
  border-left: 3px solid #dc2626;
}

.legend-color.added {
  background-color: rgba(34, 197, 94, 0.15);
  border-left: 3px solid #16a34a;
}

.legend-color.common {
  background-color: var(--color-background);
}

/* Responsive design */
@media (max-width: 1024px) {
  .plans-container {
    grid-template-columns: 1fr;
  }
}

/* Scrollbar styling */
.plan-content::-webkit-scrollbar {
  width: 8px;
  height: 8px;
}

.plan-content::-webkit-scrollbar-track {
  background: var(--color-background-soft);
}

.plan-content::-webkit-scrollbar-thumb {
  background: var(--color-border);
  border-radius: 4px;
}

.plan-content::-webkit-scrollbar-thumb:hover {
  background: var(--color-text-secondary);
}
</style>
