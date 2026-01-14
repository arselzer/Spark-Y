<template>
  <div class="query-plan-viewer">
    <div class="plan-header">
      <h3>Spark Query Plan</h3>
      <div class="plan-controls">
        <select v-model="selectedPlanType" @change="fetchPlan" class="plan-type-select">
          <option value="analyzed">Analyzed Plan</option>
          <option value="logical">Logical Plan</option>
          <option value="optimized">Optimized Plan</option>
        </select>
        <button @click="toggleExpanded" class="btn btn-secondary btn-sm">
          {{ allExpanded ? 'Collapse All' : 'Expand All' }}
        </button>
        <button @click="copyPlanJson" class="btn btn-secondary btn-sm">
          Copy JSON
        </button>
      </div>
    </div>

    <div v-if="loading" class="loading">
      <div class="spinner"></div>
      <p>Loading query plan...</p>
    </div>

    <div v-else-if="error" class="error-message">
      <strong>Error:</strong> {{ error }}
    </div>

    <div v-else-if="planData" class="plan-tree">
      <QueryPlanNode
        :node="planData.plan"
        :depth="0"
        :expanded-nodes="expandedNodes"
        @toggle="toggleNode"
      />
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, watch } from 'vue'
import { hypergraphApi } from '@/services/api'
import type { QueryPlanResponse, QueryPlanNode as PlanNode } from '@/types'
import QueryPlanNode from './QueryPlanNode.vue'

interface Props {
  sql: string
}

const props = defineProps<Props>()

const selectedPlanType = ref<'logical' | 'analyzed' | 'optimized'>('analyzed')
const loading = ref(false)
const error = ref('')
const planData = ref<QueryPlanResponse | null>(null)
const expandedNodes = ref(new Set<string>())
const allExpanded = ref(false)

async function fetchPlan() {
  if (!props.sql) return

  loading.value = true
  error.value = ''

  try {
    planData.value = await hypergraphApi.getQueryPlanJson(props.sql, selectedPlanType.value)

    // Auto-expand top 2 levels
    expandTopLevels(planData.value.plan, 0, 2)
  } catch (err: any) {
    error.value = err.response?.data?.detail || err.message || 'Failed to fetch query plan'
    console.error('Error fetching query plan:', err)
  } finally {
    loading.value = false
  }
}

function expandTopLevels(node: PlanNode, currentDepth: number, maxDepth: number, path: string = '0') {
  if (currentDepth < maxDepth) {
    expandedNodes.value.add(path)
    node.children?.forEach((child, index) => {
      expandTopLevels(child, currentDepth + 1, maxDepth, `${path}-${index}`)
    })
  }
}

function toggleNode(path: string) {
  if (expandedNodes.value.has(path)) {
    expandedNodes.value.delete(path)
  } else {
    expandedNodes.value.add(path)
  }
}

function toggleExpanded() {
  if (allExpanded.value) {
    expandedNodes.value.clear()
  } else {
    expandAllNodes(planData.value!.plan, '0')
  }
  allExpanded.value = !allExpanded.value
}

function expandAllNodes(node: PlanNode, path: string) {
  expandedNodes.value.add(path)
  node.children?.forEach((child, index) => {
    expandAllNodes(child, `${path}-${index}`)
  })
}

function copyPlanJson() {
  if (planData.value) {
    navigator.clipboard.writeText(JSON.stringify(planData.value.plan, null, 2))
      .then(() => alert('Query plan JSON copied to clipboard!'))
      .catch(err => console.error('Failed to copy:', err))
  }
}

// Watch for SQL changes
watch(() => props.sql, () => {
  if (props.sql) {
    fetchPlan()
  }
}, { immediate: true })
</script>

<style scoped>
.query-plan-viewer {
  background: white;
  border-radius: 0.5rem;
  padding: 1.5rem;
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
  margin-top: 2rem;
}

.plan-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 1.5rem;
  padding-bottom: 1rem;
  border-bottom: 2px solid var(--color-border);
}

.plan-header h3 {
  margin: 0;
  font-size: 1.25rem;
}

.plan-controls {
  display: flex;
  gap: 0.5rem;
  align-items: center;
}

.plan-type-select {
  padding: 0.375rem 0.75rem;
  border: 1px solid var(--color-border);
  border-radius: 0.25rem;
  background: white;
  font-size: 0.875rem;
  cursor: pointer;
}

.plan-type-select:focus {
  outline: none;
  border-color: var(--color-primary);
}

.btn-sm {
  padding: 0.375rem 0.75rem;
  font-size: 0.875rem;
}

.loading {
  text-align: center;
  padding: 2rem;
}

.loading .spinner {
  margin: 0 auto 1rem;
}

.loading p {
  color: var(--color-text-secondary);
}

.error-message {
  background: #FEE2E2;
  border: 1px solid #FCA5A5;
  color: #991B1B;
  padding: 1rem;
  border-radius: 0.375rem;
}

.plan-tree {
  font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', 'Consolas', monospace;
  font-size: 0.875rem;
  overflow-x: auto;
}
</style>
