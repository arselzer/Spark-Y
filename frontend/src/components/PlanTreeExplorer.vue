<template>
  <div class="plan-tree-explorer">
    <h4>Interactive Plan Tree</h4>
    <p class="description">Click operators to expand/collapse and view details.</p>

    <div class="trees-container">
      <!-- Reference Plan Tree -->
      <div class="tree-panel">
        <h5>Reference Plan</h5>
        <div class="tree-content">
          <div v-if="!referencePlan.plan_tree" class="no-tree">
            <p>JSON plan tree not available. Using text representation.</p>
            <p class="hint">Requires Spark with toJSON() support.</p>
          </div>
          <TreeNode
            v-else
            :node="referencePlan.plan_tree"
            :depth="0"
            :expanded-nodes="expandedReference"
            @toggle="toggleReferenceNode"
          />
        </div>
      </div>

      <!-- Optimized Plan Tree -->
      <div class="tree-panel">
        <h5>Optimized Plan</h5>
        <div class="tree-content">
          <div v-if="!optimizedPlan.plan_tree" class="no-tree">
            <p>JSON plan tree not available. Using text representation.</p>
            <p class="hint">Requires Spark with toJSON() support.</p>
          </div>
          <TreeNode
            v-else
            :node="optimizedPlan.plan_tree"
            :depth="0"
            :expanded-nodes="expandedOptimized"
            @toggle="toggleOptimizedNode"
          />
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref } from 'vue'
import TreeNode from './TreeNode.vue'

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

const expandedReference = ref<Set<string>>(new Set())
const expandedOptimized = ref<Set<string>>(new Set())

function toggleReferenceNode(nodeId: string) {
  if (expandedReference.value.has(nodeId)) {
    expandedReference.value.delete(nodeId)
  } else {
    expandedReference.value.add(nodeId)
  }
}

function toggleOptimizedNode(nodeId: string) {
  if (expandedOptimized.value.has(nodeId)) {
    expandedOptimized.value.delete(nodeId)
  } else {
    expandedOptimized.value.add(nodeId)
  }
}
</script>

<style scoped>
.plan-tree-explorer {
  margin-top: 2rem;
  padding: 1.5rem;
  background: var(--color-surface);
  border-radius: 8px;
  border: 1px solid var(--color-border);
}

.plan-tree-explorer h4 {
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

.trees-container {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 1.5rem;
}

.tree-panel {
  background: var(--color-background);
  border-radius: 6px;
  border: 1px solid var(--color-border);
  overflow: hidden;
}

.tree-panel h5 {
  margin: 0;
  padding: 0.75rem 1rem;
  background: var(--color-background-soft);
  border-bottom: 1px solid var(--color-border);
  font-size: 0.9375rem;
  font-weight: 600;
  color: var(--color-text);
}

.tree-content {
  padding: 1rem;
  max-height: 600px;
  overflow: auto;
}

.no-tree {
  padding: 2rem;
  text-align: center;
  color: var(--color-text-secondary);
}

.no-tree p {
  margin: 0.5rem 0;
}

.no-tree .hint {
  font-size: 0.8125rem;
  font-style: italic;
}

/* Responsive design */
@media (max-width: 1024px) {
  .trees-container {
    grid-template-columns: 1fr;
  }
}

/* Scrollbar styling */
.tree-content::-webkit-scrollbar {
  width: 8px;
}

.tree-content::-webkit-scrollbar-track {
  background: var(--color-background-soft);
}

.tree-content::-webkit-scrollbar-thumb {
  background: var(--color-border);
  border-radius: 4px;
}

.tree-content::-webkit-scrollbar-thumb:hover {
  background: var(--color-text-secondary);
}
</style>
