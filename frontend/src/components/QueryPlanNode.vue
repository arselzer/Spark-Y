<template>
  <div class="plan-node">
    <div class="node-header" :style="{ paddingLeft: `${depth * 1.5}rem` }">
      <button
        v-if="node.children && node.children.length > 0"
        @click="$emit('toggle', nodePath)"
        class="toggle-btn"
      >
        {{ isExpanded ? '▼' : '▶' }}
      </button>
      <span v-else class="toggle-placeholder"></span>

      <div class="node-info">
        <span class="node-type" :class="`node-${node.nodeType.toLowerCase()}`">
          {{ node.nodeType }}
        </span>
        <span v-if="node.nodeName !== node.nodeType" class="node-name">
          {{ node.nodeName }}
        </span>
      </div>

      <button @click="toggleDetails" class="details-btn">
        {{ showDetails ? 'Hide Details' : 'Show Details' }}
      </button>
    </div>

    <div v-if="showDetails" class="node-details" :style="{ paddingLeft: `${depth * 1.5 + 2}rem` }">
      <!-- Metadata -->
      <div v-if="Object.keys(node.metadata || {}).length > 0" class="metadata-section">
        <strong>Metadata:</strong>
        <div class="metadata-content">
          <div v-for="(value, key) in node.metadata" :key="key" class="metadata-item">
            <span class="metadata-key">{{ key }}:</span>
            <span class="metadata-value">{{ formatValue(value) }}</span>
          </div>
        </div>
      </div>

      <!-- Output Schema -->
      <div v-if="node.output && node.output.length > 0" class="output-section">
        <strong>Output:</strong>
        <div class="output-list">
          <span v-for="(attr, idx) in node.output" :key="idx" class="output-attr">
            {{ attr.name }}: {{ attr.dataType }}
          </span>
        </div>
      </div>
    </div>

    <!-- Children -->
    <div v-if="isExpanded && node.children && node.children.length > 0" class="node-children">
      <QueryPlanNode
        v-for="(child, index) in node.children"
        :key="index"
        :node="child"
        :depth="depth + 1"
        :node-path="`${nodePath}-${index}`"
        :expanded-nodes="expandedNodes"
        @toggle="$emit('toggle', $event)"
      />
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed } from 'vue'
import type { QueryPlanNode as PlanNode } from '@/types'

interface Props {
  node: PlanNode
  depth: number
  nodePath?: string
  expandedNodes: Set<string>
}

const props = withDefaults(defineProps<Props>(), {
  nodePath: '0'
})

const emit = defineEmits<{
  toggle: [path: string]
}>()

const showDetails = ref(false)

const isExpanded = computed(() => props.expandedNodes.has(props.nodePath))

function toggleDetails() {
  showDetails.value = !showDetails.value
}

function formatValue(value: any): string {
  if (Array.isArray(value)) {
    return value.join(', ')
  }
  if (typeof value === 'object' && value !== null) {
    return JSON.stringify(value, null, 2)
  }
  return String(value)
}
</script>

<style scoped>
.plan-node {
  margin: 0.25rem 0;
}

.node-header {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  padding: 0.5rem;
  background: var(--color-bg);
  border-radius: 0.25rem;
  transition: background 0.15s;
}

.node-header:hover {
  background: #E5E7EB;
}

.toggle-btn {
  width: 1.5rem;
  height: 1.5rem;
  padding: 0;
  border: none;
  background: transparent;
  cursor: pointer;
  font-size: 0.75rem;
  color: var(--color-text-secondary);
  flex-shrink: 0;
}

.toggle-btn:hover {
  color: var(--color-primary);
}

.toggle-placeholder {
  width: 1.5rem;
  flex-shrink: 0;
}

.node-info {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  flex: 1;
}

.node-type {
  font-weight: 600;
  padding: 0.125rem 0.5rem;
  border-radius: 0.25rem;
  background: #DBEAFE;
  color: #1E40AF;
  font-size: 0.8125rem;
}

.node-type.node-join {
  background: #FEF3C7;
  color: #92400E;
}

.node-type.node-aggregate {
  background: #DCFCE7;
  color: #166534;
}

.node-type.node-filter {
  background: #FCE7F3;
  color: #9F1239;
}

.node-type.node-project {
  background: #E0E7FF;
  color: #3730A3;
}

.node-type.node-subqueryalias {
  background: #F3E8FF;
  color: #6B21A8;
}

.node-name {
  color: var(--color-text-secondary);
  font-size: 0.8125rem;
}

.details-btn {
  padding: 0.25rem 0.5rem;
  font-size: 0.75rem;
  border: 1px solid var(--color-border);
  background: white;
  border-radius: 0.25rem;
  cursor: pointer;
  color: var(--color-text-secondary);
  transition: all 0.15s;
}

.details-btn:hover {
  border-color: var(--color-primary);
  color: var(--color-primary);
}

.node-details {
  margin-top: 0.5rem;
  padding: 1rem;
  background: #F9FAFB;
  border-left: 3px solid var(--color-primary);
  border-radius: 0.25rem;
  font-size: 0.8125rem;
}

.metadata-section,
.output-section {
  margin-bottom: 0.75rem;
}

.metadata-section:last-child,
.output-section:last-child {
  margin-bottom: 0;
}

.metadata-section strong,
.output-section strong {
  display: block;
  margin-bottom: 0.5rem;
  color: var(--color-text);
}

.metadata-content {
  display: flex;
  flex-direction: column;
  gap: 0.375rem;
}

.metadata-item {
  display: flex;
  gap: 0.5rem;
}

.metadata-key {
  font-weight: 500;
  color: var(--color-text);
  min-width: 8rem;
}

.metadata-value {
  color: var(--color-text-secondary);
  word-break: break-word;
  white-space: pre-wrap;
}

.output-list {
  display: flex;
  flex-wrap: wrap;
  gap: 0.5rem;
}

.output-attr {
  padding: 0.25rem 0.5rem;
  background: white;
  border: 1px solid var(--color-border);
  border-radius: 0.25rem;
  font-family: 'Monaco', 'Menlo', monospace;
  font-size: 0.75rem;
}

.node-children {
  margin-top: 0.25rem;
}
</style>
