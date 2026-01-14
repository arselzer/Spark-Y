<template>
  <div class="tree-node" :style="{ marginLeft: depth * 20 + 'px' }">
    <div class="node-header" @click="handleToggle">
      <span class="toggle-icon">
        {{ hasChildren ? (isExpanded ? '▼' : '▶') : '•' }}
      </span>
      <span class="node-type">{{ nodeType }}</span>
      <span v-if="nodeStats" class="node-stats">{{ nodeStats }}</span>
    </div>

    <div v-if="isExpanded && nodeDetails" class="node-details">
      <div v-for="(value, key) in nodeDetails" :key="key" class="detail-row">
        <span class="detail-key">{{ key }}:</span>
        <span class="detail-value">{{ formatValue(value) }}</span>
      </div>
    </div>

    <div v-if="isExpanded && hasChildren" class="node-children">
      <TreeNode
        v-for="(child, index) in children"
        :key="`${nodeId}-child-${index}`"
        :node="child"
        :depth="depth + 1"
        :expanded-nodes="expandedNodes"
        @toggle="(id) => $emit('toggle', id)"
      />
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'

interface Props {
  node: any
  depth: number
  expandedNodes: Set<string>
}

const props = defineProps<Props>()
const emit = defineEmits<{
  toggle: [nodeId: string]
}>()

const nodeId = computed(() => {
  // Generate unique ID from node path
  return `node-${props.depth}-${props.node.class || 'unknown'}-${Math.random().toString(36).substr(2, 9)}`
})

const nodeType = computed(() => {
  if (props.node.class) {
    // Extract simple class name from full path
    const parts = props.node.class.split('.')
    return parts[parts.length - 1]
  }
  return 'Unknown'
})

const nodeStats = computed(() => {
  if (props.node.statistics) {
    const stats = props.node.statistics
    const parts: string[] = []

    if (stats.rowCount !== undefined) {
      parts.push(`${formatNumber(stats.rowCount)} rows`)
    }
    if (stats.sizeInBytes !== undefined) {
      parts.push(formatBytes(stats.sizeInBytes))
    }

    return parts.length > 0 ? `(${parts.join(', ')})` : ''
  }
  return ''
})

const nodeDetails = computed(() => {
  const details: Record<string, any> = {}

  // Include key properties
  if (props.node.expressions) {
    details['Expressions'] = Array.isArray(props.node.expressions)
      ? props.node.expressions.length
      : 'N/A'
  }

  if (props.node.condition) {
    details['Condition'] = truncate(String(props.node.condition), 100)
  }

  if (props.node.output) {
    details['Output Columns'] = Array.isArray(props.node.output)
      ? props.node.output.length
      : 'N/A'
  }

  if (props.node.partitioning) {
    details['Partitioning'] = props.node.partitioning
  }

  return Object.keys(details).length > 0 ? details : null
})

const children = computed(() => {
  if (Array.isArray(props.node.children)) {
    return props.node.children
  }
  if (props.node.child) {
    return [props.node.child]
  }
  return []
})

const hasChildren = computed(() => children.value.length > 0)

const isExpanded = computed(() => props.expandedNodes.has(nodeId.value))

function handleToggle() {
  emit('toggle', nodeId.value)
}

function formatValue(value: any): string {
  if (typeof value === 'object') {
    return JSON.stringify(value)
  }
  return String(value)
}

function formatNumber(num: number): string {
  if (num >= 1e9) return `${(num / 1e9).toFixed(1)}B`
  if (num >= 1e6) return `${(num / 1e6).toFixed(1)}M`
  if (num >= 1e3) return `${(num / 1e3).toFixed(1)}K`
  return String(num)
}

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B'
  const k = 1024
  const sizes = ['B', 'KB', 'MB', 'GB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i]
}

function truncate(str: string, maxLen: number): string {
  return str.length > maxLen ? str.substring(0, maxLen) + '...' : str
}
</script>

<style scoped>
.tree-node {
  font-family: 'Monaco', 'Courier New', monospace;
  font-size: 0.8125rem;
}

.node-header {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  padding: 0.25rem 0.5rem;
  cursor: pointer;
  border-radius: 4px;
  transition: background-color 0.2s ease;
}

.node-header:hover {
  background-color: var(--color-background-soft);
}

.toggle-icon {
  flex: 0 0 16px;
  color: var(--color-text-secondary);
  font-size: 0.75rem;
}

.node-type {
  font-weight: 600;
  color: var(--color-primary);
}

.node-stats {
  color: var(--color-text-secondary);
  font-size: 0.75rem;
}

.node-details {
  margin-left: 36px;
  padding: 0.5rem;
  background: var(--color-background-soft);
  border-left: 2px solid var(--color-border);
  border-radius: 4px;
  margin-top: 0.25rem;
  margin-bottom: 0.5rem;
}

.detail-row {
  display: flex;
  gap: 0.5rem;
  padding: 0.25rem 0;
}

.detail-key {
  font-weight: 600;
  color: var(--color-text);
  min-width: 120px;
}

.detail-value {
  color: var(--color-text-secondary);
  word-break: break-word;
}

.node-children {
  margin-top: 0.25rem;
}
</style>
