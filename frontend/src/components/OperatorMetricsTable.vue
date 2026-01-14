<template>
  <div class="operator-metrics-table">
    <h4>Operator Comparison</h4>
    <p class="description">Breakdown of physical operators used in query execution.</p>

    <table class="metrics-table">
      <thead>
        <tr>
          <th>Operator</th>
          <th>Reference</th>
          <th>Optimized</th>
          <th>Change</th>
        </tr>
      </thead>
      <tbody>
        <tr v-for="operator in operators" :key="operator.name">
          <td class="operator-name">
            <span class="operator-icon">{{ operator.icon }}</span>
            {{ operator.label }}
          </td>
          <td class="count-cell">{{ operator.referenceCount }}</td>
          <td class="count-cell">{{ operator.optimizedCount }}</td>
          <td :class="['change-cell', getChangeClass(operator.change)]">
            <span v-if="operator.change !== 0">
              {{ operator.change > 0 ? '+' : '' }}{{ operator.change }}
            </span>
            <span v-else>-</span>
          </td>
        </tr>
      </tbody>
      <tfoot>
        <tr class="total-row">
          <td><strong>Total Operators</strong></td>
          <td><strong>{{ totalReference }}</strong></td>
          <td><strong>{{ totalOptimized }}</strong></td>
          <td :class="['change-cell', getChangeClass(totalChange)]">
            <strong>
              <span v-if="totalChange !== 0">
                {{ totalChange > 0 ? '+' : '' }}{{ totalChange }}
              </span>
              <span v-else>-</span>
            </strong>
          </td>
        </tr>
      </tfoot>
    </table>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'

interface ExecutionMetrics {
  operator_counts?: {
    scans?: number
    joins?: number
    aggregates?: number
    exchanges?: number
    sorts?: number
  }
}

interface Props {
  referenceMetrics: ExecutionMetrics
  optimizedMetrics: ExecutionMetrics
}

const props = defineProps<Props>()

interface OperatorInfo {
  name: string
  label: string
  icon: string
  referenceCount: number
  optimizedCount: number
  change: number
}

const operators = computed<OperatorInfo[]>(() => {
  const refCounts = props.referenceMetrics.operator_counts || {}
  const optCounts = props.optimizedMetrics.operator_counts || {}

  const operatorTypes = [
    { name: 'scans', label: 'Scan', icon: '📄' },
    { name: 'joins', label: 'Join', icon: '🔗' },
    { name: 'aggregates', label: 'Aggregate', icon: '📊' },
    { name: 'exchanges', label: 'Exchange (Shuffle)', icon: '🔄' },
    { name: 'sorts', label: 'Sort', icon: '📈' }
  ]

  return operatorTypes.map(op => {
    const refCount = (refCounts as any)[op.name] || 0
    const optCount = (optCounts as any)[op.name] || 0
    return {
      name: op.name,
      label: op.label,
      icon: op.icon,
      referenceCount: refCount,
      optimizedCount: optCount,
      change: optCount - refCount
    }
  })
})

const totalReference = computed(() => {
  return operators.value.reduce((sum, op) => sum + op.referenceCount, 0)
})

const totalOptimized = computed(() => {
  return operators.value.reduce((sum, op) => sum + op.optimizedCount, 0)
})

const totalChange = computed(() => {
  return totalOptimized.value - totalReference.value
})

function getChangeClass(change: number): string {
  if (change < 0) return 'improvement'
  if (change > 0) return 'regression'
  return 'neutral'
}
</script>

<style scoped>
.operator-metrics-table {
  margin-top: 2rem;
  padding: 1.5rem;
  background: var(--color-surface);
  border-radius: 8px;
  border: 1px solid var(--color-border);
}

.operator-metrics-table h4 {
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

.metrics-table {
  width: 100%;
  border-collapse: collapse;
  background: var(--color-background);
  border-radius: 6px;
  overflow: hidden;
}

.metrics-table thead {
  background: var(--color-background-soft);
}

.metrics-table th {
  padding: 0.75rem 1rem;
  text-align: left;
  font-weight: 600;
  color: var(--color-text);
  font-size: 0.875rem;
  border-bottom: 2px solid var(--color-border);
}

.metrics-table tbody tr {
  border-bottom: 1px solid var(--color-border);
  transition: background-color 0.2s ease;
}

.metrics-table tbody tr:hover {
  background: var(--color-background-soft);
}

.metrics-table td {
  padding: 0.75rem 1rem;
  color: var(--color-text);
  font-size: 0.9375rem;
}

.operator-name {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  font-weight: 500;
}

.operator-icon {
  font-size: 1.125rem;
  width: 1.5rem;
  text-align: center;
}

.count-cell {
  text-align: center;
  font-family: 'Monaco', 'Courier New', monospace;
  font-weight: 600;
}

.change-cell {
  text-align: center;
  font-family: 'Monaco', 'Courier New', monospace;
  font-weight: 600;
}

.change-cell.improvement {
  color: #22c55e;
}

.change-cell.regression {
  color: #ef4444;
}

.change-cell.neutral {
  color: var(--color-text-secondary);
}

.total-row {
  background: var(--color-background-soft);
  border-top: 2px solid var(--color-border);
}

.total-row td {
  padding: 1rem;
  font-size: 1rem;
}

/* Responsive design */
@media (max-width: 768px) {
  .metrics-table {
    font-size: 0.8125rem;
  }

  .metrics-table th,
  .metrics-table td {
    padding: 0.5rem;
  }

  .operator-icon {
    font-size: 1rem;
  }
}
</style>
