<template>
  <div class="time-breakdown">
    <h4>Time Breakdown</h4>
    <p class="description">Bar width represents total time. Segments show planning vs execution.</p>

    <div class="breakdown-row">
      <div class="breakdown-label">Reference</div>
      <div class="breakdown-bar-container">
        <div class="breakdown-bar" :style="{ width: referenceBarWidth + '%' }">
          <div
            class="bar-segment planning"
            :style="{ width: referencePercentages.planning + '%' }"
            :title="`Planning: ${formatTime(referenceMetrics.planning_time_ms || 0)}`"
          >
            <span v-if="referencePercentages.planning > 15" class="segment-label">
              {{ formatTime(referenceMetrics.planning_time_ms || 0) }}
            </span>
          </div>
          <div
            class="bar-segment execution"
            :style="{ width: referencePercentages.execution + '%' }"
            :title="`Execution: ${formatTime(referenceMetrics.execution_time_ms)}`"
          >
            <span v-if="referencePercentages.execution > 15" class="segment-label">
              {{ formatTime(referenceMetrics.execution_time_ms) }}
            </span>
          </div>
        </div>
        <div class="total-time">{{ formatTime(referenceTotalTime) }}</div>
      </div>
    </div>

    <div class="breakdown-row">
      <div class="breakdown-label">Optimized</div>
      <div class="breakdown-bar-container">
        <div class="breakdown-bar" :style="{ width: optimizedBarWidth + '%' }">
          <div
            class="bar-segment planning"
            :style="{ width: optimizedPercentages.planning + '%' }"
            :title="`Planning: ${formatTime(optimizedMetrics.planning_time_ms || 0)}`"
          >
            <span v-if="optimizedPercentages.planning > 15" class="segment-label">
              {{ formatTime(optimizedMetrics.planning_time_ms || 0) }}
            </span>
          </div>
          <div
            class="bar-segment execution"
            :style="{ width: optimizedPercentages.execution + '%' }"
            :title="`Execution: ${formatTime(optimizedMetrics.execution_time_ms)}`"
          >
            <span v-if="optimizedPercentages.execution > 15" class="segment-label">
              {{ formatTime(optimizedMetrics.execution_time_ms) }}
            </span>
          </div>
        </div>
        <div class="total-time">{{ formatTime(optimizedTotalTime) }}</div>
      </div>
    </div>

    <!-- Legend -->
    <div class="legend">
      <div class="legend-item">
        <span class="legend-color planning"></span>
        <span>Planning Time</span>
      </div>
      <div class="legend-item">
        <span class="legend-color execution"></span>
        <span>Execution Time</span>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'

interface ExecutionMetrics {
  execution_time_ms: number
  planning_time_ms?: number
}

interface Props {
  referenceMetrics: ExecutionMetrics
  optimizedMetrics: ExecutionMetrics
}

const props = defineProps<Props>()

const referenceTotalTime = computed(() => {
  return props.referenceMetrics.execution_time_ms + (props.referenceMetrics.planning_time_ms || 0)
})

const optimizedTotalTime = computed(() => {
  return props.optimizedMetrics.execution_time_ms + (props.optimizedMetrics.planning_time_ms || 0)
})

const maxTotalTime = computed(() => {
  return Math.max(referenceTotalTime.value, optimizedTotalTime.value)
})

// Bar widths relative to the maximum time
const referenceBarWidth = computed(() => {
  if (maxTotalTime.value === 0) return 100
  return (referenceTotalTime.value / maxTotalTime.value) * 100
})

const optimizedBarWidth = computed(() => {
  if (maxTotalTime.value === 0) return 100
  return (optimizedTotalTime.value / maxTotalTime.value) * 100
})

// Percentages within each bar (planning vs execution)
const referencePercentages = computed(() => {
  const total = referenceTotalTime.value
  if (total === 0) return { planning: 0, execution: 100 }

  const planning = ((props.referenceMetrics.planning_time_ms || 0) / total) * 100
  const execution = (props.referenceMetrics.execution_time_ms / total) * 100

  return { planning, execution }
})

const optimizedPercentages = computed(() => {
  const total = optimizedTotalTime.value
  if (total === 0) return { planning: 0, execution: 100 }

  const planning = ((props.optimizedMetrics.planning_time_ms || 0) / total) * 100
  const execution = (props.optimizedMetrics.execution_time_ms / total) * 100

  return { planning, execution }
})

function formatTime(ms: number): string {
  if (ms < 1000) {
    return `${ms.toFixed(0)}ms`
  }
  return `${(ms / 1000).toFixed(2)}s`
}
</script>

<style scoped>
.time-breakdown {
  margin-top: 2rem;
  padding: 1.5rem;
  background: var(--color-surface);
  border-radius: 8px;
  border: 1px solid var(--color-border);
}

.time-breakdown h4 {
  margin: 0 0 1.5rem 0;
  font-size: 1.125rem;
  font-weight: 600;
  color: var(--color-text);
}

.breakdown-row {
  display: flex;
  align-items: center;
  gap: 1rem;
  margin-bottom: 1rem;
}

.breakdown-label {
  flex: 0 0 100px;
  font-weight: 600;
  font-size: 0.875rem;
  color: var(--color-text);
  text-align: right;
}

.breakdown-bar-container {
  flex: 1;
  display: flex;
  align-items: center;
  gap: 1rem;
}

.breakdown-bar {
  /* Don't use flex: 1 here - it overrides the width set by the component */
  /* width is set dynamically based on execution time */
  height: 40px;
  display: flex;
  background: var(--color-background);
  border-radius: 6px;
  border: 1px solid var(--color-border);
  overflow: hidden;
  min-width: 50px; /* Ensure bar is always visible even for very fast queries */
}

.bar-segment {
  display: flex;
  align-items: center;
  justify-content: center;
  transition: all 0.3s ease;
  cursor: help;
}

.bar-segment.planning {
  background: linear-gradient(90deg, #f59e0b, #f97316);
}

.bar-segment.execution {
  background: linear-gradient(90deg, #3b82f6, #2563eb);
}

.segment-label {
  font-size: 0.75rem;
  font-weight: 600;
  color: white;
  text-shadow: 0 1px 2px rgba(0, 0, 0, 0.3);
}

.total-time {
  flex: 0 0 80px;
  font-family: 'Monaco', 'Courier New', monospace;
  font-size: 0.875rem;
  font-weight: 600;
  color: var(--color-text);
  text-align: right;
}

.legend {
  display: flex;
  gap: 1.5rem;
  margin-top: 1.5rem;
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

.legend-color.planning {
  background: linear-gradient(90deg, #f59e0b, #f97316);
}

.legend-color.execution {
  background: linear-gradient(90deg, #3b82f6, #2563eb);
}

/* Responsive design */
@media (max-width: 768px) {
  .breakdown-row {
    flex-direction: column;
    align-items: stretch;
  }

  .breakdown-label {
    text-align: left;
  }

  .total-time {
    text-align: left;
  }
}
</style>
