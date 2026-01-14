<template>
  <div class="execution-timeline">
    <h4>Execution Timeline</h4>
    <p class="timeline-description">
      Stage execution structure comparison. Width represents actual execution time (when available) or estimated relative complexity based on operator types.
    </p>

    <div class="timeline-container">
      <!-- Reference Execution Timeline -->
      <div class="timeline-row">
        <div class="timeline-label">
          <span class="label-text">Reference</span>
          <span class="label-time">{{ formatTime(referenceMetrics.execution_time_ms) }}</span>
        </div>
        <div class="timeline-track">
          <div
            v-for="stage in referenceStages"
            :key="`ref-${stage.stage_id}`"
            class="stage-bar reference-stage"
            :style="getStageStyle(stage, referenceStages, referenceMetrics.execution_time_ms)"
            :title="getStageTooltip(stage)"
          >
            <span class="stage-label">S{{ stage.stage_id }}</span>
          </div>
        </div>
      </div>

      <!-- Optimized Execution Timeline -->
      <div class="timeline-row">
        <div class="timeline-label">
          <span class="label-text">Optimized</span>
          <span class="label-time">{{ formatTime(optimizedMetrics.execution_time_ms) }}</span>
        </div>
        <div class="timeline-track">
          <div
            v-for="stage in optimizedStages"
            :key="`opt-${stage.stage_id}`"
            class="stage-bar optimized-stage"
            :style="getStageStyle(stage, optimizedStages, optimizedMetrics.execution_time_ms)"
            :title="getStageTooltip(stage)"
          >
            <span class="stage-label">S{{ stage.stage_id }}</span>
          </div>
        </div>
      </div>
    </div>

    <!-- Legend -->
    <div class="timeline-legend">
      <div class="legend-item">
        <span class="legend-color scan"></span>
        <span class="legend-text">Scan</span>
      </div>
      <div class="legend-item">
        <span class="legend-color shuffle"></span>
        <span class="legend-text">Shuffle</span>
      </div>
      <div class="legend-item">
        <span class="legend-color join"></span>
        <span class="legend-text">Join</span>
      </div>
      <div class="legend-item">
        <span class="legend-color aggregate"></span>
        <span class="legend-text">Aggregate</span>
      </div>
      <div class="legend-item">
        <span class="legend-color other"></span>
        <span class="legend-text">Other</span>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'

interface Stage {
  stage_id: number
  // For estimated metrics (from plan)
  operators?: string[]
  operator_types?: string[]
  has_shuffle?: boolean
  estimated_complexity?: number
  // For real metrics (from StageMetricsCollector)
  name?: string
  execution_time_ms?: number
  num_tasks?: number
  shuffle_read_bytes?: number
  shuffle_write_bytes?: number
}

interface ExecutionMetrics {
  execution_time_ms: number
  stages?: Stage[]
}

interface Props {
  referenceMetrics: ExecutionMetrics
  optimizedMetrics: ExecutionMetrics
}

const props = defineProps<Props>()

const referenceStages = computed(() => props.referenceMetrics.stages || [])
const optimizedStages = computed(() => props.optimizedMetrics.stages || [])

function formatTime(ms: number): string {
  if (ms < 1000) {
    return `${ms.toFixed(0)}ms`
  }
  return `${(ms / 1000).toFixed(2)}s`
}

function getStageStyle(stage: Stage, allStages: Stage[], totalTime: number): object {
  let widthPercent = 0

  // Check if we have real timing data
  if (stage.execution_time_ms !== undefined && stage.execution_time_ms > 0) {
    // Use real execution time for width calculation
    const totalRealTime = allStages.reduce((sum, s) => sum + (s.execution_time_ms || 0), 0)
    if (totalRealTime > 0) {
      widthPercent = (stage.execution_time_ms / totalRealTime) * 100
    } else {
      widthPercent = 100 / allStages.length // Equal distribution if no timing
    }
  } else if (stage.estimated_complexity !== undefined) {
    // Fall back to estimated complexity
    const totalComplexity = allStages.reduce((sum, s) => sum + (s.estimated_complexity || 1), 0)
    widthPercent = ((stage.estimated_complexity || 1) / totalComplexity) * 100
  } else {
    // Last resort: equal distribution
    widthPercent = 100 / allStages.length
  }

  // Color based on primary operation type or shuffle presence
  let backgroundColor = '#6b7280' // Default gray

  if (stage.operator_types && stage.operator_types.length > 0) {
    backgroundColor = getStageColor(stage.operator_types)
  } else if (stage.shuffle_read_bytes || stage.shuffle_write_bytes) {
    backgroundColor = '#9333ea' // Purple for shuffle
  } else if (stage.name) {
    // Try to infer from name
    const nameLower = stage.name.toLowerCase()
    if (nameLower.includes('shuffle') || nameLower.includes('exchange')) {
      backgroundColor = '#9333ea' // Purple
    } else if (nameLower.includes('join')) {
      backgroundColor = '#f59e0b' // Orange
    } else if (nameLower.includes('aggregate') || nameLower.includes('group')) {
      backgroundColor = '#10b981' // Green
    } else if (nameLower.includes('scan')) {
      backgroundColor = '#3b82f6' // Blue
    }
  }

  return {
    width: `${widthPercent}%`,
    backgroundColor
  }
}

function getStageColor(operatorTypes: string[]): string {
  // Priority order for coloring
  if (operatorTypes.includes('shuffle')) return '#9333ea' // Purple for shuffle
  if (operatorTypes.includes('join')) return '#f59e0b'    // Orange for join
  if (operatorTypes.includes('aggregate')) return '#10b981' // Green for aggregate
  if (operatorTypes.includes('scan')) return '#3b82f6'     // Blue for scan
  return '#6b7280' // Gray for other
}

function getStageTooltip(stage: Stage): string {
  // Format tooltip based on available data
  let tooltip = `Stage ${stage.stage_id}`

  if (stage.name) {
    tooltip += `\nName: ${stage.name}`
  }

  if (stage.execution_time_ms !== undefined) {
    tooltip += `\nExecution Time: ${formatTime(stage.execution_time_ms)}`
  } else if (stage.estimated_complexity !== undefined) {
    tooltip += `\nEstimated Complexity: ${stage.estimated_complexity.toFixed(1)}`
  }

  if (stage.operators && stage.operators.length > 0) {
    tooltip += `\nOperators: ${stage.operators.join(', ')}`
  }

  if (stage.num_tasks) {
    tooltip += `\nTasks: ${stage.num_tasks}`
  }

  if (stage.shuffle_write_bytes) {
    tooltip += `\nShuffle Write: ${formatBytes(stage.shuffle_write_bytes)}`
  }

  if (stage.shuffle_read_bytes) {
    tooltip += `\nShuffle Read: ${formatBytes(stage.shuffle_read_bytes)}`
  }

  return tooltip
}

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B'
  if (bytes < 0) return '0 B'
  const k = 1024
  const sizes = ['B', 'KB', 'MB', 'GB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i]
}
</script>

<style scoped>
.execution-timeline {
  margin-top: 2rem;
  padding: 1.5rem;
  background: var(--color-surface);
  border-radius: 8px;
  border: 1px solid var(--color-border);
}

.execution-timeline h4 {
  margin: 0 0 0.5rem 0;
  font-size: 1.125rem;
  font-weight: 600;
  color: var(--color-text);
}

.timeline-description {
  margin: 0 0 1.5rem 0;
  font-size: 0.875rem;
  color: var(--color-text-secondary);
}

.timeline-container {
  display: flex;
  flex-direction: column;
  gap: 1.5rem;
}

.timeline-row {
  display: flex;
  gap: 1rem;
  align-items: center;
}

.timeline-label {
  flex: 0 0 120px;
  display: flex;
  flex-direction: column;
  align-items: flex-end;
  gap: 0.25rem;
}

.label-text {
  font-weight: 600;
  font-size: 0.875rem;
  color: var(--color-text);
}

.label-time {
  font-size: 0.75rem;
  color: var(--color-text-secondary);
  font-family: 'Monaco', 'Courier New', monospace;
}

.timeline-track {
  flex: 1;
  height: 40px;
  display: flex;
  gap: 2px;
  background: var(--color-background);
  border-radius: 4px;
  border: 1px solid var(--color-border);
  padding: 2px;
  overflow: hidden;
}

.stage-bar {
  display: flex;
  align-items: center;
  justify-content: center;
  border-radius: 3px;
  cursor: pointer;
  transition: all 0.2s ease;
  position: relative;
  min-width: 30px;
}

.stage-bar:hover {
  opacity: 0.8;
  transform: translateY(-2px);
  box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
}

.stage-label {
  font-size: 0.75rem;
  font-weight: 600;
  color: white;
  text-shadow: 0 1px 2px rgba(0, 0, 0, 0.3);
}

.reference-stage {
  border: 2px solid rgba(0, 0, 0, 0.1);
}

.optimized-stage {
  border: 2px solid rgba(255, 255, 255, 0.2);
}

.timeline-legend {
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
}

.legend-color {
  width: 20px;
  height: 20px;
  border-radius: 3px;
  border: 1px solid var(--color-border);
}

.legend-color.scan {
  background-color: #3b82f6;
}

.legend-color.shuffle {
  background-color: #9333ea;
}

.legend-color.join {
  background-color: #f59e0b;
}

.legend-color.aggregate {
  background-color: #10b981;
}

.legend-color.other {
  background-color: #6b7280;
}

.legend-text {
  font-size: 0.875rem;
  color: var(--color-text-secondary);
}

/* Responsive design */
@media (max-width: 768px) {
  .timeline-row {
    flex-direction: column;
    align-items: stretch;
  }

  .timeline-label {
    flex-direction: row;
    justify-content: space-between;
    align-items: center;
  }

  .timeline-track {
    height: 50px;
  }

  .timeline-legend {
    gap: 1rem;
  }
}
</style>
