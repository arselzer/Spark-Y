<template>
  <div class="row-reduction-viz">
    <h4>Intermediate Row Reduction</h4>
    <p class="description">
      Visual comparison of row counts at each stage. Box heights represent relative row counts.
    </p>

    <div v-if="!hasStageData" class="no-data-message">
      <p>No stage-level row count data available. Metrics may be based on plan estimates.</p>
    </div>

    <div v-else class="visualization-container">
      <!-- Side-by-side comparison -->
      <div class="stage-comparison">
        <div class="comparison-header">
          <div class="column-header reference">
            <strong>Reference</strong>
            <span class="total-badge">{{ formatNumber(totalReferenceRows) }} rows total</span>
          </div>
          <div class="column-header optimized">
            <strong>Optimized</strong>
            <span class="total-badge">{{ formatNumber(totalOptimizedRows) }} rows total</span>
          </div>
        </div>

        <div class="stages-grid">
          <div
            v-for="(_, index) in maxStages"
            :key="index"
            class="stage-row"
          >
            <!-- Reference Stage -->
            <div class="stage-cell">
              <div
                v-if="index < referenceStages.length"
                class="stage-bar reference"
                :style="{ height: getBarHeight(referenceStages[index], maxRowCount) + '%' }"
                :title="`${formatNumber(getStageRows(referenceStages[index]))} rows`"
              >
                <div class="stage-label">{{ getOperatorLabel(referenceStages[index], index) }}</div>
                <div class="stage-count">{{ formatNumber(getStageRows(referenceStages[index])) }}</div>
              </div>
            </div>

            <!-- Stage connector arrows -->
            <div class="stage-connector">
              <svg width="40" height="60" viewBox="0 0 40 60">
                <path d="M 5 30 L 30 30 M 30 30 L 25 25 M 30 30 L 25 35" stroke="var(--color-border)" stroke-width="2" fill="none"/>
              </svg>
            </div>

            <!-- Optimized Stage -->
            <div class="stage-cell">
              <div
                v-if="index < optimizedStages.length"
                class="stage-bar optimized"
                :style="{ height: getBarHeight(optimizedStages[index], maxRowCount) + '%' }"
                :title="`${formatNumber(getStageRows(optimizedStages[index]))} rows`"
              >
                <div class="stage-label">{{ getOperatorLabel(optimizedStages[index], index) }}</div>
                <div class="stage-count">{{ formatNumber(getStageRows(optimizedStages[index])) }}</div>
              </div>
            </div>

            <!-- Difference indicator -->
            <div class="difference-cell">
              <div v-if="index < Math.min(referenceStages.length, optimizedStages.length)" class="difference-badge">
                <template v-if="getRowDifference(index) > 0">
                  <span class="diff-icon">↓</span>
                  <span class="diff-value">-{{ formatNumber(getRowDifference(index)) }}</span>
                </template>
                <template v-else-if="getRowDifference(index) < 0">
                  <span class="diff-icon">↑</span>
                  <span class="diff-value">+{{ formatNumber(Math.abs(getRowDifference(index))) }}</span>
                </template>
                <template v-else>
                  <span class="diff-value">same</span>
                </template>
              </div>
            </div>
          </div>
        </div>
      </div>

      <!-- Summary -->
      <div v-if="rowsAvoided > 0" class="summary-card">
        <div class="summary-icon">💡</div>
        <div class="summary-content">
          <div class="summary-title">Total Intermediate Rows Avoided</div>
          <div class="summary-value">{{ formatNumber(rowsAvoided) }}</div>
          <div class="summary-subtitle">
            {{ reductionPercentage.toFixed(1) }}% reduction compared to reference execution
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'

interface Stage {
  stage_id?: number
  name?: string
  input_records?: number
  output_records?: number
  operator_types?: string[]
}

interface ExecutionMetrics {
  stages?: Stage[]
}

interface Props {
  referenceMetrics: ExecutionMetrics
  optimizedMetrics: ExecutionMetrics
}

const props = defineProps<Props>()

const referenceStages = computed(() => props.referenceMetrics.stages || [])
const optimizedStages = computed(() => props.optimizedMetrics.stages || [])

const hasStageData = computed(() => {
  return referenceStages.value.length > 0 || optimizedStages.value.length > 0
})

const maxStages = computed(() => {
  return Math.max(referenceStages.value.length, optimizedStages.value.length)
})

const maxRowCount = computed(() => {
  const allCounts = [
    ...referenceStages.value.map(s => getStageRows(s)),
    ...optimizedStages.value.map(s => getStageRows(s))
  ]
  return Math.max(...allCounts, 1)
})

const totalReferenceRows = computed(() => {
  return referenceStages.value.reduce((sum, stage) => sum + getStageRows(stage), 0)
})

const totalOptimizedRows = computed(() => {
  return optimizedStages.value.reduce((sum, stage) => sum + getStageRows(stage), 0)
})

const rowsAvoided = computed(() => {
  return Math.max(0, totalReferenceRows.value - totalOptimizedRows.value)
})

const reductionPercentage = computed(() => {
  if (totalReferenceRows.value === 0) return 0
  return (rowsAvoided.value / totalReferenceRows.value) * 100
})

function getStageRows(stage: Stage): number {
  return stage.output_records || stage.input_records || 0
}

function getBarHeight(stage: Stage, max: number): number {
  const rows = getStageRows(stage)
  if (max === 0) return 10
  // Min 10%, max 100%
  return Math.max(10, (rows / max) * 100)
}

function getRowDifference(index: number): number {
  const refRows = index < referenceStages.value.length ? getStageRows(referenceStages.value[index]) : 0
  const optRows = index < optimizedStages.value.length ? getStageRows(optimizedStages.value[index]) : 0
  return refRows - optRows
}

function getOperatorLabel(stage: Stage, index: number): string {
  if (stage.name) {
    const nameParts = stage.name.split(' ')
    return nameParts[0].charAt(0).toUpperCase() + nameParts[0].slice(1)
  }
  if (stage.operator_types && stage.operator_types.length > 0) {
    return stage.operator_types[0].charAt(0).toUpperCase() + stage.operator_types[0].slice(1)
  }
  return `S${index + 1}`
}

function formatNumber(num: number): string {
  if (num >= 1e9) return `${(num / 1e9).toFixed(2)}B`
  if (num >= 1e6) return `${(num / 1e6).toFixed(2)}M`
  if (num >= 1e3) return `${(num / 1e3).toFixed(1)}K`
  return num.toLocaleString()
}
</script>

<style scoped>
.row-reduction-viz {
  margin-top: 2rem;
  padding: 1.5rem;
  background: var(--color-surface);
  border-radius: 8px;
  border: 1px solid var(--color-border);
}

.row-reduction-viz h4 {
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

.no-data-message {
  padding: 1.5rem;
  text-align: center;
  color: var(--color-text-secondary);
  background: var(--color-background);
  border-radius: 6px;
  border: 1px dashed var(--color-border);
}

.visualization-container {
  display: flex;
  flex-direction: column;
  gap: 2rem;
}

.stage-comparison {
  background: var(--color-background);
  border-radius: 6px;
  padding: 1.5rem;
  border: 1px solid var(--color-border);
}

.comparison-header {
  display: grid;
  grid-template-columns: 1fr 40px 1fr 120px;
  gap: 1rem;
  margin-bottom: 1.5rem;
  padding-bottom: 1rem;
  border-bottom: 2px solid var(--color-border);
}

.column-header {
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
}

.column-header strong {
  font-size: 1rem;
  color: var(--color-text);
}

.total-badge {
  font-size: 0.75rem;
  font-weight: 500;
  padding: 0.25rem 0.5rem;
  border-radius: 12px;
  width: fit-content;
}

.column-header.reference .total-badge {
  background: rgba(59, 130, 246, 0.1);
  color: #3b82f6;
}

.column-header.optimized .total-badge {
  background: rgba(16, 185, 129, 0.1);
  color: #10b981;
}

.stages-grid {
  display: flex;
  flex-direction: column;
  gap: 1rem;
}

.stage-row {
  display: grid;
  grid-template-columns: 1fr 40px 1fr 120px;
  gap: 1rem;
  align-items: center;
}

.stage-cell {
  height: 120px;
  display: flex;
  align-items: flex-end;
  justify-content: center;
}

.stage-bar {
  width: 100%;
  min-height: 40px;
  border-radius: 6px 6px 0 0;
  padding: 0.75rem;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: space-between;
  cursor: help;
  transition: all 0.2s ease;
  position: relative;
}

.stage-bar:hover {
  transform: translateY(-4px);
  box-shadow: 0 6px 12px rgba(0, 0, 0, 0.15);
}

.stage-bar.reference {
  background: linear-gradient(180deg, #3b82f6, #2563eb);
  color: white;
}

.stage-bar.optimized {
  background: linear-gradient(180deg, #10b981, #059669);
  color: white;
}

.stage-label {
  font-size: 0.75rem;
  font-weight: 600;
  text-transform: uppercase;
  opacity: 0.9;
  text-align: center;
}

.stage-count {
  font-size: 0.9375rem;
  font-weight: 700;
  font-family: 'Monaco', 'Courier New', monospace;
}

.stage-connector {
  display: flex;
  align-items: center;
  justify-content: center;
}

.difference-cell {
  display: flex;
  align-items: center;
  justify-content: center;
}

.difference-badge {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  padding: 0.5rem 0.75rem;
  background: var(--color-surface);
  border: 1px solid var(--color-border);
  border-radius: 6px;
  font-size: 0.875rem;
  font-weight: 600;
}

.diff-icon {
  font-size: 1.125rem;
  color: #10b981;
}

.diff-value {
  font-family: 'Monaco', 'Courier New', monospace;
  color: var(--color-text);
}

.summary-card {
  display: flex;
  align-items: center;
  gap: 1.5rem;
  padding: 1.5rem;
  background: linear-gradient(135deg, rgba(59, 130, 246, 0.1), rgba(16, 185, 129, 0.1));
  border-radius: 8px;
  border: 1px solid rgba(59, 130, 246, 0.3);
}

.summary-icon {
  font-size: 2.5rem;
}

.summary-content {
  flex: 1;
}

.summary-title {
  font-size: 0.875rem;
  font-weight: 600;
  text-transform: uppercase;
  color: var(--color-text-secondary);
  margin-bottom: 0.5rem;
}

.summary-value {
  font-size: 2rem;
  font-weight: 700;
  color: var(--color-primary);
  font-family: 'Monaco', 'Courier New', monospace;
  margin-bottom: 0.25rem;
}

.summary-subtitle {
  font-size: 0.9375rem;
  color: var(--color-text-secondary);
}

/* Responsive design */
@media (max-width: 1024px) {
  .comparison-header,
  .stage-row {
    grid-template-columns: 1fr 1fr;
    grid-template-rows: auto auto;
  }

  .stage-connector {
    display: none;
  }

  .difference-cell {
    grid-column: 1 / -1;
    margin-top: 0.5rem;
  }
}

@media (max-width: 768px) {
  .summary-card {
    flex-direction: column;
    text-align: center;
  }

  .stage-bar {
    min-height: 60px;
  }

  .stage-cell {
    height: 100px;
  }
}
</style>
