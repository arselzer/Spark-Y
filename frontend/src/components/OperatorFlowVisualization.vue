<script setup lang="ts">
import { computed } from 'vue'

interface Stage {
  stage_id: number
  name?: string
  operator_types?: string[]
  input_records?: number
  output_records?: number
  shuffle_read_bytes?: number
  shuffle_write_bytes?: number
}

interface Props {
  referenceStages: Stage[]
  optimizedStages: Stage[]
}

const props = defineProps<Props>()

// Helper functions
function formatNumber(num: number): string {
  if (num >= 1_000_000) return `${(num / 1_000_000).toFixed(1)}M`
  if (num >= 1_000) return `${(num / 1_000).toFixed(1)}K`
  return num.toString()
}

function getStageLabel(stage: Stage, index: number): string {
  if (stage.name) {
    return stage.name.split(' ')[0]
  }
  if (stage.operator_types && stage.operator_types.length > 0) {
    return stage.operator_types[0].charAt(0).toUpperCase() + stage.operator_types[0].slice(1)
  }
  return `Stage ${index}`
}

function getRowsProcessed(stage: Stage): number {
  return stage.output_records || stage.input_records || 0
}

// Calculate sum of all intermediate results
const referenceTotalIntermediate = computed(() => {
  if (props.referenceStages.length === 0) return 0
  return props.referenceStages.reduce((sum, stage) =>
    sum + (stage.output_records || stage.input_records || 0), 0
  )
})

const optimizedTotalIntermediate = computed(() => {
  if (props.optimizedStages.length === 0) return 0
  return props.optimizedStages.reduce((sum, stage) =>
    sum + (stage.output_records || stage.input_records || 0), 0
  )
})

const rowsSaved = computed(() => {
  return referenceTotalIntermediate.value - optimizedTotalIntermediate.value
})

// Calculate max rows for arrow thickness scaling
const maxRows = computed(() => {
  let max = 0
  props.referenceStages.forEach(stage => {
    const rows = getRowsProcessed(stage)
    if (rows > max) max = rows
  })
  props.optimizedStages.forEach(stage => {
    const rows = getRowsProcessed(stage)
    if (rows > max) max = rows
  })
  return max
})

// Calculate arrow thickness based on row count
function getArrowThickness(rowCount: number): number {
  if (maxRows.value === 0) return 2
  // Scale from 2 to 6 pixels based on row count
  const minThickness = 2
  const maxThickness = 6
  const ratio = rowCount / maxRows.value
  return minThickness + (maxThickness - minThickness) * ratio
}

// Calculate arrowhead size based on thickness
function getArrowheadSize(thickness: number): { width: number, height: number, refX: number } {
  // Scale arrowhead proportionally with thickness
  const scale = thickness / 2
  return {
    width: 10 * scale,
    height: 10 * scale,
    refX: 5 * scale
  }
}
</script>

<template>
  <div class="operator-flow">
    <div class="flow-header">
      <h3>Stage Row Flow Comparison</h3>
      <p class="description">
        Shows how rows flow through each stage in the execution pipeline
      </p>
    </div>

    <div class="summary-cards">
      <div class="summary-card">
        <div class="card-label">Total Rows Saved</div>
        <div class="card-value highlight">{{ formatNumber(rowsSaved) }}</div>
        <div class="card-sublabel">Intermediate rows avoided</div>
      </div>

      <div class="summary-card">
        <div class="card-label">Reference Pipeline</div>
        <div class="card-value">{{ formatNumber(referenceTotalIntermediate) }}</div>
        <div class="card-sublabel">{{ referenceStages.length }} stages total intermediate results</div>
      </div>

      <div class="summary-card">
        <div class="card-label">Optimized Pipeline</div>
        <div class="card-value">{{ formatNumber(optimizedTotalIntermediate) }}</div>
        <div class="card-sublabel">{{ optimizedStages.length }} stages total intermediate results</div>
      </div>
    </div>

    <div class="flow-comparison">
      <!-- Reference Flow -->
      <div class="flow-column">
        <h4>Reference Execution</h4>
        <div class="flow-pipeline">
          <div
            v-for="(stage, index) in referenceStages"
            :key="`ref-${index}`"
            class="flow-stage"
          >
            <div class="stage-box reference">
              <div class="stage-name">{{ getStageLabel(stage, index) }}</div>
              <div class="stage-rows">
                <span class="row-count">{{ formatNumber(getRowsProcessed(stage)) }}</span>
                <span class="row-label">rows</span>
              </div>
            </div>

            <div v-if="index < referenceStages.length - 1" class="flow-arrow">
              <svg viewBox="0 0 40 60" class="arrow-svg">
                <defs>
                  <marker
                    :id="`arrowhead-ref-${index}`"
                    :markerWidth="getArrowheadSize(getArrowThickness(getRowsProcessed(referenceStages[index + 1]))).width"
                    :markerHeight="getArrowheadSize(getArrowThickness(getRowsProcessed(referenceStages[index + 1]))).height"
                    :refX="getArrowheadSize(getArrowThickness(getRowsProcessed(referenceStages[index + 1]))).refX"
                    refY="3"
                    orient="auto">
                    <polygon points="0 0, 10 3, 0 6" fill="#6b7280" />
                  </marker>
                </defs>
                <line x1="20" y1="0" x2="20" y2="55"
                      stroke="#6b7280"
                      :stroke-width="getArrowThickness(getRowsProcessed(referenceStages[index + 1]))"
                      :marker-end="`url(#arrowhead-ref-${index})`" />
                <text x="25" y="30" class="arrow-label">
                  {{ formatNumber(getRowsProcessed(referenceStages[index + 1])) }}
                </text>
              </svg>
            </div>
          </div>
        </div>
      </div>

      <!-- Optimized Flow -->
      <div class="flow-column">
        <h4>Optimized Execution</h4>
        <div class="flow-pipeline">
          <div
            v-for="(stage, index) in optimizedStages"
            :key="`opt-${index}`"
            class="flow-stage"
          >
            <div class="stage-box optimized">
              <div class="stage-name">{{ getStageLabel(stage, index) }}</div>
              <div class="stage-rows">
                <span class="row-count">{{ formatNumber(getRowsProcessed(stage)) }}</span>
                <span class="row-label">rows</span>
              </div>
            </div>

            <div v-if="index < optimizedStages.length - 1" class="flow-arrow">
              <svg viewBox="0 0 40 60" class="arrow-svg">
                <defs>
                  <marker
                    :id="`arrowhead-opt-${index}`"
                    :markerWidth="getArrowheadSize(getArrowThickness(getRowsProcessed(optimizedStages[index + 1]))).width"
                    :markerHeight="getArrowheadSize(getArrowThickness(getRowsProcessed(optimizedStages[index + 1]))).height"
                    :refX="getArrowheadSize(getArrowThickness(getRowsProcessed(optimizedStages[index + 1]))).refX"
                    refY="3"
                    orient="auto">
                    <polygon points="0 0, 10 3, 0 6" fill="#10b981" />
                  </marker>
                </defs>
                <line x1="20" y1="0" x2="20" y2="55"
                      stroke="#10b981"
                      :stroke-width="getArrowThickness(getRowsProcessed(optimizedStages[index + 1]))"
                      :marker-end="`url(#arrowhead-opt-${index})`" />
                <text x="25" y="30" class="arrow-label">
                  {{ formatNumber(getRowsProcessed(optimizedStages[index + 1])) }}
                </text>
              </svg>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<style scoped>
.operator-flow {
  padding: 1.5rem;
  background: var(--color-background);
  border-radius: 8px;
  border: 1px solid var(--color-border);
}

.flow-header {
  margin-bottom: 1.5rem;
}

.flow-header h3 {
  margin: 0 0 0.5rem 0;
  color: var(--color-text);
  font-size: 1.25rem;
}

.description {
  margin: 0;
  color: var(--color-text-secondary);
  font-size: 0.875rem;
}

.summary-cards {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
  gap: 1rem;
  margin-bottom: 2rem;
}

.summary-card {
  padding: 1rem;
  background: var(--color-background-soft);
  border-radius: 6px;
  border: 1px solid var(--color-border);
}

.card-label {
  font-size: 0.75rem;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: var(--color-text-secondary);
  margin-bottom: 0.5rem;
}

.card-value {
  font-size: 1.5rem;
  font-weight: 700;
  color: var(--color-text);
  margin-bottom: 0.25rem;
}

.card-value.highlight {
  color: var(--color-success);
}

.card-sublabel {
  font-size: 0.875rem;
  color: var(--color-text-secondary);
}

.flow-comparison {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 2rem;
}

.flow-column h4 {
  margin: 0 0 1rem 0;
  color: var(--color-text);
  font-size: 1rem;
  text-align: center;
}

.flow-pipeline {
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 0;
}

.flow-stage {
  display: flex;
  flex-direction: column;
  align-items: center;
  width: 100%;
}

.stage-box {
  padding: 1rem 1.5rem;
  border-radius: 8px;
  border: 2px solid;
  min-width: 180px;
  text-align: center;
  transition: all 0.2s;
}

.stage-box.reference {
  background: rgba(239, 68, 68, 0.1);
  border-color: #ef4444;
}

.stage-box.optimized {
  background: rgba(16, 185, 129, 0.1);
  border-color: #10b981;
}

.stage-box:hover {
  transform: translateY(-2px);
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.1);
}

.stage-name {
  font-weight: 600;
  font-size: 0.875rem;
  color: var(--color-text);
  margin-bottom: 0.5rem;
}

.stage-rows {
  display: flex;
  flex-direction: column;
  gap: 0.25rem;
}

.row-count {
  font-size: 1.25rem;
  font-weight: 700;
  color: var(--color-text);
}

.row-label {
  font-size: 0.75rem;
  color: var(--color-text-secondary);
  text-transform: uppercase;
  letter-spacing: 0.05em;
}

.flow-arrow {
  width: 100%;
  height: 60px;
  display: flex;
  justify-content: center;
}

.arrow-svg {
  width: 40px;
  height: 60px;
}

.arrow-label {
  font-size: 0.75rem;
  fill: var(--color-text-secondary);
}

@media (max-width: 768px) {
  .flow-comparison {
    grid-template-columns: 1fr;
  }

  .summary-cards {
    grid-template-columns: 1fr;
  }
}
</style>
