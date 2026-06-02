<template>
  <div class="data-flow-sankey">
    <h4>Data Flow Visualization</h4>
    <p class="description">
      Data flowing through execution stages. Box height = rows processed per stage (log scale, shared across both plans).
    </p>

    <div class="flows-container">
      <!-- Reference Flow -->
      <div class="flow-panel">
        <h5>Reference Execution</h5>
        <svg :viewBox="`0 0 ${svgWidth} ${svgHeight}`" class="flow-svg">
          <g v-for="(stage, index) in referenceStages" :key="`ref-stage-${index}`">
            <!-- Stage box -->
            <rect
              :x="getStageX(index)"
              :y="getStageY(stage)"
              :width="stageWidth"
              :height="getStageHeight(stage)"
              :fill="getStageColor(stage.operator_types)"
              opacity="0.7"
              stroke="var(--color-border)"
              stroke-width="2"
            />

            <!-- Stage label: id + real row throughput -->
            <text
              :x="getStageX(index) + stageWidth / 2"
              :y="getStageY(stage) + 18"
              text-anchor="middle" fill="white" font-size="12" font-weight="bold"
            >
              Stage {{ stage.stage_id }}
            </text>
            <text
              :x="getStageX(index) + stageWidth / 2"
              :y="getStageY(stage) + 34"
              text-anchor="middle" fill="white" font-size="12"
            >
              {{ formatRows(stageRows(stage)) }} rows
            </text>

            <!-- Flow connector to next stage -->
            <path
              v-if="index < referenceStages.length - 1"
              :d="getFlowPath(index, stage, referenceStages[index + 1])"
              fill="url(#gradient)"
              opacity="0.4"
            />
          </g>

          <!-- Gradient definition -->
          <defs>
            <linearGradient id="gradient" x1="0%" y1="0%" x2="100%" y2="0%">
              <stop offset="0%" style="stop-color: #3b82f6; stop-opacity: 0.6" />
              <stop offset="100%" style="stop-color: #8b5cf6; stop-opacity: 0.6" />
            </linearGradient>
          </defs>
        </svg>
      </div>

      <!-- Optimized Flow -->
      <div class="flow-panel">
        <h5>Optimized Execution</h5>
        <svg :viewBox="`0 0 ${svgWidth} ${svgHeight}`" class="flow-svg">
          <g v-for="(stage, index) in optimizedStages" :key="`opt-stage-${index}`">
            <!-- Stage box -->
            <rect
              :x="getStageX(index)"
              :y="getStageY(stage)"
              :width="stageWidth"
              :height="getStageHeight(stage)"
              :fill="getStageColor(stage.operator_types)"
              opacity="0.7"
              stroke="var(--color-border)"
              stroke-width="2"
            />

            <!-- Stage label: id + real row throughput -->
            <text
              :x="getStageX(index) + stageWidth / 2"
              :y="getStageY(stage) + 18"
              text-anchor="middle" fill="white" font-size="12" font-weight="bold"
            >
              Stage {{ stage.stage_id }}
            </text>
            <text
              :x="getStageX(index) + stageWidth / 2"
              :y="getStageY(stage) + 34"
              text-anchor="middle" fill="white" font-size="12"
            >
              {{ formatRows(stageRows(stage)) }} rows
            </text>

            <!-- Flow connector to next stage -->
            <path
              v-if="index < optimizedStages.length - 1"
              :d="getFlowPath(index, stage, optimizedStages[index + 1])"
              fill="url(#gradient)"
              opacity="0.4"
            />
          </g>
        </svg>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'

interface Stage {
  stage_id: number
  operators?: string[]
  operator_types?: string[]
  has_shuffle?: boolean
  estimated_complexity?: number
  // Real per-stage row metrics (from the Spark REST collector).
  input_records?: number
  output_records?: number
  shuffle_read_records?: number
  shuffle_write_records?: number
}

interface ExecutionMetrics {
  stages?: Stage[]
}

interface Props {
  referenceMetrics: ExecutionMetrics
  optimizedMetrics: ExecutionMetrics
}

const props = defineProps<Props>()

const svgWidth = 800
const svgHeight = 300
const stageWidth = 100
const stageGap = 80
const maxStageHeight = 200
const minStageHeight = 40

const referenceStages = computed(() => props.referenceMetrics.stages || [])
const optimizedStages = computed(() => props.optimizedMetrics.stages || [])

// Real rows moving through a stage: the largest of its record counts. Falls
// back to estimated_complexity only when no real metrics are present.
function stageRows(stage: Stage): number {
  const real = Math.max(
    stage.input_records || 0,
    stage.output_records || 0,
    stage.shuffle_read_records || 0,
    stage.shuffle_write_records || 0
  )
  if (real > 0) return real
  return stage.estimated_complexity || 0
}

// Shared max across BOTH plans so reference and optimised box heights are on
// the same scale (otherwise a small optimised stage looks as tall as a huge
// reference one). Log scale because row counts span many orders of magnitude.
const sharedLogMax = computed(() => {
  const all = [...referenceStages.value, ...optimizedStages.value].map(stageRows)
  return Math.log10(Math.max(...all, 1) + 1)
})

function formatRows(n: number): string {
  if (n >= 1e9) return `${(n / 1e9).toFixed(1)}B`
  if (n >= 1e6) return `${(n / 1e6).toFixed(1)}M`
  if (n >= 1e3) return `${(n / 1e3).toFixed(1)}k`
  return Math.round(n).toString()
}

function getStageX(index: number): number {
  return 50 + index * (stageWidth + stageGap)
}

function getStageY(stage: Stage): number {
  const height = getStageHeight(stage)
  return (svgHeight - height) / 2
}

function getStageHeight(stage: Stage): number {
  // Height proportional to (log) real row throughput, normalised across both
  // plans so the two Sankeys are directly comparable.
  const rows = stageRows(stage)
  const ratio = rows > 0 ? Math.log10(rows + 1) / sharedLogMax.value : 0
  return minStageHeight + (maxStageHeight - minStageHeight) * ratio
}

function getStageColor(operatorTypes?: string[]): string {
  // Color based on dominant operation type
  if (!operatorTypes || operatorTypes.length === 0) return '#6b7280' // Gray
  if (operatorTypes.includes('shuffle')) return '#9333ea' // Purple
  if (operatorTypes.includes('join')) return '#f59e0b'    // Orange
  if (operatorTypes.includes('aggregate')) return '#10b981' // Green
  if (operatorTypes.includes('scan')) return '#3b82f6'     // Blue
  return '#6b7280' // Gray
}

function getFlowPath(
  fromIndex: number,
  fromStage: Stage,
  toStage: Stage
): string {
  const x1 = getStageX(fromIndex) + stageWidth
  const x2 = getStageX(fromIndex + 1)

  const y1Top = getStageY(fromStage)
  const y1Bottom = y1Top + getStageHeight(fromStage)

  const y2Top = getStageY(toStage)
  const y2Bottom = y2Top + getStageHeight(toStage)

  // Create a curved path between stages
  const controlX = (x1 + x2) / 2

  return `
    M ${x1} ${y1Top}
    C ${controlX} ${y1Top}, ${controlX} ${y2Top}, ${x2} ${y2Top}
    L ${x2} ${y2Bottom}
    C ${controlX} ${y2Bottom}, ${controlX} ${y1Bottom}, ${x1} ${y1Bottom}
    Z
  `
}
</script>

<style scoped>
.data-flow-sankey {
  margin-top: 2rem;
  padding: 1.5rem;
  background: var(--color-surface);
  border-radius: 8px;
  border: 1px solid var(--color-border);
}

.data-flow-sankey h4 {
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

.flows-container {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 1.5rem;
}

.flow-panel {
  background: var(--color-background);
  border-radius: 6px;
  border: 1px solid var(--color-border);
  overflow: hidden;
}

.flow-panel h5 {
  margin: 0;
  padding: 0.75rem 1rem;
  background: var(--color-background-soft);
  border-bottom: 1px solid var(--color-border);
  font-size: 0.9375rem;
  font-weight: 600;
  color: var(--color-text);
}

.flow-svg {
  width: 100%;
  height: auto;
  display: block;
}

/* Responsive design */
@media (max-width: 1024px) {
  .flows-container {
    grid-template-columns: 1fr;
  }
}
</style>
