<template>
  <div class="data-flow-sankey">
    <h4>Data Flow Visualization</h4>
    <p class="description">
      Visual representation of data flowing through execution stages. Width indicates estimated data volume.
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
              :y="getStageY(stage, referenceStages)"
              :width="stageWidth"
              :height="getStageHeight(stage, referenceStages)"
              :fill="getStageColor(stage.operator_types)"
              opacity="0.7"
              stroke="var(--color-border)"
              stroke-width="2"
            />

            <!-- Stage label -->
            <text
              :x="getStageX(index) + stageWidth / 2"
              :y="getStageY(stage, referenceStages) + 20"
              text-anchor="middle"
              fill="white"
              font-size="12"
              font-weight="bold"
            >
              Stage {{ stage.stage_id }}
            </text>

            <!-- Flow connector to next stage -->
            <path
              v-if="index < referenceStages.length - 1"
              :d="getFlowPath(
                index,
                stage,
                referenceStages[index + 1],
                referenceStages
              )"
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
              :y="getStageY(stage, optimizedStages)"
              :width="stageWidth"
              :height="getStageHeight(stage, optimizedStages)"
              :fill="getStageColor(stage.operator_types)"
              opacity="0.7"
              stroke="var(--color-border)"
              stroke-width="2"
            />

            <!-- Stage label -->
            <text
              :x="getStageX(index) + stageWidth / 2"
              :y="getStageY(stage, optimizedStages) + 20"
              text-anchor="middle"
              fill="white"
              font-size="12"
              font-weight="bold"
            >
              Stage {{ stage.stage_id }}
            </text>

            <!-- Flow connector to next stage -->
            <path
              v-if="index < optimizedStages.length - 1"
              :d="getFlowPath(
                index,
                stage,
                optimizedStages[index + 1],
                optimizedStages
              )"
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
  estimated_complexity?: number  // Optional - may be undefined for real stage metrics
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

function getStageX(index: number): number {
  return 50 + index * (stageWidth + stageGap)
}

function getStageY(stage: Stage, allStages: Stage[]): number {
  // Center vertically, with height based on complexity
  const height = getStageHeight(stage, allStages)
  return (svgHeight - height) / 2
}

function getStageHeight(stage: Stage, allStages: Stage[]): number {
  // Height proportional to complexity
  // Use default value of 1 if estimated_complexity is undefined
  const maxComplexity = Math.max(...allStages.map(s => s.estimated_complexity || 1), 1)
  const ratio = (stage.estimated_complexity || 1) / maxComplexity
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
  toStage: Stage,
  allStages: Stage[]
): string {
  const x1 = getStageX(fromIndex) + stageWidth
  const x2 = getStageX(fromIndex + 1)

  const y1Top = getStageY(fromStage, allStages)
  const y1Bottom = y1Top + getStageHeight(fromStage, allStages)
  const y1Mid = (y1Top + y1Bottom) / 2

  const y2Top = getStageY(toStage, allStages)
  const y2Bottom = y2Top + getStageHeight(toStage, allStages)
  const y2Mid = (y2Top + y2Bottom) / 2

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
