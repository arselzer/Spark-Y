<template>
  <div class="performance-comparison-wrapper" :style="{ maxWidth: containerWidth + 'px' }">
    <!-- Width resize handle (right edge) -->
    <div
      class="width-resize-handle"
      @mousedown="startWidthResize"
      title="Drag to resize width"
    ></div>
  <div class="performance-comparison">
    <!-- Validation error message -->
    <div v-if="!hasValidMetrics" class="error-box">
      <strong>⚠️ Invalid Metrics Data</strong>
      <p>Unable to display performance comparison due to missing or invalid metrics data.</p>
      <details>
        <summary>Debug Information</summary>
        <pre>{{ {
          hasReferenceMetrics: !!referenceMetrics,
          hasOptimizedMetrics: !!optimizedMetrics,
          referenceExecutionTime: referenceMetrics?.execution_time_ms,
          optimizedExecutionTime: optimizedMetrics?.execution_time_ms
        } }}</pre>
      </details>
    </div>

    <div v-else>
    <h2>Performance Analysis</h2>

    <!-- Executive Summary Cards -->
    <div class="summary-cards">
      <div class="metric-card speedup">
        <div class="metric-label">Speedup</div>
        <div class="metric-value">{{ speedup.toFixed(2) }}x</div>
        <div class="metric-change">{{ ((speedup - 1) * 100).toFixed(0) }}% faster</div>
      </div>

      <div class="metric-card memory">
        <div class="metric-label">Shuffle Reduced</div>
        <div class="metric-value">{{ formatBytes(shuffleReduced) }}</div>
        <div class="metric-change" v-if="shuffleReductionPct > 0">{{ shuffleReductionPct }}% less</div>
      </div>

      <div class="metric-card stages">
        <div class="metric-label">Stages</div>
        <div class="metric-value">{{ stagesReduced >= 0 ? stagesReduced : 0 }}</div>
        <div class="metric-change">{{ referenceMetrics.num_stages }} → {{ optimizedMetrics.num_stages }}</div>
      </div>
    </div>

    <!-- Side-by-Side Execution Breakdown -->
    <div class="execution-breakdown collapsible-section">
      <h3 class="section-header" @click="toggleSection('breakdown')">
        <span class="toggle-icon">{{ sectionsExpanded.breakdown ? '▼' : '▶' }}</span>
        Execution Breakdown
      </h3>

      <div v-if="sectionsExpanded.breakdown" class="section-content">
      <div class="breakdown-container">
        <!-- Reference Execution -->
        <div class="execution-column">
          <h4>Reference <span class="badge badge-reference">Baseline</span></h4>

          <div class="time-info">
            <div class="time-label">Total Time</div>
            <div class="time-value">{{ referenceMetrics.execution_time_ms.toFixed(2) }} ms</div>
          </div>

          <div class="shuffle-info" v-if="referenceMetrics.shuffle_write_bytes > 0">
            <div class="info-label">Shuffle Write</div>
            <div class="info-value">{{ formatBytes(referenceMetrics.shuffle_write_bytes) }}</div>
          </div>

          <div class="shuffle-info" v-if="referenceMetrics.num_stages > 0">
            <div class="info-label">Stages</div>
            <div class="info-value">{{ referenceMetrics.num_stages }}</div>
          </div>
        </div>

        <!-- Optimized Execution -->
        <div class="execution-column optimized">
          <h4>Optimized <span class="badge badge-optimized">Yannakakis</span></h4>

          <div class="time-info">
            <div class="time-label">Total Time</div>
            <div class="time-value improved">{{ optimizedMetrics.execution_time_ms.toFixed(2) }} ms</div>
          </div>

          <div class="shuffle-info" v-if="optimizedMetrics.shuffle_write_bytes >= 0">
            <div class="info-label">Shuffle Write</div>
            <div class="info-value improved">{{ formatBytes(optimizedMetrics.shuffle_write_bytes) }}</div>
          </div>

          <div class="shuffle-info" v-if="optimizedMetrics.num_stages > 0">
            <div class="info-label">Stages</div>
            <div class="info-value improved">{{ optimizedMetrics.num_stages }}</div>
          </div>
        </div>
      </div>
      </div>
    </div>

    <!-- Detailed Metrics Table -->
    <div class="detailed-metrics collapsible-section">
      <h3 class="section-header" @click="toggleSection('metrics')">
        <span class="toggle-icon">{{ sectionsExpanded.metrics ? '▼' : '▶' }}</span>
        Detailed Metrics
      </h3>

      <div v-if="sectionsExpanded.metrics" class="section-content">

      <table class="metrics-table">
        <thead>
          <tr>
            <th>Metric</th>
            <th>Reference</th>
            <th>Optimized</th>
            <th>Difference</th>
          </tr>
        </thead>
        <tbody>
          <tr>
            <td>Execution Time</td>
            <td>{{ referenceMetrics.execution_time_ms.toFixed(2) }} ms</td>
            <td>{{ optimizedMetrics.execution_time_ms.toFixed(2) }} ms</td>
            <td class="improvement">
              {{ (referenceMetrics.execution_time_ms - optimizedMetrics.execution_time_ms).toFixed(2) }} ms
            </td>
          </tr>
          <tr v-if="referenceMetrics.planning_time_ms">
            <td>Planning Time</td>
            <td>{{ referenceMetrics.planning_time_ms.toFixed(2) }} ms</td>
            <td>{{ optimizedMetrics.planning_time_ms?.toFixed(2) || 'N/A' }} ms</td>
            <td>-</td>
          </tr>
          <tr>
            <td>Number of Stages</td>
            <td>{{ referenceMetrics.num_stages }}</td>
            <td>{{ optimizedMetrics.num_stages }}</td>
            <td class="improvement">{{ stagesReduced }} fewer</td>
          </tr>
          <tr v-if="referenceMetrics.shuffle_read_bytes > 0">
            <td>Shuffle Read</td>
            <td>{{ formatBytes(referenceMetrics.shuffle_read_bytes) }}</td>
            <td>{{ formatBytes(optimizedMetrics.shuffle_read_bytes) }}</td>
            <td class="improvement">{{ formatBytes(referenceMetrics.shuffle_read_bytes - optimizedMetrics.shuffle_read_bytes) }}</td>
          </tr>
          <tr v-if="referenceMetrics.shuffle_write_bytes > 0">
            <td>Shuffle Write</td>
            <td>{{ formatBytes(referenceMetrics.shuffle_write_bytes) }}</td>
            <td>{{ formatBytes(optimizedMetrics.shuffle_write_bytes) }}</td>
            <td class="improvement">{{ formatBytes(referenceMetrics.shuffle_write_bytes - optimizedMetrics.shuffle_write_bytes) }}</td>
          </tr>
          <tr v-if="referenceMetrics.num_tasks > 0">
            <td>Number of Tasks</td>
            <td>{{ referenceMetrics.num_tasks }}</td>
            <td>{{ optimizedMetrics.num_tasks }}</td>
            <td class="improvement">{{ referenceMetrics.num_tasks - optimizedMetrics.num_tasks }}</td>
          </tr>
          <tr v-if="referenceMetrics.total_input_rows > 0">
            <td>Input Rows</td>
            <td>{{ referenceMetrics.total_input_rows.toLocaleString() }}</td>
            <td>{{ optimizedMetrics.total_input_rows.toLocaleString() }}</td>
            <td>-</td>
          </tr>
          <tr v-if="referenceMetrics.total_output_rows > 0">
            <td>Output Rows</td>
            <td>{{ referenceMetrics.total_output_rows.toLocaleString() }}</td>
            <td>{{ optimizedMetrics.total_output_rows.toLocaleString() }}</td>
            <td>-</td>
          </tr>
        </tbody>
      </table>
      </div>
    </div>

    <!-- Time Breakdown -->
    <div class="collapsible-section">
      <h3 class="section-header" @click="toggleSection('timeBreakdown')">
        <span class="toggle-icon">{{ sectionsExpanded.timeBreakdown ? '▼' : '▶' }}</span>
        Time Breakdown
      </h3>
      <div v-if="sectionsExpanded.timeBreakdown" class="section-content">
    <TimeBreakdown
      :reference-metrics="referenceMetrics"
      :optimized-metrics="optimizedMetrics"
    />
      </div>
    </div>

    <!-- Operator Metrics Table -->
    <div class="collapsible-section">
      <h3 class="section-header" @click="toggleSection('operatorMetrics')">
        <span class="toggle-icon">{{ sectionsExpanded.operatorMetrics ? '▼' : '▶' }}</span>
        Operator Comparison
      </h3>
      <div v-if="sectionsExpanded.operatorMetrics" class="section-content">
    <OperatorMetricsTable
      :reference-metrics="referenceMetrics"
      :optimized-metrics="optimizedMetrics"
    />
      </div>
    </div>

    <!-- Operator DAG Visualization -->
    <div class="collapsible-section">
      <h3 class="section-header" @click="toggleSection('operatorDag')">
        <span class="toggle-icon">{{ sectionsExpanded.operatorDag ? '▼' : '▶' }}</span>
        Operator Flow DAG
        <label class="layout-toggle" @click.stop title="Toggle between side-by-side and stacked layout">
          <input type="checkbox" v-model="dagLayoutStacked" />
          <span>Stack vertically</span>
        </label>
      </h3>
      <div v-if="sectionsExpanded.operatorDag" class="section-content resizable-section" :style="{ height: sectionHeights.operatorDag + 'px' }">
        <div :class="['dag-comparison', { 'dag-stacked': dagLayoutStacked }]">
          <OperatorDagVisualization
            v-if="referenceMetrics.operator_metrics && referenceMetrics.operator_metrics.length > 0"
            :operator-metrics="referenceMetrics.operator_metrics"
            title="Reference Execution"
          />
          <div v-else class="no-operator-data">
            <p>No operator metrics available for reference execution</p>
          </div>

          <OperatorDagVisualization
            v-if="optimizedMetrics.operator_metrics && optimizedMetrics.operator_metrics.length > 0"
            :operator-metrics="optimizedMetrics.operator_metrics"
            title="Optimized Execution"
          />
          <div v-else class="no-operator-data">
            <p>No operator metrics available for optimized execution</p>
          </div>
        </div>
        <!-- Section resize handle -->
        <div class="section-resize-handle" @mousedown="startSectionResize($event, 'operatorDag')" title="Drag to resize section height"></div>
      </div>
    </div>

    <!-- Execution Timeline (moved below DAG) -->
    <div class="collapsible-section">
      <h3 class="section-header" @click="toggleSection('timeline')">
        <span class="toggle-icon">{{ sectionsExpanded.timeline ? '▼' : '▶' }}</span>
        Execution Timeline
      </h3>
      <div v-if="sectionsExpanded.timeline" class="section-content resizable-section" :style="{ height: sectionHeights.timeline + 'px' }">
        <ExecutionTimeline
          v-if="hasStageInfo"
          :reference-metrics="referenceMetrics"
          :optimized-metrics="optimizedMetrics"
        />
        <!-- Section resize handle -->
        <div class="section-resize-handle" @mousedown="startSectionResize($event, 'timeline')" title="Drag to resize section height"></div>
      </div>
    </div>

    <!-- Hypergraph Visualization -->
    <div v-if="hasHypergraph" class="collapsible-section">
      <h3 class="section-header" @click="toggleSection('hypergraph')">
        <span class="toggle-icon">{{ sectionsExpanded.hypergraph ? '▼' : '▶' }}</span>
        Hypergraph Visualization
      </h3>
      <div v-if="sectionsExpanded.hypergraph" class="section-content resizable-section" :style="{ height: sectionHeights.hypergraph + 'px' }">
        <!-- Visualization mode tabs -->
        <div class="hypergraph-viz-tabs">
          <button
            :class="['viz-tab', { active: hypergraphVizMode === 'graph' }]"
            @click="hypergraphVizMode = 'graph'"
          >
            Graph View
          </button>
          <button
            :class="['viz-tab', { active: hypergraphVizMode === 'graph-bubbles' }]"
            @click="hypergraphVizMode = 'graph-bubbles'"
          >
            Graph + Pie Charts
          </button>
          <button
            :class="['viz-tab', { active: hypergraphVizMode === 'bubble-sets' }]"
            @click="hypergraphVizMode = 'bubble-sets'"
          >
            Bubble Sets (Convex Hulls)
          </button>
        </div>
        <div class="hypergraph-container">
          <HypergraphViewer
            :visualization-data="visualizationData"
            :show-bubble-sets="hypergraphVizMode === 'graph-bubbles'"
            :show-convex-hulls="hypergraphVizMode === 'bubble-sets'"
          />
        </div>
        <!-- Section resize handle -->
        <div class="section-resize-handle" @mousedown="startSectionResize($event, 'hypergraph')" title="Drag to resize section height"></div>
      </div>
    </div>

    <!-- Stage Row Flow -->
    <div class="collapsible-section">
      <h3 class="section-header" @click="toggleSection('operatorFlow')">
        <span class="toggle-icon">{{ sectionsExpanded.operatorFlow ? '▼' : '▶' }}</span>
        Stage Row Flow
      </h3>
      <div v-if="sectionsExpanded.operatorFlow" class="section-content">
    <OperatorFlowVisualization
      :reference-stages="referenceMetrics.stages || []"
      :optimized-stages="optimizedMetrics.stages || []"
    />
      </div>
    </div>

    <!-- Data Flow Sankey -->
    <div class="collapsible-section">
      <h3 class="section-header" @click="toggleSection('dataFlow')">
        <span class="toggle-icon">{{ sectionsExpanded.dataFlow ? '▼' : '▶' }}</span>
        Data Flow Visualization
      </h3>
      <div v-if="sectionsExpanded.dataFlow" class="section-content resizable-section" :style="{ height: sectionHeights.dataFlow + 'px' }">
    <DataFlowSankey
      v-if="hasStageInfo"
      :reference-metrics="referenceMetrics"
      :optimized-metrics="optimizedMetrics"
    />
        <!-- Section resize handle -->
        <div class="section-resize-handle" @mousedown="startSectionResize($event, 'dataFlow')" title="Drag to resize section height"></div>
      </div>
    </div>

    <!-- Physical Plan Comparison -->
    <div class="collapsible-section">
      <h3 class="section-header" @click="toggleSection('planComparison')">
        <span class="toggle-icon">{{ sectionsExpanded.planComparison ? '▼' : '▶' }}</span>
        Physical Plan Comparison
      </h3>
      <div v-if="sectionsExpanded.planComparison" class="section-content">
    <PhysicalPlanComparison
      v-if="referencePlan && optimizedPlan"
      :reference-plan="referencePlan"
      :optimized-plan="optimizedPlan"
    />
      </div>
    </div>

    <!-- Plan Tree Explorer -->
    <div class="collapsible-section">
      <h3 class="section-header" @click="toggleSection('planTree')">
        <span class="toggle-icon">{{ sectionsExpanded.planTree ? '▼' : '▶' }}</span>
        Plan Tree Explorer
      </h3>
      <div v-if="sectionsExpanded.planTree" class="section-content">
    <PlanTreeExplorer
      v-if="referencePlan && optimizedPlan"
      :reference-plan="referencePlan"
      :optimized-plan="optimizedPlan"
    />
      </div>
    </div>
    </div> <!-- end v-else -->
  </div>
  </div> <!-- end wrapper -->
</template>

<script setup lang="ts">
import { computed, ref, watch } from 'vue'
import { usePreferences } from '@/composables/usePreferences'
import ExecutionTimeline from './ExecutionTimeline.vue'
import TimeBreakdown from './TimeBreakdown.vue'
import OperatorFlowVisualization from './OperatorFlowVisualization.vue'
import OperatorMetricsTable from './OperatorMetricsTable.vue'
import OperatorDagVisualization from './OperatorDagVisualization.vue'
import DataFlowSankey from './DataFlowSankey.vue'
import PhysicalPlanComparison from './PhysicalPlanComparison.vue'
import PlanTreeExplorer from './PlanTreeExplorer.vue'
import HypergraphViewer from './HypergraphViewer.vue'

interface Props {
  referenceMetrics: any
  optimizedMetrics: any
  referencePlan?: any
  optimizedPlan?: any
  visualizationData?: any
  hypergraphData?: any
}

const props = defineProps<Props>()

// Load preferences
const { preferences, setDagLayoutStacked, setSectionExpanded } = usePreferences()

// Hypergraph visualization mode
const hypergraphVizMode = ref<'graph' | 'graph-bubbles' | 'bubble-sets'>('graph')

// Check if hypergraph data is available
const hasHypergraph = computed(() => {
  return props.visualizationData &&
         props.visualizationData.stats &&
         (props.visualizationData.stats.num_nodes > 0 || props.visualizationData.stats.num_edges > 0)
})

// Collapsible section state - initialize from preferences
const sectionsExpanded = ref({ ...preferences.value.expandedSections })

// DAG layout toggle - initialize from preferences
const dagLayoutStacked = ref(preferences.value.dagLayoutStacked)

// Watch for changes and save to preferences
watch(dagLayoutStacked, (newValue) => setDagLayoutStacked(newValue))
watch(sectionsExpanded, (newValue) => {
  Object.entries(newValue).forEach(([key, value]) => {
    setSectionExpanded(key, value)
  })
}, { deep: true })

function toggleSection(section: keyof typeof sectionsExpanded.value) {
  sectionsExpanded.value[section] = !sectionsExpanded.value[section]
}

const speedup = computed(() => {
  if (!props.optimizedMetrics?.execution_time_ms || props.optimizedMetrics.execution_time_ms === 0) return 1
  if (!props.referenceMetrics?.execution_time_ms) return 1
  return props.referenceMetrics.execution_time_ms / props.optimizedMetrics.execution_time_ms
})

const stagesReduced = computed(() => {
  const refStages = props.referenceMetrics?.num_stages ?? 0
  const optStages = props.optimizedMetrics?.num_stages ?? 0
  return refStages - optStages
})

const shuffleReduced = computed(() => {
  const refShuffle = props.referenceMetrics?.shuffle_write_bytes ?? 0
  const optShuffle = props.optimizedMetrics?.shuffle_write_bytes ?? 0
  return refShuffle - optShuffle
})

const shuffleReductionPct = computed(() => {
  const refShuffle = props.referenceMetrics?.shuffle_write_bytes ?? 0
  if (refShuffle === 0) return 0
  return ((shuffleReduced.value / refShuffle) * 100).toFixed(1)
})

const hasStageInfo = computed(() => {
  return (
    props.referenceMetrics?.stages &&
    props.optimizedMetrics?.stages &&
    props.referenceMetrics.stages.length > 0 &&
    props.optimizedMetrics.stages.length > 0
  )
})

// Validation: Check if we have valid metrics data
const hasValidMetrics = computed(() => {
  return (
    props.referenceMetrics &&
    props.optimizedMetrics &&
    typeof props.referenceMetrics.execution_time_ms === 'number' &&
    typeof props.optimizedMetrics.execution_time_ms === 'number'
  )
})

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B'
  if (bytes < 0) return '0 B'
  const k = 1024
  const sizes = ['B', 'KB', 'MB', 'GB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return parseFloat((bytes / Math.pow(k, i)).toFixed(2)) + ' ' + sizes[i]
}

// ===== Resizable Container Width =====
const containerWidth = ref(1800)

// ===== Resizable Section Heights =====
const sectionHeights = ref<Record<string, number>>({
  operatorDag: 1000,
  timeline: 300,
  hypergraph: 500,
  dataFlow: 400
})

function startSectionResize(e: MouseEvent, sectionKey: string) {
  e.preventDefault()
  const startY = e.clientY
  const startHeight = sectionHeights.value[sectionKey] || 300

  function onMouseMove(moveEvent: MouseEvent) {
    const delta = moveEvent.clientY - startY
    const newHeight = Math.max(150, Math.min(1200, startHeight + delta))
    sectionHeights.value[sectionKey] = newHeight
  }

  function onMouseUp() {
    document.removeEventListener('mousemove', onMouseMove)
    document.removeEventListener('mouseup', onMouseUp)
    document.body.style.cursor = ''
    document.body.style.userSelect = ''
  }

  document.addEventListener('mousemove', onMouseMove)
  document.addEventListener('mouseup', onMouseUp)
  document.body.style.cursor = 'ns-resize'
  document.body.style.userSelect = 'none'
}

function startWidthResize(e: MouseEvent) {
  e.preventDefault()
  const startX = e.clientX
  const startWidth = containerWidth.value

  function onMouseMove(moveEvent: MouseEvent) {
    const delta = moveEvent.clientX - startX
    const newWidth = Math.max(600, Math.min(4000, startWidth + delta))
    containerWidth.value = newWidth
  }

  function onMouseUp() {
    document.removeEventListener('mousemove', onMouseMove)
    document.removeEventListener('mouseup', onMouseUp)
    document.body.style.cursor = ''
    document.body.style.userSelect = ''
  }

  document.addEventListener('mousemove', onMouseMove)
  document.addEventListener('mouseup', onMouseUp)
  document.body.style.cursor = 'ew-resize'
  document.body.style.userSelect = 'none'
}
</script>

<style scoped>
/* ===== Resizable Wrapper ===== */
.performance-comparison-wrapper {
  position: relative;
  margin: 0 auto;
  width: 100%;
}

.width-resize-handle {
  position: absolute;
  top: 0;
  right: -6px;
  width: 12px;
  height: 100%;
  cursor: ew-resize;
  background: transparent;
  z-index: 10;
  transition: background 0.2s;
}

.width-resize-handle:hover {
  background: linear-gradient(
    to right,
    transparent 0%,
    var(--color-primary) 40%,
    var(--color-primary) 60%,
    transparent 100%
  );
  opacity: 0.6;
}

.width-resize-handle:active {
  background: linear-gradient(
    to right,
    transparent 0%,
    var(--color-primary) 40%,
    var(--color-primary) 60%,
    transparent 100%
  );
  opacity: 0.8;
}

/* ===== Resizable Sections ===== */
.resizable-section {
  position: relative;
  overflow: auto;
  min-height: 150px;
}

.section-resize-handle {
  position: absolute;
  bottom: 0;
  left: 0;
  right: 0;
  height: 8px;
  cursor: ns-resize;
  background: transparent;
  transition: background 0.2s;
  z-index: 5;
}

.section-resize-handle:hover {
  background: linear-gradient(
    to bottom,
    transparent 0%,
    var(--color-primary) 40%,
    var(--color-primary) 60%,
    transparent 100%
  );
  opacity: 0.5;
}

.section-resize-handle:active {
  background: linear-gradient(
    to bottom,
    transparent 0%,
    var(--color-primary) 40%,
    var(--color-primary) 60%,
    transparent 100%
  );
  opacity: 0.7;
}

.performance-comparison {
  padding: 2rem;
  background: var(--color-background);
  border-radius: 8px;
  border: 1px solid var(--color-border);
}

.performance-comparison h2 {
  margin-top: 0;
  margin-bottom: 2rem;
  color: var(--color-text);
}

/* Collapsible sections */
.collapsible-section {
  margin-bottom: 2rem;
}

.section-header {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  cursor: pointer;
  user-select: none;
  padding: 0.75rem 0;
  transition: color 0.2s ease;
  margin-bottom: 0;
}

.section-header:hover {
  color: var(--color-primary);
}

.toggle-icon {
  font-size: 0.875rem;
  color: var(--color-text-secondary);
  transition: transform 0.2s ease;
  width: 1rem;
  display: inline-block;
}

.section-content {
  margin-top: 1rem;
  animation: slideDown 0.3s ease;
}

@keyframes slideDown {
  from {
    opacity: 0;
    transform: translateY(-10px);
  }
  to {
    opacity: 1;
    transform: translateY(0);
  }
}

/* Error handling */
.error-box {
  background: rgba(239, 68, 68, 0.1);
  border: 2px solid #ef4444;
  border-radius: 8px;
  padding: 1.5rem;
  margin-bottom: 1.5rem;
}

.error-box strong {
  display: block;
  color: #ef4444;
  font-size: 1.125rem;
  margin-bottom: 0.5rem;
}

.error-box p {
  color: var(--color-text);
  margin-bottom: 1rem;
}

.error-box details {
  background: var(--color-background);
  border: 1px solid var(--color-border);
  border-radius: 4px;
  padding: 0.75rem;
  margin-top: 1rem;
}

.error-box summary {
  cursor: pointer;
  font-weight: 600;
  color: var(--color-text-secondary);
  user-select: none;
}

.error-box summary:hover {
  color: var(--color-text);
}

.error-box pre {
  margin-top: 0.75rem;
  font-size: 0.8125rem;
  color: var(--color-text-secondary);
  overflow-x: auto;
}

.summary-cards {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
  gap: 1.5rem;
  margin-bottom: 3rem;
}

.metric-card {
  background: var(--color-background-soft);
  border: 2px solid var(--color-border);
  border-radius: 12px;
  padding: 1.5rem;
  text-align: center;
  transition: transform 0.2s ease, box-shadow 0.2s ease;
}

.metric-card:hover {
  transform: translateY(-4px);
  box-shadow: 0 8px 16px rgba(0, 0, 0, 0.1);
}

.metric-card.speedup {
  border-color: #22c55e;
}

.metric-card.memory {
  border-color: #3b82f6;
}

.metric-card.stages {
  border-color: #f59e0b;
}

.metric-card.operators {
  border-color: #8b5cf6;
}

.metric-label {
  font-size: 0.875rem;
  color: var(--color-text-secondary);
  font-weight: 500;
  margin-bottom: 0.5rem;
}

.metric-value {
  font-size: 2.5rem;
  font-weight: 700;
  color: var(--color-text);
  margin-bottom: 0.25rem;
}

.metric-change {
  font-size: 0.9375rem;
  color: #22c55e;
  font-weight: 600;
}

.execution-breakdown {
  margin-bottom: 3rem;
}

.execution-breakdown h3 {
  margin-bottom: 1.5rem;
  color: var(--color-text);
}

.breakdown-container {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 2rem;
}

.execution-column {
  background: var(--color-background-soft);
  border: 1px solid var(--color-border);
  border-radius: 8px;
  padding: 1.5rem;
}

.execution-column h4 {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  margin-top: 0;
  margin-bottom: 1.5rem;
  color: var(--color-text);
}

.badge {
  font-size: 0.75rem;
  padding: 0.25rem 0.625rem;
  border-radius: 12px;
  font-weight: 600;
}

.badge-reference {
  background: #e5e7eb;
  color: #374151;
}

.badge-optimized {
  background: #dcfce7;
  color: #16a34a;
}

.time-info {
  padding: 1rem;
  background: var(--color-background);
  border-radius: 6px;
  margin-bottom: 1rem;
}

.time-label {
  font-size: 0.875rem;
  color: var(--color-text-secondary);
  margin-bottom: 0.25rem;
}

.time-value {
  font-size: 1.5rem;
  font-weight: 700;
  color: var(--color-text);
}

.time-value.improved {
  color: #22c55e;
}

.shuffle-info {
  margin-top: 1rem;
  padding: 1rem;
  background: var(--color-background);
  border-radius: 6px;
}

.info-label {
  font-size: 0.875rem;
  color: var(--color-text-secondary);
  margin-bottom: 0.25rem;
}

.info-value {
  font-size: 1.25rem;
  font-weight: 600;
  color: var(--color-text);
}

.info-value.improved {
  color: #22c55e;
}

.detailed-metrics h3 {
  margin-bottom: 1rem;
  color: var(--color-text);
}

.metrics-table {
  width: 100%;
  border-collapse: collapse;
  margin-top: 1rem;
}

.metrics-table th,
.metrics-table td {
  padding: 0.75rem 1rem;
  text-align: left;
  border-bottom: 1px solid var(--color-border);
}

.metrics-table th {
  background: var(--color-background-soft);
  font-weight: 600;
  color: var(--color-text);
  font-size: 0.875rem;
}

.metrics-table td {
  color: var(--color-text);
}

.metrics-table .improvement {
  color: #22c55e;
  font-weight: 600;
}

/* Operator DAG Visualization */
.dag-comparison {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 1.5rem;
  margin-top: 1rem;
  height: calc(100% - 1rem);
}

/* DAG fills parent height */
.dag-comparison :deep(.operator-dag) {
  height: 100%;
  max-width: 100%;
  width: 100%;
  overflow: hidden;
}

/* Ensure Cytoscape container respects width */
.dag-comparison :deep(.cytoscape-wrapper) {
  max-width: 100%;
  width: 100%;
}

/* Make controls wrap to fit narrow width */
.dag-comparison:not(.dag-stacked) :deep(.dag-header) {
  flex-wrap: wrap;
}

.dag-comparison.dag-stacked {
  grid-template-columns: 1fr;
}

.layout-toggle {
  display: inline-flex;
  align-items: center;
  gap: 0.375rem;
  margin-left: auto;
  padding: 0.375rem 0.625rem;
  background: var(--color-background-soft);
  border: 1px solid var(--color-border);
  border-radius: 4px;
  font-size: 0.8125rem;
  font-weight: normal;
  cursor: pointer;
  transition: all 0.2s;
  user-select: none;
}

.layout-toggle:hover {
  border-color: var(--color-primary);
  background: var(--color-background);
}

.layout-toggle input[type="checkbox"] {
  cursor: pointer;
}

.layout-toggle span {
  white-space: nowrap;
}

.no-operator-data {
  background: var(--color-background-soft);
  border: 1px solid var(--color-border);
  border-radius: 8px;
  padding: 3rem 2rem;
  text-align: center;
  color: var(--color-text-secondary);
  display: flex;
  align-items: center;
  justify-content: center;
  min-height: 400px;
}

.no-operator-data p {
  margin: 0;
  font-size: 0.9375rem;
}

@media (max-width: 1200px) {
  .dag-comparison {
    grid-template-columns: 1fr;
  }

  /* On smaller screens, use stacked height */
  .dag-comparison :deep(.operator-dag) {
    height: 900px;
  }
}

@media (max-width: 768px) {
  .breakdown-container {
    grid-template-columns: 1fr;
  }

  .summary-cards {
    grid-template-columns: 1fr 1fr;
  }

  .metric-value {
    font-size: 2rem;
  }
}

@media (max-width: 480px) {
  .summary-cards {
    grid-template-columns: 1fr;
  }
}

/* Hypergraph Visualization Tabs */
.hypergraph-viz-tabs {
  display: flex;
  gap: 0.5rem;
  margin-bottom: 1rem;
  border-bottom: 2px solid var(--color-border);
  padding-bottom: 0;
}

.viz-tab {
  padding: 0.75rem 1.5rem;
  background: transparent;
  border: none;
  border-bottom: 3px solid transparent;
  cursor: pointer;
  font-size: 0.938rem;
  font-weight: 500;
  color: var(--color-text-secondary);
  transition: all 0.2s;
  position: relative;
  bottom: -2px;
}

.viz-tab:hover {
  color: var(--color-text);
  background: var(--color-background-soft);
}

.viz-tab.active {
  color: var(--color-primary);
  border-bottom-color: var(--color-primary);
  font-weight: 600;
}

.hypergraph-container {
  height: calc(100% - 58px); /* Account for tabs height */
  background: var(--color-background);
  border: 1px solid var(--color-border);
  border-radius: 8px;
  overflow: hidden;
}

@media (max-width: 768px) {
  .hypergraph-viz-tabs {
    overflow-x: auto;
    -webkit-overflow-scrolling: touch;
  }

  .viz-tab {
    white-space: nowrap;
    font-size: 0.813rem;
    padding: 0.625rem 1rem;
  }
}
</style>
