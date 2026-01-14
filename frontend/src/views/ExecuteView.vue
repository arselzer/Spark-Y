<template>
  <div class="container">
    <div class="execute-view-container" :class="{ 'with-sidebar': showHistory }">
      <!-- Query History Sidebar -->
      <div v-if="showHistory" class="history-sidebar">
        <QueryHistory @select="loadFromHistory" />
      </div>

      <!-- Main Content -->
      <div class="execute-view">
        <div class="header-with-actions">
          <div class="header-content">
            <h2>Execute and Compare Queries</h2>
            <p class="description">
              Write or select a SQL query to execute and compare the original execution
              with the optimized version that avoids intermediate materialization.
            </p>
          </div>
          <div class="header-actions">
            <button
              @click="showHistory = !showHistory"
              class="btn btn-secondary history-toggle"
              :class="{ active: showHistory }"
              :title="showHistory ? 'Hide history' : 'Show history'"
            >
              <span class="label">{{ showHistory ? 'Hide History' : 'History' }}</span>
            </button>
          </div>
        </div>

        <!-- Status Bar -->
        <div class="status-bar" role="status" aria-live="polite">
          <div class="status-item" :class="{ 'status-connected': sparkConnected, 'status-disconnected': !sparkConnected }">
            <span class="status-indicator" :aria-label="sparkConnected ? 'Spark Connected' : 'Spark Disconnected'"></span>
            <span class="status-text">{{ sparkConnected ? 'Spark Connected' : 'Spark Disconnected' }}</span>
          </div>
          <div v-if="lastExecutionTime" class="status-item">
            <span class="status-icon" aria-hidden="true">⏱️</span>
            <span class="status-text">Last: {{ formatTimestamp(lastExecutionTime) }}</span>
          </div>
          <div v-if="hypergraphData" class="status-item">
            <span :class="['status-badge', hypergraphData.is_acyclic ? 'badge-acyclic' : 'badge-cyclic']">
              {{ hypergraphData.is_acyclic ? '✓ Acyclic' : '⚠ Cyclic' }}
            </span>
          </div>
          <div v-if="hypergraphData?.guardedness_type" class="status-item">
            <span :class="['status-badge', getGuardednessBadgeClass(hypergraphData.guardedness_type)]">
              {{ getGuardednessLabel(hypergraphData.guardedness_type) }}
            </span>
          </div>
        </div>

        <!-- Editor + Hypergraph Toggle -->
        <div class="editor-header">
          <div class="editor-title-group">
            <h3>Query Editor</h3>
            <label class="sidebar-toggle" title="Show hypergraph beside editor">
              <input type="checkbox" v-model="showHypergraphSidebar" />
              <span>Show hypergraph sidebar</span>
            </label>
          </div>
          <div class="editor-controls">
            <button
              @click="executeQuery"
              class="btn btn-primary execute-btn"
              :disabled="loading || !sqlQuery.trim()"
              title="Execute query (Ctrl/Cmd + Enter)"
            >
              <span class="btn-icon">▶</span>
              Execute
              <span class="keyboard-hint">Ctrl+Enter</span>
            </button>
            <button
              v-if="loading"
              @click="cancelExecution"
              class="btn btn-secondary cancel-btn"
              title="Cancel execution"
              aria-label="Cancel query execution"
            >
              <span class="btn-icon" aria-hidden="true">⏹</span>
              Cancel
            </button>
          </div>
        </div>

        <!-- Resizable Editor + Hypergraph Container -->
        <div v-if="showHypergraphSidebar && (visualizationData || hypergraphData)" class="editor-hypergraph-container">
          <div class="editor-panel" :style="{ width: editorWidth + 'px' }">
            <QueryEditor v-model="sqlQuery" @execute="executeQuery" />
          </div>
          <div
            class="resize-handle"
            @mousedown="startResize"
            title="Drag to resize"
          ></div>
          <div class="hypergraph-sidebar-panel">
            <div class="hypergraph-sidebar-content">
              <h4>Hypergraph Preview</h4>
              <div class="mini-viz-tabs">
                <button
                  :class="['mini-tab', { active: sidebarVizMode === 'graph' }]"
                  @click="sidebarVizMode = 'graph'"
                >
                  Graph
                </button>
                <button
                  :class="['mini-tab', { active: sidebarVizMode === 'bubbles' }]"
                  @click="sidebarVizMode = 'bubbles'"
                >
                  Pie Charts
                </button>
                <button
                  :class="['mini-tab', { active: sidebarVizMode === 'hulls' }]"
                  @click="sidebarVizMode = 'hulls'"
                >
                  Bubble Sets
                </button>
              </div>
              <div class="sidebar-viz-container">
                <HypergraphViewer
                  v-if="visualizationData"
                  :visualization-data="visualizationData"
                  :show-bubble-sets="sidebarVizMode === 'bubbles'"
                  :show-convex-hulls="sidebarVizMode === 'hulls'"
                />
              </div>
            </div>
          </div>
        </div>
        <div v-else>
          <QueryEditor v-model="sqlQuery" @execute="executeQuery" />
        </div>


      <!-- Enhanced Loading State -->
      <div v-if="loading" class="loading-section enhanced">
        <div class="loading-content">
          <div class="spinner"></div>
          <div class="loading-progress">
            <h4>{{ loadingStage }}</h4>
            <div class="progress-bar">
              <div class="progress-fill" :style="{ width: loadingProgress + '%' }"></div>
            </div>
            <p class="loading-detail">{{ loadingDetail }}</p>
          </div>
        </div>
      </div>

      <!-- Enhanced Error Message -->
      <div v-if="error" class="error-message enhanced">
        <div class="error-header">
          <strong>⚠️ Error</strong>
          <div class="error-actions">
            <button @click="copyError" class="btn-text" :class="{ 'copied': copiedFeedback }" title="Copy error details">
              <span class="btn-icon">{{ copiedFeedback ? '✓' : '📋' }}</span>
              {{ copiedFeedback ? 'Copied!' : 'Copy' }}
            </button>
            <button @click="retryExecution" class="btn-text" title="Retry execution">
              <span class="btn-icon">🔄</span>
              Retry
            </button>
          </div>
        </div>
        <div class="error-body">
          {{ error }}
        </div>
        <div v-if="errorSuggestion" class="error-suggestion">
          💡 <strong>Suggestion:</strong> {{ errorSuggestion }}
        </div>
      </div>

      <!-- Hypergraph Visualization -->
      <div v-if="visualizationData || hypergraphData" class="section">
        <div v-if="visualizationData && visualizationData.stats.num_nodes === 0 && visualizationData.stats.num_edges === 0" class="info-message">
          <strong>No hypergraph to visualize.</strong> The query may not contain joins or tables.
        </div>
        <div v-else>
          <!-- Visualization mode tabs -->
          <div class="viz-tabs">
            <button
              :class="['tab-button', { active: vizMode === 'graph' }]"
              @click="vizMode = 'graph'"
            >
              Graph View
            </button>
            <button
              :class="['tab-button', { active: vizMode === 'graph-bubbles' }]"
              @click="vizMode = 'graph-bubbles'"
            >
              Graph + Pie Charts
            </button>
            <button
              :class="['tab-button', { active: vizMode === 'bubble-sets' }]"
              @click="vizMode = 'bubble-sets'"
            >
              Bubble Sets (Convex Hulls)
            </button>
            <button
              :class="['tab-button', { active: vizMode === 'upset' }]"
              @click="vizMode = 'upset'"
            >
              UpSet Plot
            </button>
          </div>

          <!-- Visualization components -->
          <div v-if="vizMode === 'graph' || vizMode === 'graph-bubbles' || vizMode === 'bubble-sets'" class="visualization-container">
            <div class="viz-panel">
              <HypergraphViewer
                v-if="visualizationData"
                :visualization-data="visualizationData"
                :show-bubble-sets="vizMode === 'graph-bubbles'"
                :show-convex-hulls="vizMode === 'bubble-sets'"
              />
            </div>
            <div v-if="hasJoinTree" class="viz-panel join-tree-panel">
              <JoinTreeViewer :join-tree="hypergraphData?.join_tree" />
            </div>
          </div>
          <UpSetViewer
            v-if="hypergraphData && vizMode === 'upset'"
            :hypergraph="hypergraphData"
          />
        </div>
      </div>

      <!-- GYO Animation Section (combined graph + steps) -->
      <div v-if="hasGYOSteps" class="section gyo-animation-section">
        <div class="gyo-section-header" @click="gyoSectionExpanded = !gyoSectionExpanded">
          <div class="gyo-header-content">
            <h3 class="section-title">GYO Algorithm Animation</h3>
            <span class="expand-icon">{{ gyoSectionExpanded ? '▼' : '▶' }}</span>
          </div>
          <p v-if="!gyoSectionExpanded" class="section-description-collapsed">
            Click to watch the GYO algorithm reduce the hypergraph step-by-step
          </p>
        </div>

        <div v-if="gyoSectionExpanded" class="gyo-animation-content">
          <p class="section-description">
            Watch the GYO (Graham-Yu-Özsoyoglu) algorithm reduce the hypergraph step-by-step.
            Elements being removed are highlighted in red, already removed elements are grayed out.
          </p>

          <div class="gyo-animation-container">
          <!-- Hypergraph visualization -->
          <div class="gyo-graph-panel">
            <HypergraphViewer
              v-if="visualizationData"
              :visualization-data="visualizationData"
              :show-bubble-sets="false"
              :gyo-step="currentGYOStep"
              :gyo-steps="hypergraphData?.gyo_steps"
            />
          </div>

          <!-- GYO step controls -->
          <div class="gyo-steps-panel">
            <GYOStepViewer
              :steps="hypergraphData?.gyo_steps"
              :hypergraph="hypergraphData"
              @step-change="handleGYOStepChange"
            />
          </div>
        </div>
        </div>
      </div>

      <!-- Query Complexity Metrics -->
      <QueryComplexityMetrics
        v-if="hypergraphData?.complexity_metrics"
        :metrics="hypergraphData.complexity_metrics"
        :is-acyclic="hypergraphData.is_acyclic"
      />

      <!-- Spark SQL Query Plan Comparison -->
      <SparkQueryPlanComparison
        v-if="executionResult?.original_plan && executionResult?.optimized_plan"
        :reference-plan="executionResult.original_plan"
        :optimized-plan="executionResult.optimized_plan"
      />

      <!-- Performance Comparison -->
      <PerformanceComparison
        v-if="executionResult?.original_metrics && executionResult?.optimized_metrics"
        :reference-metrics="executionResult.original_metrics"
        :optimized-metrics="executionResult.optimized_metrics"
        :reference-plan="executionResult.original_plan"
        :optimized-plan="executionResult.optimized_plan"
        :visualization-data="visualizationData"
        :hypergraph-data="hypergraphData"
      />
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted, watch } from 'vue'
import { useRouter } from 'vue-router'
import QueryEditor from '@/components/QueryEditor.vue'
import HypergraphViewer from '@/components/HypergraphViewer.vue'
import JoinTreeViewer from '@/components/JoinTreeViewer.vue'
import GYOStepViewer from '@/components/GYOStepViewer.vue'
import QueryHistory from '@/components/QueryHistory.vue'
import QueryComplexityMetrics from '@/components/QueryComplexityMetrics.vue'
import UpSetViewer from '@/components/UpSetViewer.vue'
import SparkQueryPlanComparison from '@/components/SparkQueryPlanComparison.vue'
import ExecutionComparison from '@/components/ExecutionComparison.vue'
import PerformanceComparison from '@/components/PerformanceComparison.vue'
import { executionApi, hypergraphApi } from '@/services/api'
import { useQueryHistory } from '@/composables/useQueryHistory'
import { usePreferences } from '@/composables/usePreferences'
import type { ExecutionResult, VisualizationData, Hypergraph } from '@/types'

const router = useRouter()
const { addQuery } = useQueryHistory()
const { preferences, setEditorWidth, setShowHypergraphSidebar, setSidebarVizMode } = usePreferences()

// UI state - initialize from preferences
const showHistory = ref(false)
const showHypergraphSidebar = ref(preferences.value.showHypergraphSidebar)
const editorWidth = ref(preferences.value.editorWidth)
const sidebarVizMode = ref<'graph' | 'bubbles' | 'hulls'>(preferences.value.sidebarVizMode)
const isResizing = ref(false)

// Watch for changes and save to preferences
watch(editorWidth, (newWidth) => setEditorWidth(newWidth))
watch(showHypergraphSidebar, (newValue) => setShowHypergraphSidebar(newValue))
watch(sidebarVizMode, (newMode) => setSidebarVizMode(newMode))

// Status indicators
const sparkConnected = ref(true) // Assume connected initially
const lastExecutionTime = ref<Date | null>(null)

// Loading states
const loadingStage = ref('Initializing...')
const loadingDetail = ref('Preparing query execution')
const loadingProgress = ref(0)

// Cancellation support
let abortController: AbortController | null = null

// Default query as fallback
const defaultQuery = `SELECT
  COUNT(*) as cnt,
  AVG(mi_idx.info) as avg_info
FROM
  movie_info_idx AS mi_idx
  JOIN title AS t ON t.id = mi_idx.movie_id
  JOIN movie_info AS mi ON mi.movie_id = t.id
WHERE
  t.production_year > 2000
GROUP BY
  t.id`

// Check for incoming query from router state
const incomingState = history.state as { sql?: string; queryId?: string }
const sqlQuery = ref(incomingState?.sql || defaultQuery)

const loading = ref(false)
const error = ref('')
const errorSuggestion = ref('')
const copiedFeedback = ref(false)
const executionResult = ref<ExecutionResult | null>(null)
const visualizationData = ref<VisualizationData | null>(null)
const hypergraphData = ref<Hypergraph | null>(null)
const vizMode = ref<'graph' | 'graph-bubbles' | 'bubble-sets' | 'upset'>('graph')

// Helper functions
function formatTimestamp(date: Date): string {
  const now = new Date()
  const diff = now.getTime() - date.getTime()
  const seconds = Math.floor(diff / 1000)
  const minutes = Math.floor(seconds / 60)
  const hours = Math.floor(minutes / 60)

  if (seconds < 60) return `${seconds}s ago`
  if (minutes < 60) return `${minutes}m ago`
  if (hours < 24) return `${hours}h ago`
  return date.toLocaleString()
}

function getGuardednessBadgeClass(type: string): string {
  switch (type) {
    case 'guarded':
      return 'badge-guarded'
    case 'piecewise_guarded':
      return 'badge-piecewise'
    case 'unguarded':
      return 'badge-unguarded'
    default:
      return 'badge-unknown'
  }
}

function getGuardednessLabel(type: string): string {
  switch (type) {
    case 'guarded':
      return '✓ Guarded'
    case 'piecewise_guarded':
      return '◐ Piecewise'
    case 'unguarded':
      return '✗ Unguarded'
    default:
      return '? Unknown'
  }
}

function getErrorSuggestion(errorMessage: string): string {
  if (!errorMessage) return ''

  const msg = errorMessage.toLowerCase()

  if (msg.includes('table') && (msg.includes('not found') || msg.includes('does not exist'))) {
    return 'Check if PostgreSQL is connected and the table exists in your database.'
  }
  if (msg.includes('connection') || msg.includes('connect')) {
    return 'Verify that the PostgreSQL and Spark services are running.'
  }
  if (msg.includes('syntax') || msg.includes('parse')) {
    return 'Check your SQL syntax. Try using a query template as a starting point.'
  }
  if (msg.includes('timeout') || msg.includes('timed out')) {
    return 'The query is taking too long. Try simplifying it or adding WHERE clauses to reduce data.'
  }
  if (msg.includes('memory') || msg.includes('out of')) {
    return 'The query requires too much memory. Try reducing the dataset size or increasing Spark memory.'
  }
  return ''
}

// Copy error to clipboard
async function copyError() {
  try {
    await navigator.clipboard.writeText(error.value)
    copiedFeedback.value = true
    // Hide feedback after 2 seconds
    setTimeout(() => {
      copiedFeedback.value = false
    }, 2000)
  } catch (e) {
    console.error('Failed to copy error:', e)
  }
}

// Retry execution
function retryExecution() {
  error.value = ''
  errorSuggestion.value = ''
  executeQuery()
}

// Cancel execution
function cancelExecution() {
  if (abortController) {
    abortController.abort()
    abortController = null
  }
  loading.value = false
  loadingProgress.value = 0
  error.value = 'Query execution cancelled by user'
}

// GYO animation state
const currentGYOStep = ref<number | null>(null)
const gyoSectionExpanded = ref(false)

// Check if we have a join tree to display
const hasJoinTree = computed(() => {
  return hypergraphData.value?.join_tree &&
         hypergraphData.value.join_tree.length > 0 &&
         hypergraphData.value.is_acyclic
})

// Check if we have GYO steps for animation
const hasGYOSteps = computed(() => {
  return hypergraphData.value?.gyo_steps && hypergraphData.value.gyo_steps.length > 0
})

// Handle GYO step changes from the step viewer
const handleGYOStepChange = (stepNumber: number) => {
  currentGYOStep.value = stepNumber
  console.log('GYO step changed to:', stepNumber)
}

// Keyboard shortcut handler
const handleKeyboardShortcut = (event: KeyboardEvent) => {
  // Ctrl+Enter or Cmd+Enter to execute query
  if ((event.ctrlKey || event.metaKey) && event.key === 'Enter') {
    event.preventDefault()
    if (!loading.value && sqlQuery.value.trim()) {
      executeQuery()
    }
  }
}

// Setup keyboard listener on mount
onMounted(() => {
  document.addEventListener('keydown', handleKeyboardShortcut)

  // Check Spark health on mount
  checkSparkHealth()
})

// Cleanup keyboard listener on unmount
onUnmounted(() => {
  document.removeEventListener('keydown', handleKeyboardShortcut)
})

// Check Spark health
async function checkSparkHealth() {
  try {
    const response = await fetch('/api/health')
    const data = await response.json()
    sparkConnected.value = data.spark_ready === true
  } catch (error) {
    console.error('Health check failed:', error)
    sparkConnected.value = false
  }
}

// Load query from history
const loadFromHistory = (sql: string) => {
  sqlQuery.value = sql
  showHistory.value = false // Hide sidebar after selection
}

async function executeQuery() {
  if (!sqlQuery.value.trim()) {
    error.value = 'Please enter a SQL query'
    errorSuggestion.value = ''
    return
  }

  // Reset state
  loading.value = true
  error.value = ''
  errorSuggestion.value = ''
  executionResult.value = null
  visualizationData.value = null
  currentGYOStep.value = null  // Reset GYO animation state
  loadingProgress.value = 0

  // Create abort controller for cancellation
  abortController = new AbortController()

  try {
    // Stage 1: Configuration loading
    loadingStage.value = 'Loading configuration...'
    loadingDetail.value = 'Reading Spark SQL settings'
    loadingProgress.value = 10
    // Load configuration from localStorage
    const savedConfig = localStorage.getItem('sparkSqlConfig')
    let referenceConfig: any
    let optimizedConfig: any

    if (savedConfig) {
      try {
        const parsed = JSON.parse(savedConfig)

        // Load reference configuration
        const refCustomOptions: Record<string, string> = {}
        if (parsed.referenceCustomOptions) {
          parsed.referenceCustomOptions
            .filter((opt: any) => opt.key && opt.value)
            .forEach((opt: any) => {
              refCustomOptions[opt.key] = opt.value
            })
        }

        referenceConfig = {
          yannakakis_enabled: parsed.referenceConfig?.yannakakis_enabled ?? false,
          physical_count_join_enabled: parsed.referenceConfig?.physical_count_join_enabled ?? false,
          unguarded_enabled: parsed.referenceConfig?.unguarded_enabled ?? false,
          custom_options: Object.keys(refCustomOptions).length > 0 ? refCustomOptions : null
        }

        // Load optimized configuration
        const optCustomOptions: Record<string, string> = {}
        if (parsed.optimizedCustomOptions) {
          parsed.optimizedCustomOptions
            .filter((opt: any) => opt.key && opt.value)
            .forEach((opt: any) => {
              optCustomOptions[opt.key] = opt.value
            })
        }

        optimizedConfig = {
          yannakakis_enabled: parsed.optimizedConfig?.yannakakis_enabled ?? true,
          physical_count_join_enabled: parsed.optimizedConfig?.physical_count_join_enabled ?? true,
          unguarded_enabled: parsed.optimizedConfig?.unguarded_enabled ?? true,
          custom_options: Object.keys(optCustomOptions).length > 0 ? optCustomOptions : null
        }
      } catch (e) {
        console.error('Failed to parse configuration:', e)
        // Use defaults if parsing fails
        referenceConfig = {
          yannakakis_enabled: false,
          physical_count_join_enabled: false,
          unguarded_enabled: false,
          custom_options: null
        }
        optimizedConfig = {
          yannakakis_enabled: true,
          physical_count_join_enabled: true,
          unguarded_enabled: true,
          custom_options: null
        }
      }
    } else {
      // No saved configuration - use defaults
      referenceConfig = {
        yannakakis_enabled: false,
        physical_count_join_enabled: false,
        unguarded_enabled: false,
        custom_options: null
      }
      optimizedConfig = {
        yannakakis_enabled: true,
        physical_count_join_enabled: true,
        unguarded_enabled: true,
        custom_options: null
      }
    }

    // Stage 2: Executing queries
    loadingStage.value = 'Executing queries...'
    loadingDetail.value = 'Running reference and optimized queries in Spark'
    loadingProgress.value = 30

    const [result, hypergraph] = await Promise.all([
      executionApi.executeQuery({
        sql: sqlQuery.value,
        use_optimization: true,
        collect_metrics: true,
        reference_config: referenceConfig,
        optimized_config: optimizedConfig
      }, abortController.signal),
      hypergraphApi.extractHypergraph(sqlQuery.value, undefined, true, abortController.signal)
    ])

    // Stage 3: Processing results
    loadingStage.value = 'Processing results...'
    loadingDetail.value = 'Analyzing hypergraph and metrics'
    loadingProgress.value = 70

    console.log('Hypergraph extracted:', hypergraph)
    console.log('Nodes:', hypergraph.nodes?.length, 'Edges:', hypergraph.edges?.length)

    if (!result.success) {
      error.value = result.error || 'Query execution failed'
      errorSuggestion.value = getErrorSuggestion(result.error || '')
      sparkConnected.value = false
      return
    }

    executionResult.value = result
    hypergraphData.value = hypergraph
    sparkConnected.value = true

    // Initialize GYO animation to step 0 if we have GYO steps
    if (hypergraph.gyo_steps && hypergraph.gyo_steps.length > 0) {
      currentGYOStep.value = 0
      console.log('Initialized GYO animation with', hypergraph.gyo_steps.length, 'steps')
    }

    // Stage 4: Generating visualizations
    loadingStage.value = 'Generating visualizations...'
    loadingDetail.value = 'Creating hypergraph visualization'
    loadingProgress.value = 90

    console.log('Generating visualization from hypergraph...')
    visualizationData.value = await hypergraphApi.generateVisualization(hypergraph)
    console.log('Visualization data:', visualizationData.value)

    // Stage 5: Finalizing
    loadingStage.value = 'Finalizing...'
    loadingDetail.value = 'Saving to history'
    loadingProgress.value = 95

    // Save to query history
    addQuery(sqlQuery.value, {
      acyclic: hypergraph.is_acyclic,
      numTables: hypergraph.edges?.length || 0,
      numJoins: hypergraph.nodes?.length || 0,
      executionTime: result.execution_time_ms
    })

    // Update last execution time
    lastExecutionTime.value = new Date()
    loadingProgress.value = 100

  } catch (err: any) {
    // Check if aborted
    if (err.name === 'AbortError' || err.message?.includes('abort')) {
      error.value = 'Query execution cancelled'
      errorSuggestion.value = ''
    } else {
      error.value = err.response?.data?.detail || err.message || 'An error occurred'
      errorSuggestion.value = getErrorSuggestion(error.value)
      sparkConnected.value = false
    }
    console.error('Execution error:', err)
  } finally {
    loading.value = false
    loadingProgress.value = 0
    abortController = null
  }
}

// Resize handlers for editor/hypergraph split
function startResize(event: MouseEvent) {
  isResizing.value = true
  event.preventDefault()

  const onMouseMove = (e: MouseEvent) => {
    if (!isResizing.value) return

    // Calculate new width based on mouse position
    const container = (event.target as HTMLElement).parentElement
    if (!container) return

    const containerRect = container.getBoundingClientRect()
    const newWidth = e.clientX - containerRect.left

    // Constrain width between 300px and container width - 400px (to leave space for hypergraph)
    const minWidth = 300
    const maxWidth = containerRect.width - 400

    editorWidth.value = Math.max(minWidth, Math.min(newWidth, maxWidth))
  }

  const onMouseUp = () => {
    isResizing.value = false
    document.removeEventListener('mousemove', onMouseMove)
    document.removeEventListener('mouseup', onMouseUp)
  }

  document.addEventListener('mousemove', onMouseMove)
  document.addEventListener('mouseup', onMouseUp)
}
</script>

<style scoped>
.execute-view {
  max-width: 1400px;
  margin: 0 auto;
}

.execute-view h2 {
  font-size: 2rem;
  margin-bottom: 0.5rem;
}

.description {
  color: var(--color-text-secondary);
  margin-bottom: 2rem;
}

.section {
  margin-top: 2rem;
}

.loading-section {
  text-align: center;
  padding: 3rem;
  background: white;
  border-radius: 0.5rem;
  margin-top: 2rem;
}

.loading-section .spinner {
  margin: 0 auto 1rem;
}

.error-message {
  background: #FEE2E2;
  border: 1px solid #FCA5A5;
  color: #991B1B;
  padding: 1rem;
  border-radius: 0.5rem;
  margin-top: 1rem;
}

.info-message {
  background: #DBEAFE;
  border: 1px solid #93C5FD;
  color: #1E40AF;
  padding: 1rem;
  border-radius: 0.5rem;
  margin-bottom: 1rem;
}

.viz-tabs {
  display: flex;
  gap: 0.5rem;
  margin-bottom: 1rem;
  border-bottom: 2px solid #E5E7EB;
  padding-bottom: 0;
}

.tab-button {
  padding: 0.75rem 1.5rem;
  background: transparent;
  border: none;
  border-bottom: 3px solid transparent;
  cursor: pointer;
  font-size: 0.938rem;
  font-weight: 500;
  color: #6B7280;
  transition: all 0.2s;
  position: relative;
  bottom: -2px;
}

.tab-button:hover {
  color: #374151;
  background: #F9FAFB;
}

.tab-button.active {
  color: #3B82F6;
  border-bottom-color: #3B82F6;
  font-weight: 600;
}

.visualization-container {
  display: flex;
  flex-direction: column;
  gap: 1rem;
  min-height: 500px;
}

/* GYO Animation Section */
.gyo-animation-section {
  background: #F9FAFB;
  border-radius: 0.5rem;
  border: 2px solid #E5E7EB;
  overflow: hidden;
}

.gyo-section-header {
  padding: 1.5rem;
  cursor: pointer;
  user-select: none;
  transition: background-color 0.2s;
}

.gyo-section-header:hover {
  background: #F3F4F6;
}

.gyo-header-content {
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.section-title {
  font-size: 1.5rem;
  font-weight: 600;
  margin: 0;
  color: #1F2937;
}

.expand-icon {
  font-size: 1.25rem;
  color: #6B7280;
  transition: transform 0.2s;
}

.section-description-collapsed {
  font-size: 0.875rem;
  color: #9CA3AF;
  margin: 0.5rem 0 0 0;
  font-style: italic;
}

.gyo-animation-content {
  padding: 0 1.5rem 1.5rem 1.5rem;
}

.section-description {
  font-size: 0.938rem;
  color: #6B7280;
  margin: 0 0 1.5rem 0;
  line-height: 1.5;
}

.gyo-animation-container {
  display: grid;
  grid-template-columns: 2fr 1fr;
  gap: 1.5rem;
  align-items: start;
}

.gyo-graph-panel {
  background: white;
  border-radius: 0.5rem;
  overflow: hidden;
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
}

.gyo-steps-panel {
  background: white;
  border-radius: 0.5rem;
  overflow: hidden;
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
  position: sticky;
  top: 1rem;
}

/* Responsive layout */
@media (max-width: 1200px) {
  .gyo-animation-container {
    grid-template-columns: 1fr;
  }

  .gyo-steps-panel {
    position: static;
  }
}

.viz-panel {
  width: 100%;
  min-height: 500px;
}

.viz-panel.join-tree-panel {
  margin-top: 1rem;
  min-height: 640px;
}

/* Tablet and mobile responsive */
@media (max-width: 1200px) {
  .viz-panel {
    min-height: 400px;
  }

  .viz-panel.join-tree-panel {
    min-height: 560px;
  }
}

@media (max-width: 768px) {
  .execute-view h2 {
    font-size: 1.5rem;
  }

  .viz-tabs {
    overflow-x: auto;
    -webkit-overflow-scrolling: touch;
  }

  .tab-button {
    white-space: nowrap;
    font-size: 0.813rem;
    padding: 0.625rem 1rem;
  }

  .visualization-container {
    min-height: 300px;
  }

  .viz-panel {
    min-height: 300px;
  }
}

@media (max-width: 480px) {
  .viz-tabs {
    gap: 0.25rem;
  }

  .tab-button {
    padding: 0.5rem 0.75rem;
    font-size: 0.75rem;
  }
}

/* Query History Sidebar */
.execute-view-container {
  display: flex;
  gap: 1.5rem;
  max-width: 1800px;
  margin: 0 auto;
}

.execute-view-container.with-sidebar {
  max-width: none;
}

.history-sidebar {
  width: 350px;
  min-width: 350px;
  max-height: calc(100vh - 120px);
  position: sticky;
  top: 80px;
}

.execute-view {
  flex: 1;
  max-width: 1400px;
  margin: 0 auto;
}

.header-with-actions {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  margin-bottom: 2rem;
}

.header-content {
  flex: 1;
}

.header-actions {
  display: flex;
  gap: 0.75rem;
  align-items: center;
  padding-top: 0.25rem;
}

.history-toggle {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  padding: 0.625rem 1rem;
  font-size: 0.938rem;
  font-weight: 500;
  transition: all 0.2s;
}

.history-toggle .icon {
  font-size: 1.125rem;
}

.history-toggle:hover {
  transform: translateY(-1px);
  box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
}

.history-toggle.active {
  background-color: var(--color-primary);
  color: white;
  border-color: var(--color-primary);
}

@media (max-width: 1024px) {
  .execute-view-container {
    flex-direction: column;
  }

  .history-sidebar {
    width: 100%;
    min-width: auto;
    max-height: 400px;
    position: static;
  }

  .header-with-actions {
    flex-direction: column;
    gap: 1rem;
  }

  .header-actions {
    width: 100%;
    justify-content: flex-start;
  }
}

@media (max-width: 768px) {
  .history-toggle .label {
    display: none;
  }

  .history-toggle {
    padding: 0.5rem;
  }
}

/* Status Bar */
.status-bar {
  display: flex;
  gap: 1.5rem;
  align-items: center;
  padding: 0.75rem 1rem;
  background: var(--color-background-soft);
  border: 1px solid var(--color-border);
  border-radius: 0.5rem;
  margin-top: 2rem;
  flex-wrap: wrap;
}

.status-item {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  font-size: 0.875rem;
}

.status-indicator {
  width: 8px;
  height: 8px;
  border-radius: 50%;
  background: var(--color-border);
}

.status-connected .status-indicator {
  background: #10b981;
  box-shadow: 0 0 8px rgba(16, 185, 129, 0.5);
}

.status-disconnected .status-indicator {
  background: #ef4444;
  box-shadow: 0 0 8px rgba(239, 68, 68, 0.5);
}

.status-icon {
  font-size: 1rem;
}

.status-text {
  color: var(--color-text-secondary);
  font-weight: 500;
}

.status-badge {
  padding: 0.25rem 0.625rem;
  border-radius: 12px;
  font-size: 0.813rem;
  font-weight: 600;
}

.badge-acyclic {
  background: rgba(16, 185, 129, 0.1);
  color: #10b981;
  border: 1px solid rgba(16, 185, 129, 0.3);
}

.badge-cyclic {
  background: rgba(251, 191, 36, 0.1);
  color: #f59e0b;
  border: 1px solid rgba(251, 191, 36, 0.3);
}

.badge-guarded {
  background: rgba(16, 185, 129, 0.1);
  color: #10b981;
  border: 1px solid rgba(16, 185, 129, 0.3);
}

.badge-piecewise {
  background: rgba(99, 102, 241, 0.1);
  color: #6366f1;
  border: 1px solid rgba(99, 102, 241, 0.3);
}

.badge-unguarded {
  background: rgba(239, 68, 68, 0.1);
  color: #ef4444;
  border: 1px solid rgba(239, 68, 68, 0.3);
}

.badge-unknown {
  background: rgba(107, 114, 128, 0.1);
  color: #6b7280;
  border: 1px solid rgba(107, 114, 128, 0.3);
}

/* Editor Header */
.editor-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-top: 1.5rem;
  margin-bottom: 0.75rem;
  gap: 1rem;
  flex-wrap: wrap;
}

.editor-title-group {
  display: flex;
  align-items: center;
  gap: 1rem;
  flex: 1;
}

.editor-header h3 {
  font-size: 1.25rem;
  font-weight: 600;
  color: var(--color-text);
  margin: 0;
}

.sidebar-toggle {
  display: inline-flex;
  align-items: center;
  gap: 0.5rem;
  padding: 0.5rem 0.75rem;
  background: var(--color-background-soft);
  border: 1px solid var(--color-border);
  border-radius: 4px;
  font-size: 0.875rem;
  cursor: pointer;
  transition: all 0.2s;
  user-select: none;
}

.sidebar-toggle:hover {
  border-color: var(--color-primary);
  background: var(--color-background);
}

.sidebar-toggle input[type="checkbox"] {
  cursor: pointer;
}

.editor-controls {
  display: flex;
  gap: 0.75rem;
  align-items: center;
}

.execute-btn {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  font-weight: 600;
}

.execute-btn .btn-icon {
  font-size: 0.875rem;
}

.keyboard-hint {
  font-size: 0.75rem;
  opacity: 0.7;
  padding: 0.125rem 0.375rem;
  background: rgba(255, 255, 255, 0.2);
  border-radius: 3px;
  margin-left: 0.25rem;
}

.cancel-btn {
  display: flex;
  align-items: center;
  gap: 0.5rem;
}

.cancel-btn .btn-icon {
  font-size: 1rem;
}

/* Resizable Editor + Hypergraph Container */
.editor-hypergraph-container {
  display: flex;
  gap: 0;
  margin-top: 0.75rem;
  border: 1px solid var(--color-border);
  border-radius: 0.5rem;
  overflow: hidden;
  background: var(--color-surface);
  min-height: 400px;
}

.editor-panel {
  flex-shrink: 0;
  overflow: auto;
}

.resize-handle {
  width: 6px;
  background: var(--color-border);
  cursor: col-resize;
  position: relative;
  flex-shrink: 0;
  transition: background 0.2s;
}

.resize-handle:hover {
  background: var(--color-primary);
}

.resize-handle::before {
  content: '';
  position: absolute;
  top: 50%;
  left: 50%;
  transform: translate(-50%, -50%);
  width: 20px;
  height: 40px;
  background: transparent;
}

.hypergraph-sidebar-panel {
  flex: 1;
  overflow: hidden;
  display: flex;
  flex-direction: column;
}

.hypergraph-sidebar-content {
  padding: 1rem;
  display: flex;
  flex-direction: column;
  height: 100%;
}

.hypergraph-sidebar-content h4 {
  font-size: 1rem;
  font-weight: 600;
  margin: 0 0 0.75rem 0;
  color: var(--color-text);
}

.mini-viz-tabs {
  display: flex;
  gap: 0.5rem;
  margin-bottom: 0.75rem;
  border-bottom: 1px solid var(--color-border);
  padding-bottom: 0;
}

.mini-tab {
  padding: 0.5rem 1rem;
  background: transparent;
  border: none;
  border-bottom: 2px solid transparent;
  cursor: pointer;
  font-size: 0.875rem;
  font-weight: 500;
  color: #6B7280;
  transition: all 0.2s;
  position: relative;
  bottom: -1px;
}

.mini-tab:hover {
  color: #374151;
  background: #F9FAFB;
}

.mini-tab.active {
  color: var(--color-primary);
  border-bottom-color: var(--color-primary);
  font-weight: 600;
}

.sidebar-viz-container {
  flex: 1;
  min-height: 300px;
  overflow: hidden;
  border-radius: 0.25rem;
  border: 1px solid var(--color-border);
}

/* Responsive adjustments */
@media (max-width: 1200px) {
  .editor-hypergraph-container {
    flex-direction: column;
    min-height: auto;
  }

  .editor-panel {
    width: 100% !important;
  }

  .resize-handle {
    display: none;
  }

  .hypergraph-sidebar-panel {
    min-height: 400px;
  }
}

@media (max-width: 768px) {
  .editor-header {
    flex-direction: column;
    align-items: flex-start;
    gap: 0.75rem;
  }

  .sidebar-toggle {
    width: 100%;
    justify-content: center;
  }

  .hypergraph-sidebar-panel {
    min-height: 300px;
  }
}

/* Empty State */
.empty-state {
  text-align: center;
  padding: 4rem 2rem;
  background: linear-gradient(135deg, rgba(59, 130, 246, 0.05) 0%, rgba(168, 85, 247, 0.05) 100%);
  border-radius: 1rem;
  border: 2px dashed var(--color-border);
  margin: 2rem 0;
}

.empty-state-icon {
  font-size: 4rem;
  margin-bottom: 1rem;
}

.empty-state h3 {
  font-size: 1.75rem;
  font-weight: 700;
  color: var(--color-text);
  margin: 0 0 1rem 0;
}

.empty-state-description {
  font-size: 1.063rem;
  color: var(--color-text-secondary);
  max-width: 600px;
  margin: 0 auto 2rem auto;
  line-height: 1.6;
}

.empty-state-features {
  display: flex;
  gap: 2rem;
  justify-content: center;
  margin-bottom: 2rem;
  flex-wrap: wrap;
}

.feature-item {
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 0.5rem;
}

.feature-icon {
  font-size: 2rem;
}

.feature-text {
  font-size: 0.875rem;
  font-weight: 600;
  color: var(--color-text);
}

.empty-state-hint {
  display: inline-block;
  padding: 0.75rem 1.5rem;
  background: rgba(59, 130, 246, 0.1);
  border: 1px solid rgba(59, 130, 246, 0.3);
  border-radius: 0.5rem;
  font-size: 0.938rem;
  color: var(--color-primary);
}

/* Enhanced Loading State */
.loading-section.enhanced {
  padding: 3rem 2rem;
  background: white;
  border-radius: 0.75rem;
  margin-top: 2rem;
  border: 1px solid var(--color-border);
}

.loading-content {
  display: flex;
  flex-direction: column;
  align-items: center;
  gap: 2rem;
}

.loading-progress {
  width: 100%;
  max-width: 500px;
  text-align: center;
}

.loading-progress h4 {
  font-size: 1.125rem;
  font-weight: 600;
  color: var(--color-text);
  margin: 0 0 1rem 0;
}

.progress-bar {
  width: 100%;
  height: 8px;
  background: var(--color-background-soft);
  border-radius: 4px;
  overflow: hidden;
  margin-bottom: 0.75rem;
}

.progress-fill {
  height: 100%;
  background: linear-gradient(90deg, var(--color-primary) 0%, #60a5fa 100%);
  transition: width 0.3s ease;
  border-radius: 4px;
}

.loading-detail {
  font-size: 0.875rem;
  color: var(--color-text-secondary);
  margin: 0;
}

/* Enhanced Error Message */
.error-message.enhanced {
  background: rgba(239, 68, 68, 0.05);
  border: 1px solid rgba(239, 68, 68, 0.3);
  border-radius: 0.75rem;
  margin-top: 2rem;
  overflow: hidden;
}

.error-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 1rem 1.25rem;
  background: rgba(239, 68, 68, 0.1);
  border-bottom: 1px solid rgba(239, 68, 68, 0.2);
}

.error-header strong {
  color: #dc2626;
  font-size: 1rem;
}

.error-actions {
  display: flex;
  gap: 0.75rem;
}

.btn-text {
  display: flex;
  align-items: center;
  gap: 0.375rem;
  padding: 0.375rem 0.75rem;
  background: rgba(255, 255, 255, 0.8);
  border: 1px solid rgba(239, 68, 68, 0.3);
  border-radius: 4px;
  font-size: 0.875rem;
  font-weight: 500;
  color: #dc2626;
  cursor: pointer;
  transition: all 0.2s;
}

.btn-text:hover {
  background: white;
  border-color: #dc2626;
  transform: translateY(-1px);
}

.btn-text.copied {
  background: rgba(16, 185, 129, 0.1);
  border-color: #10b981;
  color: #10b981;
}

.btn-text .btn-icon {
  font-size: 1rem;
}

.error-body {
  padding: 1.25rem;
  color: #7f1d1d;
  font-family: 'Courier New', monospace;
  font-size: 0.875rem;
  line-height: 1.6;
  word-break: break-word;
}

.error-suggestion {
  padding: 1rem 1.25rem;
  background: rgba(251, 191, 36, 0.1);
  border-top: 1px solid rgba(251, 191, 36, 0.3);
  color: #92400e;
  font-size: 0.875rem;
  line-height: 1.5;
}

.error-suggestion strong {
  color: #78350f;
}

@media (max-width: 768px) {
  .status-bar {
    flex-direction: column;
    align-items: flex-start;
    gap: 0.75rem;
  }

  .editor-controls {
    width: 100%;
    flex-direction: column;
  }

  .execute-btn,
  .cancel-btn {
    width: 100%;
    justify-content: center;
  }

  .keyboard-hint {
    display: none;
  }

  .empty-state {
    padding: 2rem 1rem;
  }

  .empty-state-features {
    flex-direction: column;
    gap: 1rem;
  }

  .error-actions {
    flex-direction: column;
    width: 100%;
  }

  .btn-text {
    width: 100%;
    justify-content: center;
  }
}
</style>
