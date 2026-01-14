<template>
  <div class="container">
    <div v-if="loading" class="loading-section">
      <div class="spinner"></div>
      <p>Loading query...</p>
    </div>

    <div v-else-if="error" class="error-message">
      {{ error }}
    </div>

    <div v-else-if="query" class="query-detail">
      <div class="detail-header">
        <div>
          <h2>{{ query.name }}</h2>
          <div class="query-id">ID: {{ query.query_id }}</div>
        </div>
        <span class="badge badge-primary">{{ query.category.toUpperCase() }}</span>
      </div>

      <div class="detail-grid">
        <div class="detail-card">
          <h3>Query Statistics</h3>
          <div class="stats-list">
            <div class="stat-item">
              <span class="stat-label">Number of Joins</span>
              <span class="stat-value">{{ query.num_joins }}</span>
            </div>
            <div class="stat-item">
              <span class="stat-label">Number of Aggregates</span>
              <span class="stat-value">{{ query.num_aggregates }}</span>
            </div>
            <div class="stat-item">
              <span class="stat-label">Tables Involved</span>
              <span class="stat-value">{{ query.tables.length }}</span>
            </div>
          </div>
          <div class="tables-list">
            <strong>Tables:</strong>
            <ul>
              <li v-for="table in query.tables" :key="table">{{ table }}</li>
            </ul>
          </div>
        </div>

        <div class="detail-card">
          <h3>SQL Query</h3>
          <SqlDisplay v-if="query && query.sql" :sql="query.sql" height="600px" />
          <p v-else-if="query && !query.sql" class="no-sql">No SQL content available</p>
        </div>
      </div>

      <div class="actions-section">
        <button @click="executeQuery" class="btn btn-primary">Execute This Query</button>
        <button @click="viewHypergraph" class="btn btn-secondary">View Hypergraph</button>
      </div>

      <div v-if="hypergraphData || rawHypergraphData" class="hypergraph-section">
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
              v-if="hypergraphData"
              :visualization-data="hypergraphData"
              :show-bubble-sets="vizMode === 'graph-bubbles'"
              :show-convex-hulls="vizMode === 'bubble-sets'"
            />
          </div>
          <div v-if="hasJoinTree" class="viz-panel join-tree-panel">
            <JoinTreeViewer :join-tree="rawHypergraphData?.join_tree" />
          </div>
        </div>
        <UpSetViewer
          v-if="rawHypergraphData && vizMode === 'upset'"
          :hypergraph="rawHypergraphData"
        />
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
              v-if="hypergraphData"
              :visualization-data="hypergraphData"
              :show-bubble-sets="false"
              :gyo-step="currentGYOStep"
              :gyo-steps="rawHypergraphData?.gyo_steps"
            />
          </div>

          <!-- GYO step controls -->
          <div class="gyo-steps-panel">
            <GYOStepViewer
              :steps="rawHypergraphData?.gyo_steps"
              :hypergraph="rawHypergraphData"
              @step-change="handleGYOStepChange"
            />
          </div>
        </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { useRoute, useRouter } from 'vue-router'
import { queryApi, hypergraphApi } from '@/services/api'
import HypergraphViewer from '@/components/HypergraphViewer.vue'
import JoinTreeViewer from '@/components/JoinTreeViewer.vue'
import GYOStepViewer from '@/components/GYOStepViewer.vue'
import UpSetViewer from '@/components/UpSetViewer.vue'
import SqlDisplay from '@/components/SqlDisplay.vue'
import type { QueryMetadata, VisualizationData, Hypergraph } from '@/types'

const route = useRoute()
const router = useRouter()

const query = ref<QueryMetadata | null>(null)
const hypergraphData = ref<VisualizationData | null>(null)
const rawHypergraphData = ref<Hypergraph | null>(null)
const vizMode = ref<'graph' | 'graph-bubbles' | 'bubble-sets' | 'upset'>('graph')
const loading = ref(false)
const error = ref('')

// GYO animation state
const currentGYOStep = ref<number | null>(null)
const gyoSectionExpanded = ref(false)

// Check if we have a join tree to display
const hasJoinTree = computed(() => {
  return rawHypergraphData.value?.join_tree &&
         rawHypergraphData.value.join_tree.length > 0 &&
         rawHypergraphData.value.is_acyclic
})

// Check if we have GYO steps for animation
const hasGYOSteps = computed(() => {
  return rawHypergraphData.value?.gyo_steps && rawHypergraphData.value.gyo_steps.length > 0
})

// Handle GYO step changes from the step viewer
const handleGYOStepChange = (stepNumber: number) => {
  currentGYOStep.value = stepNumber
  console.log('GYO step changed to:', stepNumber)
}

onMounted(async () => {
  await loadQuery()
})

async function loadQuery() {
  loading.value = true
  error.value = ''

  try {
    const queryId = route.params.id as string
    query.value = await queryApi.getQuery(queryId)
  } catch (err: any) {
    error.value = err.response?.data?.detail || err.message || 'Failed to load query'
  } finally {
    loading.value = false
  }
}

async function viewHypergraph() {
  if (!query.value) return

  try {
    const hypergraph = await hypergraphApi.getQueryHypergraph(query.value.query_id, true)

    // Store raw hypergraph for UpSet viewer
    rawHypergraphData.value = hypergraph

    // Initialize GYO animation to step 0 if we have GYO steps
    if (hypergraph.gyo_steps && hypergraph.gyo_steps.length > 0) {
      currentGYOStep.value = 0
      console.log('Initialized GYO animation with', hypergraph.gyo_steps.length, 'steps')
    }

    // Generate visualization data for graph viewers
    hypergraphData.value = await hypergraphApi.generateVisualization(hypergraph)
  } catch (err: any) {
    error.value = 'Failed to load hypergraph: ' + err.message
  }
}

function executeQuery() {
  if (!query.value) return

  router.push({
    name: 'execute',
    state: { sql: query.value.sql, queryId: query.value.query_id }
  })
}
</script>

<style scoped>
.query-detail {
  max-width: 1200px;
  margin: 0 auto;
}

.detail-header {
  display: flex;
  justify-content: space-between;
  align-items: flex-start;
  margin-bottom: 2rem;
}

.detail-header h2 {
  font-size: 2rem;
  margin-bottom: 0.5rem;
}

.query-id {
  color: var(--color-text-secondary);
  font-family: monospace;
  font-size: 0.875rem;
}

.detail-grid {
  display: grid;
  grid-template-columns: 300px 1fr;
  gap: 1.5rem;
  margin-bottom: 2rem;
}

.detail-card {
  background: white;
  border-radius: 0.5rem;
  padding: 1.5rem;
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
}

.detail-card h3 {
  margin: 0 0 1rem 0;
  font-size: 1.125rem;
  font-weight: 600;
}

.stats-list {
  display: grid;
  gap: 1rem;
  margin-bottom: 1.5rem;
}

.stat-item {
  display: flex;
  justify-content: space-between;
  padding: 0.75rem;
  background: #F9FAFB;
  border-radius: 0.375rem;
}

.stat-label {
  color: var(--color-text-secondary);
}

.stat-value {
  font-weight: 600;
  font-size: 1.125rem;
}

.tables-list {
  padding-top: 1rem;
  border-top: 1px solid var(--color-border);
}

.tables-list ul {
  margin: 0.5rem 0 0 0;
  padding-left: 1.5rem;
}

.actions-section {
  display: flex;
  gap: 1rem;
  margin-bottom: 2rem;
}

.hypergraph-section {
  margin-top: 2rem;
}

.loading-section {
  text-align: center;
  padding: 3rem;
}

.error-message {
  background: #FEE2E2;
  border: 1px solid #FCA5A5;
  color: #991B1B;
  padding: 1rem;
  border-radius: 0.5rem;
}

@media (max-width: 768px) {
  .detail-grid {
    grid-template-columns: 1fr;
  }
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

.viz-panel {
  width: 100%;
  min-height: 500px;
}

.viz-panel.join-tree-panel {
  margin-top: 1rem;
  min-height: 400px;
}

@media (max-width: 1200px) {
  .viz-panel {
    min-height: 400px;
  }

  .viz-panel.join-tree-panel {
    min-height: 350px;
  }
}

/* GYO Animation Section */
.section {
  margin-top: 2rem;
}

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

/* Responsive layout for GYO animation */
@media (max-width: 1200px) {
  .gyo-animation-container {
    grid-template-columns: 1fr;
  }

  .gyo-steps-panel {
    position: static;
  }
}
</style>
