<template>
  <div class="gyo-step-viewer">
    <div class="viewer-header">
      <h3>GYO Algorithm Steps</h3>
      <div class="step-counter">
        Step {{ currentStep + 1 }} of {{ totalSteps }}
      </div>
    </div>

    <div v-if="!hasSteps" class="no-steps-message">
      <p>No GYO steps available.</p>
      <p class="hint">GYO steps are recorded during acyclicity checking.</p>
    </div>

    <div v-else class="step-content">
      <!-- Step Navigation -->
      <div class="step-controls">
        <button
          @click="previousStep"
          :disabled="currentStep === 0"
          class="btn btn-icon"
        >
          ← Previous
        </button>

        <div class="step-indicator">
          <div
            v-for="(step, index) in steps"
            :key="index"
            :class="['step-dot', { active: index === currentStep, completed: index < currentStep }]"
            @click="goToStep(index)"
            :title="`Step ${index + 1}`"
          ></div>
        </div>

        <button
          @click="nextStep"
          :disabled="currentStep === totalSteps - 1"
          class="btn btn-icon"
        >
          Next →
        </button>
      </div>

      <!-- Step Details -->
      <div v-if="currentStepData" class="step-details">
        <div class="step-action-badge" :class="`action-${currentStepData.action}`">
          {{ currentStepData.action === 'remove_ear' ? 'Remove Ear' : 'Remove Vertex' }}
        </div>

        <p class="step-description">{{ currentStepData.description }}</p>

        <div class="step-info-grid">
          <div v-if="currentStepData.removed_edges.length > 0" class="info-card removed">
            <h4>Removed Edges</h4>
            <ul>
              <li v-for="edge in currentStepData.removed_edges" :key="edge">
                {{ getEdgeLabel(edge) }}
              </li>
            </ul>
          </div>

          <div v-if="filterSchemaAttributes(currentStepData.removed_nodes).length > 0" class="info-card removed">
            <h4>Removed Vertices</h4>
            <ul>
              <li v-for="node in filterSchemaAttributes(currentStepData.removed_nodes)" :key="node">
                {{ getNodeLabel(node) }}
              </li>
            </ul>
          </div>

          <div v-if="currentStepData.remaining_edges.length > 0" class="info-card remaining">
            <h4>Remaining Edges</h4>
            <ul>
              <li v-for="edge in currentStepData.remaining_edges" :key="edge">
                {{ getEdgeLabel(edge) }}
              </li>
            </ul>
          </div>

          <div v-if="filterSchemaAttributes(currentStepData.remaining_nodes).length > 0" class="info-card remaining">
            <h4>Remaining Vertices</h4>
            <ul>
              <li v-for="node in filterSchemaAttributes(currentStepData.remaining_nodes)" :key="node">
                {{ getNodeLabel(node) }}
              </li>
            </ul>
          </div>
        </div>

        <!-- Phase 3: Subset Relationship Details (for ear removal steps) -->
        <div v-if="currentStepData.action === 'remove_ear'" class="subset-details">
          <div v-if="currentStepData.why_ear">
            <h4 class="subset-title">Why This is an Ear</h4>
            <p class="why-ear-explanation">{{ currentStepData.why_ear }}</p>
          </div>

          <div v-if="currentStepData.subset_relationships && currentStepData.subset_relationships.length > 0" class="subset-relationships">
            <h5>Detailed Subset Relationships</h5>
            <div
              v-for="(rel, index) in currentStepData.subset_relationships"
              :key="index"
              class="relationship-card"
            >
              <div class="relationship-header">
                <span class="ear-label">{{ getEdgeLabel(rel.ear) }}</span>
                <span class="subset-symbol">⊆</span>
                <span class="container-label">{{ getEdgeLabel(rel.container) }}</span>
              </div>

              <div class="relationship-details">
                <div class="node-list">
                  <strong>Ear vertices:</strong>
                  <span class="node-tags">
                    <span v-for="node in filterSchemaAttributes(rel.ear_nodes)" :key="node" class="node-tag">{{ getNodeLabel(node) }}</span>
                  </span>
                </div>

                <div class="node-list">
                  <strong>Container vertices:</strong>
                  <span class="node-tags">
                    <span v-for="node in filterSchemaAttributes(rel.container_nodes)" :key="node" class="node-tag">{{ getNodeLabel(node) }}</span>
                  </span>
                </div>

                <div v-if="filterSchemaAttributes(rel.extra_in_container).length > 0" class="node-list">
                  <strong>Extra in container:</strong>
                  <span class="node-tags">
                    <span v-for="node in filterSchemaAttributes(rel.extra_in_container)" :key="node" class="node-tag extra">{{ getNodeLabel(node) }}</span>
                  </span>
                </div>
              </div>
            </div>
          </div>
        </div>

        <!-- Progress Indicator -->
        <div class="progress-indicator">
          <div class="progress-bar">
            <div
              class="progress-fill"
              :style="{ width: `${((currentStep + 1) / totalSteps) * 100}%` }"
            ></div>
          </div>
          <p class="progress-text">
            {{ currentStepData.remaining_edges.length === 0
              ? '✓ Reduction complete - Query is ACYCLIC!'
              : `${currentStepData.remaining_edges.length} edges remaining`
            }}
          </p>
        </div>
      </div>

      <!-- Auto-play Controls -->
      <div class="playback-controls">
        <button @click="toggleAutoPlay" class="btn btn-secondary">
          {{ isAutoPlaying ? '⏸ Pause' : '▶ Auto-play' }}
        </button>
        <label class="speed-control">
          Speed:
          <select v-model="playbackSpeed">
            <option :value="2000">Slow</option>
            <option :value="1000">Normal</option>
            <option :value="500">Fast</option>
          </select>
        </label>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch, onUnmounted } from 'vue'
import type { GYOStep, Hypergraph, HypergraphNode, HypergraphEdge } from '@/types'

interface Props {
  steps: GYOStep[] | null | undefined
  hypergraph?: Hypergraph | null
}

const props = defineProps<Props>()

// Helper functions to convert IDs to labels
const getNodeLabel = (nodeId: string): string => {
  if (!props.hypergraph) return nodeId
  const node = props.hypergraph.nodes.find((n: HypergraphNode) => n.id === nodeId)
  return node?.label || nodeId
}

const getEdgeLabel = (edgeId: string): string => {
  if (!props.hypergraph) return edgeId
  const edge = props.hypergraph.edges.find((e: HypergraphEdge) => e.id === edgeId)
  return edge?.label || edgeId
}

// Filter out schema_attribute nodes (non-join, non-output attributes)
const filterSchemaAttributes = (nodeIds: string[]): string[] => {
  if (!props.hypergraph) return nodeIds
  return nodeIds.filter(nodeId => {
    const node = props.hypergraph!.nodes.find((n: HypergraphNode) => n.id === nodeId)
    return node?.type !== 'schema_attribute'
  })
}

interface Emits {
  (e: 'stepChange', stepNumber: number): void
}

const emit = defineEmits<Emits>()

const currentStep = ref(0)
const isAutoPlaying = ref(false)
const playbackSpeed = ref(1000)
let autoPlayInterval: number | null = null

const hasSteps = computed(() => props.steps && props.steps.length > 0)
const totalSteps = computed(() => props.steps?.length || 0)

const currentStepData = computed(() => {
  if (!props.steps || currentStep.value >= props.steps.length) return null
  return props.steps[currentStep.value]
})

const nextStep = () => {
  if (currentStep.value < totalSteps.value - 1) {
    currentStep.value++
    emit('stepChange', currentStep.value)
  } else if (isAutoPlaying.value) {
    stopAutoPlay()
  }
}

const previousStep = () => {
  if (currentStep.value > 0) {
    currentStep.value--
    emit('stepChange', currentStep.value)
  }
}

const goToStep = (index: number) => {
  if (index >= 0 && index < totalSteps.value) {
    currentStep.value = index
    emit('stepChange', currentStep.value)
  }
}

const toggleAutoPlay = () => {
  if (isAutoPlaying.value) {
    stopAutoPlay()
  } else {
    startAutoPlay()
  }
}

const startAutoPlay = () => {
  isAutoPlaying.value = true
  autoPlayInterval = window.setInterval(() => {
    nextStep()
  }, playbackSpeed.value)
}

const stopAutoPlay = () => {
  isAutoPlaying.value = false
  if (autoPlayInterval !== null) {
    clearInterval(autoPlayInterval)
    autoPlayInterval = null
  }
}

// Restart auto-play when speed changes
watch(playbackSpeed, () => {
  if (isAutoPlaying.value) {
    stopAutoPlay()
    startAutoPlay()
  }
})

// Reset step when steps change
watch(() => props.steps, () => {
  currentStep.value = 0
  stopAutoPlay()
  emit('stepChange', 0)
}, { deep: true })

// Cleanup on unmount
onUnmounted(() => {
  stopAutoPlay()
})
</script>

<style scoped>
.gyo-step-viewer {
  background: white;
  border-radius: 8px;
  box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
  overflow: hidden;
}

.viewer-header {
  padding: 1rem 1.5rem;
  border-bottom: 1px solid #e5e7eb;
  display: flex;
  justify-content: space-between;
  align-items: center;
  background: #f9fafb;
}

.viewer-header h3 {
  margin: 0;
  font-size: 1.25rem;
  color: #1f2937;
}

.step-counter {
  font-size: 0.875rem;
  color: #6b7280;
  font-weight: 600;
  background: #e5e7eb;
  padding: 0.25rem 0.75rem;
  border-radius: 9999px;
}

.no-steps-message {
  padding: 3rem;
  text-align: center;
  color: #6b7280;
}

.no-steps-message p {
  margin: 0.5rem 0;
}

.no-steps-message .hint {
  font-size: 0.875rem;
  color: #9ca3af;
}

.step-content {
  padding: 1.5rem;
}

.step-controls {
  display: flex;
  align-items: center;
  gap: 1rem;
  margin-bottom: 1.5rem;
}

.step-indicator {
  display: flex;
  gap: 0.5rem;
  flex: 1;
  justify-content: center;
  align-items: center;
}

.step-dot {
  width: 12px;
  height: 12px;
  border-radius: 50%;
  background: #e5e7eb;
  cursor: pointer;
  transition: all 0.2s;
}

.step-dot:hover {
  transform: scale(1.2);
}

.step-dot.active {
  background: #3b82f6;
  transform: scale(1.3);
}

.step-dot.completed {
  background: #10b981;
}

.step-details {
  background: #f9fafb;
  border-radius: 6px;
  padding: 1.5rem;
}

.step-action-badge {
  display: inline-block;
  padding: 0.25rem 0.75rem;
  border-radius: 4px;
  font-size: 0.75rem;
  font-weight: 600;
  text-transform: uppercase;
  margin-bottom: 1rem;
}

.action-remove_ear {
  background: #dbeafe;
  color: #1e40af;
}

.action-remove_node {
  background: #dcfce7;
  color: #166534;
}

.step-description {
  font-size: 1rem;
  color: #374151;
  margin: 0 0 1.5rem 0;
  line-height: 1.6;
}

.step-info-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
  gap: 1rem;
  margin-bottom: 1.5rem;
}

.info-card {
  background: white;
  border-radius: 6px;
  padding: 1rem;
  border-left: 4px solid;
}

.info-card.removed {
  border-left-color: #ef4444;
  background: #fef2f2;
}

.info-card.remaining {
  border-left-color: #3b82f6;
  background: #eff6ff;
}

.info-card h4 {
  margin: 0 0 0.5rem 0;
  font-size: 0.875rem;
  font-weight: 600;
  color: #1f2937;
}

.info-card ul {
  margin: 0;
  padding-left: 1.25rem;
  list-style: disc;
}

.info-card li {
  font-size: 0.875rem;
  color: #4b5563;
  margin: 0.25rem 0;
  font-family: 'Courier New', monospace;
}

.progress-indicator {
  margin-top: 1.5rem;
}

.progress-bar {
  width: 100%;
  height: 8px;
  background: #e5e7eb;
  border-radius: 9999px;
  overflow: hidden;
  margin-bottom: 0.5rem;
}

.progress-fill {
  height: 100%;
  background: linear-gradient(to right, #3b82f6, #10b981);
  transition: width 0.3s ease;
}

.progress-text {
  text-align: center;
  font-size: 0.875rem;
  font-weight: 600;
  color: #374151;
  margin: 0;
}

.playback-controls {
  display: flex;
  align-items: center;
  gap: 1rem;
  justify-content: center;
  padding-top: 1.5rem;
  border-top: 1px solid #e5e7eb;
  margin-top: 1.5rem;
}

.speed-control {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  font-size: 0.875rem;
  color: #6b7280;
}

.speed-control select {
  padding: 0.25rem 0.5rem;
  border: 1px solid #d1d5db;
  border-radius: 4px;
  font-size: 0.875rem;
  background: white;
  cursor: pointer;
}

.btn {
  padding: 0.5rem 1rem;
  border: none;
  border-radius: 4px;
  font-size: 0.875rem;
  font-weight: 500;
  cursor: pointer;
  transition: all 0.2s;
}

.btn-icon {
  padding: 0.5rem 0.75rem;
  background: #e5e7eb;
  color: #374151;
}

.btn-icon:hover:not(:disabled) {
  background: #d1d5db;
}

.btn-icon:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.btn-secondary {
  background: #3b82f6;
  color: white;
}

.btn-secondary:hover {
  background: #2563eb;
}

.btn:active {
  transform: translateY(1px);
}

/* Phase 3: Subset Relationship Styles */
.subset-details {
  margin-top: 1.5rem;
  padding: 1rem;
  background: #f0f9ff;
  border-left: 4px solid #3b82f6;
  border-radius: 6px;
}

.subset-title {
  margin: 0 0 0.75rem 0;
  font-size: 0.938rem;
  font-weight: 600;
  color: #1e40af;
}

.why-ear-explanation {
  font-size: 0.938rem;
  color: #1e40af;
  margin: 0 0 1rem 0;
  font-family: 'Courier New', monospace;
  background: white;
  padding: 0.75rem;
  border-radius: 4px;
}

.subset-relationships {
  margin-top: 1rem;
}

.subset-relationships h5 {
  margin: 0 0 0.75rem 0;
  font-size: 0.875rem;
  font-weight: 600;
  color: #374151;
}

.relationship-card {
  background: white;
  border: 1px solid #bfdbfe;
  border-radius: 6px;
  padding: 1rem;
  margin-bottom: 0.75rem;
}

.relationship-card:last-child {
  margin-bottom: 0;
}

.relationship-header {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  margin-bottom: 0.75rem;
  padding-bottom: 0.75rem;
  border-bottom: 1px solid #e5e7eb;
}

.ear-label {
  font-family: 'Courier New', monospace;
  font-weight: 600;
  color: #ef4444;
  background: #fef2f2;
  padding: 0.25rem 0.5rem;
  border-radius: 4px;
  font-size: 0.875rem;
}

.subset-symbol {
  font-size: 1.25rem;
  color: #3b82f6;
  font-weight: bold;
}

.container-label {
  font-family: 'Courier New', monospace;
  font-weight: 600;
  color: #3b82f6;
  background: #eff6ff;
  padding: 0.25rem 0.5rem;
  border-radius: 4px;
  font-size: 0.875rem;
}

.relationship-details {
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
}

.node-list {
  display: flex;
  flex-direction: column;
  gap: 0.25rem;
}

.node-list strong {
  font-size: 0.813rem;
  color: #6b7280;
}

.node-tags {
  display: flex;
  flex-wrap: wrap;
  gap: 0.25rem;
}

.node-tag {
  font-family: 'Courier New', monospace;
  font-size: 0.75rem;
  background: #f3f4f6;
  color: #374151;
  padding: 0.125rem 0.375rem;
  border-radius: 3px;
  border: 1px solid #d1d5db;
}

.node-tag.extra {
  background: #fef3c7;
  color: #92400e;
  border-color: #fde047;
}

/* Debug notice */
.debug-notice {
  background: #fef3c7;
  border: 1px solid #f59e0b;
  border-left: 4px solid #f59e0b;
  padding: 0.75rem;
  border-radius: 4px;
  font-size: 0.875rem;
  color: #92400e;
}

.debug-notice strong {
  color: #78350f;
}
</style>
