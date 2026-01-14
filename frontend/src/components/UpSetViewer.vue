<template>
  <div class="upset-viewer">
    <div class="viewer-header">
      <h3>UpSet Plot - Set Intersections</h3>
      <div class="stats">
        <span class="stat">
          <strong>{{ stats.num_edges }}</strong> Relations (Sets)
        </span>
        <span class="stat">
          <strong>{{ stats.num_nodes }}</strong> Attributes
        </span>
        <span class="stat">
          <strong>{{ intersections.length }}</strong> Intersections
        </span>
      </div>
    </div>

    <div class="upset-container">
      <!-- Intersection size bar chart -->
      <div class="intersection-bars">
        <div class="bars-header">
          <span>Intersection Size</span>
        </div>
        <div class="bars-chart">
          <div
            v-for="(intersection, idx) in sortedIntersections"
            :key="idx"
            class="bar-container"
            @mouseenter="highlightIntersection(idx)"
            @mouseleave="clearHighlight"
          >
            <div
              class="bar"
              :style="{
                height: (intersection.size / maxIntersectionSize * 100) + '%',
                backgroundColor: highlightedIdx === idx ? '#3B82F6' : '#94A3B8'
              }"
            >
              <span class="bar-label">{{ intersection.size }}</span>
            </div>
          </div>
        </div>
      </div>

      <!-- Intersection matrix -->
      <div class="intersection-matrix">
        <div class="matrix-header">
          <div class="relation-labels">
            <div
              v-for="relation in relations"
              :key="relation"
              class="relation-label"
              :style="{ color: getRelationColor(relation) }"
            >
              {{ relation }}
            </div>
          </div>
        </div>
        <div class="matrix-body">
          <div
            v-for="(intersection, idx) in sortedIntersections"
            :key="idx"
            class="matrix-column"
            :class="{ highlighted: highlightedIdx === idx }"
            @mouseenter="highlightIntersection(idx)"
            @mouseleave="clearHighlight"
          >
            <div
              v-for="relation in relations"
              :key="relation"
              class="matrix-cell"
            >
              <div
                v-if="intersection.relations.has(relation)"
                class="cell-dot"
                :style="{ backgroundColor: getRelationColor(relation) }"
              ></div>
              <div
                v-if="intersection.relations.has(relation) && intersection.relations.size > 1"
                class="connecting-line"
                :style="{ backgroundColor: getRelationColor(relation) }"
              ></div>
            </div>
          </div>
        </div>
      </div>

      <!-- Attributes list for selected intersection -->
      <div v-if="highlightedIdx !== null" class="intersection-details">
        <h4>Intersection {{ highlightedIdx + 1 }}</h4>
        <div class="detail-relations">
          <strong>Relations:</strong>
          {{ Array.from(sortedIntersections[highlightedIdx].relations).join(', ') }}
        </div>
        <div class="detail-attributes">
          <strong>Attributes ({{ sortedIntersections[highlightedIdx].attributes.length }}):</strong>
          <ul>
            <li v-for="attr in sortedIntersections[highlightedIdx].attributes" :key="attr">
              {{ attr }}
            </li>
          </ul>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch } from 'vue'
import type { Hypergraph } from '@/types'

interface Props {
  hypergraph: Hypergraph
}

interface Intersection {
  relations: Set<string>
  attributes: string[]
  size: number
}

const props = defineProps<Props>()

const highlightedIdx = ref<number | null>(null)

const stats = computed(() => ({
  num_nodes: props.hypergraph.nodes.length,
  num_edges: props.hypergraph.edges.length
}))

// Extract unique relations
const relations = computed(() => {
  return props.hypergraph.edges.map(e => e.id).sort()
})

// Build intersections
const intersections = computed(() => {
  const result: Intersection[] = []
  const nodeToRelations = new Map<string, Set<string>>()

  // Build mapping: node -> set of relations containing it
  for (const edge of props.hypergraph.edges) {
    for (const nodeId of edge.nodes) {
      if (!nodeToRelations.has(nodeId)) {
        nodeToRelations.set(nodeId, new Set())
      }
      nodeToRelations.get(nodeId)!.add(edge.id)
    }
  }

  // Find all unique combinations of relations
  const combinationMap = new Map<string, Intersection>()

  for (const [nodeId, rels] of nodeToRelations.entries()) {
    const key = Array.from(rels).sort().join(',')

    if (!combinationMap.has(key)) {
      combinationMap.set(key, {
        relations: new Set(rels),
        attributes: [],
        size: 0
      })
    }

    const node = props.hypergraph.nodes.find(n => n.id === nodeId)
    if (node) {
      combinationMap.get(key)!.attributes.push(...node.attributes)
    }
  }

  // Convert to array and calculate sizes
  for (const intersection of combinationMap.values()) {
    intersection.size = intersection.attributes.length
    result.push(intersection)
  }

  return result
})

const sortedIntersections = computed(() => {
  return [...intersections.value].sort((a, b) => b.size - a.size)
})

const maxIntersectionSize = computed(() => {
  return Math.max(...intersections.value.map(i => i.size), 1)
})

// Color mapping for relations
const colorPalette = [
  '#3B82F6', '#10B981', '#F59E0B', '#EF4444', '#8B5CF6',
  '#EC4899', '#06B6D4', '#84CC16', '#F97316', '#6366F1'
]

function getRelationColor(relation: string): string {
  const idx = relations.value.indexOf(relation)
  return colorPalette[idx % colorPalette.length]
}

function highlightIntersection(idx: number) {
  highlightedIdx.value = idx
}

function clearHighlight() {
  highlightedIdx.value = null
}
</script>

<style scoped>
.upset-viewer {
  display: flex;
  flex-direction: column;
  height: 1200px;
  background: white;
  border-radius: 0.5rem;
  overflow: hidden;
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
}

.viewer-header {
  padding: 1rem;
  border-bottom: 1px solid var(--color-border);
}

.viewer-header h3 {
  margin: 0 0 0.5rem 0;
  font-size: 1.25rem;
  font-weight: 600;
}

.stats {
  display: flex;
  gap: 1.5rem;
  flex-wrap: wrap;
}

.stat {
  font-size: 0.875rem;
  color: #6B7280;
}

.stat strong {
  color: #1F2937;
  font-size: 1rem;
}

.upset-container {
  display: flex;
  flex-direction: column;
  flex: 1;
  overflow: auto;
  padding: 1rem;
}

/* Intersection bars */
.intersection-bars {
  display: flex;
  gap: 1rem;
  margin-bottom: 1rem;
}

.bars-header {
  width: 150px;
  display: flex;
  align-items: flex-end;
  padding-bottom: 0.5rem;
  font-weight: 600;
  font-size: 0.875rem;
  color: #4B5563;
}

.bars-chart {
  flex: 1;
  display: flex;
  gap: 4px;
  align-items: flex-end;
  height: 200px;
  border-bottom: 2px solid #E5E7EB;
  padding: 0 0.5rem;
}

.bar-container {
  flex: 1;
  min-width: 20px;
  max-width: 60px;
  display: flex;
  align-items: flex-end;
  cursor: pointer;
  transition: opacity 0.2s;
}

.bar-container:hover {
  opacity: 0.8;
}

.bar {
  width: 100%;
  background: #94A3B8;
  border-radius: 4px 4px 0 0;
  position: relative;
  min-height: 4px;
  transition: background-color 0.2s, transform 0.2s;
  display: flex;
  align-items: flex-start;
  justify-content: center;
}

.bar:hover {
  transform: translateY(-2px);
}

.bar-label {
  font-size: 0.75rem;
  font-weight: 600;
  color: white;
  padding: 0.125rem 0.25rem;
  text-shadow: 0 1px 2px rgba(0, 0, 0, 0.3);
}

/* Intersection matrix */
.intersection-matrix {
  display: flex;
  gap: 1rem;
}

.matrix-header {
  width: 150px;
}

.relation-labels {
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
  padding-top: 0.5rem;
}

.relation-label {
  font-size: 0.875rem;
  font-weight: 600;
  height: 24px;
  display: flex;
  align-items: center;
}

.matrix-body {
  flex: 1;
  display: flex;
  gap: 4px;
  padding: 0 0.5rem;
}

.matrix-column {
  flex: 1;
  min-width: 20px;
  max-width: 60px;
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
  padding: 0.5rem 0;
  cursor: pointer;
  border-radius: 4px;
  transition: background-color 0.2s;
}

.matrix-column:hover,
.matrix-column.highlighted {
  background-color: #F3F4F6;
}

.matrix-cell {
  height: 24px;
  display: flex;
  align-items: center;
  justify-content: center;
  position: relative;
}

.cell-dot {
  width: 10px;
  height: 10px;
  border-radius: 50%;
  background: #3B82F6;
  z-index: 2;
  position: relative;
}

.connecting-line {
  position: absolute;
  width: 3px;
  height: 100%;
  background: #94A3B8;
  z-index: 1;
}

/* Intersection details */
.intersection-details {
  margin-top: 2rem;
  padding: 1rem;
  background: #F9FAFB;
  border-radius: 0.5rem;
  border: 1px solid #E5E7EB;
}

.intersection-details h4 {
  margin: 0 0 0.75rem 0;
  font-size: 1rem;
  font-weight: 600;
}

.detail-relations,
.detail-attributes {
  margin-bottom: 0.75rem;
  font-size: 0.875rem;
}

.detail-attributes ul {
  margin: 0.5rem 0 0 0;
  padding-left: 1.5rem;
  list-style-type: disc;
  max-height: 200px;
  overflow-y: auto;
}

.detail-attributes li {
  margin: 0.25rem 0;
  font-family: 'Monaco', 'Menlo', 'Consolas', monospace;
  font-size: 0.813rem;
}
</style>
