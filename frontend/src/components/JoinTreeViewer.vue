<template>
  <div class="join-tree-viewer">
    <div class="viewer-header">
      <h3>Join Tree</h3>
      <div class="stats">
        <span class="stat">
          <strong>{{ treeNodes.length }}</strong> Relations
        </span>
        <span class="stat">
          <strong>{{ maxDepth }}</strong> Max Depth
        </span>
        <span class="badge badge-success">
          Acyclic Query
        </span>
      </div>
    </div>

    <div v-if="!hasJoinTree" class="no-tree-message">
      <p>No join tree available.</p>
      <p class="hint">Join trees are only computed for acyclic queries.</p>
    </div>

    <div v-else ref="cyContainer" class="cy-container"></div>

    <!-- Tooltip for node hover -->
    <div
      v-if="tooltipData"
      class="node-tooltip"
      :style="{ left: tooltipData.x + 'px', top: tooltipData.y + 'px' }"
    >
      <div class="tooltip-header">{{ tooltipData.relation }}</div>
      <div class="tooltip-content">
        <strong>Level:</strong> {{ tooltipData.level }}<br>
        <strong>Attributes:</strong>
        <ul v-if="tooltipData.attributes.length > 0">
          <li v-for="(attr, idx) in tooltipData.attributes" :key="idx">{{ attr }}</li>
        </ul>
        <span v-else class="text-muted">No attributes</span>
      </div>
    </div>

    <div v-if="hasJoinTree" class="viewer-controls">
      <div class="layout-selector">
        <label for="tree-layout-select">Layout:</label>
        <select id="tree-layout-select" v-model="selectedLayout" @change="applyLayout">
          <option value="breadthfirst">Hierarchical (Top-Down)</option>
          <option value="dagre">Hierarchical (Optimized)</option>
          <option value="cose-bilkent">Force-Directed</option>
        </select>
      </div>
      <button @click="fitGraph" class="btn btn-secondary">Fit to View</button>
      <div class="export-dropdown">
        <button @click="toggleExportMenu" class="btn btn-secondary export-btn">
          Export <span class="dropdown-arrow">{{ showExportMenu ? '▲' : '▼' }}</span>
        </button>
        <div v-if="showExportMenu" class="export-menu">
          <button @click="exportImage('png', 2)" class="export-option">PNG (2x)</button>
          <button @click="exportImage('png', 4)" class="export-option">PNG (4x High-Res)</button>
          <button @click="exportImage('svg')" class="export-option">SVG (Vector)</button>
          <button @click="exportImage('jpg')" class="export-option">JPG</button>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, watch, nextTick } from 'vue'
import cytoscape, { type Core, type ElementDefinition } from 'cytoscape'
import type { JoinTreeNode } from '@/types'

interface Props {
  joinTree: JoinTreeNode[] | null | undefined
}

const props = defineProps<Props>()

const cyContainer = ref<HTMLElement | null>(null)
let cy: Core | null = null

const treeNodes = computed(() => props.joinTree || [])
const hasJoinTree = computed(() => treeNodes.value.length > 0)

const maxDepth = computed(() => {
  if (treeNodes.value.length === 0) return 0
  return Math.max(...treeNodes.value.map(n => n.level)) + 1
})

const tooltipData = ref<{
  x: number
  y: number
  relation: string
  level: number
  attributes: string[]
} | null>(null)

const selectedLayout = ref('breadthfirst')
const showExportMenu = ref(false)

// Convert join tree to Cytoscape elements
const convertToElements = (): ElementDefinition[] => {
  const elements: ElementDefinition[] = []

  // Add nodes
  for (const node of treeNodes.value) {
    elements.push({
      data: {
        id: node.id,
        label: node.relation,
        relation: node.relation,
        level: node.level,
        attributes: node.attributes,
        type: 'relation'
      },
      classes: 'tree-node'
    })
  }

  // Add edges (parent-child relationships) with shared attributes
  for (const node of treeNodes.value) {
    if (node.parent) {
      // Find parent node to get shared attributes
      const parentNode = treeNodes.value.find(n => n.id === node.parent)
      let sharedAttrs: string[] = []

      if (parentNode && parentNode.shared_attributes && parentNode.shared_attributes[node.id]) {
        sharedAttrs = parentNode.shared_attributes[node.id]
      }

      // Format label: show first few shared attributes
      const label = sharedAttrs.length > 0
        ? sharedAttrs.slice(0, 2).join(', ') + (sharedAttrs.length > 2 ? '...' : '')
        : ''

      elements.push({
        data: {
          id: `edge-${node.parent}-${node.id}`,
          source: node.parent,
          target: node.id,
          label: label,
          sharedAttributes: sharedAttrs
        },
        classes: 'tree-edge'
      })
    }
  }

  return elements
}

const initializeCytoscape = () => {
  if (!cyContainer.value || !hasJoinTree.value) return

  const elements = convertToElements()

  cy = cytoscape({
    container: cyContainer.value,
    elements: elements,
    style: [
      {
        selector: 'node',
        style: {
          'background-color': '#3b82f6',
          'label': 'data(label)',
          'color': '#fff',
          'text-valign': 'center',
          'text-halign': 'center',
          'font-size': '14px',
          'font-weight': 'bold',
          'width': '80px',
          'height': '50px',
          'shape': 'roundrectangle',
          'border-width': '2px',
          'border-color': '#2563eb',
          'text-outline-width': '2px',
          'text-outline-color': '#3b82f6'
        }
      },
      {
        selector: 'node:selected',
        style: {
          'background-color': '#10b981',
          'border-color': '#059669',
          'border-width': '3px'
        }
      },
      {
        selector: 'edge',
        style: {
          'width': 3,
          'line-color': '#94a3b8',
          'target-arrow-color': '#94a3b8',
          'target-arrow-shape': 'triangle',
          'curve-style': 'bezier',
          'arrow-scale': 1.5,
          'label': 'data(label)',
          'font-size': '10px',
          'text-rotation': 'autorotate',
          'text-background-color': '#ffffff',
          'text-background-opacity': 0.8,
          'text-background-padding': '2px',
          'color': '#64748b'
        }
      },
      {
        selector: 'edge:selected',
        style: {
          'line-color': '#10b981',
          'target-arrow-color': '#10b981',
          'width': 4
        }
      }
    ],
    layout: {
      name: 'breadthfirst',
      directed: true,
      spacingFactor: 1.5,
      padding: 30,
      animate: true,
      animationDuration: 500
    },
    minZoom: 0.3,
    maxZoom: 3,
    wheelSensitivity: 0.2
  })

  // Add event listeners
  cy.on('mouseover', 'node', (event) => {
    const node = event.target
    const pos = event.renderedPosition || event.position

    tooltipData.value = {
      x: pos.x + 15,
      y: pos.y - 15,
      relation: node.data('relation'),
      level: node.data('level'),
      attributes: node.data('attributes') || []
    }
  })

  cy.on('mouseout', 'node', () => {
    tooltipData.value = null
  })

  cy.on('tap', 'node', (event) => {
    const node = event.target
    console.log('Selected join tree node:', node.data())
  })

  // Fit to container
  nextTick(() => {
    if (cy) {
      cy.fit(undefined, 50)
    }
  })
}

const applyLayout = () => {
  if (!cy) return

  let layoutConfig: any = {
    name: selectedLayout.value,
    animate: true,
    animationDuration: 500,
    padding: 30
  }

  if (selectedLayout.value === 'breadthfirst') {
    layoutConfig = {
      ...layoutConfig,
      directed: true,
      spacingFactor: 1.5,
      roots: treeNodes.value.filter(n => n.parent === null).map(n => `#${n.id}`)
    }
  } else if (selectedLayout.value === 'dagre') {
    layoutConfig = {
      name: 'breadthfirst', // fallback if dagre not available
      directed: true,
      spacingFactor: 1.8
    }
  } else if (selectedLayout.value === 'cose-bilkent') {
    layoutConfig = {
      name: 'cose', // fallback if cose-bilkent not available
      animate: true,
      nodeRepulsion: 8000,
      idealEdgeLength: 100
    }
  }

  const layout = cy.layout(layoutConfig)
  layout.run()
}

const fitGraph = () => {
  if (cy) {
    cy.fit(undefined, 50)
  }
}

const toggleExportMenu = () => {
  showExportMenu.value = !showExportMenu.value
}

const exportImage = (format: 'png' | 'svg' | 'jpg' = 'png', scale: number = 2) => {
  if (!cy) return

  let blob: Blob
  let filename: string
  const timestamp = new Date().toISOString().split('T')[0]

  if (format === 'svg') {
    // Export as SVG
    const svgContent = cy.svg({ full: true, bg: 'white' })
    blob = new Blob([svgContent], { type: 'image/svg+xml' })
    filename = `join-tree-${timestamp}.svg`
  } else if (format === 'jpg') {
    // Export as JPG
    blob = cy.jpg({
      output: 'blob',
      bg: 'white',
      full: true,
      quality: 0.95,
      scale: scale
    })
    filename = `join-tree-${timestamp}.jpg`
  } else {
    // Export as PNG (default)
    blob = cy.png({
      output: 'blob',
      bg: 'white',
      full: true,
      scale: scale
    })
    filename = `join-tree-${timestamp}-${scale}x.png`
  }

  const url = URL.createObjectURL(blob)
  const link = document.createElement('a')
  link.href = url
  link.download = filename
  link.click()
  URL.revokeObjectURL(url)

  // Close menu
  showExportMenu.value = false
}

// Watchers
watch(() => props.joinTree, () => {
  if (cy) {
    cy.destroy()
    cy = null
  }
  nextTick(() => {
    initializeCytoscape()
  })
}, { deep: true })

onMounted(() => {
  initializeCytoscape()
})
</script>

<style scoped>
.join-tree-viewer {
  display: flex;
  flex-direction: column;
  height: 100%;
  background: white;
  border-radius: 8px;
  box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
  overflow: hidden;
}

.viewer-header {
  padding: 1rem;
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

.stats {
  display: flex;
  gap: 1rem;
  align-items: center;
  flex-wrap: wrap;
}

.stat {
  font-size: 0.875rem;
  color: #6b7280;
}

.stat strong {
  color: #1f2937;
  font-weight: 600;
}

.badge {
  padding: 0.25rem 0.75rem;
  border-radius: 9999px;
  font-size: 0.75rem;
  font-weight: 600;
  text-transform: uppercase;
}

.badge-success {
  background: #d1fae5;
  color: #065f46;
}

.badge-warning {
  background: #fef3c7;
  color: #92400e;
}

.no-tree-message {
  flex: 1;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  color: #6b7280;
  padding: 2rem;
}

.no-tree-message p {
  margin: 0.5rem 0;
  font-size: 1rem;
}

.no-tree-message .hint {
  font-size: 0.875rem;
  color: #9ca3af;
}

.cy-container {
  flex: 1;
  min-height: 640px;
  background: #ffffff;
  position: relative;
}

.node-tooltip {
  position: fixed;
  background: rgba(31, 41, 55, 0.95);
  color: white;
  padding: 0.75rem;
  border-radius: 6px;
  font-size: 0.875rem;
  pointer-events: none;
  z-index: 1000;
  max-width: 300px;
  box-shadow: 0 4px 6px rgba(0, 0, 0, 0.3);
}

.tooltip-header {
  font-weight: 600;
  font-size: 1rem;
  margin-bottom: 0.5rem;
  color: #10b981;
}

.tooltip-content {
  line-height: 1.6;
}

.tooltip-content ul {
  margin: 0.5rem 0 0 0;
  padding-left: 1.25rem;
  list-style: disc;
}

.tooltip-content li {
  margin: 0.25rem 0;
  font-family: 'Courier New', monospace;
  font-size: 0.8rem;
}

.text-muted {
  color: #9ca3af;
  font-style: italic;
}

.viewer-controls {
  padding: 1rem;
  border-top: 1px solid #e5e7eb;
  display: flex;
  gap: 1rem;
  align-items: center;
  background: #f9fafb;
  flex-wrap: wrap;
}

.layout-selector {
  display: flex;
  align-items: center;
  gap: 0.5rem;
}

.layout-selector label {
  font-size: 0.875rem;
  color: #6b7280;
  font-weight: 500;
}

.layout-selector select {
  padding: 0.5rem;
  border: 1px solid #d1d5db;
  border-radius: 4px;
  font-size: 0.875rem;
  background: white;
  cursor: pointer;
}

.layout-selector select:focus {
  outline: none;
  border-color: #3b82f6;
  box-shadow: 0 0 0 3px rgba(59, 130, 246, 0.1);
}

/* Export dropdown */
.export-dropdown {
  position: relative;
  display: inline-block;
}

.export-btn {
  display: flex;
  align-items: center;
  gap: 0.5rem;
}

.dropdown-arrow {
  font-size: 0.75rem;
  transition: transform 0.2s;
}

.export-menu {
  position: absolute;
  top: calc(100% + 0.5rem);
  right: 0;
  background: white;
  border: 1px solid #D1D5DB;
  border-radius: 0.5rem;
  box-shadow: 0 4px 6px -1px rgba(0, 0, 0, 0.1), 0 2px 4px -1px rgba(0, 0, 0, 0.06);
  z-index: 1000;
  min-width: 180px;
  overflow: hidden;
}

.export-option {
  display: block;
  width: 100%;
  padding: 0.75rem 1rem;
  text-align: left;
  background: white;
  border: none;
  cursor: pointer;
  font-size: 0.875rem;
  color: #1F2937;
  transition: background-color 0.2s;
}

.export-option:hover {
  background: #F3F4F6;
  color: #3B82F6;
}

.export-option:not(:last-child) {
  border-bottom: 1px solid #E5E7EB;
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

.btn-secondary {
  background: #e5e7eb;
  color: #374151;
}

.btn-secondary:hover {
  background: #d1d5db;
}

.btn:active {
  transform: translateY(1px);
}
</style>
