<template>
  <div class="hypergraph-viewer">
    <div class="viewer-header">
      <h3>Query Hypergraph</h3>
      <div class="stats">
        <span class="stat">
          <strong>{{ stats.num_nodes }}</strong> Equivalence Classes
        </span>
        <span class="stat">
          <strong>{{ stats.num_edges }}</strong> Relations
        </span>
        <span v-if="stats.num_joins !== undefined" class="stat">
          <strong>{{ stats.num_joins }}</strong> Joins
        </span>
        <span v-if="stats.num_aggregates !== undefined && stats.num_aggregates > 0" class="stat">
          <strong>{{ stats.num_aggregates }}</strong> Aggregates
        </span>
        <span class="badge" :class="stats.is_acyclic ? 'badge-success' : 'badge-warning'">
          {{ stats.is_acyclic ? 'Acyclic' : 'Cyclic' }}
        </span>
        <span v-if="stats.hypertree_width" class="stat">
          Width: <strong>{{ stats.hypertree_width }}</strong>
        </span>
      </div>
    </div>

    <div ref="cyContainer" class="cy-container"></div>

    <!-- Tooltip for node hover -->
    <div
      v-if="tooltipData"
      class="node-tooltip"
      :style="{ left: tooltipData.x + 'px', top: tooltipData.y + 'px' }"
    >
      <div class="tooltip-header">{{ tooltipData.label }}</div>
      <div class="tooltip-content">
        <strong>Type:</strong> {{ tooltipData.type }}<br>
        <strong>Attributes:</strong>
        <ul>
          <li
            v-for="(attr, idx) in tooltipData.attributes"
            :key="idx"
            :class="{ 'output-attribute': tooltipData.outputAttributes.includes(attr) }"
          >
            {{ attr }}
            <span v-if="tooltipData.outputAttributes.includes(attr)" class="output-badge">OUTPUT</span>
          </li>
        </ul>
        <!-- Phase 2: Show GYO subset relationship info -->
        <div v-if="tooltipData.gyoTooltip" class="gyo-tooltip-section">
          <strong>Subset Relationship:</strong><br>
          <span class="gyo-tooltip-text">{{ tooltipData.gyoTooltip }}</span>
        </div>
      </div>
    </div>

    <div class="viewer-controls">
      <label class="toggle-control">
        <input type="checkbox" v-model="hideSingletons" @change="toggleSingletons">
        <span>Hide singleton attributes</span>
      </label>
      <label class="toggle-control">
        <input type="checkbox" v-model="showSchemaAttributes" @change="toggleSchemaAttributes">
        <span>Show all schema attributes</span>
      </label>
      <div class="layout-selector">
        <label for="layout-select">Layout:</label>
        <select id="layout-select" v-model="selectedLayout" @change="applyLayout">
          <option value="auto">Force-Directed (Optimized)</option>
          <option value="breadthfirst">Hierarchical (Top-Down)</option>
          <option value="cose">Force-Directed (Standard)</option>
          <option value="circle">Circle</option>
          <option value="concentric">Concentric</option>
        </select>
      </div>
      <button @click="resetLayout" class="btn btn-secondary">Reset Layout</button>
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
import { ref, onMounted, watch, nextTick } from 'vue'
import cytoscape, { type Core, type ElementDefinition } from 'cytoscape'
// @ts-ignore - cytoscape-layers doesn't have proper types
import cytoscapeLayers from 'cytoscape-layers'
// @ts-ignore - cytoscape-bubblesets doesn't have proper types
import BubbleSets from 'cytoscape-bubblesets'
import type { VisualizationData, GYOStep } from '@/types'
import { useHighlightCoordination } from '@/composables/useHighlightCoordination'

// Register the extensions in order
cytoscape.use(cytoscapeLayers)
cytoscape.use(BubbleSets)

interface Props {
  visualizationData: VisualizationData | null
  showBubbleSets?: boolean
  showConvexHulls?: boolean
  gyoStep?: number | null
  gyoSteps?: GYOStep[] | null
}

const props = withDefaults(defineProps<Props>(), {
  showBubbleSets: false,
  showConvexHulls: false,
  gyoStep: null,
  gyoSteps: null
})
const cyContainer = ref<HTMLElement | null>(null)
let cy: Core | null = null
let bubbleSetsPlugin: any = null

const stats = ref({
  num_nodes: 0,
  num_edges: 0,
  is_acyclic: false,
  hypertree_width: undefined as number | undefined,
  num_relations: 0,
  num_joins: 0,
  num_aggregates: 0
})

const tooltipData = ref<{
  x: number
  y: number
  label: string
  type: string
  attributes: string[]
  outputAttributes: string[]
  gyoTooltip?: string | null
} | null>(null)

const hideSingletons = ref(false)
const showSchemaAttributes = ref(false)
const showExportMenu = ref(false)
const selectedLayout = ref<'auto' | 'breadthfirst' | 'cose' | 'circle' | 'concentric'>('auto')
// Counter to track automatic layout resets (to avoid infinite loops)
const autoResetCount = ref(0)

// Highlight coordination with SQL editor
const { currentHighlight, highlightElement, clearHighlight } = useHighlightCoordination()

onMounted(() => {
  initCytoscape()

  // If visualization data is already present when component mounts, update immediately
  if (props.visualizationData) {
    console.log('Visualization data already present on mount, updating graph...')
    nextTick(() => {
      updateGraph(props.visualizationData!)

      // Apply GYO styling if GYO step is already set
      if (props.gyoStep !== null && props.gyoSteps && props.gyoSteps.length > 0) {
        console.log('Applying initial GYO styling for step', props.gyoStep)
        applyGYOStyling()
      }
    })
  }
})

watch(() => props.visualizationData, (newData) => {
  console.log('Watcher triggered, newData:', newData)
  if (newData) {
    updateGraph(newData)
  }
}, { deep: true, immediate: false })

// Watch for highlights from SQL editor
watch(currentHighlight, (highlight) => {
  if (!cy) return

  // Remove previous highlights
  cy.elements().removeClass('sql-highlighted')

  if (!highlight) return

  // Apply highlight based on type
  if (highlight.type === 'edge') {
    // Highlight hyperedge (table/relation)
    // Try both as edge and as compound node
    cy.$(`edge[id="${highlight.id}"]`).addClass('sql-highlighted')
    cy.$(`node[id="${highlight.id}"]`).addClass('sql-highlighted')

    // Also try matching by label for more flexible matching
    cy.$(`edge[label="${highlight.label}"]`).addClass('sql-highlighted')
    cy.$(`node[label="${highlight.label}"]`).addClass('sql-highlighted')
  } else if (highlight.type === 'node') {
    // Highlight node (equivalence class)
    // Get all attributes in the equivalence class
    const attributesToMatch = highlight.attributes || [highlight.label]

    // Normalize all attributes for comparison (strip Spark numbering)
    const normalizedAttributes = attributesToMatch.map(attr => {
      let normalized = attr
      if (normalized.includes('#')) {
        normalized = normalized.substring(0, normalized.indexOf('#'))
      }
      return normalized.toLowerCase()
    })

    console.log(`Hypergraph: Highlighting nodes containing any of:`, normalizedAttributes)

    // First try direct ID match
    cy.$(`node[id="${highlight.id}"]`).addClass('sql-highlighted')

    // Track if we need to enrich the highlight with full equivalence class
    let matchedNode: any = null
    let totalNodesChecked = 0

    // Search all nodes and highlight if they contain ANY attribute from the equivalence class
    cy.nodes().forEach(node => {
      totalNodesChecked++
      const nodeData = node.data()
      const rawAttributes = nodeData.attributes

      // Ensure attributes is an array
      let nodeAttributes: string[] = []
      if (Array.isArray(rawAttributes)) {
        nodeAttributes = rawAttributes
      } else if (typeof rawAttributes === 'string') {
        // Split comma-separated string into array
        nodeAttributes = rawAttributes.split(',').map(s => s.trim()).filter(Boolean)
      }

      // Normalize node's attributes for comparison
      const normalizedNodeAttrs = nodeAttributes.map((attr: string) => {
        let normalized = attr
        if (normalized.includes('#')) {
          normalized = normalized.substring(0, normalized.indexOf('#'))
        }
        return normalized.toLowerCase()
      })

      // Check if this node contains any attribute from the equivalence class
      const hasMatch = normalizedNodeAttrs.some((nodeAttr: string) =>
        normalizedAttributes.includes(nodeAttr)
      )

      if (hasMatch) {
        node.addClass('sql-highlighted')
        // Remember the first matched node to get full equivalence class
        if (!matchedNode) {
          matchedNode = node
          console.log(`First matched node:`, {
            id: nodeData.id,
            label: nodeData.label,
            type: nodeData.type,
            attributesCount: nodeAttributes.length,
            normalizedAttrs: normalizedNodeAttrs
          })
        }
      }
    })

    console.log(`Searched ${totalNodesChecked} nodes, matched: ${matchedNode ? 'yes' : 'no'}`)

    // If highlight came from SQL (no attributes) and we found a matching node,
    // enrich it with the full equivalence class so SQL can highlight all attributes
    if (!highlight.attributes && matchedNode) {
      const nodeData = matchedNode.data()
      const rawAttributes = nodeData.attributes

      // Ensure attributes is an array
      let attributesArray: string[] = []
      if (Array.isArray(rawAttributes)) {
        attributesArray = rawAttributes
      } else if (typeof rawAttributes === 'string') {
        // Split comma-separated string into array
        attributesArray = rawAttributes.split(',').map(s => s.trim()).filter(Boolean)
      }

      console.log(`Matched node data:`, {
        id: nodeData.id,
        label: nodeData.label,
        type: nodeData.type,
        rawAttributes: rawAttributes,
        attributesArray: attributesArray,
        attributesArrayLength: attributesArray.length
      })

      if (attributesArray.length > 0) {
        console.log(`Enriching highlight with equivalence class (${attributesArray.length} attributes):`, attributesArray)
        // Update the highlight with full attributes array
        // Preserve hoveredAttribute if it exists (came from SQL hover)
        highlightElement({
          type: 'node',
          id: nodeData.id,
          label: nodeData.label,
          attributes: attributesArray,
          hoveredAttribute: highlight.hoveredAttribute  // Preserve the original hovered attribute
        })
      } else {
        console.warn(`Node found but has no attributes array:`, nodeData)
      }
    }
  }
})

function initCytoscape() {
  if (!cyContainer.value) {
    console.error('Cytoscape container not found')
    return
  }

  console.log('Initializing Cytoscape...')

  cy = cytoscape({
    container: cyContainer.value,
    style: [
      {
        selector: 'node',
        style: {
          'background-color': '#3B82F6',
          'label': 'data(label)',
          'color': '#1F2937',
          'text-valign': 'center',
          'text-halign': 'center',
          'font-size': '12px',
          'font-weight': '600',
          'text-outline-color': '#FFFFFF',
          'text-outline-width': 2,
          'width': '60px',
          'height': '60px',
          'border-width': 2,
          'border-color': '#2563EB'
        }
      },
      {
        selector: 'node.attribute',
        style: {
          'background-color': '#3B82F6',
          'shape': 'ellipse'
        }
      },
      {
        selector: 'node.attribute[output_attributes]',
        style: {
          'border-width': (node: any) => {
            const outputAttrs = node.data('output_attributes')
            const hasOutput = outputAttrs && outputAttrs.length > 0 && outputAttrs !== ''
            return hasOutput ? '4px' : '2px'
          },
          'border-color': (node: any) => {
            const outputAttrs = node.data('output_attributes')
            const hasOutput = outputAttrs && outputAttrs.length > 0 && outputAttrs !== ''
            return hasOutput ? '#10B981' : '#2563EB'
          },
          'border-style': (node: any) => {
            const outputAttrs = node.data('output_attributes')
            const hasOutput = outputAttrs && outputAttrs.length > 0 && outputAttrs !== ''
            return hasOutput ? 'double' : 'solid'
          }
        }
      },
      {
        selector: 'node.relation',
        style: {
          'background-color': '#10B981',
          'shape': 'roundrectangle'
        }
      },
      {
        selector: 'node.hyperedge',
        style: {
          'background-color': '#F59E0B',
          'shape': 'diamond',
          'width': '40px',
          'height': '40px'
        }
      },
      {
        selector: 'node.hyperedge[output_attributes]',
        style: {
          'border-width': (node: any) => {
            const outputAttrs = node.data('output_attributes')
            const hasOutput = outputAttrs && outputAttrs.length > 0 && outputAttrs !== ''
            return hasOutput ? '4px' : '2px'
          },
          'border-color': (node: any) => {
            const outputAttrs = node.data('output_attributes')
            const hasOutput = outputAttrs && outputAttrs.length > 0 && outputAttrs !== ''
            return hasOutput ? '#10B981' : '#2563EB'
          }
        }
      },
      {
        selector: 'node.output_attribute',
        style: {
          'background-color': '#10B981',
          'shape': 'ellipse',
          'width': '50px',
          'height': '50px',
          'border-width': '3px',
          'border-color': '#059669',
          'border-style': 'solid',
          'color': '#1F2937',
          'text-outline-color': '#FFFFFF',
          'text-outline-width': 2,
          'font-weight': 'bold',
          'font-size': '11px',
          'text-valign': 'center',
          'text-halign': 'center'
        }
      },
      {
        selector: 'node.schema_attribute',
        style: {
          'background-color': '#9CA3AF',
          'shape': 'ellipse',
          'width': '45px',
          'height': '45px',
          'border-width': '2px',
          'border-color': '#6B7280',
          'border-style': 'solid',
          'color': '#1F2937',
          'text-outline-color': '#FFFFFF',
          'text-outline-width': 2,
          'font-weight': 'normal',
          'font-size': '10px',
          'text-valign': 'center',
          'text-halign': 'center'
        }
      },
      {
        selector: 'edge',
        style: {
          'width': 2,
          'line-color': '#9CA3AF',
          'target-arrow-color': '#9CA3AF',
          'target-arrow-shape': 'triangle',
          'curve-style': 'bezier'
        }
      },
      {
        selector: 'edge[label]',
        style: {
          'label': 'data(label)',
          'font-size': '10px',
          'text-rotation': 'autorotate',
          'text-margin-y': -10
        }
      },
      {
        selector: 'edge.hyperedge-connection',
        style: {
          'line-color': '#F59E0B',
          'line-style': 'dashed',
          'target-arrow-shape': 'none'
        }
      },
      {
        selector: 'edge[output_attributes]',
        style: {
          'width': 3,
          'line-color': (ele: any) => {
            const outputAttrs = ele.data('output_attributes')
            if (outputAttrs && outputAttrs.trim()) {
              return '#10B981' // Green for edges with output attributes
            }
            return '#9CA3AF'
          }
        }
      },
      {
        selector: ':selected',
        style: {
          'background-color': '#10B981',
          'line-color': '#10B981',
          'target-arrow-color': '#10B981',
          'border-color': '#059669'
        }
      },
      // GYO Animation Styles
      {
        selector: '.gyo-being-removed',
        style: {
          'background-color': '#EF4444',
          'border-color': '#DC2626',
          'border-width': '4px',
          'opacity': 1.0,
          'transition-property': 'background-color, border-color, opacity',
          'transition-duration': '0.3s'
        }
      },
      {
        selector: 'node.gyo-being-removed',
        style: {
          // Pulsing animation for nodes being removed (will use CSS keyframes)
          'z-index': 999
        }
      },
      {
        selector: '.gyo-remaining',
        style: {
          'border-width': '3px',
          'opacity': 1.0,
          'transition-property': 'border-width, opacity',
          'transition-duration': '0.3s'
        }
      },
      // Phase 2: Subset relationship visual indicators
      {
        selector: '.gyo-container',
        style: {
          'border-color': '#3B82F6',
          'border-width': '5px',
          'background-color': '#DBEAFE',
          'opacity': 1.0,
          'z-index': 998,
          'transition-property': 'background-color, border-color, border-width',
          'transition-duration': '0.3s'
        }
      },
      {
        selector: 'edge.subset-indicator',
        style: {
          'width': 3,
          'line-color': '#9333EA',
          'target-arrow-color': '#9333EA',
          'target-arrow-shape': 'triangle',
          'curve-style': 'bezier',
          'line-style': 'dashed',
          'line-dash-pattern': [6, 3],
          'opacity': 0.7,
          'z-index': 1000,
          'label': 'data(label)',
          'font-size': '16px',
          'color': '#9333EA',
          'text-background-color': '#FFFFFF',
          'text-background-opacity': 0.8,
          'text-background-padding': '3px',
          'text-background-shape': 'roundrectangle'
        }
      },
      // SQL-to-Hypergraph highlighting
      {
        selector: '.sql-highlighted',
        style: {
          'border-color': '#EA580C',  // Darker orange (orange-600) for better visibility
          'border-width': '6px',
          'line-color': '#EA580C',
          'target-arrow-color': '#EA580C',
          'z-index': 1001,
          'opacity': 1.0,
          'transition-property': 'border-color, border-width, line-color',
          'transition-duration': '0.2s'
        }
      }
    ],
    layout: {
      name: 'cose',
      animate: true,
      animationDuration: 500
    }
  })

  // Add interaction handlers
  cy.on('tap', 'node', (evt) => {
    const node = evt.target
    console.log('Selected node:', node.data())
  })

  // Helper function to parse and show tooltip
  const showTooltip = (element: any, evt: any) => {
    const data = element.data()
    const position = element.renderedPosition()

    // Parse attributes (might be comma-separated string or array)
    let attributes: string[]
    if (typeof data.attributes === 'string') {
      attributes = data.attributes.split(',').map((s: string) => s.trim()).filter(Boolean)
    } else if (Array.isArray(data.attributes)) {
      attributes = data.attributes
    } else {
      attributes = []
    }

    // Parse output attributes
    let outputAttributes: string[]
    if (typeof data.output_attributes === 'string') {
      outputAttributes = data.output_attributes.split(',').map((s: string) => s.trim()).filter(Boolean)
    } else if (Array.isArray(data.output_attributes)) {
      outputAttributes = data.output_attributes
    } else {
      outputAttributes = []
    }

    tooltipData.value = {
      x: position.x + 10,
      y: position.y - 10,
      label: data.label || data.id,
      type: data.type || 'table',
      attributes: attributes,
      outputAttributes: outputAttributes,
      // Phase 2: Add GYO subset relationship info if available
      gyoTooltip: data.gyoTooltip || null
    }
  }

  // Add tooltip and highlight handlers for nodes (equivalence classes)
  cy.on('mouseover', 'node', (evt) => {
    const element = evt.target
    showTooltip(element, evt)

    // Highlight this node in SQL
    const data = element.data()
    if (data.label && data.attributes) {
      // Ensure attributes is an array
      let attributesArray: string[] = []
      if (Array.isArray(data.attributes)) {
        attributesArray = data.attributes
      } else if (typeof data.attributes === 'string') {
        attributesArray = data.attributes.split(',').map(s => s.trim()).filter(Boolean)
      }

      console.log(`Hypergraph node hover: id="${data.id}", label="${data.label}", type="${data.type}"`)
      highlightElement({
        type: 'node',
        id: data.id,
        label: data.label,
        attributes: attributesArray
      })
    }
  })

  cy.on('mouseout', 'node', () => {
    tooltipData.value = null
    clearHighlight()
  })

  // Add tooltip and highlight handlers for edges (2-vertex hyperedges rendered as simple edges)
  cy.on('mouseover', 'edge.edge', (evt) => {
    const element = evt.target
    showTooltip(element, evt)

    // Highlight this edge in SQL
    const data = element.data()
    if (data.label) {
      console.log(`Hypergraph edge hover: id="${data.id}", label="${data.label}"`)
      highlightElement({
        type: 'edge',
        id: data.id,
        label: data.label,
        attributes: data.attributes || []
      })
    }
  })

  cy.on('mouseout', 'edge.edge', () => {
    tooltipData.value = null
    clearHighlight()
  })

  // Add handlers for hyperedge nodes (compound nodes representing tables)
  cy.on('mouseover', 'node.hyperedge', (evt) => {
    const element = evt.target
    showTooltip(element, evt)

    // Highlight this relation/table in SQL
    const data = element.data()
    if (data.label) {
      // Ensure attributes is an array
      let attributesArray: string[] = []
      if (Array.isArray(data.attributes)) {
        attributesArray = data.attributes
      } else if (typeof data.attributes === 'string') {
        attributesArray = data.attributes.split(',').map(s => s.trim()).filter(Boolean)
      }

      console.log(`Hypergraph hyperedge node hover: id="${data.id}", label="${data.label}", attributes count: ${attributesArray.length}`)
      highlightElement({
        type: 'edge',
        id: data.id,
        label: data.label,
        attributes: attributesArray
      })
    }
  })

  cy.on('mouseout', 'node.hyperedge', () => {
    tooltipData.value = null
    clearHighlight()
  })
}

function updateGraph(data: VisualizationData) {
  if (!cy) {
    console.error('Cytoscape not initialized')
    return
  }

  console.log('Updating graph with data:', data)
  console.log('Elements to add:', data.elements)

  // Reset auto layout counter when new data is loaded
  autoResetCount.value = 0

  // Update stats
  stats.value = data.stats

  // Clear existing elements
  cy.elements().remove()

  // Validate and add new elements
  if (!data.elements || data.elements.length === 0) {
    console.warn('No elements to add to graph')
    return
  }

  try {
    cy.add(data.elements)
    console.log(`Added ${data.elements.length} elements to Cytoscape`)
  } catch (err) {
    console.error('Error adding elements to Cytoscape:', err)
    return
  }

  // Hide schema_attribute nodes by default (user must enable the checkbox to show them)
  if (!showSchemaAttributes.value) {
    cy.nodes('.schema_attribute').forEach(node => {
      node.style('display', 'none')
      node.connectedEdges().style('display', 'none')
    })
  }

  // Apply layout - choose based on graph properties and user selection
  const layoutName = getOptimalLayoutName()
  console.log(`Applying ${layoutName} layout`)

  let layoutOptions: any = {
    name: layoutName,
    animate: true,
    animationDuration: 500
  }

  // Configure layout-specific options (same as applySelectedLayout)
  if (layoutName === 'breadthfirst') {
    layoutOptions = {
      ...layoutOptions,
      directed: true,
      spacingFactor: 1.5,
      nodeDimensionsIncludeLabels: true,
      avoidOverlap: true,
      roots: cy.nodes('.hyperedge').map(n => n.id())
    }
  } else if (layoutName === 'cose') {
    layoutOptions = {
      ...layoutOptions,
      nodeDimensionsIncludeLabels: true,
      randomize: true,
      idealEdgeLength: 100,
      nodeOverlap: 20,
      nodeRepulsion: 400000,
      edgeElasticity: 100,
      nestingFactor: 1.2,
      gravity: 80,
      numIter: 1000,
      initialTemp: 200,
      coolingFactor: 0.95,
      minTemp: 1.0
    }
  } else if (layoutName === 'circle') {
    layoutOptions = {
      ...layoutOptions,
      avoidOverlap: true,
      nodeDimensionsIncludeLabels: true,
      spacingFactor: 1.5
    }
  } else if (layoutName === 'concentric') {
    layoutOptions = {
      ...layoutOptions,
      concentric: (node: any) => {
        return node.hasClass('hyperedge') ? 10 : 1
      },
      levelWidth: () => 2,
      spacingFactor: 1.5,
      avoidOverlap: true,
      nodeDimensionsIncludeLabels: true
    }
  }

  const layout = cy.layout(layoutOptions)

  // Apply bubble sets (pie charts) if enabled - before layout
  if (props.showBubbleSets) {
    applyBubbleSets()
  }

  // Use layout stop event for better timing
  layout.one('layoutstop', () => {
    console.log('Layout stopped, applying post-layout actions')

    if (cy) {
      cy.fit(undefined, 50)

      // Apply convex hulls after layout completes (using final positions)
      if (props.showConvexHulls) {
        // Add a delay to ensure rendering is complete
        setTimeout(() => {
          console.log('Applying convex hulls after layout stop')
          applyConvexHulls()
        }, 300)
      }

      console.log('Graph layout complete and fitted to view')

      // Automatically trigger reset layout 1-2 times for better initial appearance
      // This mimics the user pressing "Reset Layout" button to avoid crossing edges
      if (autoResetCount.value < 2) {
        autoResetCount.value++
        console.log(`Auto-triggering reset layout (attempt ${autoResetCount.value}/2)`)
        setTimeout(() => {
          resetLayout()
        }, 800) // Wait for convex hulls to finish (300ms) + buffer
      }
    }
  })

  layout.run()
}

// Apply GYO step styling to highlight elements being removed/already removed
function applyGYOStyling() {
  if (!cy || props.gyoStep === null || !props.gyoSteps || props.gyoSteps.length === 0) {
    // Clear all GYO styling if no step is active and restore visibility
    if (cy) {
      cy.elements().removeClass('gyo-being-removed gyo-already-removed gyo-remaining gyo-container')
      cy.elements().style('display', 'element')
      // Remove any subset indicator edges
      cy.edges('.subset-indicator').remove()
    }
    return
  }

  const stepIndex = props.gyoStep
  if (stepIndex < 0 || stepIndex >= props.gyoSteps.length) {
    return
  }

  const currentStep = props.gyoSteps[stepIndex]

  // Clear all previous GYO classes and restore visibility
  cy.elements().removeClass('gyo-being-removed gyo-already-removed gyo-remaining gyo-container')
  cy.elements().style('display', 'element')

  // Remove any previous subset indicator edges
  cy.edges('.subset-indicator').remove()

  // Hide all schema attributes during GYO walkthrough (non-join, non-output vertices)
  cy.nodes('.schema_attribute').forEach(vertex => {
    vertex.style('display', 'none')
    vertex.connectedEdges().style('display', 'none')
  })

  // Mark elements being removed in current step (highlight but keep visible)
  currentStep.removed_edges.forEach(edgeName => {
    // Fix: Append _node suffix to match Cytoscape node IDs
    const edgeNodeId = `${edgeName}_node`
    const hyperedgeNode = cy.$(`node[id="${edgeNodeId}"]`)
    if (hyperedgeNode.length > 0) {
      hyperedgeNode.addClass('gyo-being-removed')
      // Also mark connected attribute vertices (only join and output attributes, not schema)
      hyperedgeNode.neighborhood('.attribute, .output_attribute').addClass('gyo-being-removed')
    }
  })

  // Mark vertices being removed
  currentStep.removed_nodes.forEach(vertexName => {
    const vertex = cy.$(`node[id="${vertexName}"]`)
    if (vertex.length > 0) {
      vertex.addClass('gyo-being-removed')
    }
  })

  // Phase 2: Add visual indicators for subset relationships (ears)
  if (currentStep.action === 'remove_ear' && currentStep.container_edges && currentStep.container_edges.length > 0) {
    // Highlight container edges
    currentStep.container_edges.forEach(containerName => {
      // Fix: Append _node suffix to match Cytoscape node IDs
      const containerNodeId = `${containerName}_node`
      const containerNode = cy.$(`node[id="${containerNodeId}"]`)
      if (containerNode.length > 0) {
        containerNode.addClass('gyo-container')
      }
    })

    // Add tooltips with subset relationship info
    currentStep.removed_edges.forEach(earName => {
      // Fix: Append _node suffix to match Cytoscape node IDs
      const earNodeId = `${earName}_node`
      const earNode = cy.$(`node[id="${earNodeId}"]`)
      if (earNode.length > 0 && currentStep.why_ear) {
        // Store tooltip info in node data
        earNode.data('gyoTooltip', currentStep.why_ear)
      }
    })

    // Draw dashed lines from ears to containers
    if (currentStep.subset_relationships && currentStep.subset_relationships.length > 0) {
      currentStep.subset_relationships.forEach((rel, index) => {
        // Fix: Append _node suffix to match Cytoscape node IDs
        const earNodeId = `${rel.ear}_node`
        const containerNodeId = `${rel.container}_node`
        const earNode = cy.$(`node[id="${earNodeId}"]`)
        const containerNode = cy.$(`node[id="${containerNodeId}"]`)

        if (earNode.length > 0 && containerNode.length > 0) {
          // Create a temporary dashed edge showing the subset relationship
          cy.add({
            group: 'edges',
            data: {
              id: `subset-indicator-${index}`,
              source: earNodeId,  // Use node ID with _node suffix
              target: containerNodeId,  // Use node ID with _node suffix
              label: '⊆'
            },
            classes: 'subset-indicator'
          })
        }
      })
    }
  }

  // Hide elements already removed in previous steps
  for (let i = 0; i < stepIndex; i++) {
    const pastStep = props.gyoSteps[i]

    pastStep.removed_edges.forEach(edgeName => {
      // Fix: Append _node suffix to match Cytoscape node IDs
      const edgeNodeId = `${edgeName}_node`
      const hyperedgeNode = cy.$(`node[id="${edgeNodeId}"]`)
      if (hyperedgeNode.length > 0) {
        // Hide the hyperedge and its connected attribute vertices (only join and output)
        hyperedgeNode.style('display', 'none')
        hyperedgeNode.neighborhood('.attribute, .output_attribute').style('display', 'none')
        hyperedgeNode.connectedEdges().style('display', 'none')
      }
    })

    pastStep.removed_nodes.forEach(vertexName => {
      const vertex = cy.$(`node[id="${vertexName}"]`)
      if (vertex.length > 0) {
        vertex.style('display', 'none')
        vertex.connectedEdges().style('display', 'none')
      }
    })
  }

  // Mark remaining elements
  currentStep.remaining_edges.forEach(edgeName => {
    // Fix: Append _node suffix to match Cytoscape node IDs
    const edgeNodeId = `${edgeName}_node`
    const hyperedgeNode = cy.$(`node[id="${edgeNodeId}"]`)
    if (hyperedgeNode.length > 0 && !hyperedgeNode.hasClass('gyo-being-removed')) {
      hyperedgeNode.addClass('gyo-remaining')
      // Connected vertices that are still visible are also remaining (only join and output)
      hyperedgeNode.neighborhood('.attribute, .output_attribute').forEach(vertex => {
        if (vertex.style('display') !== 'none') {
          vertex.addClass('gyo-remaining')
        }
      })
    }
  })
}

// Watch for GYO step changes
watch(() => props.gyoStep, () => {
  applyGYOStyling()
})

// Watch for GYO steps data changes
watch(() => props.gyoSteps, () => {
  applyGYOStyling()
}, { deep: true })

// Expanded color palette for bubble sets (25 distinct colors)
const bubbleColors = [
  'rgba(59, 130, 246, 0.2)',   // Blue
  'rgba(16, 185, 129, 0.2)',   // Green
  'rgba(245, 158, 11, 0.2)',   // Orange
  'rgba(239, 68, 68, 0.2)',    // Red
  'rgba(139, 92, 246, 0.2)',   // Purple
  'rgba(236, 72, 153, 0.2)',   // Pink
  'rgba(6, 182, 212, 0.2)',    // Cyan
  'rgba(132, 204, 22, 0.2)',   // Lime
  'rgba(251, 146, 60, 0.2)',   // Bright Orange
  'rgba(14, 165, 233, 0.2)',   // Sky Blue
  'rgba(168, 85, 247, 0.2)',   // Electric Purple
  'rgba(244, 63, 94, 0.2)',    // Rose
  'rgba(34, 197, 94, 0.2)',    // Emerald
  'rgba(251, 191, 36, 0.2)',   // Amber
  'rgba(99, 102, 241, 0.2)',   // Indigo
  'rgba(20, 184, 166, 0.2)',   // Teal
  'rgba(217, 70, 239, 0.2)',   // Fuchsia
  'rgba(163, 230, 53, 0.2)',   // Lime Green
  'rgba(248, 113, 113, 0.2)',  // Light Red
  'rgba(96, 165, 250, 0.2)',   // Light Blue
  'rgba(167, 139, 250, 0.2)',  // Light Purple
  'rgba(251, 113, 133, 0.2)',  // Light Pink
  'rgba(52, 211, 153, 0.2)',   // Light Teal
  'rgba(253, 224, 71, 0.2)',   // Yellow
  'rgba(156, 163, 175, 0.2)',  // Gray
]

const bubbleBorderColors = [
  '#3B82F6', '#10B981', '#F59E0B', '#EF4444',
  '#8B5CF6', '#EC4899', '#06B6D4', '#84CC16',
  '#FB923C', '#0EA5E9', '#A855F7', '#F43F5E',
  '#22C55E', '#FBBF24', '#6366F1', '#14B8A6',
  '#D946EF', '#A3E635', '#F87171', '#60A5FA',
  '#A78BFA', '#FB7185', '#34D399', '#FDE047',
  '#9CA3AF',
]

function applyBubbleSets() {
  if (!cy) return

  console.log('Applying bubble sets visualization')

  // Map all hyperedge nodes to colors (all hyperedges are now nodes)
  const hyperedgeColors = new Map<string, { bg: string, border: string }>()
  const hyperedgeNodes = cy.nodes('.hyperedge')

  let colorIdx = 0

  // Assign colors to all hyperedge nodes
  hyperedgeNodes.forEach((node) => {
    const currentColorIdx = colorIdx % bubbleColors.length
    hyperedgeColors.set(node.id(), {
      bg: bubbleColors[currentColorIdx],
      border: bubbleBorderColors[currentColorIdx]
    })
    colorIdx++
  })

  // Apply background color to attribute nodes based on connected hyperedges
  // Only include .attribute nodes (keep output_attribute green and schema_attribute gray)
  cy.nodes('.attribute').forEach(node => {
    // Find connected hyperedge nodes
    const connectedHyperedges = node.neighborhood('.hyperedge')

    const totalConnections = connectedHyperedges.length

    if (totalConnections === 0) {
      return
    }

    if (totalConnections === 1) {
      // Single set: use simple background color
      const connection = connectedHyperedges[0]
      const colors = hyperedgeColors.get(connection.id())

      if (colors) {
        node.style({
          'background-color': colors.bg,
          'border-width': '3px',
          'border-color': colors.border,
          'pie-size': '0%'  // Disable pie chart
        })
      }
    } else {
      // Multiple sets: use pie chart with slices ordered by angular position
      const numSlices = Math.min(totalConnections, 8)  // Limit to 8 slices
      const sliceSize = Math.floor(100 / numSlices)

      // Get node position
      const nodePos = node.position()

      // Calculate angle for each connected hyperedge
      interface ConnectionWithAngle {
        connection: any
        angle: number
      }

      const connectionsWithAngles: ConnectionWithAngle[] = []

      // Add all hyperedge nodes (use their position)
      connectedHyperedges.slice(0, numSlices).forEach(hyperedge => {
        const hyperedgePos = hyperedge.position()
        // Calculate angle from attribute node to hyperedge
        // atan2 returns angle in radians from -π to π
        const dx = hyperedgePos.x - nodePos.x
        const dy = hyperedgePos.y - nodePos.y
        let angle = Math.atan2(dy, dx) * (180 / Math.PI)

        // Convert to 0-360 range with 0 at top (12 o'clock)
        // atan2 gives 0 at right (3 o'clock), so adjust by -90 degrees
        angle = (angle + 90 + 360) % 360

        connectionsWithAngles.push({ connection: hyperedge, angle })
      })

      // Sort by angle (clockwise from top)
      connectionsWithAngles.sort((a, b) => a.angle - b.angle)

      // Optimize slice alignment: rotate the assignment so slices point toward connections
      // Pie slices start at 0° (top) and are centered at (i-0.5) * sliceSize degrees
      // Find the best rotation offset to minimize angular distance
      const sliceSizeDegrees = 360 / numSlices
      const firstSliceCenterAngle = sliceSizeDegrees / 2  // Center of first slice

      // Find which connection is closest to the first slice center
      let bestOffset = 0
      let minDistance = 360

      for (let offset = 0; offset < numSlices; offset++) {
        let totalDistance = 0
        for (let i = 0; i < numSlices; i++) {
          const sliceCenter = (i + 0.5) * sliceSizeDegrees
          const connectionIdx = (i + offset) % numSlices
          const connectionAngle = connectionsWithAngles[connectionIdx].angle

          // Calculate angular distance (accounting for wraparound)
          let distance = Math.abs(sliceCenter - connectionAngle)
          if (distance > 180) distance = 360 - distance
          totalDistance += distance
        }

        if (totalDistance < minDistance) {
          minDistance = totalDistance
          bestOffset = offset
        }
      }

      const styles: any = {
        'pie-size': '100%',
        'border-width': '3px'
      }

      // Set up pie slices with optimal rotation
      let borderColors: string[] = []
      for (let i = 0; i < numSlices; i++) {
        const connectionIdx = (i + bestOffset) % numSlices
        const item = connectionsWithAngles[connectionIdx]
        const colors = hyperedgeColors.get(item.connection.id())
        if (colors) {
          styles[`pie-${i + 1}-background-color`] = colors.border
          styles[`pie-${i + 1}-background-size`] = sliceSize
          borderColors.push(colors.border)
        }
      }

      // Use first connection's border color as node border
      if (borderColors.length > 0) {
        styles['border-color'] = borderColors[0]
      }

      node.style(styles)
    }
  })

  // Make hyperedge nodes more prominent
  hyperedgeNodes.forEach(node => {
    const colors = hyperedgeColors.get(node.id())
    if (colors) {
      node.style({
        'background-color': colors.border,
        'border-width': '4px',
        'border-color': colors.border
      })
    }
  })
}

// Apply convex hulls using the bubble sets extension
function applyConvexHulls() {
  if (!cy) return

  const timestamp = Date.now()
  console.log(`[${timestamp}] === applyConvexHulls START ===`)

  // Remove existing bubble sets if any
  removeConvexHulls()

  // Initialize the bubble sets plugin
  try {
    bubbleSetsPlugin = cy.bubbleSets()
    console.log('Bubble sets plugin initialized:', bubbleSetsPlugin)

    // Check if the layer was created
    if (cy.container()) {
      const container = cy.container()
      const svgLayers = container.querySelectorAll('svg')
      console.log(`Found ${svgLayers.length} SVG layers in container`)
      svgLayers.forEach((svg, idx) => {
        console.log(`SVG layer ${idx}:`, {
          class: svg.getAttribute('class'),
          style: svg.getAttribute('style'),
          children: svg.children.length
        })
      })
    }
  } catch (err) {
    console.error('Failed to initialize bubble sets plugin:', err)
    return
  }

  // Get all hyperedge nodes (all hyperedges are now nodes, including 2-vertex and isolated ones)
  const hyperedgeNodes = cy.nodes('.hyperedge')
  console.log(`Found ${hyperedgeNodes.length} hyperedge nodes`)

  // No more simple edges - all hyperedges are now nodes
  const twoVertexEdges = cy.collection()  // Empty collection for backward compatibility

  let successCount = 0
  let failCount = 0
  let colorIdx = 0

  // Create a bubble set for each hyperedge node
  hyperedgeNodes.forEach((hyperedgeNode, idx) => {
    // Find all attribute nodes connected to this hyperedge (including output and schema attributes)
    const connectedNodes = hyperedgeNode.neighborhood('.attribute, .output_attribute, .schema_attribute')

    // Filter out hidden nodes (e.g., singletons that are hidden)
    const visibleNodes = connectedNodes.filter(node => node.style('display') !== 'none')

    console.log(`Hyperedge ${idx}: ${connectedNodes.length} connected nodes, ${visibleNodes.length} visible`)

    // Bubble sets require at least 2 nodes to create a meaningful hull
    // With just the hyperedge node itself (1 node), it can't draw a hull
    if (visibleNodes.length >= 1) {
      // Get color for this hyperedge
      const currentColorIdx = colorIdx % bubbleBorderColors.length
      const color = bubbleBorderColors[currentColorIdx]
      const bgColor = bubbleColors[currentColorIdx]
      colorIdx++

      // Add a bubble set path for this hyperedge's nodes
      // Include the hyperedge node itself
      const nodesToInclude = visibleNodes.union(hyperedgeNode)

      console.log(`Creating bubble set for hyperedge ${idx} with ${nodesToInclude.length} nodes (${visibleNodes.length} attributes + 1 hyperedge), color: ${color}`)

      // Check if nodes have valid positions
      let allNodesHavePositions = true
      nodesToInclude.forEach(node => {
        const pos = node.position()
        if (!pos || (pos.x === 0 && pos.y === 0)) {
          console.warn(`Node ${node.id()} has invalid position:`, pos)
          allNodesHavePositions = false
        }
      })

      if (!allNodesHavePositions) {
        console.warn(`Skipping hyperedge ${idx}: some nodes don't have valid positions yet`)
        failCount++
      } else {
        try {
          const path = bubbleSetsPlugin.addPath(nodesToInclude, null, null, {
            nodeR1: 80,  // Increase influence radius (default: 50)
            edgeR1: 40,  // Increase edge influence (default: 20)
            morphBuffer: 15,  // Increase buffer (default: 10)
            virtualEdges: false,  // Disable virtual edges (can cause issues)
            style: {
              fill: bgColor,
              stroke: color,
              strokeWidth: 3,
              opacity: 0.8
            }
          })

          if (path) {
            successCount++
            console.log(`Successfully created bubble set ${idx}`)
          } else {
            failCount++
            console.warn(`Failed to create bubble set ${idx}: addPath returned null/undefined`)
          }
        } catch (err) {
          failCount++
          console.error(`Error creating bubble set for hyperedge ${idx}:`, err)
        }
      }
    } else {
      console.log(`Skipping hyperedge ${idx}: no visible connected nodes (needs at least 1 attribute node)`)
    }
  })

  // Create a bubble set for each 2-vertex hyperedge (simple edge)
  twoVertexEdges.forEach((edge, idx) => {
    // Get source and target nodes
    const sourceNode = edge.source()
    const targetNode = edge.target()

    // Check if both nodes are visible
    if (sourceNode.style('display') === 'none' || targetNode.style('display') === 'none') {
      console.log(`Skipping 2-vertex edge ${idx}: one or both nodes are hidden`)
      return
    }

    // Get color for this edge
    const currentColorIdx = colorIdx % bubbleBorderColors.length
    const color = bubbleBorderColors[currentColorIdx]
    const bgColor = bubbleColors[currentColorIdx]
    colorIdx++

    // Create collection of nodes to include in bubble set
    const nodesToInclude = cy.collection([sourceNode, targetNode])

    console.log(`Creating bubble set for 2-vertex edge ${idx} (${edge.id()}) connecting ${sourceNode.id()} and ${targetNode.id()}, color: ${color}`)

    // Check if nodes have valid positions
    let allNodesHavePositions = true
    nodesToInclude.forEach(node => {
      const pos = node.position()
      if (!pos || (pos.x === 0 && pos.y === 0)) {
        console.warn(`Node ${node.id()} has invalid position:`, pos)
        allNodesHavePositions = false
      }
    })

    if (!allNodesHavePositions) {
      console.warn(`Skipping 2-vertex edge ${idx}: some nodes don't have valid positions yet`)
      failCount++
    } else {
      try {
        const path = bubbleSetsPlugin.addPath(nodesToInclude, null, null, {
          nodeR1: 80,  // Increase influence radius (default: 50)
          edgeR1: 40,  // Increase edge influence (default: 20)
          morphBuffer: 15,  // Increase buffer (default: 10)
          virtualEdges: false,  // Disable virtual edges (can cause issues)
          style: {
            fill: bgColor,
            stroke: color,
            strokeWidth: 3,
            opacity: 0.8
          }
        })

        if (path) {
          successCount++
          console.log(`Successfully created bubble set for 2-vertex edge ${idx}`)
        } else {
          failCount++
          console.warn(`Failed to create bubble set for 2-vertex edge ${idx}: addPath returned null/undefined`)
        }
      } catch (err) {
        failCount++
        console.error(`Error creating bubble set for 2-vertex edge ${idx}:`, err)
      }
    }
  })

  const createTimestamp = Date.now()
  console.log(`[${createTimestamp}] Convex hulls applied: ${successCount} successful, ${failCount} failed (${hyperedgeNodes.length} hyperedges + ${twoVertexEdges.length} 2-vertex edges)`)
  console.log(`[${createTimestamp}] === applyConvexHulls END ===`)
}

// Remove convex hulls
function removeConvexHulls() {
  const timestamp = Date.now()
  console.log(`[${timestamp}] removeConvexHulls called`)
  console.log('removeConvexHulls stack trace:', new Error().stack)

  if (bubbleSetsPlugin) {
    console.log(`[${timestamp}] Removing convex hulls`)
    // Remove all paths
    const paths = bubbleSetsPlugin.getPaths()
    console.log(`[${timestamp}] Removing ${paths.length} paths`)
    paths.forEach((path: any) => {
      bubbleSetsPlugin.removePath(path)
    })

    // Destroy the plugin instance to clean up the layer
    if (bubbleSetsPlugin.destroy) {
      bubbleSetsPlugin.destroy()
      console.log(`[${timestamp}] Bubble sets plugin destroyed`)
    }

    bubbleSetsPlugin = null
  } else {
    console.log(`[${timestamp}] removeConvexHulls called but bubbleSetsPlugin is null`)
  }

  // Manually clean up any leftover SVG layers
  if (cy && cy.container()) {
    const container = cy.container()
    // Find all SVG layers that are NOT the main Cytoscape canvas
    const svgLayers = container.querySelectorAll('svg')
    console.log(`Found ${svgLayers.length} SVG layers before cleanup`)

    svgLayers.forEach((svg, idx) => {
      // The first SVG is usually the main Cytoscape canvas with lots of children
      // Additional SVGs with few children are likely bubble sets layers
      if (idx > 0) {
        console.log(`Removing extra SVG layer ${idx}`)
        svg.remove()
      }
    })

    const remainingSvgs = container.querySelectorAll('svg')
    console.log(`${remainingSvgs.length} SVG layers remaining after cleanup`)
  }
}

// Watch for bubble sets toggle
watch(() => props.showBubbleSets, (newVal) => {
  if (!cy) return

  if (newVal) {
    applyBubbleSets()
  } else {
    // Reset to default styles and clear pie chart styling
    cy.nodes('.attribute').style({
      'background-color': '#3B82F6',
      'border-width': '2px',
      'border-color': '#2563EB',
      'pie-size': '0%'  // Disable pie chart
    })
    cy.nodes('.hyperedge').style({
      'background-color': '#F59E0B',
      'border-width': '2px',
      'border-color': '#2563EB'
    })
  }
})

// Watch for convex hulls toggle
watch(() => props.showConvexHulls, (newVal, oldVal) => {
  console.log(`showConvexHulls watcher triggered: ${oldVal} -> ${newVal}`)
  if (!cy) return

  if (newVal) {
    console.log('Watcher calling applyConvexHulls()')
    applyConvexHulls()
  } else {
    console.log('Watcher calling removeConvexHulls()')
    removeConvexHulls()
  }
})

function toggleSingletons() {
  if (!cy) return

  console.log('Toggling singleton nodes:', hideSingletons.value)

  // Find singleton attribute nodes (nodes with only one attribute)
  // Include .attribute, .output_attribute, and .schema_attribute nodes
  const attributeNodes = cy.nodes('.attribute, .output_attribute, .schema_attribute')

  attributeNodes.forEach(node => {
    const data = node.data()

    // Parse attributes
    let attributes: string[]
    if (typeof data.attributes === 'string') {
      attributes = data.attributes.split(',').map((s: string) => s.trim()).filter(Boolean)
    } else if (Array.isArray(data.attributes)) {
      attributes = data.attributes
    } else {
      attributes = []
    }

    // Check if this is a singleton (only one attribute)
    // Output_attribute and schema_attribute nodes are always singletons by definition
    const isSingleton = attributes.length === 1 || data.type === 'output_attribute' || data.type === 'schema_attribute'

    if (isSingleton) {
      if (hideSingletons.value) {
        // Hide the node and its connected edges
        node.style('display', 'none')
        node.connectedEdges().style('display', 'none')
      } else {
        // Show the node and its connected edges
        node.style('display', 'element')
        node.connectedEdges().style('display', 'element')
      }
    }
  })

  // Re-apply layout to adjust positions
  if (hideSingletons.value) {
    const layout = cy.layout({
      name: 'cose',
      animate: true,
      animationDuration: 300,
      randomize: false
    })
    layout.run()
  }
}

function toggleSchemaAttributes() {
  if (!cy) return

  console.log('Toggling schema attributes:', showSchemaAttributes.value)

  // Find schema_attribute nodes
  const schemaNodes = cy.nodes('.schema_attribute')

  schemaNodes.forEach(node => {
    if (showSchemaAttributes.value) {
      // Show schema attribute nodes and their connected edges
      node.style('display', 'element')
      node.connectedEdges().style('display', 'element')
    } else {
      // Hide schema attribute nodes and their connected edges
      node.style('display', 'none')
      node.connectedEdges().style('display', 'none')
    }
  })

  // Re-apply layout to adjust positions
  const layout = cy.layout({
    name: 'cose',
    animate: true,
    animationDuration: 300,
    randomize: false
  })
  layout.run()
}

function getOptimalLayoutName(): string {
  // Auto mode: use force-directed layout
  if (selectedLayout.value !== 'auto') {
    return selectedLayout.value
  }

  // Always use force-directed for auto mode
  console.log('Using cose layout (force-directed)')
  return 'cose'
}

function applySelectedLayout() {
  if (!cy) return

  const layoutName = getOptimalLayoutName()
  console.log(`Applying ${layoutName} layout`)

  let layoutOptions: any = {
    name: layoutName,
    animate: true,
    animationDuration: 500
  }

  // Configure layout-specific options
  if (layoutName === 'breadthfirst') {
    layoutOptions = {
      ...layoutOptions,
      directed: true,
      spacingFactor: 1.5,
      nodeDimensionsIncludeLabels: true,
      avoidOverlap: true,
      roots: cy.nodes('.hyperedge').map(n => n.id()) // Use hyperedge nodes as roots
    }
  } else if (layoutName === 'cose') {
    layoutOptions = {
      ...layoutOptions,
      nodeDimensionsIncludeLabels: true,
      randomize: true,
      idealEdgeLength: 100,
      nodeOverlap: 20,
      nodeRepulsion: 400000,
      edgeElasticity: 100,
      nestingFactor: 1.2,
      gravity: 80,
      numIter: 1000,
      initialTemp: 200,
      coolingFactor: 0.95,
      minTemp: 1.0
    }
  } else if (layoutName === 'circle') {
    layoutOptions = {
      ...layoutOptions,
      avoidOverlap: true,
      nodeDimensionsIncludeLabels: true,
      spacingFactor: 1.5
    }
  } else if (layoutName === 'concentric') {
    layoutOptions = {
      ...layoutOptions,
      concentric: (node: any) => {
        // Put hyperedge nodes in the center
        return node.hasClass('hyperedge') ? 10 : 1
      },
      levelWidth: () => 2,
      spacingFactor: 1.5,
      avoidOverlap: true,
      nodeDimensionsIncludeLabels: true
    }
  }

  const layout = cy.layout(layoutOptions)

  // Add layoutstop handler for convex hulls reapplication
  layout.one('layoutstop', () => {
    console.log('applySelectedLayout: Layout stopped')
    if (props.showConvexHulls && cy) {
      setTimeout(() => {
        console.log('applySelectedLayout: Applying convex hulls after layout stop')
        applyConvexHulls()
      }, 300)
    }
  })

  layout.run()
}

function applyLayout() {
  // Called when user changes layout selector
  applySelectedLayout()
  // Note: applyConvexHulls() will be called by the layoutstop event handler
  // No need to call it here - that was causing duplicate calls
  setTimeout(() => {
    if (cy) {
      cy.fit(undefined, 50)
      // Reapply bubble sets after layout to update pie slice ordering
      if (props.showBubbleSets) {
        applyBubbleSets()
      }
    }
  }, 600)
}

function resetLayout() {
  if (!cy) return

  // Reset to current layout with randomization
  const layoutName = getOptimalLayoutName()
  const layout = cy.layout({
    name: layoutName,
    animate: true,
    animationDuration: 500,
    randomize: true
  })

  // Add layoutstop handler for convex hulls and GYO styling reapplication
  layout.one('layoutstop', () => {
    console.log('resetLayout: Layout stopped')
    if (props.showConvexHulls && cy) {
      setTimeout(() => {
        console.log('resetLayout: Applying convex hulls after layout stop')
        applyConvexHulls()
      }, 300)
    }

    // Reapply GYO styling if we're viewing a GYO step
    if (props.gyoStep !== null && props.gyoSteps && props.gyoSteps.length > 0) {
      setTimeout(() => {
        console.log('resetLayout: Reapplying GYO styling for step', props.gyoStep)
        applyGYOStyling()
      }, 350)
    }
  })

  layout.run()

  // Reapply bubble sets after layout to update pie slice ordering
  setTimeout(() => {
    if (cy) {
      if (props.showBubbleSets) {
        applyBubbleSets()
      }
    }
  }, 600)
}

function fitGraph() {
  if (!cy) return
  cy.fit(undefined, 50)
}

function toggleExportMenu() {
  showExportMenu.value = !showExportMenu.value
}

function exportImage(format: 'png' | 'svg' | 'jpg' = 'png', scale: number = 2) {
  if (!cy) return

  let blob: Blob
  let filename: string
  const timestamp = new Date().toISOString().split('T')[0]

  if (format === 'svg') {
    // Export as SVG
    const svgContent = cy.svg({ full: true, bg: 'white' })
    blob = new Blob([svgContent], { type: 'image/svg+xml' })
    filename = `hypergraph-${timestamp}.svg`
  } else if (format === 'jpg') {
    // Export as JPG
    blob = cy.jpg({
      output: 'blob',
      bg: 'white',
      full: true,
      quality: 0.95,
      scale: scale
    })
    filename = `hypergraph-${timestamp}.jpg`
  } else {
    // Export as PNG (default)
    blob = cy.png({
      output: 'blob',
      bg: 'white',
      full: true,
      scale: scale
    })
    filename = `hypergraph-${timestamp}-${scale}x.png`
  }

  // Create download link
  const url = URL.createObjectURL(blob)
  const link = document.createElement('a')
  link.href = url
  link.download = filename
  link.click()
  URL.revokeObjectURL(url)

  // Close menu
  showExportMenu.value = false
}
</script>

<style scoped>
.hypergraph-viewer {
  position: relative;
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
  display: flex;
  justify-content: space-between;
  align-items: center;
}

.viewer-header h3 {
  margin: 0;
  font-size: 1.125rem;
  font-weight: 600;
}

.stats {
  display: flex;
  gap: 1rem;
  align-items: center;
}

.stat {
  font-size: 0.875rem;
  color: var(--color-text-secondary);
}

.cy-container {
  flex: 1;
  background: #F9FAFB;
}

.viewer-controls {
  padding: 1rem;
  border-top: 1px solid var(--color-border);
  display: flex;
  gap: 0.5rem;
  align-items: center;
}

.toggle-control {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  margin-right: auto;
  cursor: pointer;
  user-select: none;
}

.toggle-control input[type="checkbox"] {
  width: 18px;
  height: 18px;
  cursor: pointer;
}

.toggle-control span {
  font-size: 0.875rem;
  color: #4B5563;
  font-weight: 500;
}

.toggle-control:hover span {
  color: #1F2937;
}

.layout-selector {
  display: flex;
  align-items: center;
  gap: 0.5rem;
}

.layout-selector label {
  font-size: 0.875rem;
  color: #4B5563;
  font-weight: 500;
}

.layout-selector select {
  padding: 0.375rem 0.75rem;
  border: 1px solid #D1D5DB;
  border-radius: 0.375rem;
  background: white;
  font-size: 0.875rem;
  color: #1F2937;
  cursor: pointer;
  transition: border-color 0.2s;
}

.layout-selector select:hover {
  border-color: #9CA3AF;
}

.layout-selector select:focus {
  outline: none;
  border-color: #3B82F6;
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

.node-tooltip {
  position: absolute;
  background: white;
  border: 1px solid #cbd5e0;
  border-radius: 0.375rem;
  padding: 0.75rem;
  box-shadow: 0 4px 6px rgba(0, 0, 0, 0.1);
  z-index: 1000;
  max-width: 400px;
  pointer-events: none;
}

.tooltip-header {
  font-weight: 600;
  font-size: 0.875rem;
  margin-bottom: 0.5rem;
  color: #1f2937;
  border-bottom: 1px solid #e5e7eb;
  padding-bottom: 0.25rem;
}

.tooltip-content {
  font-size: 0.813rem;
  color: #4b5563;
}

.tooltip-content strong {
  color: #1f2937;
}

.tooltip-content ul {
  margin: 0.25rem 0 0 0;
  padding-left: 1.25rem;
  list-style-type: disc;
}

.tooltip-content li {
  margin: 0.125rem 0;
  font-family: 'Monaco', 'Menlo', 'Consolas', monospace;
  font-size: 0.75rem;
}

.tooltip-content li.output-attribute {
  font-weight: 600;
  color: #059669;
}

.output-badge {
  display: inline-block;
  margin-left: 0.5rem;
  padding: 0.125rem 0.375rem;
  background: #10B981;
  color: white;
  font-size: 0.625rem;
  font-weight: 700;
  border-radius: 0.25rem;
  text-transform: uppercase;
  letter-spacing: 0.025em;
}

/* Phase 2: GYO Tooltip Styles */
.gyo-tooltip-section {
  margin-top: 0.75rem;
  padding-top: 0.75rem;
  border-top: 1px solid #e5e7eb;
}

.gyo-tooltip-text {
  font-family: 'Monaco', 'Menlo', 'Consolas', monospace;
  font-size: 0.75rem;
  color: #9333EA;
  background: #F3E8FF;
  padding: 0.25rem 0.5rem;
  border-radius: 0.25rem;
  display: inline-block;
  margin-top: 0.25rem;
}

/* Pulsing animation for GYO elements being removed */
@keyframes gyoPulse {
  0%, 100% {
    opacity: 1;
    transform: scale(1);
  }
  50% {
    opacity: 0.7;
    transform: scale(1.05);
  }
}

/* Apply animation to canvas elements - note: Cytoscape uses canvas, so this targets the container */
.cy-container :global(.gyo-being-removed) {
  animation: gyoPulse 1.5s ease-in-out infinite;
}
</style>
