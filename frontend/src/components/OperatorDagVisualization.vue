<script setup lang="ts">
import { ref, computed, onMounted, onUnmounted, watch, nextTick } from 'vue'
import cytoscape, { Core, ElementDefinition } from 'cytoscape'
import dagre from 'cytoscape-dagre'

// Register dagre layout extension
cytoscape.use(dagre)

interface OperatorMetric {
  operator_id: string | number
  operator_name: string
  operator_type: string
  num_output_rows: number
  children: Array<string | number>
  metrics: Record<string, any>
}

interface Props {
  operatorMetrics: OperatorMetric[]
  title?: string
}

const props = withDefaults(defineProps<Props>(), {
  title: 'Operator Flow'
})

const containerRef = ref<HTMLElement | null>(null)
const dagContainerRef = ref<HTMLElement | null>(null)
let cy: Core | null = null
const isFullscreen = ref(false)
const selectedLayout = ref('dagre')
const searchQuery = ref('')
const selectedNodeId = ref<string | null>(null)
const showStageBoundaries = ref(false)
const showExportMenu = ref(false)

// Available layout options
const layoutOptions = [
  { value: 'breadthfirst', label: 'Breadthfirst (Top-Down)' },
  { value: 'dagre', label: 'Dagre (Hierarchical)' },
  { value: 'cose', label: 'Force-Directed (COSE)' },
  { value: 'concentric', label: 'Concentric (Circular Layers)' },
  { value: 'grid', label: 'Grid' },
  { value: 'circle', label: 'Circle' }
]

// Operator type filtering
// All possible operator types with labels and colors
const operatorTypes = {
  scan: { label: 'Scan', color: '#60a5fa' },
  join: { label: 'Join', color: '#f59e0b' },
  aggregate: { label: 'Aggregate', color: '#10b981' },
  exchange: { label: 'Shuffle', color: '#a78bfa' },
  sort: { label: 'Sort', color: '#ec4899' },
  filter: { label: 'Filter', color: '#14b8a6' },
  project: { label: 'Project', color: '#8b5cf6' },
  other: { label: 'Other', color: '#6b7280' }
}

// Selected operator types (start with important ones)
const selectedTypes = ref<Set<string>>(new Set(['scan', 'join', 'aggregate', 'exchange']))

// Get unique operator types present in the data
const availableTypes = computed(() => {
  if (!props.operatorMetrics) return []
  const types = new Set(props.operatorMetrics.map(op => op.operator_type))
  return Array.from(types).sort()
})

function toggleAllTypes(select: boolean) {
  if (select) {
    selectedTypes.value = new Set(Object.keys(operatorTypes))
  } else {
    selectedTypes.value = new Set()
  }
}

// Search functionality
const filteredMetrics = computed(() => {
  if (!searchQuery.value.trim()) return props.operatorMetrics

  const query = searchQuery.value.toLowerCase()
  return props.operatorMetrics.filter(op =>
    op.operator_name.toLowerCase().includes(query) ||
    op.operator_type.toLowerCase().includes(query)
  )
})

// Format number with K/M suffixes
function formatNumber(num: number): string {
  if (num >= 1_000_000) return `${(num / 1_000_000).toFixed(1)}M`
  if (num >= 1_000) return `${(num / 1_000).toFixed(1)}K`
  return num.toString()
}

// Get edge color based on row count (green for min, red for max)
function getEdgeColor(rows: number, maxRows: number): string {
  if (rows === 0 || maxRows === 0) return '#94a3b8' // Gray for zero rows

  // Use logarithmic scale for better distribution
  const logRows = Math.log10(rows + 1)
  const logMax = Math.log10(maxRows + 1)
  const ratio = logRows / logMax

  // Green (low) to Yellow (mid) to Red (high)
  // Green: #10b981, Yellow: #f59e0b, Red: #ef4444
  let r: number, g: number, b: number

  if (ratio < 0.5) {
    // Green to Yellow (0 to 0.5)
    const t = ratio * 2 // 0 to 1
    r = Math.round(16 + (245 - 16) * t)
    g = Math.round(185 + (158 - 185) * t)
    b = Math.round(129 + (11 - 129) * t)
  } else {
    // Yellow to Red (0.5 to 1)
    const t = (ratio - 0.5) * 2 // 0 to 1
    r = Math.round(245 + (239 - 245) * t)
    g = Math.round(158 + (68 - 158) * t)
    b = Math.round(11 + (68 - 11) * t)
  }

  return `rgb(${r}, ${g}, ${b})`
}

// Get color for operator type
function getOperatorColor(type: string): string {
  return operatorTypes[type as keyof typeof operatorTypes]?.color || operatorTypes.other.color
}

// Compute stage groupings based on Exchange operators
function computeStages(): Map<number, Set<string>> {
  if (!props.operatorMetrics || props.operatorMetrics.length === 0) {
    return new Map()
  }

  // Build a map of parent -> children relationships
  const childrenMap = new Map<string, string[]>()
  props.operatorMetrics.forEach(op => {
    childrenMap.set(String(op.operator_id), (op.children || []).map(c => String(c)))
  })

  // Find all Exchange operators (stage boundaries)
  const exchanges = props.operatorMetrics
    .filter(op => op.operator_type === 'exchange')
    .map(op => String(op.operator_id))

  // Assign each operator to a stage using BFS from leaves
  const stageAssignment = new Map<string, number>()
  let currentStage = 0

  // Helper to recursively assign stages, moving up the DAG
  function assignStage(opId: string, stage: number, visited = new Set<string>()) {
    if (visited.has(opId)) return
    visited.add(opId)

    // If we haven't assigned this operator yet, assign it
    if (!stageAssignment.has(opId)) {
      stageAssignment.set(opId, stage)
    }

    // If this is an Exchange, increment stage for parents
    const op = props.operatorMetrics.find(o => String(o.operator_id) === opId)
    const isExchange = op?.operator_type === 'exchange'

    // Find parents (operators that have this as a child)
    props.operatorMetrics.forEach(parent => {
      const children = parent.children || []
      if (children.some(c => String(c) === opId)) {
        const parentId = String(parent.operator_id)
        const nextStage = isExchange ? stage + 1 : stage
        assignStage(parentId, nextStage, visited)
      }
    })
  }

  // Start from leaf nodes (operators with no children)
  props.operatorMetrics.forEach(op => {
    const opId = String(op.operator_id)
    const children = childrenMap.get(opId) || []
    if (children.length === 0) {
      // This is a leaf node, start BFS from here
      assignStage(opId, currentStage)
    }
  })

  // Group operators by stage
  const stages = new Map<number, Set<string>>()
  stageAssignment.forEach((stage, opId) => {
    if (!stages.has(stage)) {
      stages.set(stage, new Set())
    }
    stages.get(stage)!.add(opId)
  })

  return stages
}

// Build Cytoscape graph data with optional filtering
const graphData = computed(() => {
  const elements: ElementDefinition[] = []

  if (!props.operatorMetrics || props.operatorMetrics.length === 0) {
    return elements
  }

  // Filter operators based on selected types
  const visibleOps = props.operatorMetrics.filter(op => selectedTypes.value.has(op.operator_type))

  const visibleOpIds = new Set(visibleOps.map(op => String(op.operator_id)))

  // Build a map of children relationships for path finding
  const childrenMap = new Map<string, string[]>()
  props.operatorMetrics.forEach(op => {
    childrenMap.set(String(op.operator_id), (op.children || []).map(c => String(c)))
  })

  // Helper function to find visible descendants (skipping hidden nodes)
  function findVisibleDescendants(opId: string, visited = new Set<string>()): Set<string> {
    if (visited.has(opId)) return new Set() // Avoid cycles
    visited.add(opId)

    const children = childrenMap.get(opId) || []
    const visibleDescendants = new Set<string>()

    for (const childId of children) {
      if (visibleOpIds.has(childId)) {
        // Child is visible, add it
        visibleDescendants.add(childId)
      } else {
        // Child is hidden, recurse to find visible descendants
        const descendants = findVisibleDescendants(childId, visited)
        descendants.forEach(d => visibleDescendants.add(d))
      }
    }

    return visibleDescendants
  }

  // Compute stages if boundaries are enabled
  let stages = new Map<number, Set<string>>()
  if (showStageBoundaries.value) {
    stages = computeStages()
  }

  // Add stage compound nodes first (if enabled)
  if (showStageBoundaries.value && stages.size > 0) {
    stages.forEach((opIds, stageNum) => {
      elements.push({
        data: {
          id: `stage_${stageNum}`,
          label: `Stage ${stageNum}`,
          type: 'stage'
        },
        classes: 'stage-boundary'
      })
    })
  }

  // Add nodes for visible operators
  visibleOps.forEach(op => {
    const opIdStr = String(op.operator_id)

    // Find which stage this operator belongs to
    let stageNum: number | null = null
    if (showStageBoundaries.value) {
      stages.forEach((opIds, sNum) => {
        if (opIds.has(opIdStr)) {
          stageNum = sNum
        }
      })
    }

    elements.push({
      data: {
        id: `op_${op.operator_id}`,
        label: op.operator_name,
        type: op.operator_type,
        rows: op.num_output_rows,
        color: getOperatorColor(op.operator_type),
        metrics: op.metrics,
        parent: stageNum !== null ? `stage_${stageNum}` : undefined
      }
    })
  })

  // Build operator lookup map
  const opMap = new Map<string, OperatorMetric>()
  props.operatorMetrics.forEach(op => {
    opMap.set(String(op.operator_id), op)
  })

  // Calculate maximum row count across all operators for scaling
  const maxRows = Math.max(...props.operatorMetrics.map(op => op.num_output_rows), 1)

  // Add edges, reconnecting through hidden nodes
  visibleOps.forEach(op => {
    const opIdStr = String(op.operator_id)
    const visibleDescendants = findVisibleDescendants(opIdStr)

    // IMPORTANT: Reverse edge direction so data flows bottom-up (scans → output)
    // Create edges from visible descendants to this operator
    // Edge label should show SOURCE node's output (child's rows flowing into parent)
    visibleDescendants.forEach(childId => {
      // Get the child operator's output rows
      const childOp = opMap.get(childId)
      const childRows = childOp ? childOp.num_output_rows : 0

      elements.push({
        data: {
          id: `edge_${childId}_${op.operator_id}`,
          source: `op_${childId}`,  // Child (lower level) is source
          target: `op_${op.operator_id}`,  // Parent (upper level) is target
          label: formatNumber(childRows),  // Show child's output rows
          rows: childRows,  // Store for width calculation
          maxRows: maxRows  // Store max for relative scaling
        }
      })
    })
  })

  return elements
})

function initializeGraph() {
  if (!containerRef.value) return

  cy = cytoscape({
    container: containerRef.value,
    elements: graphData.value,
    style: [
      {
        selector: 'node',
        style: {
          'background-color': 'data(color)',
          'label': 'data(label)',
          'color': '#000000',  // Black text
          'text-outline-color': '#ffffff',  // White outline/border
          'text-outline-width': 3,  // Thick outline for visibility
          'text-valign': 'center',
          'text-halign': 'center',
          'font-size': '14px',  // Increased from 12px
          'font-weight': 'bold',
          'width': '150px',  // Increased from 120px
          'height': '70px',  // Increased from 60px
          'shape': 'roundrectangle',
          'border-width': 2,
          'border-color': '#ffffff',
          'text-wrap': 'wrap',
          'text-max-width': '140px'  // Increased from 110px
        }
      },
      {
        selector: 'node.highlighted',
        style: {
          'border-width': 4,
          'border-color': '#3b82f6',
          'z-index': 999
        }
      },
      {
        selector: 'node.dimmed',
        style: {
          'opacity': 0.3
        }
      },
      {
        selector: 'node.search-match',
        style: {
          'border-width': 4,
          'border-color': '#10b981',
          'z-index': 998
        }
      },
      {
        selector: 'node.search-no-match',
        style: {
          'opacity': 0.4
        }
      },
      {
        selector: 'edge',
        style: {
          // Width scaled relative to max rows in plan (logarithmic scale: 1-10 pixels)
          'width': function(ele: any) {
            const rows = ele.data('rows') || 0
            const maxRows = ele.data('maxRows') || 1

            if (rows === 0) return 1
            if (maxRows === 0) return 1

            // Use logarithmic scale for both rows and maxRows to handle wide ranges
            const logRows = Math.log10(rows + 1)
            const logMax = Math.log10(maxRows + 1)

            // Scale from 1 to 10 pixels based on ratio
            const ratio = logRows / logMax
            return Math.max(1, Math.min(10, 1 + ratio * 9))
          },
          // Dotted line for zero rows, solid for non-zero
          'line-style': function(ele: any) {
            const rows = ele.data('rows') || 0
            return rows === 0 ? 'dotted' : 'solid'
          },
          // Increased spacing for dotted lines (dot length, gap length)
          'line-dash-pattern': function(ele: any) {
            const rows = ele.data('rows') || 0
            return rows === 0 ? [2, 8] : [1, 0]  // For zero rows: 2px dots with 8px gaps
          },
          // Color gradient based on row count (green=low, yellow=mid, red=high)
          'line-color': function(ele: any) {
            const rows = ele.data('rows') || 0
            const maxRows = ele.data('maxRows') || 1
            return getEdgeColor(rows, maxRows)
          },
          'target-arrow-color': function(ele: any) {
            const rows = ele.data('rows') || 0
            const maxRows = ele.data('maxRows') || 1
            return getEdgeColor(rows, maxRows)
          },
          'target-arrow-shape': 'triangle',
          'curve-style': 'bezier',
          'label': 'data(label)',
          'font-size': '13px',  // Increased from 11px
          'font-weight': 'bold',
          'color': '#000000',  // Black text
          'text-outline-color': '#ffffff',  // White outline/border
          'text-outline-width': 3,  // Thick outline for visibility
          'text-background-opacity': 0  // Remove background (outline is enough)
        }
      },
      {
        selector: 'edge.highlighted',
        style: {
          'line-color': '#3b82f6',
          'target-arrow-color': '#3b82f6',
          'width': 4,
          'z-index': 999
        }
      },
      {
        selector: 'edge.dimmed',
        style: {
          'opacity': 0.2
        }
      },
      {
        selector: '.stage-boundary',
        style: {
          'background-color': 'transparent',
          'background-opacity': 0.1,
          'border-width': 2,
          'border-style': 'dashed',
          'border-color': '#94a3b8',
          'padding': 20,
          'label': 'data(label)',
          'text-valign': 'top',
          'text-halign': 'center',
          'font-size': '16px',
          'font-weight': 'bold',
          'color': '#64748b',
          'text-margin-y': -10
        }
      }
    ],
    layout: {
      name: selectedLayout.value,
      directed: true,
      spacingFactor: 1.5,
      padding: 30,
      avoidOverlap: true,
      roots: undefined  // Auto-detect root nodes
    },
    minZoom: 0.1,  // Allow zooming out to 10% to view entire large graphs
    maxZoom: 3,
    wheelSensitivity: 1.0  // Default sensitivity
  })

  // Add tooltips on hover
  cy.on('mouseover', 'node', (event) => {
    const node = event.target
    const data = node.data()
    const tooltip = `${data.label}\nType: ${data.type}\nOutput Rows: ${formatNumber(data.rows)}`
    node.style('label', tooltip)
  })

  cy.on('mouseout', 'node', (event) => {
    const node = event.target
    node.style('label', node.data('label'))
  })

  // Add click handler for path highlighting
  cy.on('tap', 'node', (event) => {
    const node = event.target
    const nodeId = node.id()

    // Don't highlight stage boundary nodes
    if (node.hasClass('stage-boundary')) {
      return
    }

    // Toggle: if already selected, clear selection
    if (selectedNodeId.value === nodeId) {
      highlightPaths(null)
    } else {
      highlightPaths(nodeId)
    }
  })

  // Click on background to clear selection
  cy.on('tap', (event) => {
    if (event.target === cy) {
      highlightPaths(null)
    }
  })
}

function fitGraph() {
  if (cy) {
    cy.fit(undefined, 50)
  }
}

// Export functions with enhanced options
function exportGraph(format: 'png' | 'svg' | 'jpg', scale: number = 2, includeLabels: boolean = true) {
  if (!cy) return

  const timestamp = new Date().toISOString().replace(/[:.]/g, '-').slice(0, -5)
  const filename = `operator-dag_${timestamp}.${format}`

  // Temporarily hide labels if requested
  let originalLabelStyle: any = null
  if (!includeLabels) {
    originalLabelStyle = {
      nodeLabel: cy.style().selector('node').style('label'),
      edgeLabel: cy.style().selector('edge').style('label')
    }
    cy.style()
      .selector('node').style('label', '')
      .selector('edge').style('label', '')
      .update()
  }

  try {
    if (format === 'svg') {
      const svgContent = cy.svg({ scale: scale, full: true })
      const blob = new Blob([svgContent], { type: 'image/svg+xml' })
      const url = URL.createObjectURL(blob)
      const link = document.createElement('a')
      link.href = url
      link.download = filename
      link.click()
      URL.revokeObjectURL(url)
    } else {
      // PNG or JPG
      const options: any = {
        output: 'blob',
        bg: format === 'jpg' ? '#ffffff' : 'transparent',
        full: true,
        scale: scale
      }

      const blob = format === 'png' ? cy.png(options) : cy.jpg(options)
      const url = URL.createObjectURL(blob)
      const link = document.createElement('a')
      link.href = url
      link.download = filename
      link.click()
      URL.revokeObjectURL(url)
    }
  } finally {
    // Restore labels if they were hidden
    if (!includeLabels && originalLabelStyle) {
      cy.style()
        .selector('node').style('label', 'data(label)')
        .selector('edge').style('label', 'data(label)')
        .update()
    }
  }
}

function toggleExportMenu() {
  showExportMenu.value = !showExportMenu.value
}

function closeExportMenu() {
  showExportMenu.value = false
}

// Apply selected layout to the graph
function applyLayout() {
  if (!cy) return

  const layoutConfig: any = {
    name: selectedLayout.value,
    animate: true,
    animationDuration: 500,
    fit: true,
    padding: 30
  }

  // Layout-specific configurations
  switch (selectedLayout.value) {
    case 'breadthfirst':
      layoutConfig.directed = true
      layoutConfig.spacingFactor = 1.5
      layoutConfig.avoidOverlap = true
      layoutConfig.roots = undefined  // Auto-detect root nodes
      break
    case 'dagre':
      layoutConfig.rankDir = 'TB'  // Top to bottom
      layoutConfig.nodeSep = 50
      layoutConfig.edgeSep = 10
      layoutConfig.rankSep = 75
      break
    case 'cose':
      layoutConfig.idealEdgeLength = 150
      layoutConfig.nodeOverlap = 20
      layoutConfig.refresh = 20
      layoutConfig.randomize = false
      break
    case 'concentric':
      layoutConfig.concentric = (node: any) => node.degree()
      layoutConfig.levelWidth = () => 2
      layoutConfig.spacingFactor = 1.5
      break
    case 'grid':
      layoutConfig.spacingFactor = 1.5
      layoutConfig.avoidOverlap = true
      break
    case 'circle':
      layoutConfig.spacingFactor = 1.5
      layoutConfig.avoidOverlap = true
      break
  }

  cy.layout(layoutConfig).run()
  nextTick(() => fitGraph())
}

// Reset layout: clear selection, reset zoom/pan, reapply default layout
function resetLayout() {
  if (!cy) return

  // Clear any selected nodes
  selectedNodeId.value = null
  cy.elements().removeClass('highlighted dimmed')

  // Reset to default layout
  selectedLayout.value = 'dagre'

  // Reset zoom and pan
  cy.zoom(1)
  cy.center()

  // Apply default layout
  applyLayout()
}

// Highlight paths to/from a selected node
function highlightPaths(nodeId: string | null) {
  if (!cy) return

  // Clear previous highlighting
  cy.elements().removeClass('highlighted dimmed')

  if (!nodeId) {
    selectedNodeId.value = null
    return
  }

  selectedNodeId.value = nodeId

  const node = cy.$(`#${nodeId}`)
  if (!node || node.length === 0) return

  // Find all ancestors (nodes that lead to this node) and descendants (nodes this leads to)
  const ancestors = node.predecessors()
  const descendants = node.successors()
  const connectedElements = node.union(ancestors).union(descendants)

  // Highlight connected elements
  connectedElements.addClass('highlighted')

  // Dim non-connected elements
  cy.elements().not(connectedElements).addClass('dimmed')
}

function toggleFullscreen() {
  if (!dagContainerRef.value) return

  if (!isFullscreen.value) {
    // Enter fullscreen
    if (dagContainerRef.value.requestFullscreen) {
      dagContainerRef.value.requestFullscreen().then(() => {
        // Wait for fullscreen transition to complete
        setTimeout(() => {
          if (cy) {
            cy.resize()  // Tell Cytoscape to recalculate its size
            fitGraph()
          }
        }, 200)
      })
    }
    isFullscreen.value = true
  } else {
    // Exit fullscreen
    if (document.exitFullscreen) {
      document.exitFullscreen().then(() => {
        // Wait for exit transition to complete, then update state and resize
        setTimeout(() => {
          isFullscreen.value = false  // Update state AFTER transition
          // Use nextTick to ensure DOM has updated before resizing
          nextTick(() => {
            if (cy && containerRef.value) {
              // Save viewport state before destroying
              const zoom = cy.zoom()
              const pan = cy.pan()

              // CLEAN SOLUTION: Destroy and recreate the Cytoscape instance
              // This removes all inline styles automatically and gives us a fresh canvas
              cy.destroy()
              initializeGraph()

              // Restore viewport state after recreation
              nextTick(() => {
                if (cy) {
                  cy.zoom(zoom)
                  cy.pan(pan)
                }
              })
            }
          })
        }, 200)
      })
    } else {
      // Fallback if exitFullscreen not available
      isFullscreen.value = false
    }
  }
}

// Listen for fullscreen changes (user pressing ESC)
function handleFullscreenChange() {
  const isCurrentlyFullscreen = !!document.fullscreenElement

  // If exiting fullscreen, delay state update to prevent layout issues
  if (!isCurrentlyFullscreen && isFullscreen.value) {
    setTimeout(() => {
      isFullscreen.value = false
      nextTick(() => {
        if (cy && containerRef.value) {
          // Save viewport state before destroying
          const zoom = cy.zoom()
          const pan = cy.pan()

          // CLEAN SOLUTION: Destroy and recreate the Cytoscape instance
          // This removes all inline styles automatically and gives us a fresh canvas
          cy.destroy()
          initializeGraph()

          // Restore viewport state after recreation
          nextTick(() => {
            if (cy) {
              cy.zoom(zoom)
              cy.pan(pan)
            }
          })
        }
      })
    }, 200)
  } else {
    // Entering fullscreen or no change
    isFullscreen.value = isCurrentlyFullscreen
  }
}

// Watch for data changes
watch(() => props.operatorMetrics, () => {
  if (cy) {
    cy.elements().remove()
    cy.add(graphData.value)
    cy.layout({
      name: 'breadthfirst',
      directed: true,
      spacingFactor: 1.5,
      padding: 30,
      avoidOverlap: true
    }).run()
    nextTick(() => fitGraph())
  }
}, { deep: true })

// Watch for operator type selection changes
watch(selectedTypes, () => {
  if (cy) {
    cy.elements().remove()
    cy.add(graphData.value)
    applyLayout()
  }
}, { deep: true })

// Watch for layout changes
watch(selectedLayout, () => {
  applyLayout()
})

// Watch for search query changes - filter and re-layout
watch(searchQuery, () => {
  if (cy && searchQuery.value.trim()) {
    // Search within visible nodes
    const query = searchQuery.value.toLowerCase()
    cy.nodes().forEach((node: any) => {
      const label = node.data('label').toLowerCase()
      const type = node.data('type').toLowerCase()

      if (label.includes(query) || type.includes(query)) {
        node.addClass('search-match')
        node.removeClass('search-no-match')
      } else {
        node.removeClass('search-match')
        node.addClass('search-no-match')
      }
    })
  } else {
    // Clear search highlighting
    cy?.nodes().removeClass('search-match search-no-match')
  }
})

// Watch for stage boundaries toggle
watch(showStageBoundaries, () => {
  if (cy) {
    cy.elements().remove()
    cy.add(graphData.value)
    applyLayout()
  }
})

// Close export menu when clicking outside
function handleClickOutside(event: MouseEvent) {
  const target = event.target as HTMLElement
  if (showExportMenu.value && !target.closest('.export-dropdown')) {
    closeExportMenu()
  }
}

onMounted(() => {
  document.addEventListener('fullscreenchange', handleFullscreenChange)
  document.addEventListener('click', handleClickOutside)
  initializeGraph()
  nextTick(() => fitGraph())
})

onUnmounted(() => {
  document.removeEventListener('fullscreenchange', handleFullscreenChange)
  document.removeEventListener('click', handleClickOutside)
})
</script>

<template>
  <div class="operator-dag">
    <div class="dag-header">
      <h4>{{ title }}</h4>
      <div class="controls">
        <!-- Search and Layout Section -->
        <div class="search-layout-section">
          <div class="search-box">
            <input
              v-model="searchQuery"
              type="text"
              placeholder="Search operators..."
              class="search-input"
              title="Search by operator name or type"
            />
            <span v-if="searchQuery" @click="searchQuery = ''" class="clear-search" title="Clear search">×</span>
          </div>
          <select v-model="selectedLayout" class="layout-select" title="Choose graph layout">
            <option v-for="layout in layoutOptions" :key="layout.value" :value="layout.value">
              {{ layout.label }}
            </option>
          </select>
        </div>

        <!-- Operator Type Filters -->
        <div class="filter-section">
          <div class="filter-header">
            <span class="filter-label">Show operators:</span>
            <button @click="toggleAllTypes(true)" class="btn-toggle" title="Select all">All</button>
            <button @click="toggleAllTypes(false)" class="btn-toggle" title="Deselect all">None</button>
          </div>
          <div class="filter-types">
            <label
              v-for="type in availableTypes"
              :key="type"
              class="type-checkbox"
              :title="`Toggle ${operatorTypes[type as keyof typeof operatorTypes]?.label || type} operators`"
            >
              <input
                type="checkbox"
                :checked="selectedTypes.has(type)"
                @change="(e) => {
                  if ((e.target as HTMLInputElement).checked) {
                    selectedTypes.add(type)
                  } else {
                    selectedTypes.delete(type)
                  }
                }"
              />
              <span
                class="type-indicator"
                :style="{ backgroundColor: getOperatorColor(type) }"
              ></span>
              <span class="type-label">{{ operatorTypes[type as keyof typeof operatorTypes]?.label || type }}</span>
            </label>
          </div>
        </div>

        <!-- Stage Boundaries Toggle -->
        <div class="stage-toggle-section">
          <label class="stage-toggle" title="Show stage boundaries (grouped by shuffles)">
            <input
              type="checkbox"
              v-model="showStageBoundaries"
            />
            <span class="toggle-label">Show Stage Boundaries</span>
          </label>
        </div>

        <!-- Action Buttons -->
        <div class="action-buttons">
          <button @click="resetLayout" class="btn-control btn-reset" title="Reset layout and clear selections">
            <span>↺</span> Reset
          </button>
          <button @click="fitGraph" class="btn-control" title="Fit to view">
            <span>🔍</span> Fit
          </button>
          <button @click="toggleFullscreen" class="btn-control" :title="isFullscreen ? 'Exit fullscreen (ESC)' : 'Fullscreen view'">
            <span>{{ isFullscreen ? '⛶' : '⛶' }}</span> {{ isFullscreen ? 'Exit' : 'Fullscreen' }}
          </button>
          <div class="export-dropdown">
            <button @click="toggleExportMenu" class="btn-control" title="Export visualization">
              <span>📷</span> Export <span class="dropdown-arrow">▾</span>
            </button>
            <div v-if="showExportMenu" class="export-menu" @click.stop>
              <div class="export-section">
                <div class="export-section-title">Format & Resolution</div>
                <button @click="exportGraph('png', 2); closeExportMenu()" class="export-option">
                  PNG (2x) - Standard Quality
                </button>
                <button @click="exportGraph('png', 4); closeExportMenu()" class="export-option">
                  PNG (4x) - High Quality
                </button>
                <button @click="exportGraph('svg', 2); closeExportMenu()" class="export-option">
                  SVG - Scalable Vector
                </button>
                <button @click="exportGraph('jpg', 2); closeExportMenu()" class="export-option">
                  JPG (2x) - Compressed
                </button>
              </div>
              <div class="export-section">
                <div class="export-section-title">Without Labels</div>
                <button @click="exportGraph('png', 2, false); closeExportMenu()" class="export-option">
                  PNG (2x) - No Labels
                </button>
                <button @click="exportGraph('png', 4, false); closeExportMenu()" class="export-option">
                  PNG (4x) - No Labels
                </button>
                <button @click="exportGraph('svg', 2, false); closeExportMenu()" class="export-option">
                  SVG - No Labels
                </button>
              </div>
            </div>
          </div>
        </div>
      </div>
    </div>
    <div
      v-if="operatorMetrics && operatorMetrics.length > 0"
      ref="dagContainerRef"
      class="dag-container"
      :class="{ 'fullscreen-container': isFullscreen }"
    >
      <div ref="containerRef" class="cytoscape-wrapper"></div>
    </div>
    <div v-else class="no-data">
      <p>No operator metrics available</p>
      <small>Operator-level metrics require SQL execution tracking via Spark UI</small>
    </div>
  </div>
</template>

<style scoped>
.operator-dag {
  background: var(--color-background);
  border: 1px solid var(--color-border);
  border-radius: 8px;
  padding: 1rem;
  height: 900px;
  display: flex;
  flex-direction: column;
}

.dag-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 1rem;
  padding-bottom: 0.75rem;
  border-bottom: 1px solid var(--color-border);
}

.dag-header h4 {
  margin: 0;
  font-size: 1.125rem;
  color: var(--color-text);
}

.controls {
  display: flex;
  gap: 1rem;
  align-items: flex-start;
  flex-wrap: wrap;
}

.search-layout-section {
  display: flex;
  gap: 0.5rem;
  align-items: center;
}

.search-box {
  position: relative;
  display: flex;
  align-items: center;
}

.search-input {
  padding: 0.5rem 2rem 0.5rem 0.75rem;
  font-size: 0.875rem;
  border: 1px solid var(--color-border);
  border-radius: 4px;
  background: var(--color-background);
  color: var(--color-text);
  min-width: 200px;
  transition: all 0.2s;
}

.search-input:focus {
  outline: none;
  border-color: var(--color-primary);
  box-shadow: 0 0 0 3px rgba(59, 130, 246, 0.1);
}

.clear-search {
  position: absolute;
  right: 0.5rem;
  font-size: 1.5rem;
  color: var(--color-text-secondary);
  cursor: pointer;
  line-height: 1;
  padding: 0.25rem;
  transition: color 0.2s;
}

.clear-search:hover {
  color: var(--color-text);
}

.layout-select {
  padding: 0.5rem 0.75rem;
  font-size: 0.875rem;
  border: 1px solid var(--color-border);
  border-radius: 4px;
  background: var(--color-background);
  color: var(--color-text);
  cursor: pointer;
  transition: all 0.2s;
}

.layout-select:hover {
  border-color: var(--color-primary);
}

.layout-select:focus {
  outline: none;
  border-color: var(--color-primary);
  box-shadow: 0 0 0 3px rgba(59, 130, 246, 0.1);
}

.filter-section {
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
}

.filter-header {
  display: flex;
  align-items: center;
  gap: 0.5rem;
}

.filter-label {
  font-size: 0.875rem;
  font-weight: 500;
  color: var(--color-text);
}

.btn-toggle {
  padding: 0.25rem 0.5rem;
  font-size: 0.75rem;
  background: var(--color-background-soft);
  border: 1px solid var(--color-border);
  border-radius: 4px;
  cursor: pointer;
  color: var(--color-text);
  transition: all 0.2s;
}

.btn-toggle:hover {
  background: var(--color-border);
}

.filter-types {
  display: flex;
  flex-wrap: wrap;
  gap: 0.5rem;
}

.type-checkbox {
  display: flex;
  align-items: center;
  gap: 0.375rem;
  padding: 0.375rem 0.625rem;
  background: var(--color-background-soft);
  border: 1px solid var(--color-border);
  border-radius: 4px;
  color: var(--color-text);
  cursor: pointer;
  font-size: 0.875rem;
  transition: all 0.2s;
  user-select: none;
}

.type-checkbox:hover {
  border-color: var(--color-primary);
  background: var(--color-background);
}

.type-checkbox input[type="checkbox"] {
  cursor: pointer;
}

.type-indicator {
  width: 12px;
  height: 12px;
  border-radius: 2px;
  flex-shrink: 0;
}

.type-label {
  white-space: nowrap;
}

.action-buttons {
  display: flex;
  gap: 0.5rem;
  align-items: center;
}

.btn-control {
  display: flex;
  align-items: center;
  gap: 0.25rem;
  padding: 0.5rem 0.75rem;
  background: var(--color-background-soft);
  border: 1px solid var(--color-border);
  border-radius: 4px;
  color: var(--color-text);
  cursor: pointer;
  font-size: 0.875rem;
  transition: all 0.2s;
}

.btn-control:hover {
  background: var(--color-primary);
  color: #ffffff;
  border-color: var(--color-primary);
}

.btn-reset {
  background: var(--color-background-soft);
  border-color: var(--color-border);
}

.btn-reset:hover {
  background: #f59e0b;
  border-color: #f59e0b;
  color: #ffffff;
}

.stage-toggle-section {
  display: flex;
  align-items: center;
}

.stage-toggle {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  padding: 0.5rem 0.75rem;
  background: var(--color-background-soft);
  border: 1px solid var(--color-border);
  border-radius: 4px;
  cursor: pointer;
  font-size: 0.875rem;
  transition: all 0.2s;
  user-select: none;
}

.stage-toggle:hover {
  border-color: var(--color-primary);
  background: var(--color-background);
}

.stage-toggle input[type="checkbox"] {
  cursor: pointer;
}

.toggle-label {
  white-space: nowrap;
  color: var(--color-text);
}

.dag-container {
  flex: 1;
  background: var(--color-background-soft);
  border-radius: 4px;
  min-height: 0;
  position: relative;
  overflow: hidden;
}

.cytoscape-wrapper {
  width: 100%;
  height: 100%;
  overflow: hidden;
}

.fullscreen-container {
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  width: 100vw;
  height: 100vh;
  z-index: 9999;
  background: var(--color-background);
  border-radius: 0;
  padding: 1rem;
}

.fullscreen-container .cytoscape-wrapper {
  width: 100%;
  height: 100%;
}

.no-data {
  flex: 1;
  display: flex;
  flex-direction: column;
  align-items: center;
  justify-content: center;
  color: var(--color-text-secondary);
  text-align: center;
  padding: 2rem;
}

.no-data p {
  margin: 0 0 0.5rem 0;
  font-size: 1rem;
  color: var(--color-text);
}

.no-data small {
  font-size: 0.875rem;
  max-width: 400px;
}

/* Export Dropdown */
.export-dropdown {
  position: relative;
}

.dropdown-arrow {
  font-size: 0.75rem;
  margin-left: 0.25rem;
}

.export-menu {
  position: absolute;
  top: 100%;
  right: 0;
  margin-top: 0.5rem;
  background: var(--color-background);
  border: 1px solid var(--color-border);
  border-radius: 4px;
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.15);
  z-index: 1000;
  min-width: 220px;
  max-height: 400px;
  overflow-y: auto;
}

.export-section {
  padding: 0.5rem 0;
  border-bottom: 1px solid var(--color-border);
}

.export-section:last-child {
  border-bottom: none;
}

.export-section-title {
  padding: 0.5rem 1rem 0.375rem;
  font-size: 0.75rem;
  font-weight: 600;
  text-transform: uppercase;
  letter-spacing: 0.05em;
  color: var(--color-text-secondary);
}

.export-option {
  display: block;
  width: 100%;
  padding: 0.625rem 1rem;
  text-align: left;
  background: none;
  border: none;
  color: var(--color-text);
  font-size: 0.875rem;
  cursor: pointer;
  transition: all 0.2s;
}

.export-option:hover {
  background: var(--color-background-soft);
  color: var(--color-primary);
}

/* Dark mode refinements for stage boundaries */
:root.dark .stage-boundary {
  border-color: #64748b !important;
}

:root.dark .export-menu {
  box-shadow: 0 4px 12px rgba(0, 0, 0, 0.4);
}
</style>
