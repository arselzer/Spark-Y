<template>
  <div class="spark-query-plan-comparison">
    <div class="plan-header">
      <h3>Spark SQL Query Plan</h3>
      <div class="plan-header-controls">
        <label for="plan-type-select" class="sr-only">Select plan type</label>
        <select id="plan-type-select" v-model="selectedPlanType" class="plan-type-select" aria-label="Select plan type to display">
          <option value="analyzed-logical">Analyzed Logical Plan</option>
          <option value="optimized-logical">Optimized Logical Plan</option>
          <option value="physical">Physical Plan</option>
        </select>
      </div>
    </div>

    <div class="plans-container">
      <!-- Reference Plan -->
      <div class="plan-panel">
        <div class="panel-header">
          <h4>Reference Plan</h4>
          <div class="panel-controls">
            <button @click="toggleReferenceExpanded" class="btn btn-sm">
              {{ allReferenceExpanded ? 'Collapse All' : 'Expand All' }}
            </button>
          </div>
        </div>
        <div class="plan-content">
          <div v-if="referencePlanTree" class="plan-tree">
            <QueryPlanNode
              :node="referencePlanTree"
              :depth="0"
              :expanded-nodes="referenceExpandedNodes"
              @toggle="toggleReferenceNode"
            />
          </div>
          <div v-else class="no-plan">
            No {{ selectedPlanType }} plan available
          </div>
        </div>
      </div>

      <!-- Optimized Plan -->
      <div class="plan-panel">
        <div class="panel-header">
          <h4>Optimized Plan</h4>
          <div class="panel-controls">
            <button @click="toggleOptimizedExpanded" class="btn btn-sm">
              {{ allOptimizedExpanded ? 'Collapse All' : 'Expand All' }}
            </button>
          </div>
        </div>
        <div class="plan-content">
          <div v-if="optimizedPlanTree" class="plan-tree">
            <QueryPlanNode
              :node="optimizedPlanTree"
              :depth="0"
              :expanded-nodes="optimizedExpandedNodes"
              @toggle="toggleOptimizedNode"
            />
          </div>
          <div v-else class="no-plan">
            No {{ selectedPlanType }} plan available
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch } from 'vue'
import QueryPlanNode from './QueryPlanNode.vue'

interface ExecutionPlan {
  plan_string: string
  plan_tree?: any
  analyzed_logical_plan_json?: string
  optimized_logical_plan_json?: string
  logical_plan_json?: string  // Deprecated, fallback to optimized_logical_plan_json
  physical_plan_json?: string
  optimizations_applied?: string[]
}

interface Props {
  referencePlan?: ExecutionPlan
  optimizedPlan?: ExecutionPlan
}

const props = defineProps<Props>()

const selectedPlanType = ref<'analyzed-logical' | 'optimized-logical' | 'physical'>('optimized-logical')
const referenceExpandedNodes = ref(new Set<string>())
const optimizedExpandedNodes = ref(new Set<string>())
const allReferenceExpanded = ref(false)
const allOptimizedExpanded = ref(false)

// Helper function to transform Spark logical plan node to QueryPlanNode format
function transformLogicalPlanNode(sparkNode: any): any {
  // Extract node type from class name (e.g., "org.apache.spark.sql.catalyst.plans.logical.Aggregate" -> "Aggregate")
  const className = sparkNode.class || 'Unknown'
  const nodeType = className.split('.').pop() || 'Unknown'

  // Build metadata from all fields except structural ones
  const metadata: Record<string, any> = {}
  const excludedFields = ['class', 'num-children', 'child', 'left', 'right', 'children', 'output']

  Object.keys(sparkNode).forEach(key => {
    if (!excludedFields.includes(key)) {
      const value = sparkNode[key]
      // Serialize objects/arrays to JSON strings for display
      if (typeof value === 'object' && value !== null) {
        metadata[key] = JSON.stringify(value, null, 2)
      } else {
        metadata[key] = value
      }
    }
  })

  // Recursively transform children
  const transformedChildren = (sparkNode.children || []).map((child: any) => transformLogicalPlanNode(child))

  return {
    nodeType,
    nodeName: nodeType,  // For logical plans, use the same name
    children: transformedChildren,
    metadata,
    output: sparkNode.output || []
  }
}

// Helper function to convert flat plan array to nested tree
// Spark's logical plan JSON is a flat array in depth-first order
// We reconstruct the tree using the 'num-children' field, NOT the child/left/right fields
function convertFlatPlanToTree(flatPlan: any[]): any {
  console.log('=== convertFlatPlanToTree CALLED ===')
  console.log('Input flatPlan:', flatPlan)
  console.log('Is array:', Array.isArray(flatPlan))
  console.log('Length:', flatPlan?.length)

  if (!Array.isArray(flatPlan) || flatPlan.length === 0) {
    console.log('Returning null: not an array or empty')
    return null
  }

  // Recursive function to build tree from depth-first array
  // Returns [reconstructed node, next index to process]
  function reconstructFromArray(array: any[], startIndex: number): [any, number] {
    if (startIndex >= array.length) {
      return [null, startIndex]
    }

    const node = { ...array[startIndex] }
    const numChildren = node['num-children'] || 0

    console.log(`Reconstructing node at index ${startIndex}: ${node.class}, num-children=${numChildren}`)

    // Recursively reconstruct children
    let nextIndex = startIndex + 1
    node.children = []

    for (let i = 0; i < numChildren; i++) {
      const [childNode, newNextIndex] = reconstructFromArray(array, nextIndex)
      if (childNode) {
        node.children.push(childNode)
        nextIndex = newNextIndex
      }
    }

    console.log(`Node ${startIndex} (${node.class}) has ${node.children.length} children, next index=${nextIndex}`)
    return [node, nextIndex]
  }

  // Start reconstruction from the root (index 0)
  const [rootNode] = reconstructFromArray(flatPlan, 0)

  if (!rootNode) {
    console.error('Failed to reconstruct tree from flat array')
    return null
  }

  console.log('Successfully reconstructed tree, root class:', rootNode.class)

  // Transform to QueryPlanNode format
  return transformLogicalPlanNode(rootNode)
}

// Parse plan JSON strings to get tree structures
const referencePlanTree = computed(() => {
  console.log('=== referencePlanTree computed ===')
  console.log('selectedPlanType:', selectedPlanType.value)
  console.log('props.referencePlan:', props.referencePlan)
  console.log('props.referencePlan?.analyzed_logical_plan_json exists:', !!props.referencePlan?.analyzed_logical_plan_json)
  console.log('props.referencePlan?.optimized_logical_plan_json exists:', !!props.referencePlan?.optimized_logical_plan_json)
  console.log('props.referencePlan?.logical_plan_json exists:', !!props.referencePlan?.logical_plan_json)
  console.log('props.referencePlan?.plan_tree exists:', !!props.referencePlan?.plan_tree)

  if (selectedPlanType.value === 'analyzed-logical') {
    const jsonStr = props.referencePlan?.analyzed_logical_plan_json
    if (!jsonStr) {
      console.log('No analyzed logical plan JSON available')
      return null
    }
    console.log('Trying to parse analyzed logical plan JSON...')
    console.log('JSON length:', jsonStr.length)
    try {
      const parsed = JSON.parse(jsonStr)
      console.log('Parsed successfully:', parsed)
      console.log('Parsed is array:', Array.isArray(parsed))
      if (Array.isArray(parsed)) {
        const tree = convertFlatPlanToTree(parsed)
        console.log('convertFlatPlanToTree returned:', tree)
        return tree
      }
      console.log('Parsed is not array, returning as-is')
      return parsed
    } catch (e) {
      console.error('Error parsing reference analyzed logical plan:', e)
      return null
    }
  } else if (selectedPlanType.value === 'optimized-logical') {
    // Try new field first, then fallback to deprecated field
    const jsonStr = props.referencePlan?.optimized_logical_plan_json || props.referencePlan?.logical_plan_json
    if (!jsonStr) {
      console.log('No optimized logical plan JSON available')
      return null
    }
    console.log('Trying to parse optimized logical plan JSON...')
    console.log('JSON length:', jsonStr.length)
    try {
      const parsed = JSON.parse(jsonStr)
      console.log('Parsed successfully:', parsed)
      console.log('Parsed is array:', Array.isArray(parsed))
      if (Array.isArray(parsed)) {
        const tree = convertFlatPlanToTree(parsed)
        console.log('convertFlatPlanToTree returned:', tree)
        return tree
      }
      console.log('Parsed is not array, returning as-is')
      return parsed
    } catch (e) {
      console.error('Error parsing reference optimized logical plan:', e)
      return null
    }
  } else if (selectedPlanType.value === 'physical' && props.referencePlan?.plan_tree) {
    console.log('Returning physical plan_tree')
    return props.referencePlan.plan_tree
  }
  console.log('No plan available, returning null')
  return null
})

const optimizedPlanTree = computed(() => {
  console.log('=== optimizedPlanTree computed ===')
  console.log('selectedPlanType:', selectedPlanType.value)
  console.log('props.optimizedPlan:', props.optimizedPlan)
  console.log('props.optimizedPlan?.analyzed_logical_plan_json exists:', !!props.optimizedPlan?.analyzed_logical_plan_json)
  console.log('props.optimizedPlan?.optimized_logical_plan_json exists:', !!props.optimizedPlan?.optimized_logical_plan_json)
  console.log('props.optimizedPlan?.logical_plan_json exists:', !!props.optimizedPlan?.logical_plan_json)
  console.log('props.optimizedPlan?.plan_tree exists:', !!props.optimizedPlan?.plan_tree)

  if (selectedPlanType.value === 'analyzed-logical') {
    const jsonStr = props.optimizedPlan?.analyzed_logical_plan_json
    if (!jsonStr) {
      console.log('No analyzed logical plan JSON available')
      return null
    }
    console.log('Trying to parse analyzed logical plan JSON...')
    console.log('JSON length:', jsonStr.length)
    try {
      const parsed = JSON.parse(jsonStr)
      console.log('Parsed successfully:', parsed)
      console.log('Parsed is array:', Array.isArray(parsed))
      if (Array.isArray(parsed)) {
        const tree = convertFlatPlanToTree(parsed)
        console.log('convertFlatPlanToTree returned:', tree)
        return tree
      }
      console.log('Parsed is not array, returning as-is')
      return parsed
    } catch (e) {
      console.error('Error parsing optimized analyzed logical plan:', e)
      return null
    }
  } else if (selectedPlanType.value === 'optimized-logical') {
    // Try new field first, then fallback to deprecated field
    const jsonStr = props.optimizedPlan?.optimized_logical_plan_json || props.optimizedPlan?.logical_plan_json
    if (!jsonStr) {
      console.log('No optimized logical plan JSON available')
      return null
    }
    console.log('Trying to parse optimized logical plan JSON...')
    console.log('JSON length:', jsonStr.length)
    try {
      const parsed = JSON.parse(jsonStr)
      console.log('Parsed successfully:', parsed)
      console.log('Parsed is array:', Array.isArray(parsed))
      if (Array.isArray(parsed)) {
        const tree = convertFlatPlanToTree(parsed)
        console.log('convertFlatPlanToTree returned:', tree)
        return tree
      }
      console.log('Parsed is not array, returning as-is')
      return parsed
    } catch (e) {
      console.error('Error parsing optimized optimized logical plan:', e)
      return null
    }
  } else if (selectedPlanType.value === 'physical' && props.optimizedPlan?.plan_tree) {
    console.log('Returning physical plan_tree')
    return props.optimizedPlan.plan_tree
  }
  console.log('No plan available, returning null')
  return null
})

// Helper function to expand top N levels
function expandTopLevels(node: any, currentDepth: number, maxDepth: number, path: string, expandedSet: Set<string>) {
  if (currentDepth < maxDepth) {
    expandedSet.add(path)
    node.children?.forEach((child: any, index: number) => {
      expandTopLevels(child, currentDepth + 1, maxDepth, `${path}-${index}`, expandedSet)
    })
  }
}

// Helper function to expand all nodes
function expandAllNodes(node: any, path: string, expandedSet: Set<string>) {
  expandedSet.add(path)
  node.children?.forEach((child: any, index: number) => {
    expandAllNodes(child, `${path}-${index}`, expandedSet)
  })
}

// Reference plan controls
function toggleReferenceNode(path: string) {
  if (referenceExpandedNodes.value.has(path)) {
    referenceExpandedNodes.value.delete(path)
  } else {
    referenceExpandedNodes.value.add(path)
  }
}

function toggleReferenceExpanded() {
  if (allReferenceExpanded.value) {
    referenceExpandedNodes.value.clear()
  } else if (referencePlanTree.value) {
    expandAllNodes(referencePlanTree.value, '0', referenceExpandedNodes.value)
  }
  allReferenceExpanded.value = !allReferenceExpanded.value
}

// Optimized plan controls
function toggleOptimizedNode(path: string) {
  if (optimizedExpandedNodes.value.has(path)) {
    optimizedExpandedNodes.value.delete(path)
  } else {
    optimizedExpandedNodes.value.add(path)
  }
}

function toggleOptimizedExpanded() {
  if (allOptimizedExpanded.value) {
    optimizedExpandedNodes.value.clear()
  } else if (optimizedPlanTree.value) {
    expandAllNodes(optimizedPlanTree.value, '0', optimizedExpandedNodes.value)
  }
  allOptimizedExpanded.value = !allOptimizedExpanded.value
}

// Auto-expand top 2 levels when plans change
watch([referencePlanTree, selectedPlanType], ([newTree]) => {
  if (newTree) {
    referenceExpandedNodes.value.clear()
    expandTopLevels(newTree, 0, 2, '0', referenceExpandedNodes.value)
  }
}, { immediate: true })

watch([optimizedPlanTree, selectedPlanType], ([newTree]) => {
  if (newTree) {
    optimizedExpandedNodes.value.clear()
    expandTopLevels(newTree, 0, 2, '0', optimizedExpandedNodes.value)
  }
}, { immediate: true })
</script>

<style scoped>
.spark-query-plan-comparison {
  background: var(--color-background);
  border: 1px solid var(--color-border);
  border-radius: 8px;
  padding: 1.5rem;
  margin-top: 2rem;
}

.plan-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 1.5rem;
  padding-bottom: 1rem;
  border-bottom: 2px solid var(--color-border);
}

.plan-header h3 {
  margin: 0;
  font-size: 1.5rem;
  color: var(--color-text);
}

.plan-header-controls {
  display: flex;
  gap: 0.5rem;
  align-items: center;
}

.plan-type-select {
  padding: 0.375rem 0.75rem;
  border: 1px solid var(--color-border);
  border-radius: 4px;
  background: var(--color-background);
  font-size: 0.875rem;
  cursor: pointer;
  color: var(--color-text);
}

.plan-type-select:focus {
  outline: none;
  border-color: var(--color-primary);
}

.plans-container {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 1.5rem;
}

.plan-panel {
  background: var(--color-background-soft);
  border: 1px solid var(--color-border);
  border-radius: 6px;
  padding: 1rem;
  min-height: 400px;
  display: flex;
  flex-direction: column;
}

.panel-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 1rem;
  padding-bottom: 0.5rem;
  border-bottom: 1px solid var(--color-border);
}

.panel-header h4 {
  margin: 0;
  font-size: 1.125rem;
  color: var(--color-text);
}

.panel-controls {
  display: flex;
  gap: 0.5rem;
}

.btn-sm {
  padding: 0.375rem 0.75rem;
  font-size: 0.875rem;
  background: var(--color-background);
  border: 1px solid var(--color-border);
  border-radius: 4px;
  cursor: pointer;
  color: var(--color-text);
  transition: all 0.2s ease;
}

.btn-sm:hover {
  background: var(--color-primary);
  color: white;
  border-color: var(--color-primary);
}

.plan-content {
  flex: 1;
  overflow: auto;
  background: var(--color-background);
  border: 1px solid var(--color-border);
  border-radius: 4px;
  padding: 1rem;
}

.plan-tree {
  font-family: 'Monaco', 'Menlo', 'Ubuntu Mono', 'Consolas', monospace;
  font-size: 0.875rem;
}

.no-plan {
  color: var(--color-text-secondary);
  text-align: center;
  padding: 2rem;
  font-style: italic;
}

/* Screen reader only text */
.sr-only {
  position: absolute;
  width: 1px;
  height: 1px;
  padding: 0;
  margin: -1px;
  overflow: hidden;
  clip: rect(0, 0, 0, 0);
  white-space: nowrap;
  border-width: 0;
}

/* Responsive */
@media (max-width: 1024px) {
  .plans-container {
    grid-template-columns: 1fr;
  }
}

@media (max-width: 768px) {
  .spark-query-plan-comparison {
    padding: 1rem;
  }

  .plan-header {
    flex-direction: column;
    align-items: flex-start;
    gap: 1rem;
  }

  .plan-panel {
    min-height: 300px;
  }
}
</style>
