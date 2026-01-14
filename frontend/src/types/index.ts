/**
 * TypeScript type definitions for the query optimization demo
 */

export type QueryCategory = 'job' | 'tpch' | 'tpcds' | 'custom'

export interface QueryMetadata {
  query_id: string
  name: string
  category: QueryCategory
  description?: string
  sql: string
  tables: string[]
  num_joins: number
  num_aggregates: number
}

export interface SparkConfig {
  yannakakis_enabled: boolean
  physical_count_join_enabled: boolean
  unguarded_enabled: boolean
  custom_options?: Record<string, string> | null
}

export interface ExecutionRequest {
  query_id?: string
  sql: string
  use_optimization?: boolean
  collect_metrics?: boolean
  reference_config?: SparkConfig
  optimized_config?: SparkConfig
}

export interface ExecutionMetrics {
  execution_time_ms: number
  planning_time_ms?: number
  total_input_rows: number
  total_output_rows: number
  intermediate_result_size_bytes: number
  avoided_materialization_bytes: number
  peak_memory_mb?: number
  num_stages: number
}

export interface ExecutionPlan {
  plan_string: string
  plan_tree?: any
  analyzed_logical_plan_json?: string
  optimized_logical_plan_json?: string
  logical_plan_json?: string  // Deprecated, use optimized_logical_plan_json
  physical_plan_json?: string
  optimizations_applied: string[]
}

export interface ExecutionResult {
  query_id?: string
  sql: string
  success: boolean
  error?: string

  original_metrics?: ExecutionMetrics
  optimized_metrics?: ExecutionMetrics

  original_plan?: ExecutionPlan
  optimized_plan?: ExecutionPlan

  result_rows?: Record<string, any>[]
  result_schema?: string[]

  speedup?: number
  memory_reduction?: number
}

export interface HypergraphNode {
  id: string
  label: string
  type: string
  attributes: string[]
  cardinality?: number
}

export interface HypergraphEdge {
  id: string
  nodes: string[]
  join_conditions: string[]
  label?: string
}

export interface JoinTreeNode {
  id: string
  relation: string
  attributes: string[]
  parent: string | null
  children: string[]
  level: number
  shared_attributes: Record<string, string[]>  // Map of child_id -> shared attributes
}

export interface SubsetRelationship {
  ear: string
  container: string
  ear_nodes: string[]
  container_nodes: string[]
  shared_nodes: string[]
  extra_in_container: string[]
}

export interface GYOStep {
  step_number: number
  action: string  // "remove_ear" or "remove_node"
  description: string
  removed_edges: string[]
  removed_nodes: string[]
  remaining_edges: string[]
  remaining_nodes: string[]

  // Phase 3: Enhanced subset relationship data
  why_ear?: string | null
  container_edges?: string[]
  affected_nodes?: string[]
  subset_relationships?: SubsetRelationship[]
}

export interface Hypergraph {
  query_id?: string
  sql: string
  nodes: HypergraphNode[]
  edges: HypergraphEdge[]
  hypertree_width?: number
  hypertree_decomposition?: any
  join_tree?: JoinTreeNode[] | null
  gyo_steps?: GYOStep[] | null
  is_acyclic: boolean
  is_guarded: boolean
  guardedness_type: 'guarded' | 'piecewise_guarded' | 'unguarded' | 'unknown'
  num_relations: number
  num_joins: number
  num_aggregates: number
  layout?: any
}

export interface CytoscapeElement {
  data: {
    id: string
    label?: string
    source?: string
    target?: string
    [key: string]: any
  }
  classes?: string
}

export interface VisualizationData {
  elements: CytoscapeElement[]
  layout: any
  stats: {
    num_nodes: number
    num_edges: number
    is_acyclic: boolean
    hypertree_width?: number
    num_relations?: number
    num_joins?: number
    num_aggregates?: number
  }
}

export interface QueryPlanNode {
  nodeType: string
  nodeName: string
  children: QueryPlanNode[]
  expressions?: any[]
  metadata: Record<string, any>
  output?: Array<{
    name: string
    dataType: string
    qualifier?: string
    exprId?: string
  }>
}

export interface QueryPlanResponse {
  sql: string
  plan_type: string
  plan: QueryPlanNode
}
