/**
 * API service for communicating with FastAPI backend
 */
import axios from 'axios'
import type {
  QueryMetadata,
  ExecutionResult,
  Hypergraph,
  VisualizationData,
  QueryPlanResponse
} from '@/types'

const API_BASE_URL = import.meta.env.VITE_API_URL || '/api'

const api = axios.create({
  baseURL: API_BASE_URL,
  headers: {
    'Content-Type': 'application/json'
  }
})

// Query Management API
export const queryApi = {
  /**
   * List all queries with optional filtering
   */
  async listQueries(params?: {
    category?: string
    search?: string
    limit?: number
    offset?: number
  }): Promise<QueryMetadata[]> {
    const response = await api.get('/queries/', { params })
    return response.data
  },

  /**
   * Get a specific query by ID
   */
  async getQuery(queryId: string): Promise<QueryMetadata> {
    const response = await api.get(`/queries/${queryId}`)
    return response.data
  },

  /**
   * Get query categories with counts
   */
  async getCategories(): Promise<Record<string, { count: number; name: string }>> {
    const response = await api.get('/queries/categories')
    return response.data
  },

  /**
   * Find similar queries
   */
  async getSimilarQueries(queryId: string, limit: number = 5): Promise<QueryMetadata[]> {
    const response = await api.get(`/queries/${queryId}/similar`, { params: { limit } })
    return response.data
  }
}

// Query Execution API
export const executionApi = {
  /**
   * Execute a query and get comparison results
   */
  async executeQuery(params: {
    query_id?: string
    sql: string
    use_optimization?: boolean
    collect_metrics?: boolean
    reference_config?: any
    optimized_config?: any
  }, signal?: AbortSignal): Promise<ExecutionResult> {
    const response = await api.post('/execution/execute', params, { signal })
    return response.data
  },

  /**
   * Get execution plan without running the query
   */
  async explainQuery(sql: string): Promise<{
    logical_plan: string
    optimized_logical_plan: string
    physical_plan: string
  }> {
    const response = await api.post('/execution/explain', { sql })
    return response.data
  },

  /**
   * Create WebSocket connection for execution progress
   */
  createExecutionStream(executionId: string): WebSocket {
    const wsUrl = `${API_BASE_URL.replace('http', 'ws')}/execution/stream/${executionId}`
    return new WebSocket(wsUrl)
  }
}

// Hypergraph API
export const hypergraphApi = {
  /**
   * Extract hypergraph from SQL query
   */
  async extractHypergraph(sql: string, queryId?: string, includeSchemaAttributes?: boolean, signal?: AbortSignal): Promise<Hypergraph> {
    const response = await api.post('/hypergraph/extract', null, {
      params: {
        sql,
        query_id: queryId,
        include_schema_attributes: includeSchemaAttributes || false
      },
      signal
    })
    return response.data
  },

  /**
   * Get hypergraph for a query from catalog
   */
  async getQueryHypergraph(queryId: string, includeSchemaAttributes?: boolean): Promise<Hypergraph> {
    const response = await api.get(`/hypergraph/${queryId}`, {
      params: {
        include_schema_attributes: includeSchemaAttributes || false
      }
    })
    return response.data
  },

  /**
   * Generate visualization data for hypergraph
   */
  async generateVisualization(hypergraph: Hypergraph): Promise<VisualizationData> {
    const response = await api.post('/hypergraph/visualize', hypergraph)
    return response.data
  },

  /**
   * Analyze hypergraph properties
   */
  async analyzeProperties(queryId: string): Promise<any> {
    const response = await api.get(`/hypergraph/properties/${queryId}`)
    return response.data
  },

  /**
   * Get Spark logical plan as JSON
   */
  async getQueryPlanJson(
    sql: string,
    planType: 'logical' | 'analyzed' | 'optimized' = 'analyzed'
  ): Promise<QueryPlanResponse> {
    const response = await api.post('/hypergraph/query-plan-json', null, {
      params: { sql, plan_type: planType }
    })
    return response.data
  }
}

// Health check
export const healthApi = {
  async checkHealth(): Promise<{ status: string; spark: string }> {
    const response = await api.get('/health')
    return response.data
  }
}

export default api
