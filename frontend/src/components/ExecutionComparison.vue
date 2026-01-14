<template>
  <div class="execution-comparison">
    <h3>Execution Comparison</h3>

    <div v-if="result" class="comparison-grid">
      <!-- Original Execution -->
      <div class="execution-panel">
        <div class="panel-header original">
          <h4>Original Execution</h4>
          <span class="badge badge-warning">No Optimization</span>
        </div>

        <div v-if="result.original_metrics" class="metrics">
          <div class="metric">
            <span class="metric-label">Execution Time</span>
            <span class="metric-value">{{ formatTime(result.original_metrics.execution_time_ms) }}</span>
          </div>
          <div class="metric">
            <span class="metric-label">Intermediate Size</span>
            <span class="metric-value">{{ formatBytes(result.original_metrics.intermediate_result_size_bytes) }}</span>
          </div>
          <div class="metric">
            <span class="metric-label">Stages</span>
            <span class="metric-value">{{ result.original_metrics.num_stages }}</span>
          </div>
          <div v-if="result.original_metrics.peak_memory_mb" class="metric">
            <span class="metric-label">Peak Memory</span>
            <span class="metric-value">{{ result.original_metrics.peak_memory_mb.toFixed(1) }} MB</span>
          </div>
        </div>

        <div v-if="result.original_plan" class="plan-section">
          <h5>Physical Plan</h5>
          <div class="plan-details">
            <pre>{{ result.original_plan.plan_string }}</pre>
          </div>
        </div>
      </div>

      <!-- Optimized Execution -->
      <div class="execution-panel">
        <div class="panel-header optimized">
          <h4>Optimized Execution</h4>
          <span class="badge badge-success">With Optimization</span>
        </div>

        <div v-if="result.optimized_metrics" class="metrics">
          <div class="metric">
            <span class="metric-label">Execution Time</span>
            <span class="metric-value highlight">{{ formatTime(result.optimized_metrics.execution_time_ms) }}</span>
          </div>
          <div class="metric">
            <span class="metric-label">Intermediate Size</span>
            <span class="metric-value highlight">{{ formatBytes(result.optimized_metrics.intermediate_result_size_bytes) }}</span>
          </div>
          <div class="metric">
            <span class="metric-label">Avoided Materialization</span>
            <span class="metric-value success">{{ formatBytes(result.optimized_metrics.avoided_materialization_bytes) }}</span>
          </div>
          <div class="metric">
            <span class="metric-label">Stages</span>
            <span class="metric-value">{{ result.optimized_metrics.num_stages }}</span>
          </div>
          <div v-if="result.optimized_metrics.peak_memory_mb" class="metric">
            <span class="metric-label">Peak Memory</span>
            <span class="metric-value">{{ result.optimized_metrics.peak_memory_mb.toFixed(1) }} MB</span>
          </div>
        </div>

        <div v-if="result.optimized_plan" class="plan-section">
          <h5>Physical Plan</h5>
          <div class="plan-optimizations" v-if="result.optimized_plan.optimizations_applied.length">
            <strong>Optimizations Applied:</strong>
            <ul>
              <li v-for="opt in result.optimized_plan.optimizations_applied" :key="opt">
                ✓ {{ opt }}
              </li>
            </ul>
          </div>
          <div class="plan-details">
            <pre>{{ result.optimized_plan.plan_string }}</pre>
          </div>
        </div>
      </div>
    </div>

    <!-- Improvement Summary -->
    <div v-if="result && result.speedup" class="improvement-summary">
      <h4>Performance Improvement</h4>
      <div class="improvements">
        <div class="improvement-card">
          <div class="improvement-value">{{ result.speedup.toFixed(2) }}x</div>
          <div class="improvement-label">Speedup</div>
        </div>
        <div v-if="result.memory_reduction" class="improvement-card">
          <div class="improvement-value">{{ (result.memory_reduction * 100).toFixed(1) }}%</div>
          <div class="improvement-label">Memory Reduction</div>
        </div>
      </div>
    </div>

    <!-- Results Preview -->
    <div v-if="result && result.result_rows" class="results-section">
      <h4>Query Results (First 10 rows)</h4>
      <div class="results-table-container">
        <table class="results-table">
          <thead>
            <tr>
              <th v-for="col in result.result_schema" :key="col">{{ col }}</th>
            </tr>
          </thead>
          <tbody>
            <tr v-for="(row, idx) in result.result_rows" :key="idx">
              <td v-for="col in result.result_schema" :key="col">{{ row[col] }}</td>
            </tr>
          </tbody>
        </table>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import type { ExecutionResult } from '@/types'

interface Props {
  result: ExecutionResult | null
}

defineProps<Props>()

function formatTime(ms: number): string {
  if (ms < 1000) return `${ms.toFixed(0)} ms`
  return `${(ms / 1000).toFixed(2)} s`
}

function formatBytes(bytes: number): string {
  if (bytes === 0) return '0 B'
  const k = 1024
  const sizes = ['B', 'KB', 'MB', 'GB']
  const i = Math.floor(Math.log(bytes) / Math.log(k))
  return `${(bytes / Math.pow(k, i)).toFixed(2)} ${sizes[i]}`
}
</script>

<style scoped>
.execution-comparison {
  margin-top: 2rem;
}

.execution-comparison h3 {
  margin-bottom: 1.5rem;
  font-size: 1.5rem;
}

.comparison-grid {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 1.5rem;
  margin-bottom: 2rem;
}

.execution-panel {
  background: white;
  border-radius: 0.5rem;
  overflow: hidden;
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
}

.panel-header {
  padding: 1rem;
  display: flex;
  justify-content: space-between;
  align-items: center;
  border-bottom: 2px solid;
}

.panel-header.original {
  border-bottom-color: #F59E0B;
}

.panel-header.optimized {
  border-bottom-color: #10B981;
}

.panel-header h4 {
  margin: 0;
  font-size: 1.125rem;
}

.metrics {
  padding: 1rem;
  display: grid;
  gap: 1rem;
}

.metric {
  display: flex;
  justify-content: space-between;
  align-items: center;
  padding: 0.75rem;
  background: #F9FAFB;
  border-radius: 0.375rem;
}

.metric-label {
  font-weight: 500;
  color: var(--color-text-secondary);
}

.metric-value {
  font-size: 1.125rem;
  font-weight: 600;
}

.metric-value.highlight {
  color: var(--color-primary);
}

.metric-value.success {
  color: var(--color-secondary);
}

.plan-section {
  padding: 1rem;
  border-top: 1px solid var(--color-border);
}

.plan-section h5 {
  margin: 0 0 0.75rem 0;
  font-size: 0.875rem;
  text-transform: uppercase;
  color: var(--color-text-secondary);
}

.plan-optimizations {
  background: #D1FAE5;
  padding: 0.75rem;
  border-radius: 0.375rem;
  margin-bottom: 1rem;
  font-size: 0.875rem;
}

.plan-optimizations ul {
  margin: 0.5rem 0 0 0;
  padding-left: 1.5rem;
}

.plan-details {
  background: #F9FAFB;
  border-radius: 0.375rem;
  max-height: 300px;
  overflow: auto;
}

.plan-details pre {
  margin: 0;
  padding: 1rem;
  font-family: 'Monaco', 'Menlo', monospace;
  font-size: 12px;
  line-height: 1.4;
  white-space: pre-wrap;
  word-wrap: break-word;
}

.improvement-summary {
  background: white;
  border-radius: 0.5rem;
  padding: 1.5rem;
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
  margin-bottom: 2rem;
}

.improvement-summary h4 {
  margin: 0 0 1rem 0;
}

.improvements {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(200px, 1fr));
  gap: 1rem;
}

.improvement-card {
  background: linear-gradient(135deg, #10B981 0%, #059669 100%);
  color: white;
  padding: 1.5rem;
  border-radius: 0.5rem;
  text-align: center;
}

.improvement-value {
  font-size: 2.5rem;
  font-weight: 700;
  margin-bottom: 0.5rem;
}

.improvement-label {
  font-size: 1rem;
  opacity: 0.9;
}

.results-section {
  background: white;
  border-radius: 0.5rem;
  padding: 1.5rem;
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
}

.results-section h4 {
  margin: 0 0 1rem 0;
}

.results-table-container {
  overflow-x: auto;
}

.results-table {
  width: 100%;
  border-collapse: collapse;
  font-size: 0.875rem;
}

.results-table th,
.results-table td {
  padding: 0.75rem;
  text-align: left;
  border-bottom: 1px solid var(--color-border);
}

.results-table th {
  background: #F9FAFB;
  font-weight: 600;
  color: var(--color-text-secondary);
}

.results-table tbody tr:hover {
  background: #F9FAFB;
}

@media (max-width: 1024px) {
  .comparison-grid {
    grid-template-columns: 1fr;
  }
}
</style>
