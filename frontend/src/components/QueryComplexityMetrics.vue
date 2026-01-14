<template>
  <div v-if="metrics" class="complexity-metrics">
    <div class="metrics-header">
      <h3>Query Complexity Analysis</h3>
    </div>

    <div class="metrics-grid">
      <!-- Basic Metrics -->
      <div class="metric-section">
        <h4 class="section-title">Basic Metrics</h4>
        <div class="metric-items">
          <div class="metric-item">
            <span class="metric-label">Tables</span>
            <span class="metric-value">{{ metrics.num_tables }}</span>
          </div>
          <div class="metric-item">
            <span class="metric-label">Joins</span>
            <span class="metric-value">{{ metrics.num_joins }}</span>
          </div>
          <div class="metric-item">
            <span class="metric-label">Aggregates</span>
            <span class="metric-value">{{ metrics.num_aggregates }}</span>
          </div>
          <div class="metric-item">
            <span class="metric-label">Attributes</span>
            <span class="metric-value">{{ metrics.num_attributes }}</span>
          </div>
        </div>
      </div>

      <!-- Hypergraph Properties -->
      <div class="metric-section">
        <h4 class="section-title">Hypergraph Structure</h4>
        <div class="metric-items">
          <div class="metric-item">
            <span class="metric-label">Max Node Degree</span>
            <span class="metric-value">{{ metrics.max_node_degree }}</span>
            <span class="metric-hint">Connections per attribute</span>
          </div>
          <div class="metric-item">
            <span class="metric-label">Avg Node Degree</span>
            <span class="metric-value">{{ metrics.avg_node_degree }}</span>
          </div>
          <div class="metric-item">
            <span class="metric-label">Max Edge Size</span>
            <span class="metric-value">{{ metrics.max_edge_size }}</span>
            <span class="metric-hint">Largest table</span>
          </div>
          <div class="metric-item">
            <span class="metric-label">Avg Edge Size</span>
            <span class="metric-value">{{ metrics.avg_edge_size }}</span>
          </div>
          <div class="metric-item">
            <span class="metric-label">Graph Density</span>
            <span class="metric-value">{{ (metrics.hypergraph_density * 100).toFixed(2) }}%</span>
            <span class="metric-hint">Connectivity measure</span>
          </div>
        </div>
      </div>

      <!-- Acyclic Properties (if applicable) -->
      <div v-if="isAcyclic && metrics.join_tree_depth !== null" class="metric-section">
        <h4 class="section-title">Acyclic Query Properties</h4>
        <div class="metric-items">
          <div class="metric-item">
            <span class="metric-label">Join Tree Depth</span>
            <span class="metric-value">{{ metrics.join_tree_depth }}</span>
            <span class="metric-hint">Execution depth</span>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
interface Props {
  metrics: any | null
  isAcyclic?: boolean
}

defineProps<Props>()
</script>

<style scoped>
.complexity-metrics {
  background: var(--color-background);
  border: 1px solid var(--color-border);
  border-radius: 0.5rem;
  padding: 1.5rem;
  margin-top: 2rem;
}

.metrics-header {
  margin-bottom: 1.5rem;
  padding-bottom: 1rem;
  border-bottom: 2px solid var(--color-border);
}

.metrics-header h3 {
  margin: 0;
  font-size: 1.25rem;
  font-weight: 600;
  color: var(--color-text);
}

.metrics-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
  gap: 1.5rem;
}

.metric-section {
  background: var(--color-background-soft);
  border: 1px solid var(--color-border);
  border-radius: 0.5rem;
  padding: 1rem;
}

.section-title {
  margin: 0 0 1rem 0;
  font-size: 0.9375rem;
  font-weight: 600;
  color: var(--color-text);
  text-transform: uppercase;
  letter-spacing: 0.5px;
  font-size: 0.8125rem;
}

.metric-items {
  display: flex;
  flex-direction: column;
  gap: 0.75rem;
}

.metric-item {
  display: flex;
  flex-direction: column;
  gap: 0.25rem;
}

.metric-label {
  font-size: 0.875rem;
  color: var(--color-text-secondary);
  font-weight: 500;
}

.metric-value {
  font-size: 1.5rem;
  font-weight: 700;
  color: var(--color-primary);
}

.metric-hint {
  font-size: 0.75rem;
  color: var(--color-text-secondary);
  font-style: italic;
}

@media (max-width: 768px) {
  .metrics-grid {
    grid-template-columns: 1fr;
  }

  .metrics-header {
    flex-direction: column;
    align-items: flex-start;
    gap: 0.75rem;
  }
}
</style>
