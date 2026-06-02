<template>
  <div class="container">
    <div class="home-view">
      <div class="hero">
        <h1>Avoiding Materialisation for Guarded Aggregate Queries</h1>
        <p class="tagline">
          Interactive demonstration of query optimization techniques in Apache Spark SQL
        </p>
        <div class="hero-actions">
          <router-link to="/queries" class="btn btn-primary">Browse Queries</router-link>
          <router-link to="/execute" class="btn btn-primary">Execute Custom Query</router-link>
          <router-link to="/data-import" class="btn btn-primary">Import Data</router-link>
        </div>
      </div>

      <div class="features">
             <div class="feature-card">
                <!-- <div class="feature-icon">🎯</div> -->
                <h3>Avoid Materialisation</h3>
                <p>
                  See how the optimisation avoids materialising intermediate join results
                  for acyclic queries with aggregates, improving query performance.
                </p>
              </div>

        <div class="feature-card">
          <!-- <div class="feature-icon">📊</div> -->
          <h3>Hypergraph Visualisation</h3>
          <p>
            Visualize the hypergraph structure of SQL queries, showing relations
            and join structure.
          </p>
        </div>

        <div class="feature-card">
         <!-- <div class="feature-icon">⚡</div> -->
          <h3>Performance Comparison</h3>
          <p>
            Execute queries with and without optimization. Compare execution times,
            intermediate result sizes, etc., side-by-side.
          </p>
        </div>

      </div>

      <div class="paper-info card">
        <h2>About This Research</h2>
        <p>
          This demonstration showcases the techniques described in the VLDB 2025 paper
          <strong>"Avoiding Materialisation for Guarded Aggregate Queries"</strong>.
        </p>
        <p>
          The paper addresses the challenge of query processing for analytical queries
          that join many tables but produce small aggregate results. By making use of
          acyclic query structure and Yannakakis-style query processing, we can avoid materialising
          expensive intermediate join results.
        </p>
        <div class="paper-links">
          <a href="https://www.vldb.org/pvldb/vol18/p1398-selzer.pdf" target="_blank" class="btn btn-primary">
            VLDB Paper
          </a>
          <a href="https://github.com/arselzer/spark" target="_blank" class="btn btn-primary">
            Implementation
          </a>
        </div>
      </div>

      <div class="benchmarks-info card">
        <h2>Available Benchmarks</h2>
        <p class="benchmarks-intro">
          Queries are packaged with the demo; data dumps live under <code>data/sql-dumps</code>
          and per-benchmark schemas under <code>data/schemas</code>.
        </p>
        <div class="benchmark-list">
          <div v-for="b in shownBenchmarks" :key="b.label" class="benchmark-item">
            <div class="benchmark-item-header">
              <span :class="['badge', b.info.badgeClass]">{{ b.info.label }}</span>
              <span class="benchmark-count">{{ b.count }} {{ b.count === 1 ? 'query' : 'queries' }}</span>
            </div>
            <h3>{{ b.info.fullName }}</h3>
            <p>{{ b.info.description }}</p>
            <p class="benchmark-table-note" v-if="b.info.tableNote">{{ b.info.tableNote }}</p>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted } from 'vue'
import { queryApi } from '@/services/api'
import { benchmarkInfo } from '@/types/benchmarks'
import type { QueryCategory } from '@/types'

const categories = ref<Record<string, { count: number; name: string }>>({})

onMounted(async () => {
  try {
    categories.value = await queryApi.getCategories()
  } catch (err) {
    // Benchmarks section degrades gracefully if the API is unreachable.
    console.error('Failed to load category counts', err)
  }
})

const shownBenchmarks = computed(() => {
  // Show benchmarks that have at least one loaded query, in a fixed order
  // so the home page reads consistently between renders.
  const order: QueryCategory[] = ['job', 'tpch', 'stats-ceb', 'snap', 'tpcds']
  return order
    .filter((cat) => (categories.value[cat]?.count ?? 0) > 0)
    .map((cat) => ({
      info: benchmarkInfo(cat),
      count: categories.value[cat].count,
      label: cat,
    }))
})
</script>

<style scoped>
.home-view {
  max-width: 1200px;
  margin: 0 auto;
}

.hero {
  text-align: center;
  padding: 3rem 0;
  margin-bottom: 3rem;
}

.hero h1 {
  font-size: 2.5rem;
  font-weight: 700;
  margin-bottom: 1rem;
  color: var(--color-text);
}

.tagline {
  font-size: 1.25rem;
  color: var(--color-text-secondary);
  margin-bottom: 2rem;
}

.hero-actions {
  display: flex;
  gap: 1rem;
  justify-content: center;
}

.features {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(300px, 1fr));
  gap: 2rem;
  margin-bottom: 3rem;
}

.feature-card {
  background: var(--color-surface);
  padding: 2rem;
  border-radius: 0.5rem;
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
  text-align: center;
}

.feature-icon {
  font-size: 3rem;
  margin-bottom: 1rem;
}

.feature-card h3 {
  font-size: 1.25rem;
  font-weight: 600;
  margin-bottom: 0.75rem;
}

.feature-card p {
  color: var(--color-text-secondary);
  line-height: 1.6;
}

.paper-info,
.benchmarks-info {
  margin-bottom: 2rem;
}

.paper-info h2,
.benchmarks-info h2 {
  font-size: 1.75rem;
  margin-bottom: 1rem;
}

.paper-info p {
  margin-bottom: 1rem;
  line-height: 1.8;
}

.paper-links {
  display: flex;
  gap: 1rem;
  margin-top: 1.5rem;
}

.benchmarks-intro {
  margin-bottom: 1.5rem;
  color: var(--color-text-secondary);
  line-height: 1.6;
}

.benchmarks-intro code {
  background: var(--color-bg-secondary, #f3f4f6);
  padding: 0.1rem 0.35rem;
  border-radius: 0.25rem;
  font-size: 0.875em;
}

.benchmark-list {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(280px, 1fr));
  gap: 1.5rem;
}

.benchmark-item {
  background: var(--color-bg-secondary, #f9fafb);
  padding: 1.25rem;
  border-radius: 0.5rem;
}

.benchmark-item-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 0.5rem;
}

.benchmark-count {
  font-size: 0.875rem;
  color: var(--color-text-secondary);
}

.benchmark-item h3 {
  font-size: 1.125rem;
  font-weight: 600;
  margin-bottom: 0.5rem;
}

.benchmark-item p {
  color: var(--color-text-secondary);
  line-height: 1.6;
}

.benchmark-table-note {
  font-size: 0.85rem;
  margin-top: 0.5rem;
  opacity: 0.85;
}
</style>
