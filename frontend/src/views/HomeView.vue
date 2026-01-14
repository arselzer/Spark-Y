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
        <div class="benchmark-list">
          <div class="benchmark-item">
            <h3>JOB (Join Order Benchmark)</h3>
            <p>
              Complex join queries on the IMDB dataset with multiple tables and aggregates.
            </p>
          </div>
          <!--
          <div class="benchmark-item">
            <h3>TPC-H</h3>
            <p>
              Standard decision support benchmark with business-oriented queries
              involving joins and aggregations.
            </p>
          </div>
          -->
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
// No script needed for static home page
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
  background: white;
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

.benchmark-list {
  display: grid;
  gap: 1.5rem;
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
</style>
