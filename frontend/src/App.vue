<template>
  <div id="app">
    <header class="app-header">
      <div class="container">
        <nav class="nav">
          <div class="nav-brand">
            <h1>Spark-Y</h1>
            <p class="subtitle">Avoiding Materialisation for Guarded Aggregate Queries</p>
          </div>
          <div class="nav-links">
            <router-link to="/" class="nav-link">Home</router-link>
            <router-link to="/queries" class="nav-link">Queries</router-link>
            <router-link to="/execute" class="nav-link">Execute</router-link>
            <router-link to="/data-import" class="nav-link">Data Import</router-link>
            <router-link to="/settings" class="nav-link">Settings</router-link>
          </div>
          <div class="nav-right">
            <a href="http://localhost:4060" target="_blank" class="spark-ui-link" title="Open Spark Web UI">
              <svg width="20" height="20" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <rect x="3" y="3" width="18" height="18" rx="2" ry="2"></rect>
                <line x1="9" y1="3" x2="9" y2="21"></line>
                <line x1="15" y1="3" x2="15" y2="21"></line>
                <line x1="3" y1="9" x2="21" y2="9"></line>
                <line x1="3" y1="15" x2="21" y2="15"></line>
              </svg>
              <span>Spark UI</span>
            </a>
            <div v-if="tableCount > 0" class="data-info">
              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <path d="M3 3h18v18H3z"></path>
                <path d="M3 9h18M3 15h18M9 3v18"></path>
              </svg>
              <span>{{ tableCount }} {{ tableCount === 1 ? 'table' : 'tables' }}</span>
              <span v-if="databaseInfo" class="db-name">• {{ databaseInfo.database }}</span>
            </div>
            <router-link v-else-if="healthStatus === 'healthy'" to="/data-import" class="import-prompt">
              <svg width="16" height="16" viewBox="0 0 24 24" fill="none" stroke="currentColor" stroke-width="2">
                <path d="M21 15v4a2 2 0 0 1-2 2H5a2 2 0 0 1-2-2v-4"></path>
                <polyline points="7 10 12 15 17 10"></polyline>
                <line x1="12" y1="15" x2="12" y2="3"></line>
              </svg>
              <span>Import Data</span>
            </router-link>
            <div class="health-indicator" :class="healthStatus">
              <span class="status-dot"></span>
              <span>{{ healthText }}</span>
            </div>
          </div>
        </nav>
      </div>
    </header>

    <main class="app-main">
      <router-view />
    </main>

    <footer class="app-footer">
      <div class="container">
        <div class="footer-content">
          <p>Spark-Y Demo | Avoiding Materialization for Guarded Aggregate Queries</p>
          <ThemeToggle />
        </div>
      </div>
    </footer>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'
import { healthApi } from '@/services/api'
import ThemeToggle from '@/components/ThemeToggle.vue'
import { useTheme } from '@/composables/useTheme'

const healthStatus = ref('unknown')
const healthText = ref('Checking...')
const tableCount = ref(0)
const databaseInfo = ref<{ type: string; database: string } | null>(null)

// Initialize theme
const { } = useTheme()

async function checkHealth() {
  try {
    const response = await healthApi.checkHealth()
    if (response.status === 'healthy' && response.spark === 'ready') {
      healthStatus.value = 'healthy'
      healthText.value = 'Ready'
      tableCount.value = response.table_count || 0
      databaseInfo.value = response.database || null
    } else {
      healthStatus.value = 'unhealthy'
      healthText.value = 'Spark not ready'
      tableCount.value = 0
      databaseInfo.value = null
    }
  } catch (error) {
    healthStatus.value = 'error'
    healthText.value = 'Backend offline'
    tableCount.value = 0
    databaseInfo.value = null
  }
}

onMounted(() => {
  checkHealth()
  // Check health every 30 seconds
  setInterval(checkHealth, 30000)
})
</script>

<style scoped>
.app-header {
  position: relative;
  background: linear-gradient(135deg, #0F766E 0%, #0891B2 100%);
  border-bottom: none;
  padding: 1.5rem 0;
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.1);
}

.app-header::before {
  content: '';
  position: absolute;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  opacity: 0.1;
  background-image:
    linear-gradient(45deg, transparent 48%, rgba(255,255,255,0.5) 50%, transparent 52%),
    linear-gradient(-45deg, transparent 48%, rgba(255,255,255,0.5) 50%, transparent 52%);
  background-size: 20px 20px;
  pointer-events: none;
}

.nav {
  position: relative;
  z-index: 1;
  display: flex;
  align-items: center;
  justify-content: space-between;
  gap: 2rem;
}

.nav-brand h1 {
  font-size: 1.5rem;
  font-weight: 700;
  color: white;
}

.subtitle {
  font-size: 0.875rem;
  color: rgba(255, 255, 255, 0.9);
  margin-top: 0.25rem;
}

.nav-links {
  display: flex;
  gap: 1.5rem;
}

.nav-right {
  display: flex;
  align-items: center;
  gap: 1rem;
}

.nav-link {
  text-decoration: none;
  color: rgba(255, 255, 255, 0.85);
  font-weight: 500;
  transition: all 0.2s;
  padding-bottom: 2px;
  border-bottom: 2px solid transparent;
}

.nav-link:hover,
.nav-link.router-link-active {
  color: white;
  border-bottom-color: white;
}

.spark-ui-link {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  text-decoration: none;
  color: rgba(255, 255, 255, 0.85);
  font-size: 0.875rem;
  font-weight: 500;
  padding: 0.5rem 0.75rem;
  border-radius: 6px;
  background: rgba(255, 255, 255, 0.1);
  border: 1px solid rgba(255, 255, 255, 0.2);
  transition: all 0.2s;
}

.spark-ui-link:hover {
  color: white;
  background: rgba(255, 255, 255, 0.15);
  border-color: rgba(255, 255, 255, 0.3);
  transform: translateY(-1px);
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.15);
}

.spark-ui-link svg {
  flex-shrink: 0;
}

.data-info {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  font-size: 0.875rem;
  color: rgba(255, 255, 255, 0.9);
  padding: 0.5rem 0.75rem;
  border-radius: 6px;
  background: rgba(255, 255, 255, 0.1);
  border: 1px solid rgba(255, 255, 255, 0.2);
}

.data-info svg {
  flex-shrink: 0;
}

.db-name {
  color: rgba(255, 255, 255, 0.75);
}

.import-prompt {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  text-decoration: none;
  color: rgba(255, 255, 255, 0.9);
  font-size: 0.875rem;
  font-weight: 500;
  padding: 0.5rem 0.75rem;
  border-radius: 6px;
  background: rgba(52, 211, 153, 0.2);
  border: 1px solid rgba(52, 211, 153, 0.4);
  transition: all 0.2s;
}

.import-prompt:hover {
  background: rgba(52, 211, 153, 0.3);
  border-color: rgba(52, 211, 153, 0.6);
  transform: translateY(-1px);
  box-shadow: 0 2px 8px rgba(0, 0, 0, 0.15);
}

.import-prompt svg {
  flex-shrink: 0;
}

.health-indicator {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  font-size: 0.875rem;
  color: rgba(255, 255, 255, 0.9);
}

.status-dot {
  width: 10px;
  height: 10px;
  border-radius: 50%;
  background-color: #94A3B8;
  box-shadow: 0 0 0 2px rgba(255, 255, 255, 0.3);
}

.health-indicator.healthy .status-dot {
  background-color: #34D399;
  box-shadow: 0 0 0 2px rgba(52, 211, 153, 0.3);
  animation: pulse 2s ease-in-out infinite;
}

.health-indicator.unhealthy .status-dot {
  background-color: #FBBF24;
  box-shadow: 0 0 0 2px rgba(251, 191, 36, 0.3);
}

.health-indicator.error .status-dot {
  background-color: #F87171;
  box-shadow: 0 0 0 2px rgba(248, 113, 113, 0.3);
}

@keyframes pulse {
  0%, 100% {
    opacity: 1;
  }
  50% {
    opacity: 0.7;
  }
}

.app-main {
  flex: 1;
  padding: 2rem 0;
}

.app-footer {
  background: white;
  border-top: 1px solid var(--color-border);
  padding: 1.5rem 0;
  color: var(--color-text-secondary);
  font-size: 0.875rem;
}

.footer-content {
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 1rem;
}

.footer-content p {
  margin: 0;
}

@media (max-width: 768px) {
  .nav {
    flex-wrap: wrap;
  }

  .nav-links {
    order: 3;
    flex-basis: 100%;
    justify-content: center;
    margin-top: 1rem;
  }

  .spark-ui-link span {
    display: none;
  }

  .spark-ui-link {
    padding: 0.5rem;
  }

  .data-info span:not(.db-name),
  .import-prompt span {
    display: none;
  }

  .data-info,
  .import-prompt {
    padding: 0.5rem;
  }

  .footer-content {
    flex-direction: column;
    text-align: center;
  }
}
</style>
