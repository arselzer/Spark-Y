<template>
  <div class="container">
    <div class="settings-page">
      <div class="page-header">
        <h1>Spark SQL Configuration</h1>
        <p class="subtitle">Configure Spark SQL optimization settings for reference and optimized query executions</p>
      </div>

      <div class="settings-content">
        <!-- Info Banner -->
        <div class="info-banner">
          <div class="info-icon">⚙️</div>
          <div class="info-text">
            <strong>Flexible Configuration:</strong> Customize Spark SQL settings for both reference and optimized executions.
            The default configuration has optimizations disabled for reference and enabled for optimized, but you can change any setting.
          </div>
        </div>

        <!-- Reference Configuration -->
        <div class="config-section">
          <div class="section-header">
            <h2>Reference Execution</h2>
            <span class="badge badge-reference">Baseline</span>
          </div>
          <p class="section-description">
            Configuration for the reference query execution (typically the baseline for comparison).
          </p>

          <div class="config-options">
            <div class="config-item">
              <div class="config-label">
                <label>
                  <input type="checkbox" v-model="referenceConfig.yannakakis_enabled" />
                  Enable Yannakakis Optimization
                </label>
              </div>
              <p class="config-help">Main optimization flag for acyclic join optimization.</p>
            </div>

            <div class="config-item">
              <div class="config-label">
                <label>
                  <input type="checkbox" v-model="referenceConfig.physical_count_join_enabled" />
                  Enable Physical Count Join
                </label>
              </div>
              <p class="config-help">Use physical count join operators to avoid materialization.</p>
            </div>

            <div class="config-item">
              <div class="config-label">
                <label>
                  <input type="checkbox" v-model="referenceConfig.unguarded_enabled" />
                  Enable Unguarded Queries
                </label>
              </div>
              <p class="config-help">Support for unguarded aggregate queries.</p>
            </div>
          </div>

          <!-- Custom Options for Reference -->
          <div class="custom-options">
            <h4>Additional Configuration Options</h4>
            <p class="custom-help">Add custom Spark SQL configuration options for reference execution.</p>

            <div class="custom-option-list">
              <div v-for="(option, index) in referenceCustomOptions" :key="index" class="custom-option-item">
                <input
                  v-model="option.key"
                  type="text"
                  placeholder="spark.sql.example.option"
                  class="input input-sm"
                />
                <input
                  v-model="option.value"
                  type="text"
                  placeholder="value"
                  class="input input-sm"
                />
                <button @click="removeReferenceOption(index)" class="btn-icon-sm" title="Remove">×</button>
              </div>
            </div>

            <button @click="addReferenceOption" class="btn btn-secondary btn-sm">
              + Add Custom Option
            </button>
          </div>

          <div class="config-preview">
            <strong>Effective Spark Configuration:</strong>
            <pre>{{ getReferenceConfigPreview() }}</pre>
          </div>
        </div>

        <!-- Optimized Configuration -->
        <div class="config-section">
          <div class="section-header">
            <h2>Optimized Execution</h2>
            <span class="badge badge-optimized">Optimized</span>
          </div>
          <p class="section-description">
            Configuration for the optimized query execution (typically with optimizations enabled).
          </p>

          <div class="config-options">
            <div class="config-item">
              <div class="config-label">
                <label>
                  <input type="checkbox" v-model="optimizedConfig.yannakakis_enabled" />
                  Enable Yannakakis Optimization
                </label>
              </div>
              <p class="config-help">Main optimization flag for acyclic join optimization.</p>
            </div>

            <div class="config-item">
              <div class="config-label">
                <label>
                  <input type="checkbox" v-model="optimizedConfig.physical_count_join_enabled" />
                  Enable Physical Count Join
                </label>
              </div>
              <p class="config-help">Use physical count join operators to avoid materialization.</p>
            </div>

            <div class="config-item">
              <div class="config-label">
                <label>
                  <input type="checkbox" v-model="optimizedConfig.unguarded_enabled" />
                  Enable Unguarded Queries
                </label>
              </div>
              <p class="config-help">Support for unguarded aggregate queries.</p>
            </div>
          </div>

          <!-- Custom Options for Optimized -->
          <div class="custom-options">
            <h4>Additional Configuration Options</h4>
            <p class="custom-help">Add custom Spark SQL configuration options for optimized execution.</p>

            <div class="custom-option-list">
              <div v-for="(option, index) in optimizedCustomOptions" :key="index" class="custom-option-item">
                <input
                  v-model="option.key"
                  type="text"
                  placeholder="spark.sql.example.option"
                  class="input input-sm"
                />
                <input
                  v-model="option.value"
                  type="text"
                  placeholder="value"
                  class="input input-sm"
                />
                <button @click="removeOptimizedOption(index)" class="btn-icon-sm" title="Remove">×</button>
              </div>
            </div>

            <button @click="addOptimizedOption" class="btn btn-secondary btn-sm">
              + Add Custom Option
            </button>
          </div>

          <div class="config-preview">
            <strong>Effective Spark Configuration:</strong>
            <pre>{{ getOptimizedConfigPreview() }}</pre>
          </div>
        </div>

        <!-- Actions -->
        <div class="actions-section">
          <button @click="saveSettings" class="btn btn-primary">
            💾 Save Configuration
          </button>
          <button @click="resetSettings" class="btn btn-secondary">
            🔄 Reset to Defaults
          </button>
          <div v-if="saveMessage" :class="['save-message', saveMessage.type]">
            {{ saveMessage.text }}
          </div>
        </div>

        <!-- Help Section -->
        <div class="help-section">
          <h3>Configuration Details</h3>
          <div class="help-content">
            <div class="help-item">
              <h4>spark.sql.yannakakis.enabled</h4>
              <p>
                Enables the Yannakakis algorithm for acyclic join optimization. This is the main flag that
                activates the query optimization demonstrated in the VLDB 2025 paper "Avoiding Materialization
                for Guarded Aggregate Queries".
              </p>
            </div>
            <div class="help-item">
              <h4>spark.sql.yannakakis.physicalCountJoinEnabled</h4>
              <p>
                Enables the use of physical count join operators that avoid materializing intermediate join results.
                This optimization is particularly effective for acyclic queries with aggregates.
              </p>
            </div>
            <div class="help-item">
              <h4>spark.sql.yannakakis.unguardedEnabled</h4>
              <p>
                Extends the optimization to unguarded aggregate queries (queries without WHERE clauses on all tables).
                This allows the optimization to apply to a broader class of queries.
              </p>
            </div>
            <div class="help-item">
              <h4>Custom Configuration Options</h4>
              <p>
                You can add any additional Spark SQL configuration options. Common examples include:
                <code>spark.sql.adaptive.enabled</code>, <code>spark.sql.autoBroadcastJoinThreshold</code>,
                <code>spark.sql.shuffle.partitions</code>, etc.
              </p>
            </div>
            <div class="help-item">
              <h4>Default Configuration</h4>
              <p>
                By default, the reference execution has all optimizations disabled (for a baseline), and the
                optimized execution has all optimizations enabled. You can customize these settings to suit your needs.
              </p>
            </div>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, onMounted } from 'vue'

interface CustomOption {
  key: string
  value: string
}

interface SaveMessage {
  type: 'success' | 'error'
  text: string
}

interface SparkConfig {
  yannakakis_enabled: boolean
  physical_count_join_enabled: boolean
  unguarded_enabled: boolean
}

// Reference configuration state (default: all disabled)
const referenceConfig = ref<SparkConfig>({
  yannakakis_enabled: false,
  physical_count_join_enabled: false,
  unguarded_enabled: false
})

// Optimized configuration state (default: all enabled)
const optimizedConfig = ref<SparkConfig>({
  yannakakis_enabled: true,
  physical_count_join_enabled: true,
  unguarded_enabled: true
})

const referenceCustomOptions = ref<CustomOption[]>([])
const optimizedCustomOptions = ref<CustomOption[]>([])
const saveMessage = ref<SaveMessage | null>(null)

// Load settings from localStorage
function loadSettings() {
  const saved = localStorage.getItem('sparkSqlConfig')
  if (saved) {
    try {
      const parsed = JSON.parse(saved)

      // Load reference config
      if (parsed.referenceConfig) {
        referenceConfig.value = {
          yannakakis_enabled: parsed.referenceConfig.yannakakis_enabled ?? false,
          physical_count_join_enabled: parsed.referenceConfig.physical_count_join_enabled ?? false,
          unguarded_enabled: parsed.referenceConfig.unguarded_enabled ?? false
        }
      }

      // Load optimized config
      if (parsed.optimizedConfig) {
        optimizedConfig.value = {
          yannakakis_enabled: parsed.optimizedConfig.yannakakis_enabled ?? true,
          physical_count_join_enabled: parsed.optimizedConfig.physical_count_join_enabled ?? true,
          unguarded_enabled: parsed.optimizedConfig.unguarded_enabled ?? true
        }
      }

      // Load custom options
      referenceCustomOptions.value = parsed.referenceCustomOptions || []
      optimizedCustomOptions.value = parsed.optimizedCustomOptions || []
    } catch (e) {
      console.error('Failed to load settings:', e)
    }
  }
}

// Save settings to localStorage
function saveSettings() {
  const config = {
    referenceConfig: referenceConfig.value,
    optimizedConfig: optimizedConfig.value,
    referenceCustomOptions: referenceCustomOptions.value.filter(opt => opt.key && opt.value),
    optimizedCustomOptions: optimizedCustomOptions.value.filter(opt => opt.key && opt.value)
  }

  localStorage.setItem('sparkSqlConfig', JSON.stringify(config))

  saveMessage.value = {
    type: 'success',
    text: 'Configuration saved successfully! Changes will be applied to all query executions.'
  }

  setTimeout(() => {
    saveMessage.value = null
  }, 3000)
}

// Reset to defaults
function resetSettings() {
  referenceConfig.value = {
    yannakakis_enabled: false,
    physical_count_join_enabled: false,
    unguarded_enabled: false
  }

  optimizedConfig.value = {
    yannakakis_enabled: true,
    physical_count_join_enabled: true,
    unguarded_enabled: true
  }

  referenceCustomOptions.value = []
  optimizedCustomOptions.value = []

  localStorage.removeItem('sparkSqlConfig')

  saveMessage.value = {
    type: 'success',
    text: 'Configuration reset to defaults.'
  }

  setTimeout(() => {
    saveMessage.value = null
  }, 3000)
}

// Reference options management
function addReferenceOption() {
  referenceCustomOptions.value.push({ key: '', value: '' })
}

function removeReferenceOption(index: number) {
  referenceCustomOptions.value.splice(index, 1)
}

// Optimized options management
function addOptimizedOption() {
  optimizedCustomOptions.value.push({ key: '', value: '' })
}

function removeOptimizedOption(index: number) {
  optimizedCustomOptions.value.splice(index, 1)
}

// Get config preview
function getReferenceConfigPreview(): string {
  const lines = [
    `SET spark.sql.yannakakis.enabled = ${referenceConfig.value.yannakakis_enabled}`,
    `SET spark.sql.yannakakis.physicalCountJoinEnabled = ${referenceConfig.value.physical_count_join_enabled}`,
    `SET spark.sql.yannakakis.unguardedEnabled = ${referenceConfig.value.unguarded_enabled}`
  ]

  referenceCustomOptions.value
    .filter(opt => opt.key && opt.value)
    .forEach(opt => {
      lines.push(`SET ${opt.key} = ${opt.value}`)
    })

  return lines.join('\n')
}

function getOptimizedConfigPreview(): string {
  const lines = [
    `SET spark.sql.yannakakis.enabled = ${optimizedConfig.value.yannakakis_enabled}`,
    `SET spark.sql.yannakakis.physicalCountJoinEnabled = ${optimizedConfig.value.physical_count_join_enabled}`,
    `SET spark.sql.yannakakis.unguardedEnabled = ${optimizedConfig.value.unguarded_enabled}`
  ]

  optimizedCustomOptions.value
    .filter(opt => opt.key && opt.value)
    .forEach(opt => {
      lines.push(`SET ${opt.key} = ${opt.value}`)
    })

  return lines.join('\n')
}

onMounted(() => {
  loadSettings()
})
</script>

<style scoped>
.settings-page {
  max-width: 1000px;
  margin: 0 auto;
}

.page-header {
  margin-bottom: 2rem;
}

.page-header h1 {
  font-size: 2rem;
  color: var(--color-text);
  margin-bottom: 0.5rem;
}

.subtitle {
  font-size: 1rem;
  color: var(--color-text-secondary);
}

.settings-content {
  display: flex;
  flex-direction: column;
  gap: 2rem;
}

/* Info Banner */
.info-banner {
  display: flex;
  gap: 1rem;
  padding: 1rem 1.5rem;
  background: #EFF6FF;
  border: 1px solid #BFDBFE;
  border-radius: 8px;
  color: #1E40AF;
}

.info-icon {
  font-size: 1.5rem;
  flex-shrink: 0;
}

.info-text {
  flex: 1;
}

/* Config Sections */
.config-section {
  background: white;
  border: 1px solid var(--color-border);
  border-radius: 8px;
  padding: 1.5rem;
}

.section-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 0.5rem;
}

.section-header h2 {
  font-size: 1.5rem;
  color: var(--color-text);
  margin: 0;
}

.badge {
  padding: 0.25rem 0.75rem;
  border-radius: 4px;
  font-size: 0.875rem;
  font-weight: 600;
}

.badge-reference {
  background: #F3F4F6;
  color: #374151;
}

.badge-optimized {
  background: #D1FAE5;
  color: #065F46;
}

.section-description {
  color: var(--color-text-secondary);
  margin-bottom: 1.5rem;
  line-height: 1.6;
}

/* Config Options */
.config-options {
  display: flex;
  flex-direction: column;
  gap: 1rem;
  margin-bottom: 1.5rem;
}

.config-item {
  padding: 1rem;
  background: #F9FAFB;
  border: 1px solid #E5E7EB;
  border-radius: 6px;
}

.config-label {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 0.5rem;
}

.config-label label {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  font-weight: 600;
  color: var(--color-text);
  cursor: pointer;
}

.config-label input[type="checkbox"] {
  cursor: pointer;
  width: 18px;
  height: 18px;
}

.config-help {
  margin: 0;
  font-size: 0.875rem;
  color: var(--color-text-secondary);
  line-height: 1.5;
}

/* Custom Options */
.custom-options {
  margin-top: 1.5rem;
  padding-top: 1.5rem;
  border-top: 1px dashed var(--color-border);
}

.custom-options h4 {
  font-size: 1.125rem;
  color: var(--color-text);
  margin-bottom: 0.5rem;
}

.custom-help {
  font-size: 0.875rem;
  color: var(--color-text-secondary);
  margin-bottom: 1rem;
}

.custom-option-list {
  display: flex;
  flex-direction: column;
  gap: 0.75rem;
  margin-bottom: 1rem;
}

.custom-option-item {
  display: grid;
  grid-template-columns: 2fr 1fr auto;
  gap: 0.5rem;
  align-items: center;
}

.input-sm {
  padding: 0.5rem;
  font-size: 0.875rem;
}

.btn-icon-sm {
  width: 2rem;
  height: 2rem;
  display: flex;
  align-items: center;
  justify-content: center;
  background: #EF4444;
  color: white;
  border: none;
  border-radius: 4px;
  cursor: pointer;
  font-size: 1.5rem;
  line-height: 1;
  transition: background 0.2s;
}

.btn-icon-sm:hover {
  background: #DC2626;
}

.btn-sm {
  padding: 0.5rem 1rem;
  font-size: 0.875rem;
}

/* Config Preview */
.config-preview {
  background: #1F2937;
  color: #F9FAFB;
  padding: 1rem;
  border-radius: 6px;
  font-family: 'Courier New', monospace;
  margin-top: 1.5rem;
}

.config-preview strong {
  display: block;
  margin-bottom: 0.5rem;
  color: #D1D5DB;
}

.config-preview pre {
  margin: 0;
  font-size: 0.875rem;
  line-height: 1.6;
  color: #34D399;
  white-space: pre-wrap;
}

/* Actions */
.actions-section {
  display: flex;
  gap: 1rem;
  align-items: center;
  flex-wrap: wrap;
}

.save-message {
  padding: 0.75rem 1rem;
  border-radius: 6px;
  font-size: 0.875rem;
}

.save-message.success {
  background: #D1FAE5;
  color: #065F46;
  border: 1px solid #34D399;
}

.save-message.error {
  background: #FEE2E2;
  color: #991B1B;
  border: 1px solid #F87171;
}

/* Help Section */
.help-section {
  background: #F9FAFB;
  border: 1px solid var(--color-border);
  border-radius: 8px;
  padding: 1.5rem;
}

.help-section h3 {
  font-size: 1.25rem;
  color: var(--color-text);
  margin-bottom: 1rem;
}

.help-content {
  display: flex;
  flex-direction: column;
  gap: 1.5rem;
}

.help-item h4 {
  font-size: 1rem;
  color: var(--color-text);
  margin-bottom: 0.5rem;
  font-family: 'Courier New', monospace;
  background: #E5E7EB;
  padding: 0.25rem 0.5rem;
  border-radius: 4px;
  display: inline-block;
}

.help-item p {
  margin: 0;
  color: var(--color-text-secondary);
  line-height: 1.6;
}

.help-item code {
  background: #E5E7EB;
  padding: 0.125rem 0.375rem;
  border-radius: 3px;
  font-family: 'Courier New', monospace;
  font-size: 0.875rem;
}

/* Responsive */
@media (max-width: 768px) {
  .page-header h1 {
    font-size: 1.5rem;
  }

  .section-header {
    flex-direction: column;
    align-items: flex-start;
    gap: 0.5rem;
  }

  .config-label {
    flex-direction: column;
    align-items: flex-start;
    gap: 0.5rem;
  }

  .custom-option-item {
    grid-template-columns: 1fr;
  }

  .actions-section {
    flex-direction: column;
    align-items: stretch;
  }
}
</style>
