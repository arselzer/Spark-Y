<template>
  <div class="fk-map-editor">
    <div class="editor-header">
      <h3>Foreign Key Map Editor</h3>
      <p>Define custom foreign key relationships for databases without explicit FK constraints</p>
    </div>

    <!-- FK Map Name and Description -->
    <div class="map-info-section">
      <div class="form-group">
        <label for="fk-map-name">Map Name *</label>
        <input
          id="fk-map-name"
          v-model="fkMapName"
          type="text"
          placeholder="e.g., My Custom IMDB FKs"
          class="input"
          required
        />
      </div>

      <div class="form-group">
        <label for="fk-map-description">Description (optional)</label>
        <textarea
          id="fk-map-description"
          v-model="fkMapDescription"
          placeholder="Describe the purpose of this FK map..."
          class="textarea"
          rows="2"
        ></textarea>
      </div>
    </div>

    <!-- Add New FK Relationship -->
    <div class="add-fk-section">
      <h4>Add Foreign Key Relationship</h4>

      <div class="fk-relationship-form">
        <div class="form-row">
          <div class="form-group">
            <label>Source Table</label>
            <select v-model="newFK.sourceTable" class="select" @change="loadSourceColumns">
              <option value="">Select table...</option>
              <option v-for="table in availableTables" :key="table.name" :value="table.name">
                {{ table.name }}
              </option>
            </select>
          </div>

          <div class="form-group">
            <label>Source Column</label>
            <select v-model="newFK.sourceColumn" class="select" :disabled="!newFK.sourceTable">
              <option value="">Select column...</option>
              <option v-for="column in sourceColumns" :key="column" :value="column">
                {{ column }}
              </option>
            </select>
          </div>

          <div class="arrow-icon">→</div>

          <div class="form-group">
            <label>Target Table</label>
            <select v-model="newFK.targetTable" class="select" @change="loadTargetColumns">
              <option value="">Select table...</option>
              <option v-for="table in availableTables" :key="table.name" :value="table.name">
                {{ table.name }}
              </option>
            </select>
          </div>

          <div class="form-group">
            <label>Target Column</label>
            <select v-model="newFK.targetColumn" class="select" :disabled="!newFK.targetTable">
              <option value="">Select column...</option>
              <option v-for="column in targetColumns" :key="column" :value="column">
                {{ column }}
              </option>
            </select>
          </div>

          <button
            @click="addRelationship"
            class="btn btn-primary"
            :disabled="!canAddRelationship"
            title="Add this FK relationship"
          >
            Add
          </button>
        </div>
      </div>
    </div>

    <!-- Current FK Relationships -->
    <div class="relationships-section">
      <div class="section-header">
        <h4>Foreign Key Relationships ({{ relationships.length }})</h4>
        <button
          v-if="relationships.length > 0"
          @click="clearAllRelationships"
          class="btn btn-secondary btn-sm"
        >
          Clear All
        </button>
      </div>

      <div v-if="relationships.length === 0" class="no-relationships">
        No foreign key relationships defined yet. Add one above to get started.
      </div>

      <div v-else class="relationships-list">
        <div
          v-for="(rel, index) in relationships"
          :key="index"
          class="relationship-item"
        >
          <div class="relationship-content">
            <span class="table-name">{{ rel.source_table }}</span>
            <span class="column-name">{{ rel.source_column }}</span>
            <span class="arrow">→</span>
            <span class="table-name">{{ rel.target_table }}</span>
            <span class="column-name">{{ rel.target_column }}</span>
          </div>
          <button
            @click="removeRelationship(index)"
            class="btn btn-danger btn-sm"
            title="Remove this relationship"
          >
            Remove
          </button>
        </div>
      </div>
    </div>

    <!-- Actions -->
    <div class="editor-actions">
      <button
        @click="saveFKMap"
        class="btn btn-primary btn-large"
        :disabled="!canSave || saving"
      >
        {{ saving ? 'Saving...' : 'Save FK Map' }}
      </button>
      <button @click="cancelEdit" class="btn btn-secondary btn-large">
        Cancel
      </button>
    </div>

    <div v-if="saveResult" class="result-message" :class="saveResult.success ? 'success' : 'error'">
      <strong>{{ saveResult.success ? '✓' : '✗' }}</strong>
      {{ saveResult.message }}
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, watch } from 'vue'
import axios from 'axios'

interface TableInfo {
  name: string
  database: string | null
  description: string | null
  tableType: string
  isTemporary: boolean
}

interface FKRelationship {
  source_table: string
  source_column: string
  target_table: string
  target_column: string
}

interface Props {
  availableTables: TableInfo[]
}

interface Emits {
  (e: 'saved', mapName: string): void
  (e: 'cancel'): void
}

const props = defineProps<Props>()
const emit = defineEmits<Emits>()

const API_BASE_URL = import.meta.env.VITE_API_URL || '/api'

// FK Map metadata
const fkMapName = ref('')
const fkMapDescription = ref('')

// New FK relationship being added
const newFK = ref({
  sourceTable: '',
  sourceColumn: '',
  targetTable: '',
  targetColumn: ''
})

// Columns for selected tables
const sourceColumns = ref<string[]>([])
const targetColumns = ref<string[]>([])

// List of FK relationships
const relationships = ref<FKRelationship[]>([])

// State
const saving = ref(false)
const saveResult = ref<{ success: boolean; message: string } | null>(null)

// Computed properties
const canAddRelationship = computed(() => {
  return newFK.value.sourceTable &&
    newFK.value.sourceColumn &&
    newFK.value.targetTable &&
    newFK.value.targetColumn
})

const canSave = computed(() => {
  return fkMapName.value.trim().length > 0 && relationships.value.length > 0
})

// Load columns for a table
async function loadTableColumns(tableName: string): Promise<string[]> {
  try {
    const response = await axios.post(
      `${API_BASE_URL}/data-import/postgres/tables/${tableName}/columns`,
      null,
      {
        params: {
          host: 'postgres',
          port: 5432,
          database: 'imdb',
          username: 'imdb',
          password: 'imdb'
        }
      }
    )
    return response.data.columns || []
  } catch (error) {
    console.error(`Failed to load columns for table ${tableName}:`, error)
    return []
  }
}

async function loadSourceColumns() {
  if (newFK.value.sourceTable) {
    sourceColumns.value = await loadTableColumns(newFK.value.sourceTable)
    newFK.value.sourceColumn = '' // Reset selection
  } else {
    sourceColumns.value = []
  }
}

async function loadTargetColumns() {
  if (newFK.value.targetTable) {
    targetColumns.value = await loadTableColumns(newFK.value.targetTable)
    newFK.value.targetColumn = '' // Reset selection
  } else {
    targetColumns.value = []
  }
}

function addRelationship() {
  if (!canAddRelationship.value) return

  const relationship: FKRelationship = {
    source_table: newFK.value.sourceTable,
    source_column: newFK.value.sourceColumn,
    target_table: newFK.value.targetTable,
    target_column: newFK.value.targetColumn
  }

  relationships.value.push(relationship)

  // Reset form
  newFK.value = {
    sourceTable: '',
    sourceColumn: '',
    targetTable: '',
    targetColumn: ''
  }
  sourceColumns.value = []
  targetColumns.value = []
}

function removeRelationship(index: number) {
  relationships.value.splice(index, 1)
}

function clearAllRelationships() {
  if (confirm('Are you sure you want to remove all foreign key relationships?')) {
    relationships.value = []
  }
}

async function saveFKMap() {
  if (!canSave.value) return

  saving.value = true
  saveResult.value = null

  try {
    const response = await axios.post(`${API_BASE_URL}/data-import/fk-maps`, {
      name: fkMapName.value.trim(),
      description: fkMapDescription.value.trim() || null,
      relationships: relationships.value
    })

    saveResult.value = {
      success: true,
      message: response.data.message || 'FK map saved successfully'
    }

    // Emit saved event
    emit('saved', fkMapName.value)

    // Reset form after successful save
    setTimeout(() => {
      resetForm()
    }, 1500)
  } catch (error: any) {
    saveResult.value = {
      success: false,
      message: error.response?.data?.detail || error.message || 'Failed to save FK map'
    }
  } finally {
    saving.value = false
  }
}

function cancelEdit() {
  if (relationships.value.length > 0) {
    if (!confirm('Are you sure you want to cancel? All unsaved changes will be lost.')) {
      return
    }
  }
  emit('cancel')
}

function resetForm() {
  fkMapName.value = ''
  fkMapDescription.value = ''
  relationships.value = []
  newFK.value = {
    sourceTable: '',
    sourceColumn: '',
    targetTable: '',
    targetColumn: ''
  }
  sourceColumns.value = []
  targetColumns.value = []
  saveResult.value = null
}
</script>

<style scoped>
.fk-map-editor {
  background: white;
  border-radius: 0.5rem;
  padding: 1.5rem;
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
}

.editor-header {
  margin-bottom: 1.5rem;
}

.editor-header h3 {
  margin: 0 0 0.5rem 0;
  color: var(--color-text);
}

.editor-header p {
  margin: 0;
  color: var(--color-text-secondary);
  font-size: 0.875rem;
}

.map-info-section {
  margin-bottom: 2rem;
  padding-bottom: 1.5rem;
  border-bottom: 1px solid var(--color-border);
}

.add-fk-section {
  margin-bottom: 2rem;
  padding-bottom: 1.5rem;
  border-bottom: 1px solid var(--color-border);
}

.add-fk-section h4 {
  margin: 0 0 1rem 0;
  color: var(--color-text);
  font-size: 1.125rem;
}

.fk-relationship-form {
  background: var(--color-bg-secondary);
  padding: 1rem;
  border-radius: 0.375rem;
}

.form-row {
  display: flex;
  align-items: flex-end;
  gap: 1rem;
}

.form-group {
  flex: 1;
  min-width: 0;
}

.form-group label {
  display: block;
  margin-bottom: 0.5rem;
  font-weight: 500;
  color: var(--color-text);
  font-size: 0.875rem;
}

.input,
.textarea,
.select {
  width: 100%;
  padding: 0.5rem;
  border: 1px solid var(--color-border);
  border-radius: 0.25rem;
  font-size: 0.875rem;
}

.textarea {
  resize: vertical;
}

.select:disabled {
  background-color: var(--color-bg-secondary);
  cursor: not-allowed;
}

.arrow-icon {
  font-size: 1.5rem;
  color: var(--color-text-secondary);
  padding-bottom: 0.5rem;
}

.relationships-section {
  margin-bottom: 2rem;
}

.section-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 1rem;
}

.section-header h4 {
  margin: 0;
  color: var(--color-text);
  font-size: 1.125rem;
}

.no-relationships {
  padding: 2rem;
  text-align: center;
  color: var(--color-text-secondary);
  background: var(--color-bg-secondary);
  border-radius: 0.375rem;
}

.relationships-list {
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
}

.relationship-item {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 0.75rem 1rem;
  background: var(--color-bg-secondary);
  border: 1px solid var(--color-border);
  border-radius: 0.375rem;
}

.relationship-content {
  display: flex;
  align-items: center;
  gap: 0.75rem;
  flex: 1;
}

.table-name {
  font-family: monospace;
  font-weight: 600;
  color: var(--color-text);
  font-size: 0.875rem;
}

.column-name {
  font-family: monospace;
  color: var(--color-text-secondary);
  font-size: 0.875rem;
}

.arrow {
  color: var(--color-text-secondary);
  font-size: 1.25rem;
}

.editor-actions {
  display: flex;
  gap: 1rem;
  justify-content: flex-end;
  margin-bottom: 1rem;
}

.btn {
  padding: 0.5rem 1rem;
  border: none;
  border-radius: 0.375rem;
  font-size: 0.875rem;
  font-weight: 500;
  cursor: pointer;
  transition: all 0.2s;
}

.btn:disabled {
  opacity: 0.5;
  cursor: not-allowed;
}

.btn-primary {
  background-color: #3b82f6;
  color: white;
}

.btn-primary:hover:not(:disabled) {
  background-color: #2563eb;
}

.btn-secondary {
  background-color: #6b7280;
  color: white;
}

.btn-secondary:hover:not(:disabled) {
  background-color: #4b5563;
}

.btn-danger {
  background-color: #ef4444;
  color: white;
}

.btn-danger:hover {
  background-color: #dc2626;
}

.btn-sm {
  padding: 0.375rem 0.75rem;
  font-size: 0.75rem;
}

.btn-large {
  padding: 0.75rem 1.5rem;
  font-size: 1rem;
}

.result-message {
  padding: 1rem;
  border-radius: 0.375rem;
  margin-top: 1rem;
}

.result-message.success {
  background-color: #dcfce7;
  color: #166534;
  border: 1px solid #86efac;
}

.result-message.error {
  background-color: #fee2e2;
  color: #991b1b;
  border: 1px solid #fca5a5;
}

/* Responsive */
@media (max-width: 768px) {
  .form-row {
    flex-direction: column;
    align-items: stretch;
  }

  .arrow-icon {
    text-align: center;
    padding: 0;
  }

  .relationship-item {
    flex-direction: column;
    align-items: flex-start;
    gap: 0.75rem;
  }

  .editor-actions {
    flex-direction: column;
  }
}
</style>
