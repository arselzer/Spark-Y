<template>
  <div class="container">
    <div class="data-import">
      <h2>Data Import</h2>
      <p class="description">Import data from PostgreSQL into Spark SQL for query execution</p>

      <div class="import-section">
        <div class="config-card">
          <h3>PostgreSQL Connection</h3>

          <form @submit.prevent="testConnection" class="connection-form">
            <div class="form-group">
              <label for="host">Host</label>
              <input
                id="host"
                v-model="config.host"
                type="text"
                class="input"
                placeholder="postgres"
              />
              <div class="host-presets">
                <span class="preset-label">Quick set:</span>
                <button
                  type="button"
                  @click="config.host = 'postgres'"
                  class="preset-btn"
                  title="Use Docker PostgreSQL container"
                >
                  Docker Postgres
                </button>
                <button
                  type="button"
                  @click="config.host = 'host.local.db'"
                  class="preset-btn"
                  title="Connect to database on host machine (Linux)"
                >
                  Host DB (Linux)
                </button>
                <button
                  type="button"
                  @click="config.host = 'host.docker.internal'"
                  class="preset-btn"
                  title="Connect to database on host machine (Mac/Windows)"
                >
                  Host DB (Mac/Win)
                </button>
              </div>
            </div>

            <div class="form-row">
              <div class="form-group">
                <label for="port">Port</label>
                <input
                  id="port"
                  v-model.number="config.port"
                  type="number"
                  class="input"
                  placeholder="5432"
                />
              </div>

              <div class="form-group">
                <label for="database">Database</label>
                <div class="database-input-group">
                  <input
                    id="database"
                    v-model="config.database"
                    type="text"
                    class="input"
                    placeholder="imdb"
                    list="database-list"
                  />
                  <datalist id="database-list">
                    <option v-for="db in availableDatabases" :key="db.name" :value="db.name">
                      {{ db.name }} ({{ db.size }}, owner: {{ db.owner }})
                    </option>
                  </datalist>
                  <button
                    v-if="config.username === 'postgres'"
                    type="button"
                    @click="showCreateDatabase = true"
                    class="btn-icon"
                    title="Create new database (requires admin credentials)"
                  >
                    +
                  </button>
                </div>
              </div>
            </div>

            <div class="form-row">
              <div class="form-group">
                <label for="username">Username</label>
                <input
                  id="username"
                  v-model="config.username"
                  type="text"
                  class="input"
                  placeholder="imdb"
                />
                <div class="user-presets">
                  <span class="preset-label">Quick set:</span>
                  <button
                    type="button"
                    @click="config.username = 'imdb'; config.password = 'imdb'"
                    class="preset-btn"
                    title="Use regular imdb user credentials"
                  >
                    User (imdb)
                  </button>
                  <button
                    type="button"
                    @click="config.username = 'postgres'; config.password = 'postgres'"
                    class="preset-btn"
                    title="Use admin postgres credentials (for database management)"
                  >
                    Admin (postgres)
                  </button>
                </div>
              </div>

              <div class="form-group">
                <label for="password">Password</label>
                <input
                  id="password"
                  v-model="config.password"
                  type="password"
                  class="input"
                  placeholder="imdb"
                />
              </div>
            </div>

            <div class="button-group">
              <button type="submit" class="btn btn-secondary" :disabled="testing">
                {{ testing ? 'Testing...' : 'Test Connection' }}
              </button>
              <button
                type="button"
                @click="resetToDefaults"
                class="btn btn-secondary"
              >
                Reset to Defaults
              </button>
            </div>
          </form>

          <div v-if="connectionResult" class="result-message" :class="connectionResult.success ? 'success' : 'error'">
            <strong>{{ connectionResult.success ? '✓' : '✗' }}</strong>
            {{ connectionResult.message }}
            <span v-if="connectionResult.total_tables">
              ({{ connectionResult.total_tables }} tables found)
            </span>
          </div>
        </div>

        <div class="import-card">
          <h3>Import Data</h3>
          <p>Import all tables from the public schema into Spark SQL</p>

          <button
            @click="importData"
            class="btn btn-primary btn-large"
            :disabled="importing"
          >
            {{ importing ? 'Importing...' : 'Import Data from PostgreSQL' }}
          </button>

          <div v-if="importing" class="loading-section">
            <div class="spinner"></div>
            <p>Importing tables... This may take a few minutes.</p>
          </div>

          <div v-if="importResult" class="import-result">
            <div class="result-header" :class="importResult.success ? 'success' : 'error'">
              <h4>{{ importResult.success ? '✓ Import Complete' : '✗ Import Failed' }}</h4>
              <p>{{ importResult.message }}</p>
            </div>

            <div v-if="importResult.tables_imported.length > 0" class="tables-list">
              <h4>Imported Tables ({{ importResult.tables_imported.length }})</h4>
              <ul>
                <li v-for="table in importResult.tables_imported" :key="table">
                  {{ table }}
                </li>
              </ul>
            </div>

            <div v-if="importResult.errors.length > 0" class="errors-list">
              <h4>Errors ({{ importResult.errors.length }})</h4>
              <ul>
                <li v-for="(error, idx) in importResult.errors" :key="idx">
                  {{ error }}
                </li>
              </ul>
            </div>
          </div>
        </div>

        <div class="export-card">
          <h3>Export to SQL Dump</h3>
          <p>Create a demo database SQL file (samples large tables)</p>

          <div class="form-group">
            <label for="max-rows">Max Rows Per Table</label>
            <input
              id="max-rows"
              v-model.number="maxRows"
              type="number"
              class="input"
              placeholder="10000"
              min="1"
              max="100000"
            />
            <small class="help-text">
              Tables with more than this many rows will be randomly sampled
            </small>
          </div>

          <div class="form-group">
            <label class="checkbox-label">
              <input type="checkbox" v-model="saveToLibrary" />
              Save to SQL Dumps Library
            </label>
            <small class="help-text">
              Also save the dump to the library for easy reloading later
            </small>
          </div>

          <div class="form-group">
            <label class="checkbox-label">
              <input type="checkbox" v-model="smartSampling" />
              Smart FK-Aware Sampling
            </label>
            <small class="help-text">
              Use foreign key relationships to prioritize rows with matching references. Creates more realistic demo databases with meaningful join results.
            </small>
          </div>

          <div v-if="smartSampling" class="form-group">
            <div class="fk-map-options">
              <label class="radio-label">
                <input type="radio" v-model="fkMapSource" value="imdb" name="fk-source" />
                Use IMDB Foreign Keys
              </label>
              <label class="radio-label">
                <input type="radio" v-model="fkMapSource" value="custom" name="fk-source" />
                Use Custom FK Map
              </label>
              <label class="radio-label">
                <input type="radio" v-model="fkMapSource" value="none" name="fk-source" />
                No FK Map (Random Sampling)
              </label>
            </div>
            <small class="help-text">
              Choose which foreign key map to use for smart sampling. IMDB FK map covers common JOB benchmark tables.
            </small>
          </div>

          <div v-if="smartSampling && fkMapSource === 'custom'" class="custom-fk-section">
            <div class="custom-fk-header">
              <strong>Custom FK Map</strong>
              <button @click="showFKEditor = true" class="btn btn-secondary btn-sm">
                Create New FK Map
              </button>
            </div>

            <div v-if="loadingFKMaps" class="loading-section">
              <div class="spinner"></div>
              <p>Loading FK maps...</p>
            </div>

            <div v-else-if="availableFKMaps.length === 0" class="no-fk-maps">
              No custom FK maps available. Create one using the button above.
            </div>

            <div v-else class="fk-maps-list">
              <label
                v-for="map in availableFKMaps"
                :key="map.filename"
                class="fk-map-item"
                :class="{ selected: selectedFKMap === map.filename }"
              >
                <input
                  type="radio"
                  v-model="selectedFKMap"
                  :value="map.filename"
                  name="fk-map"
                />
                <div class="fk-map-info">
                  <strong>{{ map.name }}</strong>
                  <div class="fk-map-meta">
                    <span class="fk-count">{{ map.relationship_count }} relationships</span>
                    <span class="fk-date">{{ formatDate(map.modified) }}</span>
                  </div>
                  <p v-if="map.description" class="fk-description">{{ map.description }}</p>
                </div>
                <button
                  @click.prevent="deleteFKMap(map.filename)"
                  class="btn btn-danger btn-sm"
                  title="Delete this FK map"
                >
                  Delete
                </button>
              </label>
            </div>
          </div>

          <div class="form-group">
            <label class="checkbox-label">
              <input type="checkbox" v-model="compressExport" />
              Compress Export (gzip)
            </label>
            <small class="help-text">
              Compress the SQL dump using gzip to save disk space (typically 5-10x smaller file size)
            </small>
          </div>

          <div class="form-group">
            <label class="checkbox-label">
              <input type="checkbox" v-model="selectSpecificTables" />
              Select Specific Tables
            </label>
            <small class="help-text">
              Choose which tables to include in the export (default: export all tables)
            </small>
          </div>

          <div v-if="selectSpecificTables" class="table-selection-panel">
            <div class="table-selection-header">
              <strong>Select Tables to Export ({{ selectedExportTables.size }} selected)</strong>
              <div class="table-selection-actions">
                <button @click="selectAllExportTables" class="btn-link">Select All</button>
                <button @click="clearAllExportTables" class="btn-link">Clear All</button>
              </div>
            </div>
            <div v-if="loadingTables" class="loading-section">
              <div class="spinner"></div>
              <p>Loading tables...</p>
            </div>
            <div v-else-if="availableTables.length === 0" class="no-tables">
              No tables available. Import data first.
            </div>
            <div v-else class="export-tables-grid">
              <label
                v-for="table in availableTables"
                :key="table.name"
                class="export-table-item"
              >
                <input
                  type="checkbox"
                  :checked="selectedExportTables.has(table.name)"
                  @change="toggleExportTable(table.name)"
                />
                <span class="table-name">{{ table.name }}</span>
                <span class="table-type-small">{{ table.tableType }}</span>
              </label>
            </div>
          </div>

          <button
            @click="exportData"
            class="btn btn-primary btn-large"
            :disabled="exporting || (selectSpecificTables && selectedExportTables.size === 0)"
          >
            {{ exporting ? 'Exporting...' : 'Export Database to SQL File' }}
          </button>

          <div v-if="exporting" class="loading-section">
            <div class="spinner"></div>
            <p>Generating SQL dump... This may take a few minutes for large databases.</p>
          </div>

          <div v-if="exportResult" class="result-message" :class="exportResult.success ? 'success' : 'error'">
            <strong>{{ exportResult.success ? '✓' : '✗' }}</strong>
            {{ exportResult.message }}
          </div>
        </div>

        <div class="import-sql-card">
          <h3>Import from SQL Dump</h3>
          <p>Upload and execute a SQL dump file</p>

          <div class="form-group">
            <label for="sql-file">SQL File (supports .sql and .sql.gz)</label>
            <input
              id="sql-file"
              ref="fileInput"
              type="file"
              accept=".sql,.sql.gz,.gz"
              @change="handleFileSelect"
              class="file-input"
            />
            <small v-if="selectedFile" class="selected-file">
              Selected: {{ selectedFile.name }} ({{ formatFileSize(selectedFile.size) }})
            </small>
          </div>

          <button
            @click="importSql"
            class="btn btn-primary btn-large"
            :disabled="importingSql || !selectedFile"
          >
            {{ importingSql ? 'Importing...' : 'Execute SQL Dump' }}
          </button>

          <div v-if="importingSql" class="loading-section">
            <div class="spinner"></div>
            <p>Executing SQL statements... This may take a few minutes.</p>
          </div>

          <div v-if="importSqlResult" class="import-result">
            <div class="result-header" :class="importSqlResult.success ? 'success' : 'error'">
              <h4>{{ importSqlResult.success ? '✓ Import Complete' : '✗ Import Failed' }}</h4>
              <p>{{ importSqlResult.message }}</p>
            </div>

            <div v-if="importSqlResult.executed_statements" class="stats-info">
              <p><strong>Executed:</strong> {{ importSqlResult.executed_statements }} SQL statements</p>
              <p><strong>Imported to Spark:</strong> {{ importSqlResult.tables_imported_to_spark }} tables</p>
            </div>

            <div v-if="importSqlResult.errors && importSqlResult.errors.length > 0" class="errors-list">
              <h4>Errors ({{ importSqlResult.errors.length }})</h4>
              <ul>
                <li v-for="(error, idx) in importSqlResult.errors" :key="idx">
                  {{ error }}
                </li>
              </ul>
            </div>
          </div>
        </div>

        <div class="sql-library-card">
          <h3>SQL Dumps Library</h3>
          <p>Load previously saved SQL dumps</p>

          <div class="library-header">
            <button @click="loadDumpsList" class="btn btn-secondary" :disabled="loadingDumps">
              {{ loadingDumps ? 'Loading...' : 'Refresh List' }}
            </button>
          </div>

          <div v-if="loadingDumps" class="loading-section">
            <div class="spinner"></div>
          </div>

          <div v-else-if="availableDumps.length > 0" class="dumps-grid">
            <div v-for="dump in availableDumps" :key="dump.filename" class="dump-item">
              <div class="dump-info">
                <strong>{{ dump.filename }}</strong>
                <div class="dump-meta">
                  <span class="dump-size">{{ formatFileSize(dump.size) }}</span>
                  <span class="dump-date">{{ formatDate(dump.modified) }}</span>
                  <span v-if="dump.compressed" class="badge badge-info">Compressed</span>
                </div>
              </div>
              <div class="dump-actions">
                <button
                  @click="previewDumpMetadata(dump.filename)"
                  class="btn btn-secondary btn-sm"
                  :disabled="loadingMetadata === dump.filename"
                >
                  {{ loadingMetadata === dump.filename ? 'Loading...' : 'Preview' }}
                </button>
                <button
                  @click="loadDumpFromLibrary(dump.filename)"
                  class="btn btn-primary btn-sm"
                  :disabled="loadingDump"
                >
                  Load
                </button>
                <button
                  @click="downloadDumpFromLibrary(dump.filename)"
                  class="btn btn-secondary btn-sm"
                >
                  Download
                </button>
              </div>
            </div>
          </div>

          <div v-else-if="!loadingDumps" class="no-dumps">
            No SQL dumps in library. Export a database with "Save to Library" checked to create one.
          </div>

          <div v-if="loadDumpResult" class="import-result">
            <div class="result-header" :class="loadDumpResult.success ? 'success' : 'error'">
              <h4>{{ loadDumpResult.success ? '✓ Load Complete' : '✗ Load Failed' }}</h4>
              <p>{{ loadDumpResult.message }}</p>
            </div>

            <div v-if="loadDumpResult.executed_statements" class="stats-info">
              <p><strong>Executed:</strong> {{ loadDumpResult.executed_statements }} SQL statements</p>
              <p><strong>Imported to Spark:</strong> {{ loadDumpResult.tables_imported_to_spark }} tables</p>
            </div>

            <div v-if="loadDumpResult.errors && loadDumpResult.errors.length > 0" class="errors-list">
              <h4>Errors ({{ loadDumpResult.errors.length }})</h4>
              <ul>
                <li v-for="(error, idx) in loadDumpResult.errors" :key="idx">
                  {{ error }}
                </li>
              </ul>
            </div>
          </div>
        </div>

        <!-- SQL Dump Preview Modal -->
        <div v-if="previewedDump" class="modal-overlay" @click="closePreview">
          <div class="modal-content" @click.stop>
            <div class="modal-header">
              <h3>SQL Dump Preview</h3>
              <button @click="closePreview" class="btn-close">&times;</button>
            </div>

            <div class="modal-body">
              <div class="preview-file-info">
                <h4>File Information</h4>
                <div class="info-grid">
                  <div class="info-item">
                    <span class="info-label">Filename:</span>
                    <span class="info-value">{{ previewedDump.file.filename }}</span>
                  </div>
                  <div class="info-item">
                    <span class="info-label">{{ previewedDump.file.compressed ? 'Compressed Size:' : 'Size:' }}</span>
                    <span class="info-value">{{ previewedDump.file.size_mb }} MB ({{ formatFileSize(previewedDump.file.size) }})</span>
                  </div>
                  <div v-if="previewedDump.file.compressed" class="info-item">
                    <span class="info-label">Uncompressed Size:</span>
                    <span class="info-value">{{ previewedDump.file.uncompressed_size_mb }} MB ({{ formatFileSize(previewedDump.file.uncompressed_size) }})</span>
                  </div>
                  <div v-if="previewedDump.file.compressed" class="info-item">
                    <span class="info-label">Compression Ratio:</span>
                    <span class="info-value">{{ (previewedDump.file.compression_ratio * 100).toFixed(1) }}% ({{ (1 / previewedDump.file.compression_ratio).toFixed(1) }}x smaller)</span>
                  </div>
                  <div class="info-item">
                    <span class="info-label">Modified:</span>
                    <span class="info-value">{{ formatDate(previewedDump.file.modified) }}</span>
                  </div>
                </div>
              </div>

              <div class="preview-summary">
                <h4>Summary</h4>
                <div class="info-grid">
                  <div class="info-item">
                    <span class="info-label">Total Tables:</span>
                    <span class="info-value">{{ previewedDump.summary.total_tables }}</span>
                  </div>
                  <div class="info-item">
                    <span class="info-label">Total Rows:</span>
                    <span class="info-value">{{ previewedDump.summary.total_rows.toLocaleString() }}</span>
                  </div>
                  <div class="info-item">
                    <span class="info-label">Total Statements:</span>
                    <span class="info-value">{{ previewedDump.summary.total_statements.toLocaleString() }}</span>
                  </div>
                </div>
              </div>

              <div class="preview-tables">
                <h4>Tables ({{ previewedDump.tables.length }})</h4>
                <div class="tables-list-preview">
                  <table class="preview-table">
                    <thead>
                      <tr>
                        <th>Table Name</th>
                        <th>Row Count</th>
                        <th>Has Schema</th>
                      </tr>
                    </thead>
                    <tbody>
                      <tr v-for="table in previewedDump.tables" :key="table.name">
                        <td class="table-name-cell">{{ table.name }}</td>
                        <td class="row-count-cell">{{ table.row_count.toLocaleString() }}</td>
                        <td class="schema-cell">
                          <span :class="['schema-badge', table.has_create_statement ? 'yes' : 'no']">
                            {{ table.has_create_statement ? 'Yes' : 'No' }}
                          </span>
                        </td>
                      </tr>
                    </tbody>
                  </table>
                </div>
              </div>
            </div>

            <div class="modal-footer">
              <button @click="closePreview" class="btn btn-secondary">Close</button>
              <button @click="loadDumpFromPreview" class="btn btn-primary" :disabled="loadingDump">
                {{ loadingDump ? 'Loading...' : 'Load This Dump' }}
              </button>
            </div>
          </div>
        </div>

        <!-- FK Map Editor Modal -->
        <div v-if="showFKEditor" class="modal-overlay" @click="closeFKEditor">
          <div class="modal-content fk-editor-modal" @click.stop>
            <FKMapEditor
              :available-tables="availableTables"
              @saved="handleFKMapSaved"
              @cancel="closeFKEditor"
            />
          </div>
        </div>

        <!-- Create Database Modal -->
        <div v-if="showCreateDatabase" class="modal-overlay" @click="showCreateDatabase = false">
          <div class="modal-content create-database-modal" @click.stop>
            <h3>Create New Database</h3>
            <p class="modal-description">Create a new PostgreSQL database. Requires admin credentials (postgres user).</p>

            <form @submit.prevent="createDatabase" class="create-db-form">
              <div class="form-group">
                <label for="new-database-name">Database Name *</label>
                <input
                  id="new-database-name"
                  v-model="newDatabaseName"
                  type="text"
                  class="input"
                  placeholder="my_database"
                  required
                  pattern="[a-zA-Z0-9_]+"
                  title="Only alphanumeric characters and underscores allowed"
                />
                <small class="form-help">Only alphanumeric characters and underscores allowed</small>
              </div>

              <div class="form-group">
                <label>Owner</label>
                <input
                  type="text"
                  class="input"
                  value="imdb"
                  disabled
                />
                <small class="form-help">New database will be owned by the 'imdb' user</small>
              </div>

              <div v-if="createDatabaseResult" :class="['result-message', createDatabaseResult.success ? 'success' : 'error']">
                {{ createDatabaseResult.message }}
              </div>

              <div class="button-group">
                <button type="submit" class="btn btn-primary" :disabled="creatingDatabase || !newDatabaseName.trim()">
                  {{ creatingDatabase ? 'Creating...' : 'Create Database' }}
                </button>
                <button type="button" class="btn btn-secondary" @click="showCreateDatabase = false" :disabled="creatingDatabase">
                  Cancel
                </button>
              </div>
            </form>
          </div>
        </div>

        <div class="tables-card">
          <h3>Available Tables in Spark</h3>

          <div class="tables-controls">
            <input
              v-model="tableSearchQuery"
              type="text"
              placeholder="Search tables..."
              class="input search-input"
            />
            <button @click="loadTables" class="btn btn-secondary" :disabled="loadingTables">
              {{ loadingTables ? 'Loading...' : 'Refresh' }}
            </button>
            <button
              v-if="filteredTables.length > 0"
              @click="deleteSelectedTables"
              class="btn btn-danger"
              :disabled="selectedTables.size === 0 || deletingTables"
            >
              {{ deletingTables ? 'Deleting...' : `Delete Selected (${selectedTables.size})` }}
            </button>
          </div>

          <div v-if="loadingTables" class="loading-section">
            <div class="spinner"></div>
          </div>

          <div v-else-if="availableTables.length > 0">
            <div v-if="filteredTables.length === 0" class="no-results">
              No tables match "{{ tableSearchQuery }}"
            </div>
            <div v-else>
              <div class="select-all-row">
                <label class="checkbox-label">
                  <input
                    type="checkbox"
                    :checked="isAllFilteredSelected"
                    @change="toggleSelectAll"
                  />
                  Select All ({{ filteredTables.length }})
                </label>
              </div>
              <div class="tables-grid">
                <div v-for="table in filteredTables" :key="table.name" class="table-item">
                  <input
                    type="checkbox"
                    :checked="selectedTables.has(table.name)"
                    @change="toggleTableSelection(table.name)"
                    class="table-checkbox"
                  />
                  <div class="table-info">
                    <strong>{{ table.name }}</strong>
                    <div class="table-meta">
                      <span class="table-type">{{ table.tableType }}</span>
                      <span v-if="table.isTemporary" class="badge badge-warning">Temporary</span>
                    </div>
                  </div>
                  <button
                    @click="deleteTable(table.name)"
                    class="btn btn-danger btn-sm"
                    :disabled="deletingTable === table.name"
                    title="Delete this table from PostgreSQL and Spark"
                  >
                    {{ deletingTable === table.name ? 'Deleting...' : 'Delete' }}
                  </button>
                </div>
              </div>
            </div>
          </div>

          <div v-else-if="!loadingTables" class="no-tables">
            No tables available. Import data to get started.
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref, computed, onMounted, watch } from 'vue'
import axios from 'axios'
import FKMapEditor from '@/components/FKMapEditor.vue'

interface PostgresConfig {
  host: string
  port: number
  database: string
  username: string
  password: string
}

interface ConnectionResult {
  success: boolean
  message: string
  database?: string
  total_tables?: number
}

interface ImportResult {
  success: boolean
  message: string
  tables_imported: string[]
  errors: string[]
}

interface TableInfo {
  name: string
  database: string | null
  description: string | null
  tableType: string
  isTemporary: boolean
}

interface DumpTableInfo {
  name: string
  row_count: number
  has_create_statement: boolean
}

interface DumpMetadata {
  file: {
    filename: string
    size: number
    size_mb: number
    modified: string
  }
  tables: DumpTableInfo[]
  summary: {
    total_tables: number
    total_rows: number
    total_statements: number
  }
}

const API_BASE_URL = import.meta.env.VITE_API_URL || '/api'

// Default configuration from docker-compose
const defaultConfig: PostgresConfig = {
  host: 'postgres',
  port: 5432,
  database: 'imdb',
  username: 'imdb',
  password: 'imdb'
}

const config = ref<PostgresConfig>({ ...defaultConfig })
const testing = ref(false)
const importing = ref(false)
const loadingTables = ref(false)
const connectionResult = ref<ConnectionResult | null>(null)
const importResult = ref<ImportResult | null>(null)
const availableTables = ref<TableInfo[]>([])

// Database management
const availableDatabases = ref<any[]>([])
const loadingDatabases = ref(false)
const showCreateDatabase = ref(false)
const newDatabaseName = ref('')
const creatingDatabase = ref(false)
const createDatabaseResult = ref<{ success: boolean; message: string } | null>(null)

// Export/Import SQL
const maxRows = ref(10000)
const saveToLibrary = ref(false)
const smartSampling = ref(false)
const compressExport = ref(false)
const selectSpecificTables = ref(false)
const selectedExportTables = ref<Set<string>>(new Set())
const exporting = ref(false)
const exportResult = ref<{ success: boolean; message: string } | null>(null)
const selectedFile = ref<File | null>(null)
const fileInput = ref<HTMLInputElement | null>(null)
const importingSql = ref(false)
const importSqlResult = ref<any | null>(null)

// SQL Dumps Library
const loadingDumps = ref(false)
const availableDumps = ref<any[]>([])
const loadingDump = ref(false)
const loadDumpResult = ref<any | null>(null)
const loadingMetadata = ref<string | null>(null)
const previewedDump = ref<DumpMetadata | null>(null)

// Table deletion
const deletingTable = ref<string | null>(null)
const selectedTables = ref<Set<string>>(new Set())
const deletingTables = ref(false)

// Table search/filter
const tableSearchQuery = ref('')

// FK Map Management
const fkMapSource = ref('imdb') // 'imdb', 'custom', or 'none'
const showFKEditor = ref(false)
const loadingFKMaps = ref(false)
const availableFKMaps = ref<any[]>([])
const selectedFKMap = ref<string | null>(null)

// Computed property to filter tables based on search query
const filteredTables = computed(() => {
  if (!tableSearchQuery.value) {
    return availableTables.value
  }
  const query = tableSearchQuery.value.toLowerCase()
  return availableTables.value.filter(table =>
    table.name.toLowerCase().includes(query)
  )
})

// Computed property to check if all filtered tables are selected
const isAllFilteredSelected = computed(() => {
  if (filteredTables.value.length === 0) return false
  return filteredTables.value.every(table => selectedTables.value.has(table.name))
})

function resetToDefaults() {
  config.value = { ...defaultConfig }
  connectionResult.value = null
}

async function testConnection() {
  testing.value = true
  connectionResult.value = null

  try {
    const response = await axios.get(`${API_BASE_URL}/data-import/postgres/test-connection`, {
      params: config.value
    })
    connectionResult.value = response.data

    // Load databases after successful connection
    if (response.data.success && config.value.username === 'postgres') {
      await loadDatabases()
    }
  } catch (error: any) {
    connectionResult.value = {
      success: false,
      message: error.response?.data?.detail || error.message || 'Connection failed'
    }
  } finally {
    testing.value = false
  }
}

async function loadDatabases() {
  loadingDatabases.value = true
  try {
    const response = await axios.post(
      `${API_BASE_URL}/data-import/postgres/databases`,
      null,
      {
        params: {
          host: config.value.host,
          port: config.value.port,
          username: config.value.username,
          password: config.value.password
        }
      }
    )
    availableDatabases.value = response.data.databases
  } catch (error: any) {
    console.error('Failed to load databases:', error)
    availableDatabases.value = []
  } finally {
    loadingDatabases.value = false
  }
}

async function createDatabase() {
  if (!newDatabaseName.value.trim()) {
    createDatabaseResult.value = {
      success: false,
      message: 'Please enter a database name'
    }
    return
  }

  creatingDatabase.value = true
  createDatabaseResult.value = null

  try {
    const response = await axios.post(
      `${API_BASE_URL}/data-import/postgres/databases/create`,
      null,
      {
        params: {
          database_name: newDatabaseName.value.trim(),
          owner: 'imdb',
          host: config.value.host,
          port: config.value.port,
          username: config.value.username,
          password: config.value.password
        }
      }
    )

    createDatabaseResult.value = {
      success: true,
      message: response.data.message
    }

    // Refresh databases list
    await loadDatabases()

    // Clear form and close dialog
    setTimeout(() => {
      newDatabaseName.value = ''
      showCreateDatabase.value = false
      createDatabaseResult.value = null
    }, 1500)
  } catch (error: any) {
    createDatabaseResult.value = {
      success: false,
      message: error.response?.data?.detail || error.message || 'Failed to create database'
    }
  } finally {
    creatingDatabase.value = false
  }
}

async function importData() {
  importing.value = true
  importResult.value = null

  try {
    const response = await axios.post(`${API_BASE_URL}/data-import/postgres`, config.value)
    importResult.value = response.data

    // Refresh tables list after successful import
    if (response.data.success) {
      await loadTables()
    }
  } catch (error: any) {
    importResult.value = {
      success: false,
      message: error.response?.data?.detail || error.message || 'Import failed',
      tables_imported: [],
      errors: [error.response?.data?.detail || error.message]
    }
  } finally {
    importing.value = false
  }
}

async function loadTables() {
  loadingTables.value = true

  try {
    const response = await axios.get(`${API_BASE_URL}/data-import/imported-tables`)
    availableTables.value = response.data.tables
  } catch (error: any) {
    console.error('Failed to load tables:', error)
  } finally {
    loadingTables.value = false
  }
}

async function deleteTable(tableName: string) {
  if (!confirm(`Are you sure you want to delete table "${tableName}"? This will remove the table from both PostgreSQL and Spark.`)) {
    return
  }

  deletingTable.value = tableName

  try {
    const response = await axios.delete(
      `${API_BASE_URL}/data-import/postgres/tables/${tableName}`,
      {
        params: {
          host: config.value.host,
          port: config.value.port,
          database: config.value.database,
          username: config.value.username,
          password: config.value.password
        }
      }
    )

    if (response.data.success) {
      // Refresh tables list after successful deletion
      await loadTables()
    }
  } catch (error: any) {
    console.error('Failed to delete table:', error)
    alert(`Failed to delete table: ${error.response?.data?.detail || error.message}`)
  } finally {
    deletingTable.value = null
    // Remove from selected tables if it was selected
    selectedTables.value.delete(tableName)
  }
}

function toggleTableSelection(tableName: string) {
  if (selectedTables.value.has(tableName)) {
    selectedTables.value.delete(tableName)
  } else {
    selectedTables.value.add(tableName)
  }
}

function toggleSelectAll() {
  if (isAllFilteredSelected.value) {
    // Deselect all filtered tables
    filteredTables.value.forEach(table => {
      selectedTables.value.delete(table.name)
    })
  } else {
    // Select all filtered tables
    filteredTables.value.forEach(table => {
      selectedTables.value.add(table.name)
    })
  }
}

async function deleteSelectedTables() {
  if (selectedTables.value.size === 0) return

  const count = selectedTables.value.size
  const tableNames = Array.from(selectedTables.value).join(', ')

  if (!confirm(`Are you sure you want to delete ${count} table(s)?\n\n${tableNames}\n\nThis will remove them from both PostgreSQL and Spark.`)) {
    return
  }

  deletingTables.value = true

  const tablesToDelete = Array.from(selectedTables.value)
  const errors: string[] = []

  for (const tableName of tablesToDelete) {
    try {
      await axios.delete(
        `${API_BASE_URL}/data-import/postgres/tables/${tableName}`,
        {
          params: {
            host: config.value.host,
            port: config.value.port,
            database: config.value.database,
            username: config.value.username,
            password: config.value.password
          }
        }
      )
      selectedTables.value.delete(tableName)
    } catch (error: any) {
      console.error(`Failed to delete table ${tableName}:`, error)
      errors.push(`${tableName}: ${error.response?.data?.detail || error.message}`)
    }
  }

  deletingTables.value = false

  // Refresh tables list
  await loadTables()

  if (errors.length > 0) {
    alert(`Failed to delete ${errors.length} table(s):\n\n${errors.join('\n')}`)
  }
}

function toggleExportTable(tableName: string) {
  if (selectedExportTables.value.has(tableName)) {
    selectedExportTables.value.delete(tableName)
  } else {
    selectedExportTables.value.add(tableName)
  }
}

function selectAllExportTables() {
  availableTables.value.forEach(table => {
    selectedExportTables.value.add(table.name)
  })
}

function clearAllExportTables() {
  selectedExportTables.value.clear()
}

async function exportData() {
  exporting.value = true
  exportResult.value = null

  try {
    // Build request body (PostgresConfig only)
    const requestBody: any = { ...config.value }

    // Handle FK map selection
    let useImdbFks = false
    let customFkMap = null

    if (smartSampling.value) {
      if (fkMapSource.value === 'imdb') {
        useImdbFks = true
      } else if (fkMapSource.value === 'custom' && selectedFKMap.value) {
        // Load the custom FK map
        try {
          const fkMapResponse = await axios.get(`${API_BASE_URL}/data-import/fk-maps/${selectedFKMap.value}`)
          customFkMap = fkMapResponse.data
        } catch (error) {
          console.error('Failed to load custom FK map:', error)
          throw new Error('Failed to load selected FK map')
        }
      }
    }

    // Build query params
    const params: any = {
      max_rows: maxRows.value,
      save_to_library: saveToLibrary.value,
      smart_sampling: smartSampling.value,
      use_imdb_fks: useImdbFks,
      compress: compressExport.value
    }

    // Add tables list if specific tables are selected
    if (selectSpecificTables.value && selectedExportTables.value.size > 0) {
      params.tables = Array.from(selectedExportTables.value)
    }

    // Only add custom_fk_map if it's not null
    if (customFkMap !== null) {
      params.custom_fk_map = JSON.stringify(customFkMap)
    }

    const response = await axios.post(
      `${API_BASE_URL}/data-import/postgres/export`,
      requestBody,
      {
        params,
        responseType: 'blob'
      }
    )

    // Create download link
    const fileExtension = compressExport.value ? 'sql.gz' : 'sql'
    const url = window.URL.createObjectURL(new Blob([response.data]))
    const link = document.createElement('a')
    link.href = url
    link.setAttribute('download', `${config.value.database}_export.${fileExtension}`)
    document.body.appendChild(link)
    link.click()
    link.remove()
    window.URL.revokeObjectURL(url)

    let message = 'SQL dump file downloaded successfully'
    if (compressExport.value) {
      message = message.replace('file', 'file (compressed)')
    }
    if (selectSpecificTables.value && selectedExportTables.value.size > 0) {
      message += ` (${selectedExportTables.value.size} tables)`
    }
    if (saveToLibrary.value) {
      message = message.replace('downloaded successfully', 'downloaded and saved to library')
    }
    if (smartSampling.value) {
      message += ' with smart FK-aware sampling'
      if (fkMapSource.value === 'imdb') {
        message += ' using IMDB FK map'
      } else if (fkMapSource.value === 'custom') {
        message += ' using custom FK map'
      }
    }

    exportResult.value = {
      success: true,
      message: message
    }

    // Refresh dumps list if saved to library
    if (saveToLibrary.value) {
      await loadDumpsList()
    }
  } catch (error: any) {
    exportResult.value = {
      success: false,
      message: error.response?.data?.detail || error.message || 'Export failed'
    }
  } finally {
    exporting.value = false
  }
}

function handleFileSelect(event: Event) {
  const target = event.target as HTMLInputElement
  if (target.files && target.files.length > 0) {
    selectedFile.value = target.files[0]
    importSqlResult.value = null
  }
}

function formatFileSize(bytes: number): string {
  if (bytes < 1024) return bytes + ' B'
  if (bytes < 1024 * 1024) return (bytes / 1024).toFixed(1) + ' KB'
  return (bytes / (1024 * 1024)).toFixed(1) + ' MB'
}

async function importSql() {
  if (!selectedFile.value) return

  importingSql.value = true
  importSqlResult.value = null

  try {
    const formData = new FormData()
    formData.append('file', selectedFile.value)

    const response = await axios.post(
      `${API_BASE_URL}/data-import/postgres/import-sql`,
      formData,
      {
        params: {
          host: config.value.host,
          port: config.value.port,
          database: config.value.database,
          username: config.value.username,
          password: config.value.password
        },
        headers: {
          'Content-Type': 'multipart/form-data'
        }
      }
    )

    importSqlResult.value = response.data

    // Refresh tables list after successful import
    if (response.data.success) {
      await loadTables()
    }

    // Clear file selection
    selectedFile.value = null
    if (fileInput.value) {
      fileInput.value.value = ''
    }
  } catch (error: any) {
    importSqlResult.value = {
      success: false,
      message: error.response?.data?.detail || error.message || 'Import failed',
      executed_statements: 0,
      tables_imported_to_spark: 0,
      errors: [error.response?.data?.detail || error.message]
    }
  } finally {
    importingSql.value = false
  }
}

async function loadDumpsList() {
  loadingDumps.value = true

  try {
    const response = await axios.get(`${API_BASE_URL}/data-import/sql-dumps`)
    availableDumps.value = response.data.dumps
  } catch (error: any) {
    console.error('Failed to load dumps list:', error)
  } finally {
    loadingDumps.value = false
  }
}

function formatDate(isoDate: string): string {
  const date = new Date(isoDate)
  return date.toLocaleString()
}

async function loadDumpFromLibrary(filename: string) {
  loadingDump.value = true
  loadDumpResult.value = null

  try {
    const response = await axios.post(
      `${API_BASE_URL}/data-import/sql-dumps/${filename}/load`,
      null,
      {
        params: {
          host: config.value.host,
          port: config.value.port,
          database: config.value.database,
          username: config.value.username,
          password: config.value.password
        }
      }
    )

    loadDumpResult.value = response.data

    // Refresh tables list after successful load
    if (response.data.success) {
      await loadTables()
    }
  } catch (error: any) {
    loadDumpResult.value = {
      success: false,
      message: error.response?.data?.detail || error.message || 'Load failed',
      executed_statements: 0,
      tables_imported_to_spark: 0,
      errors: [error.response?.data?.detail || error.message]
    }
  } finally {
    loadingDump.value = false
  }
}

async function downloadDumpFromLibrary(filename: string) {
  try {
    const response = await axios.get(
      `${API_BASE_URL}/data-import/sql-dumps/${filename}`,
      {
        responseType: 'blob'
      }
    )

    // Create download link
    const url = window.URL.createObjectURL(new Blob([response.data]))
    const link = document.createElement('a')
    link.href = url
    link.setAttribute('download', filename)
    document.body.appendChild(link)
    link.click()
    link.remove()
    window.URL.revokeObjectURL(url)
  } catch (error: any) {
    console.error('Failed to download dump:', error)
  }
}

async function previewDumpMetadata(filename: string) {
  loadingMetadata.value = filename

  try {
    const response = await axios.get<DumpMetadata>(
      `${API_BASE_URL}/data-import/sql-dumps/${filename}/metadata`
    )
    previewedDump.value = response.data
  } catch (error: any) {
    console.error('Failed to load dump metadata:', error)
    alert('Failed to load dump preview: ' + (error.response?.data?.detail || error.message))
  } finally {
    loadingMetadata.value = null
  }
}

function closePreview() {
  previewedDump.value = null
}

async function loadDumpFromPreview() {
  if (!previewedDump.value) return

  const filename = previewedDump.value.file.filename
  closePreview()
  await loadDumpFromLibrary(filename)
}

// FK Map Management Functions
async function loadFKMaps() {
  loadingFKMaps.value = true

  try {
    const response = await axios.get(`${API_BASE_URL}/data-import/fk-maps`)
    availableFKMaps.value = response.data.maps
  } catch (error: any) {
    console.error('Failed to load FK maps:', error)
  } finally {
    loadingFKMaps.value = false
  }
}

async function deleteFKMap(filename: string) {
  if (!confirm(`Are you sure you want to delete this FK map?`)) {
    return
  }

  try {
    await axios.delete(`${API_BASE_URL}/data-import/fk-maps/${filename}`)

    // Refresh list
    await loadFKMaps()

    // Clear selection if deleted map was selected
    if (selectedFKMap.value === filename) {
      selectedFKMap.value = null
    }
  } catch (error: any) {
    alert('Failed to delete FK map: ' + (error.response?.data?.detail || error.message))
  }
}

function closeFKEditor() {
  showFKEditor.value = false
}

async function handleFKMapSaved(mapName: string) {
  // Refresh FK maps list
  await loadFKMaps()

  // Close editor
  closeFKEditor()

  // Auto-select the newly saved map
  const savedMap = availableFKMaps.value.find(m => m.name === mapName)
  if (savedMap) {
    selectedFKMap.value = savedMap.filename
  }
}

// Watch for FK map source changes
watch(fkMapSource, async (newValue) => {
  if (newValue === 'custom') {
    // Load FK maps when switching to custom
    await loadFKMaps()
  }
})

// Automatically reset FK map source when smart sampling is disabled
watch(smartSampling, (newValue) => {
  if (!newValue) {
    fkMapSource.value = 'imdb'
  }
})

onMounted(() => {
  loadTables()
  loadDumpsList()
})
</script>

<style scoped>
.data-import {
  max-width: 1200px;
  margin: 0 auto;
}

.data-import h2 {
  font-size: 2rem;
  margin-bottom: 0.5rem;
}

.description {
  color: var(--color-text-secondary);
  margin-bottom: 2rem;
}

.import-section {
  display: grid;
  gap: 2rem;
}

.config-card,
.import-card,
.export-card,
.import-sql-card,
.sql-library-card,
.tables-card {
  background: white;
  border-radius: 0.5rem;
  padding: 2rem;
  box-shadow: 0 1px 3px rgba(0, 0, 0, 0.1);
}

.config-card h3,
.import-card h3,
.export-card h3,
.import-sql-card h3,
.sql-library-card h3,
.tables-card h3 {
  margin-bottom: 1.5rem;
  font-size: 1.25rem;
}

.export-card p,
.import-sql-card p,
.sql-library-card p {
  color: var(--color-text-secondary);
  margin-bottom: 1.5rem;
}

.connection-form {
  display: flex;
  flex-direction: column;
  gap: 1rem;
}

.form-row {
  display: grid;
  grid-template-columns: 1fr 1fr;
  gap: 1rem;
}

.form-group {
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
}

.form-group label {
  font-weight: 500;
  font-size: 0.875rem;
  color: var(--color-text);
}

.button-group {
  display: flex;
  gap: 1rem;
  margin-top: 1rem;
}

.btn-large {
  padding: 1rem 2rem;
  font-size: 1rem;
  width: 100%;
}

.result-message {
  margin-top: 1rem;
  padding: 1rem;
  border-radius: 0.375rem;
  font-size: 0.875rem;
}

.result-message.success {
  background: #D1FAE5;
  color: #065F46;
  border: 1px solid #A7F3D0;
}

.result-message.error {
  background: #FEE2E2;
  color: #991B1B;
  border: 1px solid #FCA5A5;
}

.import-result {
  margin-top: 1.5rem;
}

.result-header {
  padding: 1rem;
  border-radius: 0.375rem;
  margin-bottom: 1rem;
}

.result-header.success {
  background: #D1FAE5;
  color: #065F46;
}

.result-header.error {
  background: #FEE2E2;
  color: #991B1B;
}

.result-header h4 {
  margin: 0 0 0.5rem 0;
  font-size: 1.125rem;
}

.result-header p {
  margin: 0;
}

.tables-list,
.errors-list {
  margin-top: 1rem;
}

.tables-list h4,
.errors-list h4 {
  font-size: 1rem;
  margin-bottom: 0.5rem;
}

.tables-list ul,
.errors-list ul {
  list-style: none;
  padding: 0;
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
  gap: 0.5rem;
}

.tables-list li {
  background: #F0FDF4;
  padding: 0.5rem 1rem;
  border-radius: 0.375rem;
  font-family: monospace;
  font-size: 0.875rem;
}

.errors-list li {
  background: #FEF2F2;
  padding: 0.5rem 1rem;
  border-radius: 0.375rem;
  font-size: 0.875rem;
  color: #991B1B;
}

.loading-section {
  text-align: center;
  padding: 2rem;
}

.loading-section p {
  margin-top: 1rem;
  color: var(--color-text-secondary);
}

.tables-controls {
  display: flex;
  gap: 1rem;
  align-items: center;
  margin-bottom: 1rem;
}

.search-input {
  flex: 1;
  min-width: 200px;
}

.no-results {
  text-align: center;
  padding: 2rem;
  color: var(--color-text-secondary);
  font-style: italic;
}

.select-all-row {
  padding: 0.75rem;
  background: var(--color-bg-secondary);
  border-radius: 0.375rem;
  margin-bottom: 1rem;
}

.select-all-row .checkbox-label {
  margin: 0;
  font-weight: 500;
}

.table-checkbox {
  cursor: pointer;
  width: 18px;
  height: 18px;
  flex-shrink: 0;
}

.tables-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(250px, 1fr));
  gap: 1rem;
  margin-top: 1rem;
}

.table-item {
  background: var(--color-bg);
  padding: 1rem;
  border-radius: 0.375rem;
  display: flex;
  flex-direction: row;
  justify-content: space-between;
  align-items: center;
  gap: 1rem;
}

.table-info {
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
  flex: 1;
}

.table-meta {
  display: flex;
  gap: 0.5rem;
  align-items: center;
}

.table-item strong {
  font-family: monospace;
  color: var(--color-primary);
}

.table-type {
  font-size: 0.75rem;
  color: var(--color-text-secondary);
  text-transform: uppercase;
}

.btn-sm {
  padding: 0.375rem 0.75rem;
  font-size: 0.875rem;
}

.btn-danger {
  background-color: #dc3545;
  color: white;
}

.btn-danger:hover:not(:disabled) {
  background-color: #c82333;
}

.btn-danger:disabled {
  opacity: 0.6;
  cursor: not-allowed;
}

.no-tables {
  text-align: center;
  padding: 2rem;
  color: var(--color-text-secondary);
}

.host-presets {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  margin-top: 0.5rem;
  flex-wrap: wrap;
}

.preset-label {
  font-size: 0.75rem;
  color: var(--color-text-secondary);
  font-weight: 500;
}

.preset-btn {
  padding: 0.25rem 0.75rem;
  font-size: 0.75rem;
  background: var(--color-bg);
  color: var(--color-text);
  border: 1px solid var(--color-border);
  border-radius: 0.25rem;
  cursor: pointer;
  transition: all 0.15s ease;
}

.preset-btn:hover {
  background: var(--color-primary);
  color: white;
  border-color: var(--color-primary);
}

.preset-btn:active {
  transform: scale(0.95);
}

.help-text {
  font-size: 0.75rem;
  color: var(--color-text-secondary);
  margin-top: 0.25rem;
}

.file-input {
  padding: 0.75rem;
  border: 2px dashed var(--color-border);
  border-radius: 0.375rem;
  background: var(--color-bg);
  cursor: pointer;
  transition: all 0.15s ease;
}

.file-input:hover {
  border-color: var(--color-primary);
  background: white;
}

.selected-file {
  display: block;
  margin-top: 0.5rem;
  font-size: 0.875rem;
  color: var(--color-primary);
  font-weight: 500;
}

.stats-info {
  padding: 1rem;
  background: var(--color-bg);
  border-radius: 0.375rem;
  margin: 1rem 0;
}

.stats-info p {
  margin: 0.25rem 0;
  font-size: 0.875rem;
}

.checkbox-label {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  font-size: 0.875rem;
  font-weight: 500;
  cursor: pointer;
}

.checkbox-label input[type="checkbox"] {
  width: 1.125rem;
  height: 1.125rem;
  cursor: pointer;
}

.library-header {
  margin-bottom: 1rem;
}

.dumps-grid {
  display: grid;
  gap: 1rem;
}

.dump-item {
  border: 1px solid var(--color-border);
  border-radius: 0.375rem;
  padding: 1rem;
  display: flex;
  justify-content: space-between;
  align-items: center;
  gap: 1rem;
  transition: all 0.15s ease;
}

.dump-item:hover {
  border-color: var(--color-primary);
  background: var(--color-bg);
}

.dump-info {
  flex: 1;
  min-width: 0;
}

.dump-info strong {
  font-family: monospace;
  font-size: 0.9375rem;
  color: var(--color-text);
  display: block;
  margin-bottom: 0.25rem;
  word-break: break-all;
}

.dump-meta {
  display: flex;
  gap: 1rem;
  font-size: 0.75rem;
  color: var(--color-text-secondary);
}

.dump-actions {
  display: flex;
  gap: 0.5rem;
  flex-shrink: 0;
}

.btn-sm {
  padding: 0.5rem 1rem;
  font-size: 0.875rem;
}

.no-dumps {
  text-align: center;
  padding: 2rem;
  color: var(--color-text-secondary);
  font-style: italic;
}

/* Table selection panel for export */
.table-selection-panel {
  background: var(--color-bg-secondary);
  border: 1px solid var(--color-border);
  border-radius: 0.375rem;
  padding: 1rem;
  margin-top: 1rem;
  margin-bottom: 1rem;
}

.table-selection-header {
  display: flex;
  justify-content: space-between;
  align-items: center;
  margin-bottom: 1rem;
  padding-bottom: 0.75rem;
  border-bottom: 1px solid var(--color-border);
}

.table-selection-actions {
  display: flex;
  gap: 1rem;
}

.btn-link {
  background: none;
  border: none;
  color: var(--color-primary);
  cursor: pointer;
  padding: 0;
  font-size: 0.875rem;
  text-decoration: underline;
}

.btn-link:hover {
  color: var(--color-primary-dark);
}

.export-tables-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(200px, 1fr));
  gap: 0.5rem;
  max-height: 300px;
  overflow-y: auto;
  padding: 0.5rem;
  background: var(--color-bg);
  border-radius: 0.375rem;
}

.export-table-item {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  padding: 0.5rem;
  background: white;
  border: 1px solid var(--color-border);
  border-radius: 0.25rem;
  cursor: pointer;
  transition: background-color 0.2s;
}

.export-table-item:hover {
  background-color: var(--color-bg-secondary);
}

.export-table-item input[type="checkbox"] {
  cursor: pointer;
  flex-shrink: 0;
}

.export-table-item .table-name {
  font-family: monospace;
  font-size: 0.875rem;
  flex: 1;
  color: var(--color-text);
}

.export-table-item .table-type-small {
  font-size: 0.75rem;
  color: var(--color-text-secondary);
  text-transform: uppercase;
}

/* SQL Dump Preview Modal */
.modal-overlay {
  position: fixed;
  top: 0;
  left: 0;
  right: 0;
  bottom: 0;
  background-color: rgba(0, 0, 0, 0.6);
  display: flex;
  align-items: center;
  justify-content: center;
  z-index: 1000;
  padding: 1rem;
}

.modal-content {
  background: white;
  border-radius: 0.5rem;
  box-shadow: 0 10px 25px rgba(0, 0, 0, 0.2);
  max-width: 800px;
  width: 100%;
  max-height: 90vh;
  display: flex;
  flex-direction: column;
}

.modal-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  padding: 1.5rem;
  border-bottom: 1px solid var(--color-border);
}

.modal-header h3 {
  margin: 0;
  color: var(--color-text);
}

.btn-close {
  background: none;
  border: none;
  font-size: 2rem;
  line-height: 1;
  color: var(--color-text-secondary);
  cursor: pointer;
  padding: 0;
  width: 2rem;
  height: 2rem;
  display: flex;
  align-items: center;
  justify-content: center;
  transition: color 0.2s;
}

.btn-close:hover {
  color: var(--color-text);
}

.modal-body {
  padding: 1.5rem;
  overflow-y: auto;
  flex: 1;
}

.modal-footer {
  display: flex;
  gap: 1rem;
  justify-content: flex-end;
  padding: 1.5rem;
  border-top: 1px solid var(--color-border);
}

.preview-file-info,
.preview-summary,
.preview-tables {
  margin-bottom: 2rem;
}

.preview-file-info h4,
.preview-summary h4,
.preview-tables h4 {
  margin: 0 0 1rem 0;
  color: var(--color-text);
  font-size: 1.125rem;
  font-weight: 600;
}

.info-grid {
  display: grid;
  grid-template-columns: repeat(auto-fit, minmax(250px, 1fr));
  gap: 1rem;
}

.info-item {
  display: flex;
  flex-direction: column;
  gap: 0.25rem;
}

.info-label {
  font-size: 0.875rem;
  color: var(--color-text-secondary);
  font-weight: 500;
}

.info-value {
  font-size: 1rem;
  color: var(--color-text);
  font-weight: 600;
}

.tables-list-preview {
  max-height: 400px;
  overflow-y: auto;
  border: 1px solid var(--color-border);
  border-radius: 0.375rem;
}

.preview-table {
  width: 100%;
  border-collapse: collapse;
}

.preview-table thead {
  background-color: var(--color-bg-secondary);
  position: sticky;
  top: 0;
  z-index: 10;
}

.preview-table th {
  text-align: left;
  padding: 0.75rem 1rem;
  font-weight: 600;
  color: var(--color-text);
  border-bottom: 2px solid var(--color-border);
}

.preview-table td {
  padding: 0.75rem 1rem;
  border-bottom: 1px solid var(--color-border);
}

.preview-table tbody tr:hover {
  background-color: var(--color-bg-secondary);
}

.table-name-cell {
  font-family: monospace;
  font-size: 0.875rem;
  color: var(--color-text);
}

.row-count-cell {
  color: var(--color-text);
  font-variant-numeric: tabular-nums;
}

.schema-cell {
  text-align: center;
}

.schema-badge {
  display: inline-block;
  padding: 0.25rem 0.5rem;
  border-radius: 0.25rem;
  font-size: 0.75rem;
  font-weight: 600;
  text-transform: uppercase;
}

.schema-badge.yes {
  background-color: #dcfce7;
  color: #166534;
}

.schema-badge.no {
  background-color: #fee2e2;
  color: #991b1b;
}

/* General badge styles */
.badge {
  display: inline-block;
  padding: 0.25rem 0.5rem;
  border-radius: 0.25rem;
  font-size: 0.75rem;
  font-weight: 600;
  text-transform: uppercase;
}

.badge-info {
  background-color: #dbeafe;
  color: #1e40af;
}

.badge-warning {
  background-color: #fef3c7;
  color: #92400e;
}

/* Responsive modal */
@media (max-width: 768px) {
  .modal-content {
    max-height: 95vh;
  }

  .modal-header,
  .modal-body,
  .modal-footer {
    padding: 1rem;
  }

  .info-grid {
    grid-template-columns: 1fr;
  }

  .preview-table th,
  .preview-table td {
    padding: 0.5rem;
    font-size: 0.875rem;
  }

  .fk-editor-modal {
    max-width: 95vw;
  }
}

/* FK Map Management Styles */
.fk-map-options {
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
}

.radio-label {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  cursor: pointer;
  padding: 0.5rem;
  border-radius: 0.25rem;
  transition: background-color 0.2s;
}

.radio-label:hover {
  background-color: var(--color-bg-secondary);
}

.radio-label input[type="radio"] {
  cursor: pointer;
}

.custom-fk-section {
  margin-top: 1rem;
  padding: 1rem;
  background: var(--color-bg-secondary);
  border: 1px solid var(--color-border);
  border-radius: 0.375rem;
}

.custom-fk-header {
  display: flex;
  align-items: center;
  justify-content: space-between;
  margin-bottom: 1rem;
}

.custom-fk-header strong {
  color: var(--color-text);
  font-size: 1rem;
}

.no-fk-maps {
  padding: 1.5rem;
  text-align: center;
  color: var(--color-text-secondary);
  background: white;
  border: 1px dashed var(--color-border);
  border-radius: 0.375rem;
}

.fk-maps-list {
  display: flex;
  flex-direction: column;
  gap: 0.5rem;
}

.fk-map-item {
  display: flex;
  align-items: flex-start;
  gap: 1rem;
  padding: 1rem;
  background: white;
  border: 1px solid var(--color-border);
  border-radius: 0.375rem;
  cursor: pointer;
  transition: all 0.2s;
}

.fk-map-item:hover {
  border-color: #3b82f6;
  box-shadow: 0 1px 3px rgba(59, 130, 246, 0.1);
}

.fk-map-item.selected {
  border-color: #3b82f6;
  background-color: #eff6ff;
}

.fk-map-item input[type="radio"] {
  margin-top: 0.25rem;
  cursor: pointer;
}

.fk-map-info {
  flex: 1;
  min-width: 0;
}

.fk-map-info strong {
  display: block;
  color: var(--color-text);
  font-size: 1rem;
  margin-bottom: 0.25rem;
}

.fk-map-meta {
  display: flex;
  gap: 1rem;
  font-size: 0.75rem;
  color: var(--color-text-secondary);
  margin-bottom: 0.5rem;
}

.fk-count {
  font-weight: 500;
}

.fk-description {
  font-size: 0.875rem;
  color: var(--color-text-secondary);
  margin: 0.5rem 0 0 0;
  font-style: italic;
}

.fk-editor-modal {
  max-width: 900px;
  max-height: 90vh;
}

/* Database Management Styles */
.database-input-group {
  display: flex;
  gap: 0.5rem;
  align-items: center;
}

.database-input-group .input {
  flex: 1;
}

.btn-icon {
  background: var(--color-primary);
  color: white;
  border: none;
  border-radius: 4px;
  width: 2.5rem;
  height: 2.5rem;
  font-size: 1.5rem;
  font-weight: bold;
  cursor: pointer;
  display: flex;
  align-items: center;
  justify-content: center;
  transition: all 0.2s ease;
}

.btn-icon:hover {
  background: var(--color-primary-dark);
  transform: scale(1.05);
}

.btn-icon:active {
  transform: scale(0.95);
}

.user-presets {
  display: flex;
  gap: 0.5rem;
  align-items: center;
  margin-top: 0.5rem;
  flex-wrap: wrap;
}

.preset-label {
  font-size: 0.875rem;
  color: var(--color-text-secondary);
  font-weight: 500;
}

.create-database-modal {
  max-width: 500px;
}

.modal-description {
  color: var(--color-text-secondary);
  margin-bottom: 1.5rem;
  font-size: 0.9375rem;
}

.create-db-form .form-group {
  margin-bottom: 1.5rem;
}

.form-help {
  display: block;
  font-size: 0.8125rem;
  color: var(--color-text-secondary);
  margin-top: 0.25rem;
}

.result-message {
  padding: 0.75rem 1rem;
  border-radius: 4px;
  margin-bottom: 1rem;
  font-size: 0.9375rem;
}

.result-message.success {
  background-color: rgba(72, 187, 120, 0.1);
  color: #48bb78;
  border: 1px solid rgba(72, 187, 120, 0.3);
}

.result-message.error {
  background-color: rgba(245, 101, 101, 0.1);
  color: #f56565;
  border: 1px solid rgba(245, 101, 101, 0.3);
}

/* Responsive adjustments for database management */
@media (max-width: 768px) {
  .user-presets {
    flex-direction: column;
    align-items: flex-start;
  }

  .database-input-group {
    flex-direction: column;
  }

  .database-input-group .input {
    width: 100%;
  }

  .btn-icon {
    width: 100%;
  }
}
</style>
