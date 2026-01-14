<template>
  <div class="query-templates">
    <div class="templates-header" @click="isExpanded = !isExpanded">
      <div class="header-content">
        <h3>Query Templates</h3>
        <p class="description">Select a template to quickly try different query patterns</p>
      </div>
      <button class="toggle-button" :aria-label="isExpanded ? 'Collapse' : 'Expand'">
        {{ isExpanded ? '▼' : '▶' }}
      </button>
    </div>

    <div v-if="isExpanded" class="templates-grid">
      <div
        v-for="template in templates"
        :key="template.id"
        :class="['template-card', { selected: selectedTemplate === template.id }]"
        @click="selectTemplate(template)"
      >
        <div class="template-icon" :class="`icon-${template.type}`">
          {{ template.icon }}
        </div>
        <div class="template-content">
          <h4>{{ template.name }}</h4>
          <p class="template-type">{{ template.type }}</p>
          <p class="template-desc">{{ template.description }}</p>
          <div class="template-meta">
            <span class="meta-badge" :class="template.is_acyclic ? 'acyclic' : 'cyclic'">
              {{ template.is_acyclic ? 'Acyclic' : 'Cyclic' }}
            </span>
            <span class="meta-info">{{ template.num_tables }} tables, {{ template.num_joins }} joins</span>
          </div>
        </div>
      </div>
    </div>
  </div>
</template>

<script setup lang="ts">
import { ref } from 'vue'

interface QueryTemplate {
  id: string
  name: string
  type: string
  icon: string
  description: string
  sql: string
  is_acyclic: boolean
  num_tables: number
  num_joins: number
}

interface Emits {
  (e: 'select', sql: string): void
}

const emit = defineEmits<Emits>()

const isExpanded = ref(false) // Collapsed by default
const selectedTemplate = ref<string | null>(null)

const templates: QueryTemplate[] = [
  {
    id: 'chain-3',
    name: 'Chain Join (3 tables)',
    type: 'chain',
    icon: '⛓️',
    description: 'Linear join: A → B → C. Simple acyclic pattern.',
    is_acyclic: true,
    num_tables: 3,
    num_joins: 2,
    sql: `SELECT t.title, cn.name
FROM title t
JOIN movie_companies mc ON t.id = mc.movie_id
JOIN company_name cn ON mc.company_id = cn.id
WHERE t.production_year > 2000`
  },
  {
    id: 'star-3',
    name: 'Star Join (3 branches)',
    type: 'star',
    icon: '⭐',
    description: 'Center table with multiple branches. Acyclic with center as root.',
    is_acyclic: true,
    num_tables: 4,
    num_joins: 3,
    sql: `SELECT t.title, k.keyword, cn.name
FROM title t
JOIN movie_companies mc ON t.id = mc.movie_id
JOIN movie_keyword mk ON t.id = mk.movie_id
JOIN company_name cn ON mc.company_id = cn.id
JOIN keyword k ON mk.keyword_id = k.id
WHERE t.production_year > 2000`
  },
  {
    id: 'tree-complex',
    name: 'Tree Join (Multi-level)',
    type: 'tree',
    icon: '🌳',
    description: 'Multi-level hierarchical joins. Acyclic tree structure.',
    is_acyclic: true,
    num_tables: 5,
    num_joins: 4,
    sql: `SELECT MIN(t.title) as movie_title
FROM title t
JOIN movie_companies mc ON t.id = mc.movie_id
JOIN company_name cn ON mc.company_id = cn.id
JOIN movie_keyword mk ON t.id = mk.movie_id
JOIN keyword k ON mk.keyword_id = k.id
WHERE cn.country_code = '[us]'
  AND k.keyword = 'character-name-in-title'
GROUP BY t.id`
  },
  {
    id: 'self-join',
    name: 'Self Join',
    type: 'self-join',
    icon: '🔄',
    description: 'Join table with itself. Creates a cycle in some representations.',
    is_acyclic: false,
    num_tables: 2,
    num_joins: 1,
    sql: `SELECT t1.title, t2.title
FROM title t1
JOIN title t2 ON t1.production_year = t2.production_year
WHERE t1.id < t2.id
  AND t1.production_year > 2000
LIMIT 10`
  },
  {
    id: 'triangle',
    name: 'Triangle Join',
    type: 'cyclic',
    icon: '🔺',
    description: 'Three tables forming a cycle: A-B-C-A. Classic cyclic pattern.',
    is_acyclic: false,
    num_tables: 3,
    num_joins: 3,
    sql: `-- Triangle cycle example
-- Note: This is a conceptual example
-- Actual cyclicity depends on join predicates
SELECT *
FROM title t
JOIN movie_companies mc ON t.id = mc.movie_id
JOIN company_name cn ON cn.id = mc.company_id
-- Adding cycle:
-- AND cn.country_code = t.title (if this were valid)
LIMIT 10`
  },
  {
    id: 'snowflake',
    name: 'Snowflake Schema',
    type: 'snowflake',
    icon: '❄️',
    description: 'Fact table with dimension tables and sub-dimensions. Acyclic.',
    is_acyclic: true,
    num_tables: 5,
    num_joins: 4,
    sql: `SELECT
  t.title,
  cn.name as company,
  cn.country_code,
  k.keyword,
  COUNT(*) as cnt
FROM title t
JOIN movie_companies mc ON t.id = mc.movie_id
JOIN company_name cn ON mc.company_id = cn.id
JOIN movie_keyword mk ON t.id = mk.movie_id
JOIN keyword k ON mk.keyword_id = k.id
GROUP BY t.title, cn.name, cn.country_code, k.keyword
LIMIT 100`
  },
  {
    id: 'single-table',
    name: 'Single Table (No Joins)',
    type: 'single',
    icon: '📄',
    description: 'Query on a single table. Trivially acyclic.',
    is_acyclic: true,
    num_tables: 1,
    num_joins: 0,
    sql: `SELECT
  title,
  production_year,
  id
FROM title
WHERE production_year BETWEEN 2000 AND 2020
ORDER BY production_year DESC
LIMIT 100`
  },
  {
    id: 'aggregate-star',
    name: 'Star with Aggregation',
    type: 'star',
    icon: '📊',
    description: 'Star join with GROUP BY and aggregates. Tests optimization.',
    is_acyclic: true,
    num_tables: 4,
    num_joins: 3,
    sql: `SELECT
  cn.name as company,
  COUNT(DISTINCT t.id) as movie_count,
  COUNT(DISTINCT k.id) as keyword_count,
  AVG(t.production_year) as avg_year
FROM title t
JOIN movie_companies mc ON t.id = mc.movie_id
JOIN company_name cn ON mc.company_id = cn.id
LEFT JOIN movie_keyword mk ON t.id = mk.movie_id
LEFT JOIN keyword k ON mk.keyword_id = k.id
WHERE t.production_year > 2000
GROUP BY cn.name
ORDER BY movie_count DESC
LIMIT 20`
  }
]

const selectTemplate = (template: QueryTemplate) => {
  selectedTemplate.value = template.id
  emit('select', template.sql)
}
</script>

<style scoped>
.query-templates {
  background: white;
  border-radius: 8px;
  padding: 1.5rem;
  box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
}

.templates-header {
  margin-bottom: 1.5rem;
  display: flex;
  align-items: flex-start;
  justify-content: space-between;
  cursor: pointer;
  user-select: none;
  padding: 0.5rem;
  border-radius: 6px;
  transition: background-color 0.2s;
}

.templates-header:hover {
  background-color: var(--color-surface);
}

.header-content {
  flex: 1;
}

.templates-header h3 {
  margin: 0 0 0.5rem 0;
  font-size: 1.25rem;
  color: var(--color-text);
}

.description {
  margin: 0;
  font-size: 0.875rem;
  color: var(--color-text-secondary);
}

.toggle-button {
  background: none;
  border: none;
  font-size: 1.25rem;
  color: var(--color-text-secondary);
  cursor: pointer;
  padding: 0.25rem 0.5rem;
  display: flex;
  align-items: center;
  justify-content: center;
  transition: color 0.2s;
}

.toggle-button:hover {
  color: var(--color-primary);
}

.templates-grid {
  display: grid;
  grid-template-columns: repeat(auto-fill, minmax(280px, 1fr));
  gap: 1rem;
}

.template-card {
  border: 2px solid #e5e7eb;
  border-radius: 8px;
  padding: 1rem;
  cursor: pointer;
  transition: all 0.2s;
  display: flex;
  flex-direction: column;
}

.template-card:hover {
  border-color: #3b82f6;
  box-shadow: 0 4px 6px rgba(59, 130, 246, 0.1);
  transform: translateY(-2px);
}

.template-card.selected {
  border-color: #3b82f6;
  background: #eff6ff;
}

.template-icon {
  font-size: 2rem;
  margin-bottom: 0.75rem;
  text-align: center;
}

.icon-chain { filter: grayscale(0.2); }
.icon-star { filter: hue-rotate(20deg); }
.icon-tree { filter: hue-rotate(-20deg); }
.icon-cyclic { filter: hue-rotate(180deg); }
.icon-snowflake { filter: brightness(1.1); }
.icon-single { filter: saturate(0.7); }
.icon-self-join { filter: hue-rotate(90deg); }

.template-content {
  flex: 1;
  display: flex;
  flex-direction: column;
}

.template-content h4 {
  margin: 0 0 0.25rem 0;
  font-size: 1rem;
  color: #1f2937;
}

.template-type {
  margin: 0 0 0.5rem 0;
  font-size: 0.75rem;
  color: #6b7280;
  text-transform: uppercase;
  font-weight: 600;
  letter-spacing: 0.05em;
}

.template-desc {
  margin: 0 0 0.75rem 0;
  font-size: 0.875rem;
  color: #4b5563;
  line-height: 1.4;
  flex: 1;
}

.template-meta {
  display: flex;
  align-items: center;
  gap: 0.5rem;
  flex-wrap: wrap;
  padding-top: 0.75rem;
  border-top: 1px solid #e5e7eb;
}

.meta-badge {
  padding: 0.125rem 0.5rem;
  border-radius: 9999px;
  font-size: 0.75rem;
  font-weight: 600;
}

.meta-badge.acyclic {
  background: #d1fae5;
  color: #065f46;
}

.meta-badge.cyclic {
  background: #fef3c7;
  color: #92400e;
}

.meta-info {
  font-size: 0.75rem;
  color: #6b7280;
}

@media (max-width: 768px) {
  .templates-grid {
    grid-template-columns: 1fr;
  }
}
</style>
