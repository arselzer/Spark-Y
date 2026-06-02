<template>
  <div class="peak-bar" v-if="hasData">
    <h3 class="peak-title">Avoided Materialisation</h3>
    <p class="peak-sub">
      The reference plan builds a large intermediate join result just to compute
      a small aggregate. The optimised plan propagates counts instead, so it
      never materialises that intermediate.
    </p>

    <!-- Reference intermediate -->
    <div class="bar-row">
      <div class="bar-label">Intermediate<br /><span class="bar-sublabel">reference plan</span></div>
      <div class="bar-track">
        <div class="bar-fill reference" :style="{ width: refWidthPct + '%' }"></div>
        <span class="bar-value">{{ refValueLabel }}</span>
      </div>
    </div>

    <!-- Final result -->
    <div class="bar-row">
      <div class="bar-label">Result<br /><span class="bar-sublabel">rows returned</span></div>
      <div class="bar-track">
        <div class="bar-fill result" :style="{ width: resultWidthPct + '%' }"></div>
        <span class="bar-value">{{ formatNumber(resultRows) }} {{ resultRows === 1 ? 'row' : 'rows' }}</span>
      </div>
    </div>

    <div class="peak-caption" v-if="caption">{{ caption }}</div>
  </div>
</template>

<script setup lang="ts">
import { computed } from 'vue'

interface Props {
  referenceMetrics: any
  optimizedMetrics: any
}
const props = defineProps<Props>()

const refTimedOut = computed(() => !!props.referenceMetrics?.timed_out)

// Peak join cardinality of the reference plan — the largest intermediate it
// must build. This is a standard SortMergeJoin numOutputRows, so it is
// reliable (unlike the optimised CountJoin, whose numOutputRows can report the
// carried count value rather than physical tuples).
const refPeak = computed(() => Number(props.referenceMetrics?.peak_intermediate_rows) || 0)

// Rows the query actually returns (from the collected result count).
const resultRows = computed(() =>
  Number(props.optimizedMetrics?.total_output_rows ?? props.referenceMetrics?.total_output_rows) || 0
)

const hasData = computed(() => refTimedOut.value || refPeak.value > 0)

// Log scale so a 1-row result stays visible next to a billion-row intermediate.
const logMax = computed(() => Math.log10(Math.max(refPeak.value, resultRows.value, 1) + 1))
function widthFor(n: number): number {
  if (n <= 0) return 1.5
  return Math.max(1.5, (Math.log10(n + 1) / logMax.value) * 100)
}
const refWidthPct = computed(() => (refTimedOut.value ? 100 : widthFor(refPeak.value)))
const resultWidthPct = computed(() => widthFor(resultRows.value))

const refValueLabel = computed(() =>
  refTimedOut.value ? 'did not finish — intermediate too large' : `${formatNumber(refPeak.value)} rows`
)

const caption = computed(() => {
  if (refTimedOut.value) {
    return 'The reference plan exceeded the time budget building its intermediate; the optimised plan avoids it.'
  }
  if (refPeak.value > 0 && resultRows.value >= 0 && refPeak.value > resultRows.value * 2) {
    return `The reference materialises ${formatNumber(refPeak.value)} intermediate rows to return ${formatNumber(resultRows.value)}.`
  }
  return ''
})

function formatNumber(n: number): string {
  if (n >= 1e9) return `${(n / 1e9).toFixed(2)}B`
  if (n >= 1e6) return `${(n / 1e6).toFixed(2)}M`
  if (n >= 1e3) return `${(n / 1e3).toFixed(1)}k`
  return Math.round(n).toLocaleString()
}
</script>

<style scoped>
.peak-bar {
  background: var(--color-surface);
  border: 1px solid var(--color-border);
  border-radius: 0.5rem;
  padding: 1.25rem 1.5rem;
  margin-bottom: 1rem;
}

.peak-title {
  margin: 0 0 0.25rem 0;
  font-size: 1.125rem;
  font-weight: 600;
}

.peak-sub {
  margin: 0 0 1rem 0;
  font-size: 0.85rem;
  color: var(--color-text-secondary);
  line-height: 1.4;
}

.bar-row {
  display: grid;
  grid-template-columns: 110px 1fr;
  align-items: center;
  gap: 0.75rem;
  margin-bottom: 0.6rem;
}

.bar-label {
  font-weight: 600;
  font-size: 0.9rem;
  line-height: 1.2;
}

.bar-sublabel {
  font-weight: 400;
  font-size: 0.75rem;
  color: var(--color-text-secondary);
}

.bar-track {
  position: relative;
  background: var(--color-background-mute, var(--color-border));
  border-radius: 0.375rem;
  height: 2rem;
  display: flex;
  align-items: center;
}

.bar-fill {
  height: 100%;
  border-radius: 0.375rem;
  transition: width 0.6s ease;
}

.bar-fill.reference {
  background: linear-gradient(90deg, #f97316, #ef4444);
}

.bar-fill.result {
  background: linear-gradient(90deg, #14b8a6, #10b981);
}

.bar-value {
  position: absolute;
  left: 0.75rem;
  font-size: 0.85rem;
  font-weight: 600;
  color: var(--color-text);
  text-shadow: 0 0 4px var(--color-surface), 0 0 4px var(--color-surface);
  white-space: nowrap;
}

.peak-caption {
  margin-top: 0.5rem;
  font-size: 0.9rem;
  font-weight: 600;
  color: var(--color-primary, #0f766e);
}
</style>
