import { ref } from 'vue'

export interface HighlightTarget {
  type: 'edge' | 'node'
  id: string
  label: string
  attributes?: string[]
  hoveredAttribute?: string  // The specific attribute that was hovered (for SQL hover only)
}

// Shared state for coordinating highlights between SQL and hypergraph
const currentHighlight = ref<HighlightTarget | null>(null)

export function useHighlightCoordination() {
  function highlightElement(target: HighlightTarget | null) {
    currentHighlight.value = target
  }

  function clearHighlight() {
    currentHighlight.value = null
  }

  return {
    currentHighlight,
    highlightElement,
    clearHighlight
  }
}
