import { ref, watch } from 'vue'

interface Preferences {
  editorWidth: number
  showHypergraphSidebar: boolean
  dagLayoutStacked: boolean
  expandedSections: Record<string, boolean>
  sidebarVizMode: 'graph' | 'bubbles' | 'hulls'
}

const STORAGE_KEY = 'yannasparkis_preferences'

const defaultPreferences: Preferences = {
  editorWidth: 500,
  showHypergraphSidebar: false,
  dagLayoutStacked: true,
  expandedSections: {
    breakdown: true,
    metrics: true,
    timeline: true,
    timeBreakdown: true,
    operatorFlow: true,
    operatorMetrics: false,
    operatorDag: true,
    dataFlow: false,
    planComparison: false,
    planTree: false
  },
  sidebarVizMode: 'graph'
}

// Load preferences from localStorage
function loadPreferences(): Preferences {
  try {
    const stored = localStorage.getItem(STORAGE_KEY)
    if (stored) {
      const parsed = JSON.parse(stored)
      return { ...defaultPreferences, ...parsed }
    }
  } catch (e) {
    console.error('Failed to load preferences:', e)
  }
  return { ...defaultPreferences }
}

// Save preferences to localStorage
function savePreferences(preferences: Preferences) {
  try {
    localStorage.setItem(STORAGE_KEY, JSON.stringify(preferences))
  } catch (e) {
    console.error('Failed to save preferences:', e)
  }
}

export function usePreferences() {
  const preferences = ref<Preferences>(loadPreferences())

  // Watch for changes and save automatically
  watch(preferences, (newPrefs) => {
    savePreferences(newPrefs)
  }, { deep: true })

  // Helper functions for specific preferences
  const setEditorWidth = (width: number) => {
    preferences.value.editorWidth = width
  }

  const setShowHypergraphSidebar = (show: boolean) => {
    preferences.value.showHypergraphSidebar = show
  }

  const setDagLayoutStacked = (stacked: boolean) => {
    preferences.value.dagLayoutStacked = stacked
  }

  const setSectionExpanded = (section: string, expanded: boolean) => {
    preferences.value.expandedSections[section] = expanded
  }

  const setSidebarVizMode = (mode: 'graph' | 'bubbles' | 'hulls') => {
    preferences.value.sidebarVizMode = mode
  }

  return {
    preferences,
    setEditorWidth,
    setShowHypergraphSidebar,
    setDagLayoutStacked,
    setSectionExpanded,
    setSidebarVizMode
  }
}
