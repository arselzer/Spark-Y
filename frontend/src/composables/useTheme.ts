import { ref, watch, onMounted } from 'vue'

export type Theme = 'light' | 'dark' | 'auto'

const STORAGE_KEY = 'yannasparkis_theme'

// Shared state across all instances
const currentTheme = ref<Theme>('light')
const effectiveTheme = ref<'light' | 'dark'>('light')

export function useTheme() {
  // Load theme from localStorage on first use
  onMounted(() => {
    loadTheme()
  })

  function loadTheme() {
    try {
      const stored = localStorage.getItem(STORAGE_KEY) as Theme | null
      if (stored && ['light', 'dark', 'auto'].includes(stored)) {
        currentTheme.value = stored
      }
    } catch (error) {
      console.error('Failed to load theme:', error)
    }

    updateEffectiveTheme()
  }

  function updateEffectiveTheme() {
    if (currentTheme.value === 'auto') {
      // Check system preference
      const prefersDark = window.matchMedia('(prefers-color-scheme: dark)').matches
      effectiveTheme.value = prefersDark ? 'dark' : 'light'
    } else {
      effectiveTheme.value = currentTheme.value
    }

    // Apply theme to document
    applyTheme(effectiveTheme.value)
  }

  function applyTheme(theme: 'light' | 'dark') {
    if (theme === 'dark') {
      document.documentElement.classList.add('dark-mode')
    } else {
      document.documentElement.classList.remove('dark-mode')
    }
  }

  function setTheme(theme: Theme) {
    currentTheme.value = theme
    try {
      localStorage.setItem(STORAGE_KEY, theme)
    } catch (error) {
      console.error('Failed to save theme:', error)
    }
    updateEffectiveTheme()
  }

  function toggleTheme() {
    if (currentTheme.value === 'light') {
      setTheme('dark')
    } else if (currentTheme.value === 'dark') {
      setTheme('auto')
    } else {
      setTheme('light')
    }
  }

  // Watch for system theme changes when in auto mode
  if (typeof window !== 'undefined') {
    const mediaQuery = window.matchMedia('(prefers-color-scheme: dark)')
    mediaQuery.addEventListener('change', (e) => {
      if (currentTheme.value === 'auto') {
        effectiveTheme.value = e.matches ? 'dark' : 'light'
        applyTheme(effectiveTheme.value)
      }
    })
  }

  return {
    currentTheme,
    effectiveTheme,
    setTheme,
    toggleTheme
  }
}
