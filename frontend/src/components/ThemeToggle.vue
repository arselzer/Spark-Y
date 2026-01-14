<template>
  <div class="theme-toggle">
    <button
      @click="toggleTheme"
      class="theme-btn"
      :title="getTooltip()"
      :aria-label="getTooltip()"
    >
      <span class="theme-icon">{{ getIcon() }}</span>
      <span class="theme-label">{{ getLabel() }}</span>
    </button>
  </div>
</template>

<script setup lang="ts">
import { useTheme } from '@/composables/useTheme'

const { currentTheme, toggleTheme } = useTheme()

function getIcon(): string {
  if (currentTheme.value === 'light') return '☀️'
  if (currentTheme.value === 'dark') return '🌙'
  return '🌓'
}

function getLabel(): string {
  if (currentTheme.value === 'light') return 'Light'
  if (currentTheme.value === 'dark') return 'Dark'
  return 'Auto'
}

function getTooltip(): string {
  if (currentTheme.value === 'light') return 'Switch to dark mode'
  if (currentTheme.value === 'dark') return 'Switch to auto mode'
  return 'Switch to light mode'
}
</script>

<style scoped>
.theme-toggle {
  display: inline-block;
}

.theme-btn {
  display: flex;
  align-items: center;
  gap: 0.375rem;
  padding: 0.375rem 0.625rem;
  background: var(--color-background-soft);
  border: 1px solid var(--color-border);
  border-radius: 0.375rem;
  cursor: pointer;
  font-size: 0.8125rem;
  font-weight: 500;
  color: var(--color-text);
  transition: all 0.2s;
}

.theme-btn:hover {
  background: var(--color-background-mute);
  border-color: var(--color-primary);
  transform: translateY(-1px);
  box-shadow: 0 2px 4px rgba(0, 0, 0, 0.1);
}

.theme-btn:active {
  transform: translateY(0);
}

.theme-icon {
  font-size: 1rem;
  line-height: 1;
}

.theme-label {
  min-width: 2.5rem;
  text-align: left;
}

@media (max-width: 768px) {
  .theme-label {
    display: none;
  }

  .theme-btn {
    padding: 0.5rem;
  }
}
</style>
