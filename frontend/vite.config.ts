import { defineConfig } from 'vite'
import vue from '@vitejs/plugin-vue'
import { fileURLToPath, URL } from 'node:url'

export default defineConfig({
  plugins: [vue()],
  resolve: {
    alias: {
      '@': fileURLToPath(new URL('./src', import.meta.url))
    }
  },
  server: {
    port: 5173,
    proxy: {
      '/api': {
        target: 'http://backend:8000',
        changeOrigin: true,
        // Match nginx: /execution/execute can run for ~3 min when the
        // reference query exhausts its 90s budget before the optimised run.
        timeout: 300000,
        proxyTimeout: 300000
      },
      '/ws': {
        target: 'ws://backend:8000',
        ws: true
      }
    }
  }
})
