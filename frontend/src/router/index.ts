import { createRouter, createWebHistory } from 'vue-router'

const router = createRouter({
  history: createWebHistory(import.meta.env.BASE_URL),
  routes: [
    {
      path: '/',
      name: 'home',
      component: () => import('@/views/HomeView.vue')
    },
    {
      path: '/queries',
      name: 'queries',
      component: () => import('@/views/QueryBrowserView.vue')
    },
    {
      path: '/query/:id',
      name: 'query-detail',
      component: () => import('@/views/QueryDetailView.vue')
    },
    {
      path: '/execute',
      name: 'execute',
      component: () => import('@/views/ExecuteView.vue')
    },
    {
      path: '/data-import',
      name: 'data-import',
      component: () => import('@/views/DataImportView.vue')
    },
    {
      path: '/settings',
      name: 'settings',
      component: () => import('@/views/SettingsView.vue')
    }
  ]
})

export default router
