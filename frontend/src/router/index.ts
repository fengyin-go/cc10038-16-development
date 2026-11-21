import { createRouter, createWebHistory } from 'vue-router'

import Overview from '@/views/Overview.vue'
import ParserReleaseView from '@/views/ParserReleaseView.vue'
import WorkbenchView from '@/views/WorkbenchView.vue'

const router = createRouter({
  history: createWebHistory(),
  routes: [
    { path: '/', name: 'overview', component: Overview },
    { path: '/parser-release', name: 'parser-release', component: ParserReleaseView },
    { path: '/:moduleKey', name: 'module', component: WorkbenchView, props: true },
  ],
})

export default router
