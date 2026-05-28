import { RouteRecordRaw } from 'vue-router';

const routes: RouteRecordRaw[] = [
  {
    path: '/',
    component: () => import('pages/DataDictionarySlickgrid.vue'),
  },
];

export default routes;
