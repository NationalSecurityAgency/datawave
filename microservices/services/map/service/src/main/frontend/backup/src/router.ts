import { createRouter, createWebHistory } from "vue-router";
import HelloWorld from './views/HelloWorldView.vue';
// import Map from './views/LeafletMapView.vue';

const routes = [
    // { path: "/", component: Map },
    { path: "/hello", component: HelloWorld }
];

export default createRouter({
    history: createWebHistory("/map/"),
    routes: routes
});