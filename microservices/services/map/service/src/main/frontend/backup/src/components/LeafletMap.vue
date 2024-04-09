<script setup lang="ts">
import { onMounted, onUnmounted, ref } from 'vue';

import 'leaflet/dist/leaflet.css'
import L from 'leaflet';

const basemaps = ref();
const mapInstance = ref<L.Map | null>(null);

basemaps.value = {
    'Open Street Map': L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
        maxZoom: 18,
        attribution: '&copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a>'
    })};

const initMap = () => {
    console.log("INIT MAP");

    const theMap = L.map('map').setView([0, 0], 3);

    // the first basemap in the list is the default
    (Object.values(basemaps.value)[0] as L.TileLayer).addTo(theMap);

    // add the basemap selector panel
    L.control.layers(basemaps.value, {}, { position: 'bottomright' }).addTo(theMap);

    mapInstance.value = theMap;
};

onMounted(() => {
    console.log("MOUNTED");
    initMap();
});

onUnmounted(() => {
    console.log("UNMOUNTED");
    if (mapInstance.value) {
        mapInstance.value.remove();
        mapInstance.value = null;
    }
});

</script>

<template>
    <div id='map' style='height: 90vh; width: 100%;'></div>
</template>

<style scoped>
.leaflet-control-layers-base {
    text-align: left;
}
</style>
