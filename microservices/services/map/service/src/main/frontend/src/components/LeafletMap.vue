<template>
  <div :id="mapId" style="height: 100%; width: 100%"></div>
</template>

<script setup lang="ts">
import { onMounted } from 'vue';
import 'leaflet/dist/leaflet.css';
import { simpleMapStore } from 'stores/simple-map-store'
import L from 'leaflet';
import markerIcon from 'leaflet/dist/images/marker-icon.png'
import markerShadow from 'leaflet/dist/images/marker-shadow.png'
import { basemapsStore } from 'stores/basemaps-store'
import { addFeatureInfoPanel } from 'stores/feature-info-store'

interface Props {
  mapId?: string;
  enableLayerControl?: boolean;
  enableZoomControl?: boolean;
  enableAttribution?: boolean;
}
const props = withDefaults(defineProps<Props>(), {
  mapId: 'map',
  enableLayerControl: false,
  enableZoomControl: false,
  enableAttribution: false,
});

// this is needed to make markers work after the distributable is built
L.Marker.prototype.setIcon(L.icon({
  iconUrl:markerIcon,
  shadowUrl:markerShadow
}));

const leafletMap = simpleMapStore;
const basemaps = basemapsStore();

const initMap = () => {
  basemaps.initialize()
    .then(() => {
      const bounds = L.latLngBounds(L.latLng(-90, -360), L.latLng(90, 360));

      leafletMap.createMap(props.mapId, props.enableAttribution, props.enableZoomControl, bounds, 2);

      leafletMap.setView([0, 0], 3);

      // add the basemap selector panel to the map
      leafletMap.enableLayerControl(props.enableLayerControl);

      // populate the base layers
      leafletMap.setBasemap(basemaps.getBasemap);

      // add the feature info panel
      addFeatureInfoPanel(leafletMap);

      window.onresize = () => {
        leafletMap.invalidateSize(true);
      };
    })
    .catch((reason) => {
      console.log('Unable to initialize basemaps. ', reason);
    });
};

onMounted(() => {
  initMap();
});
</script>

<style>
/* style for the info box on the query geometry map */
.info {
    padding: 6px 8px;
    font: 14px/16px Arial, Helvetica, sans-serif;
    background: white;
    background: rgba(255,255,255,0.8);
    box-shadow: 0 0 15px rgba(0,0,0,0.2);
    border-radius: 5px;
    max-width: 33vw;
    word-wrap: break-word;
}

.info h4 {
    font-size: 14px;
    font-weight: bold;
    line-height: 16px;
    text-align: center;
    margin: 0 0 5px;
    color: #777;
}
</style>
