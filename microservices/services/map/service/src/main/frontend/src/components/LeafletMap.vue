<template>
    <!-- <div :id="mapId" style='min-height: inherit; height: calc(100vh - 50px); width: 100%;'></div> -->
    <div :id="mapId" style='height: 100%; width: 100%;'></div>
</template>

<script setup lang="ts">
import { onMounted, onUnmounted, ref } from 'vue';
import 'leaflet/dist/leaflet.css'
import L from 'leaflet';
import { geoFeaturesStore } from 'stores/geo-features-store';

interface TileLayerProps {
  title: string;
  tileLayer: L.TileLayer;
  default: boolean;
}
interface Props {
    mapId?: string;
    tileLayers?: TileLayerProps[];
    enableLayerControl?: boolean;
    enableZoomControl?: boolean;
    enableAttribution?: boolean;
}
const props = withDefaults(defineProps<Props>(), {
    mapId: 'map',
    tileLayers: () => [{
        title: 'Open Street Map',
        tileLayer: L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
            maxZoom: 18,
            attribution: '&copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a>'}),
        default: true,
    }],
    enableLayerControl: false,
    enableZoomControl: false,
    enableAttribution: false,
});

const mapInstance = ref<L.Map | null>(null);

const initMap = () => {
    const bounds = L.latLngBounds(L.latLng(-90, -360), L.latLng(90, 360));

    // create the map and the default view
    const theMap = L.map(props.mapId, {attributionControl: props.enableAttribution, zoomControl: props.enableZoomControl, maxBounds: bounds, minZoom: 3}).setView([0, 0], 3);

    // add the basemap selector panel to the map
    const layersControl = props.enableLayerControl ? L.control.layers().addTo(theMap) : null;

    // populate the base layers
    props.tileLayers.forEach(function(tileLayer) {
        if (tileLayer.default) {
            tileLayer.tileLayer.addTo(theMap);
        }
        if (layersControl != null){
            layersControl.addBaseLayer(tileLayer.tileLayer, tileLayer.title);
        }
    });

    // // the first basemap in the list is the default
    // (Object.values(props.tileLayersMap)[0] as L.TileLayer).addTo(theMap);

    // // add the basemap selector panel
    // L.control.layers(basemaps.value, {}, { position: 'bottomright' }).addTo(theMap);

    mapInstance.value = theMap;

    const geoQueryFeatures = geoFeaturesStore();

    console.log('getGeoFeaturesForQuery!');
    let fieldTypes = new Map<string, string[]>([
        ['GEOMETRY_FIELD', ['datawave.data.type.GeometryType']]
    ]);

    geoQueryFeatures.loadGeoFeaturesForQuery('geowave:intersects(GEOMETRY_FIELD, "POLYGON((-180 -90, 180 -90, 180 90, -180 90, -180 -90))")', fieldTypes)
        .then((data) => {
            console.log('getGeoFeaturesForQuery response: ' + data);
            console.log(geoQueryFeatures.getGeoFeatures);
        });

    window.onresize = () => {
        console.log('invalidating size');
        theMap.invalidateSize(true);
    }
};

onMounted(() => {
    initMap();
});

onUnmounted(() => {
    if (mapInstance.value) {
        mapInstance.value.remove();
        mapInstance.value = null;
    }
});



</script>
