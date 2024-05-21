<template>
  <!-- <div :id="mapId" style='min-height: inherit; height: calc(100vh - 50px); width: 100%;'></div> -->
  <div :id="mapId" style="height: 100%; width: 100%"></div>
</template>

<script setup lang="ts">
import { onMounted, onUnmounted, ref } from 'vue';
import 'leaflet/dist/leaflet.css';
import L, { Layer } from 'leaflet';
import { geoFeaturesStore } from 'stores/geo-features-store';
import { GeoQueryFeatures } from './models';
import { GeoJsonObject } from 'geojson';
import { GeoFeatures } from './models';

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
  tileLayers: () => [
    {
      title: 'Open Street Map',
      tileLayer: L.tileLayer('https://tile.openstreetmap.org/{z}/{x}/{y}.png', {
        maxZoom: 18,
        attribution:
          '&copy; <a href="http://www.openstreetmap.org/copyright">OpenStreetMap</a>',
      }),
      default: true,
    },
  ],
  enableLayerControl: false,
  enableZoomControl: false,
  enableAttribution: false,
});

const mapInstance = ref<L.Map | null>(null);

const loadedQueries = ref<[string]>();

const initMap = () => {
  const bounds = L.latLngBounds(L.latLng(-90, -360), L.latLng(90, 360));

  // create the map and the default view
  const theMap = L.map(props.mapId, {
    attributionControl: props.enableAttribution,
    zoomControl: props.enableZoomControl,
    maxBounds: bounds,
    minZoom: 3,
  }).setView([0, 0], 3);

  // add the basemap selector panel to the map
  const layersControl = props.enableLayerControl
    ? L.control.layers().addTo(theMap)
    : null;

  // populate the base layers
  props.tileLayers.forEach(function (tileLayer) {
    if (tileLayer.default) {
      tileLayer.tileLayer.addTo(theMap);
    }
    if (layersControl != null) {
      layersControl.addBaseLayer(tileLayer.tileLayer, tileLayer.title);
    }
  });

  mapInstance.value = theMap;

  const geoQueryFeatures = geoFeaturesStore();

  window.onresize = () => {
    console.log('invalidating size');
    theMap.invalidateSize(true);
  };

  // watch for the addition of new geo query features
  geoQueryFeatures.$subscribe((mutation, state) => {

    // load all of the geo features for all queries
    for (const queryId in state.geoQueryFeatures) {

      // have we already loaded this queryId?
      if (!loadedQueries.value?.includes(queryId)) {

        // if this is a GeoQueryFeature
        if ('geoByField' in state.geoQueryFeatures[queryId]) {
          const geoQueryFeatures = state.geoQueryFeatures[queryId] as GeoQueryFeatures;

          // keep track of all layers createdfor this query
          let layers: Layer[] = [];

          // add the queryId to the array of loaded queries
          loadedQueries.value?.push(queryId);
          
          // handle the geo functions
          for (const funcGeo of geoQueryFeatures.functions) {
            let funcLayer = L.geoJSON(funcGeo.geoJson as GeoJsonObject);
            layers.push(funcLayer)
          }

          // handle the geo by field
          for (const field in geoQueryFeatures.geoByField) {
            let fieldGeo = geoQueryFeatures.geoByField[field];
            
            // load the overall geo for this field
            if (fieldGeo.geo) {
              let fieldGeoLayer = L.geoJSON(fieldGeo.geo.geoJson as GeoJsonObject);
              layers.push(fieldGeoLayer);
            }

            // load the geo by tier
            if (fieldGeo.geoByTier) {
              for (const tierKey in fieldGeo.geoByTier) {
                let tierGeo = fieldGeo.geoByTier[tierKey];
                let tierGeoLayer = L.geoJSON(tierGeo.geoJson as GeoJsonObject);
                layers.push(tierGeoLayer);
              }
            }
          }

          // load the layers as a group
          L.layerGroup(layers).addTo(theMap);
        } 
        // if this is a GeoFeature
        else {
          const geoFeatures = state.geoQueryFeatures[queryId] as GeoFeatures;

          // keep track of all layers createdfor this query
          let layers: Layer[] = [];

          // add the queryId to the array of loaded queries
          loadedQueries.value?.push(queryId);

          // load the overall geo
          if (geoFeatures.geometry) {
            let fieldGeoLayer = L.geoJSON(geoFeatures.geometry.geoJson as GeoJsonObject);
            layers.push(fieldGeoLayer);
          }

          // load the geo query ranges
          if (geoFeatures.queryRanges) {
            let queryRanges = geoFeatures.queryRanges;
            
            // load the overall geo for this field
            if (queryRanges.geo) {
              let fieldGeoLayer = L.geoJSON(queryRanges.geo.geoJson as GeoJsonObject);
              layers.push(fieldGeoLayer);
            }

            // load the geo by tier
            if (queryRanges.geoByTier) {
              for (const tierKey in queryRanges.geoByTier) {
                let tierGeo = queryRanges.geoByTier[tierKey];
                let tierGeoLayer = L.geoJSON(tierGeo.geoJson as GeoJsonObject);
                layers.push(tierGeoLayer);
              }
            }
          }

          // load the layers as a group
          L.layerGroup(layers).addTo(theMap);
        }
      }
    }
  });
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
