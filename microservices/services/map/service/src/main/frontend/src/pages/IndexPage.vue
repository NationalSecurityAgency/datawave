<template>
  <q-page style="height: 100%">
    <!-- the map goes here -->
    <div
      style="
        position: absolute;
        height: 100%;
        width: 100%;
        top: 0;
        left: 0;
        z-index: 0;
      "
    >
      <LeafletMap />
    </div>

    <div
      v-if="appState.isConfigPanelEnabled"
      style="position: absolute; height: 100%; top: 0; left: 0; z-index: 1"
    >
      <ConfigPanel :supportedGeometries="supportedGeometries"/>
    </div>
  </q-page>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue';
import LeafletMap from 'components/LeafletMap.vue';
import ConfigPanel from 'components/ConfigPanel.vue';
import { appStateStore } from 'stores/state-store';
import { enableLayers, geoFeaturesStore, getVisibleBounds } from 'stores/geo-features-store';
import { GeoQueryFeatures } from 'components/models'
import { useRoute } from 'vue-router'
import { simpleMapStore } from 'stores/simple-map-store'
import { api } from 'boot/axios';

const appState = appStateStore();
const leafletMap = simpleMapStore;
const supportedGeometries = ref<string[]>();

onMounted(() => {
  const route = useRoute();
  const queryId = route.query.queryId as string;

  if (queryId != undefined) {
    const geoQueryFeatures = geoFeaturesStore();
    geoQueryFeatures.loadGeoFeaturesForQueryId(queryId)
    .then((id) => {
      console.log('Successfully loaded query. ', queryId);

      const geoFeatures = geoQueryFeatures.getGeoFeaturesById(id as string) as GeoQueryFeatures;
      for (const geoFunction of geoFeatures.functions){
        enableLayers(geoFunction);
      }

      const bounds = getVisibleBounds(geoFeatures);
      if (bounds != undefined) {
        leafletMap.fitBounds(bounds);
      }
    })
    .catch((reason) => {
      console.log('Failed to load query. ', queryId, 'Reason: ', reason);
    });
  }

  api
    .get('/map/v1/supportedGeometries', undefined)
    .then((response) => {
      supportedGeometries.value = response.data as string[];
    })
    .catch((reason) => {
      console.log('Something went wrong? ' + reason);
    });
});
</script>
