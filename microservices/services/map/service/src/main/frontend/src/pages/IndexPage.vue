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

    <!-- the configuration panel goes here -->
    <!-- TODO: Update this to use a component which responds to menu clicks -->
    <div
      v-if="appState.isConfigPanelEnabled"
      style="position: absolute; height: 100%; top: 0; left: 0; z-index: 1"
    >
      <ConfigPanel />
    </div>
  </q-page>
</template>

<script setup lang="ts">
import { onMounted } from 'vue';
import LeafletMap from 'components/LeafletMap.vue';
import ConfigPanel from 'components/ConfigPanel.vue';
import { appStateStore } from 'stores/state-store';
import { enableLayers, geoFeaturesStore, getVisibleBounds } from 'stores/geo-features-store';
import { GeoQueryFeatures } from 'components/models'
import { useRoute } from 'vue-router'
import { simpleMapStore } from 'stores/simple-map-store'

const appState = appStateStore();
const leafletMap = simpleMapStore;

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
});
</script>
