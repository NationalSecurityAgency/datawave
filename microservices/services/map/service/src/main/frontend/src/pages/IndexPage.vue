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
import LeafletMap from 'components/LeafletMap.vue';
import ConfigPanel from 'components/ConfigPanel.vue';
import { appStateStore } from 'stores/state-store';

const appState = appStateStore();
</script>
