<template>
  <AddContentPanel v-if="appStateStore().getConfigPanelSelection === 'Add'" />
  <LayersPanel
    v-else-if="appStateStore().getConfigPanelSelection === 'Layers'"
  />
</template>

<script setup lang="ts">
import { appStateStore } from 'stores/state-store';
import AddContentPanel from 'components/AddContentPanel.vue';
import LayersPanel from 'components/LayersPanel.vue';
</script>
