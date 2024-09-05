<template>
  <AddContentPanel v-if="appStateStore().getConfigPanelSelection === 'Add'" />
  <ContentPanel v-else-if="appStateStore().getConfigPanelSelection === 'Content'" />
  <BasemapPanel v-else-if="appStateStore().getConfigPanelSelection === 'Basemaps'" />
</template>

<script setup lang="ts">
import { appStateStore } from 'stores/state-store';
import AddContentPanel from 'components/AddContentPanel.vue';
import ContentPanel from 'components/ContentPanel.vue';
import BasemapPanel from 'components/BasemapsPanel.vue';
</script>
