<template>
  <div style="height: 100%; width: 500px; background: white">
    <q-card square class="bg-secondary text-white q-px-md q-py-sm">
      <q-card-section horizontal>
        <div class="text-h6" style="flex-grow: 1">Geo Toolbox</div>
        <q-btn
          flat
          round
          size="10px"
          icon="close"
          @click="appState.disableConfigPanel"
        />
      </q-card-section>
    </q-card>
    <q-list bordered class="q-pa-none">
      <!-- Get index entries for geometry -->
      <q-expansion-item
        group="contentgroup"
        icon="grid_view"
        label="Geo Index"
        header-class="text-secondary"
      >
        <GeoIndexForm :supportedGeometries="supportedGeometries" />
      </q-expansion-item>
    </q-list>
  </div>
</template>

<script setup lang="ts">
import { appStateStore } from 'stores/state-store';
import GeoIndexForm from 'components/GeoIndexForm.vue';

const appState = appStateStore();

export interface GeoToolboxPanelProps {
  supportedGeometries: string[];
}
defineProps<GeoToolboxPanelProps>();

</script>

