<template>
  <div style="height: 100%; width: 500px; max-width: 500px; background: white; display: flex; flex-direction: column">
    <q-card square class="bg-secondary text-white q-px-md q-py-sm">
      <q-card-section horizontal>
        <div class="text-h6" style="flex-grow: 1">Basemaps</div>
        <q-btn
          flat
          round
          size="10px"
          icon="close"
          @click="appState.disableConfigPanel"
        />
      </q-card-section>
    </q-card>
    <q-scroll-area style="flex-grow: 1;" content-style="width: 500px; max-weidth: 500px;">
      <q-list bordered separator style="width: 500px; max-width: 500px;">
        <BasemapItem
            v-for="basemap in basemaps.getBasemaps"
            :key="basemap.title"
            v-bind="{ basemap: basemap, selection: selectionRef() }"
          />
      </q-list>
    </q-scroll-area>
  </div>
</template>

<script setup lang="ts">
import { ref, Ref } from 'vue';
import { appStateStore } from 'stores/state-store';
import { basemapsStore } from 'stores/basemaps-store';
import BasemapItem from 'components/BasemapItem.vue';

const appState = appStateStore();
const basemaps = basemapsStore();

const selection = ref(basemaps.getBasemap.title);

function selectionRef(): Ref<string> {
  return selection;
}

</script>
