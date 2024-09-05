<template>
    <q-item class="q-pa-sm" clickable v-ripple @click="itemClick">
        <q-item-section avatar dense>
            <q-radio ref="radio" v-model="selection" :val="basemap.title" @click="enableBasemap" />
        </q-item-section>
        <q-item-section>
            <q-item-label lines="1">
                {{ displayLabel }}
            </q-item-label>
            <q-item-label caption lines="3">
                <div v-html="caption" />
            </q-item-label>
        </q-item-section>
        <q-item-section side>
          <div style="width: 64px; height: 64px; overflow: hidden;">
            <img :src="thumbnailUrl" />
          </div>
        </q-item-section>
    </q-item>
  </template>
  
  <script setup lang="ts">
  import { ref, Ref } from 'vue';
  import { basemapsStore, Basemap } from 'stores/basemaps-store'
  import { simpleMapStore } from 'stores/simple-map-store'
  import { QRadio } from 'quasar';

  interface BasemapItem {
    basemap: Basemap;
    selection: Ref<string>;
  }

  const radio = ref<QRadio>();

  const leafletMap = simpleMapStore;
  const basemaps = basemapsStore();

  const props = defineProps<BasemapItem>();
  
  const basemap = props.basemap;
  const selection = props.selection;

  const displayLabel = ref(props.basemap.title);
  const caption = ref((props.basemap.tileLayer.getAttribution) ? props.basemap.tileLayer.getAttribution() : '');

  const thumbnailUrl = createThumbnailUrl();

  function createThumbnailUrl() {
    let url = basemap.urlTemplate.replace('{x}', '18').replace('{y}', '24').replace('{z}', '6');
    if (!url.endsWith('.png')) {
      url += '.png';
    }
    return url;
  }

  function itemClick() {
    radio.value?.set();
    enableBasemap();
  }

  function enableBasemap() {
    // if we aren't currently selected then set the basemap, otherwise do nothing
    if (basemaps.getBasemap.title != basemap.title) {
      basemaps.setBasemap(basemap);
      leafletMap.setBasemap(basemap);
    }
  }

</script>