<template>
  <div style="height: 100%; width: 500px; max-width: 500px; background: white; display: flex; flex-direction: column">
    <q-card square class="bg-secondary text-white q-px-md q-py-sm">
      <q-card-section horizontal>
        <div class="text-h6" style="flex-grow: 1">Content</div>
        <q-btn
          flat
          round
          size="10px"
          icon="close"
          @click="appState.disableConfigPanel"
        />
      </q-card-section>
    </q-card>
    <q-bar dark class="bg-primary text-white">
      <q-btn dense flat icon="home" @click="initContent" />
      <q-btn v-if="content.getFeatureChain.length > 0" dense flat icon="arrow_back" @click="content.navigateBack" />
      <GeoNav 
          v-if="content.getFeatureChain.length > 0"
          v-bind="content.getRootFeature"
        />
    </q-bar>
    <q-scroll-area style="flex-grow: 1;" content-style="width: 500px; max-weidth: 500px;">
      <q-list bordered separator style="width: 500px; max-width: 500px;">
        <q-item-label v-if="content.getFeatureChain.length > 1" header>{{ getLabel(content.getCurrentFeature) }}</q-item-label>
        <GeoItem
            v-for="typedFeature in content.getContent"
            :key="getLabel(typedFeature)"
            v-bind="{feature: typedFeature}"
          />
      </q-list>
    </q-scroll-area>
  </div>
</template>

<script setup lang="ts">
import { onMounted } from 'vue';
import { appStateStore } from 'stores/state-store';
import { getLabel } from 'stores/geo-features-store';
import { contentStore } from 'stores/content-store';
import GeoItem from 'components/GeoItem.vue';
import GeoNav from 'components/GeoNav.vue';

const appState = appStateStore();
// const geoFeatures = geoFeaturesStore();
const content = contentStore();

function initContent() {
  content.initialize();
}

onMounted(() => {
  initContent();
});

</script>
