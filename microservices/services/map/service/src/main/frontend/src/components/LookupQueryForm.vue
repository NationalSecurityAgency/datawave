<template>
  <q-card>
    <q-card-section class="q-py-none" :style="'opacity: ' + cardOpacity">
      <q-form>
        <div class="q-py-sm">
          <q-input v-model="queryId" label="Query Id" />
        </div>
      </q-form>
      <q-card-actions align="right" class="text-primary">
        <q-btn
          flat
          label="Reset"
          @click="
            queryId = '';
          "
        />
        <q-btn flat label="Load" @click="submitQuery" />
      </q-card-actions>
    </q-card-section>

    <CardLoading ref="cardLoading" @doneClick="cardOpacity=1.0;" />
  </q-card>
</template>

<script setup lang="ts">
import { ref } from 'vue';
import { geoFeaturesStore } from 'stores/geo-features-store';
import CardLoading, { CardLoadingMethods } from 'components/CardLoading.vue';

const geoQueryFeatures = geoFeaturesStore();

const cardLoading = ref<CardLoadingMethods>();
const cardOpacity = ref<number>(1.0);

const queryId = ref('');

function submitQuery() {
  cardOpacity.value = 0.05;
  cardLoading.value?.loading('Loading geometry.  Please wait...');

  geoQueryFeatures.loadGeoFeaturesForQueryId(queryId.value)
    .then((id) => {
      cardLoading.value?.success('Geometry loaded successfully!');
    })
    .catch((reason) => {
      cardLoading.value?.failure('Failed to load geometry.');
    });
}
</script>

<style>
</style>
