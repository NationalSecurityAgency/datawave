<template>
  <q-card>
    <q-card-section class="q-py-none">
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
        <q-btn flat label="Save" @click="submitQuery" />
      </q-card-actions>
    </q-card-section>
  </q-card>
</template>

<script setup lang="ts">
import { ref } from 'vue';
import { geoFeaturesStore } from 'stores/geo-features-store';

const geoQueryFeatures = geoFeaturesStore();

const queryId = ref('');

function submitQuery() {
  geoQueryFeatures.loadGeoFeaturesForQuery(
    manualQueryForm.query,
    getFieldTypeMap()
  );
}
</script>

<style>
</style>
