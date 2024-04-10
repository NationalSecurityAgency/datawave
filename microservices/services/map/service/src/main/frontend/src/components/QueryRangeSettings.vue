<template>
  <div class="row q-pt-md">
    <strong>Basic Settings:</strong>
  </div>
  <div class="row">
    <div class="col">
      <q-input
        v-model="queryRangeSettings[rangeType].maxEnvelopes"
        style="max-width: 200px"
        label="Max Envelopes"
      />
    </div>
    <div class="col">
      <q-input
        v-model="queryRangeSettings[rangeType].maxExpansion"
        style="max-width: 200px"
        label="Max Expansion"
      />
    </div>
  </div>
  <div class="row q-pt-md">
    <div class="col">
      <strong>Advanced Settings:</strong>
    </div>
    <div class="col">
      <q-toggle
        v-model="queryRangeSettings[rangeType].optimizeRanges"
        label="Optimize Ranges"
      />
    </div>
  </div>
  <div v-if="props.rangeType != GEO_POINT" class="row">
    <div class="col">
      <q-input
        v-model="queryRangeSettings[rangeType].rangeSplitThreshold"
        :disable="!queryRangeSettings[rangeType].optimizeRanges"
        style="max-width: 200px"
        label="Range Split Threshold"
      />
    </div>
    <div class="col">
      <q-input
        v-model="queryRangeSettings[rangeType].maxRangeOverlap"
        :disable="!queryRangeSettings[rangeType].optimizeRanges"
        style="max-width: 200px"
        label="Max Range Overlap"
      />
    </div>
  </div>
</template>

<script setup lang="ts">
import { getActivePinia } from 'pinia';
import {
  queryRangeSettingsStore,
  GEO_POINT,
  QueryRangeSettings,
} from 'stores/query-range-settings-store';
import { onUnmounted } from 'vue';

interface Props {
  rangeType: string;
}
const props = defineProps<Props>();

const queryRangeSettings = queryRangeSettingsStore(123);

function getQueryRangeSettings() {
  const retVal = {
    maxEnvelopes: queryRangeSettings[props.rangeType].maxEnvelopes,
    maxExpansion: queryRangeSettings[props.rangeType].maxExpansion,
    optimizeRanges: queryRangeSettings[props.rangeType].optimizeRanges,
  } as QueryRangeSettings;

  if (props.rangeType != GEO_POINT) {
    retVal.rangeSplitThreshold =
      queryRangeSettings[props.rangeType].rangeSplitThreshold;
    retVal.maxRangeOverlap =
      queryRangeSettings[props.rangeType].maxRangeOverlap;
  }
  return retVal;
}

onUnmounted(() => {
  // remove store from pinia registry
  const id = queryRangeSettings.$id;
  queryRangeSettings.$dispose();

  // delete the data stored in pinia
  const pinia = getActivePinia();
  pinia && delete pinia.state.value[id];
});

defineExpose({
  getQueryRangeSettings,
});
</script>
