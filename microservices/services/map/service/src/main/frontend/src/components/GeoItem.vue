<template>
  <q-item class="q-pa-sm">
    <q-item-section v-if="showDrillDownButton()" avatar dense style="min-width: 40px; max-width: 40px;" class="q-pr-none">
      <q-btn icon="chevron_right" flat dense size="18px" padding="none" @click="drillDown"/>
    </q-item-section>
    <q-item-section>
      <q-item-label lines="1">
        <span class="text-weight-medium" :style="[monospaceLabel ? {fontFamily: 'monospace'} : {}]">
          <q-btn v-if="showLabelCopyButton" icon="content_copy" flat round size="8px" @click="copyLabel()" />
          {{ displayLabel }}
          <q-btn v-if="showEditLabelButton()" icon="edit" flat round dense size="8px">
            <q-popup-proxy @keyup.enter="popupRef.hide()" ref="popupRef" @hide="updateLabel">
              <q-input v-model="displayLabel" label="Label" dense outlined maxlength="20" />
            </q-popup-proxy>
          </q-btn>
        </span>
        <span v-if="showDisplayTypeName()" class="text-grey-8"> - {{ displayTypeName }}</span>
      </q-item-label>
      <q-item-label caption lines="1" style="margin-top: 0px;">
        <q-btn v-if="showCaptionCopyButton()" icon="content_copy" flat round size="8px" @click="copyCaption()" />
        {{ caption }}
      </q-item-label>
    </q-item-section>
    <q-item-section side class="q-pl-xs">
      <div class="text-grey-8">
        <q-btn class="gt-xs" size="14px" flat dense round icon="my_location" @click="zoomToBounds" />
        <q-btn class="gt-xs" size="14px" flat dense round :color="isVisible ? 'primary' : 'gray'" :icon="isVisible ? 'visibility' : 'visibility_off'" @click="toggleVisibility" />
        <q-btn class="gt-xs" size="14px" flat dense round>
          <div :style="[ { 'height': '16px', 'width': '16px' }, layerColor != undefined ? {'background-color': layerColor} : {'background-image': 'linear-gradient(to bottom right, red,orange,yellow,green,blue,indigo,violet)'} ]" />
          <q-popup-proxy>
            <q-color 
            :model-value="layerColor" 
            @change="val => { layerColor = val; setLayerColor(feature, layerColor as string) }" 
            style="max-width: 250px"/>
          </q-popup-proxy>
        </q-btn>
        <q-btn v-if="showDeleteButton()" class="gt-xs" size="14px" flat dense round icon="delete" @click="deleteFeatures" />
      </div>
    </q-item-section>
  </q-item>
</template>

<script setup lang="ts">
import { useQuasar } from 'quasar';
import { ref } from 'vue';
import { contentStore } from 'stores/content-store';
import { GEO_QUERY_FEATURES, GEO_FEATURES, FEATURE, GEO_TERMS, getTypeName, getLabel, setLabel, geoFeaturesStore, getVisibleBounds, layersVisible, layersColor, setLayerColor, enableLayers, disableLayers, GEO_FUNCTION } from 'stores/geo-features-store'
import { GeoFeatures, GeoQueryFeatures, GeoTerms, TypedFeature } from 'components/models'
import { simpleMapStore } from 'stores/simple-map-store'
import { Feature } from 'geojson';

interface GeoItem {
  feature: TypedFeature;
}

const popupRef = ref();

const leafletMap = simpleMapStore;

const props = defineProps<GeoItem>();

const feature = props.feature;

const content = contentStore();
const geoFeatures = geoFeaturesStore();

const rawTypeName = ref(getTypeName(feature));
const displayTypeName = ref(getTypeName(feature));
const displayLabel = ref('');
const caption = ref('');
const isVisible = ref(layersVisible(feature));
const monospaceLabel = ref(false);
const showLabelCopyButton = ref(false);

const layerColor = ref(layersColor(feature));

if (rawTypeName.value == GEO_QUERY_FEATURES) {
  const geoQueryFeatures = feature as GeoQueryFeatures;
  if (geoQueryFeatures.query != undefined) {
    displayTypeName.value = 'Query';
    caption.value = geoQueryFeatures.query;
  } else if (geoQueryFeatures.queryId != undefined) {
    displayTypeName.value = 'Query';
    caption.value = geoQueryFeatures.queryId;
  }
  if (geoQueryFeatures.label != undefined) {
    displayLabel.value = geoQueryFeatures.label;
  }
} else if (rawTypeName.value == GEO_FEATURES) {
  const geoFeatures = feature as GeoFeatures;
  displayTypeName.value = 'Geometry';
  caption.value = geoFeatures.geometry.wkt;
  if (geoFeatures.label != undefined) {
    displayLabel.value = geoFeatures.label;
  }
} else if (rawTypeName.value == GEO_TERMS) {
  const geoTerms = feature as GeoTerms
  const label = getLabel(feature);
  if (label != undefined) {
    displayLabel.value = label;
  }
  caption.value = geoTerms.type;
} else if (rawTypeName.value == FEATURE) {
  const simpleFeature = feature as Feature;
  if (simpleFeature.properties != undefined) {
    if ('term' in simpleFeature.properties) {
      monospaceLabel.value = true;
      displayLabel.value = simpleFeature.properties.term;
      caption.value = '' + simpleFeature.properties.fields.join(', ');
    } else if ('range' in simpleFeature.properties) {
      monospaceLabel.value = true;
      displayLabel.value = '[' + simpleFeature.properties.range[0] + ', ' + simpleFeature.properties.range[1] + ']';
      caption.value = 'Fields: ' + simpleFeature.properties.fields.join(', ');
    } else {
      displayLabel.value = simpleFeature.properties.wkt;
      showLabelCopyButton.value = true;
    }
  }
} else {
  const label = getLabel(feature);
  if (label != undefined) {
    displayLabel.value = label;
  }
  showLabelCopyButton.value = rawTypeName.value == GEO_FUNCTION;
}

function showDeleteButton(): boolean {
  return rawTypeName.value == GEO_QUERY_FEATURES || rawTypeName.value == GEO_FEATURES;
}

function showDisplayTypeName(): boolean {
  return rawTypeName.value == GEO_QUERY_FEATURES || rawTypeName.value == GEO_FEATURES;
}

function showDrillDownButton(): boolean {
  return !(rawTypeName.value == FEATURE || rawTypeName.value == GEO_FUNCTION);
}

function showCaptionCopyButton(): boolean {
  return rawTypeName.value == GEO_QUERY_FEATURES || rawTypeName.value == GEO_FEATURES;
}

function showEditLabelButton(): boolean {
  return rawTypeName.value == GEO_QUERY_FEATURES || rawTypeName.value == GEO_FEATURES;
}

function drillDown() {
  content.setCurrentFeature(feature);
}

function deleteFeatures() {
  // delete the features from the feature store
  // this also removes all of the associated layers from the map
  geoFeatures.deleteGeoFeatures(props.feature.id as string);

  // remove this entry from the content list
  content.deleteFeature(feature);
}

function zoomToBounds() {
  const bounds = getVisibleBounds(feature);
  if (bounds != undefined) {
    leafletMap.fitBounds(bounds);
  }
}

function toggleVisibility() {
  if (isVisible.value) {
    disableLayers(feature);
    isVisible.value = false;
  } else {
    enableLayers(feature);
    isVisible.value = true;
  }
}

function updateLabel() {
  setLabel(feature, displayLabel.value);
}

const $q = useQuasar();
async function copyCaption() {
    await navigator.clipboard.writeText(caption.value);
    $q.notify('Text Copied to Clipboard')
}

async function copyLabel() {
    await navigator.clipboard.writeText(displayLabel.value);
    $q.notify('Text Copied to Clipboard')
}

</script>