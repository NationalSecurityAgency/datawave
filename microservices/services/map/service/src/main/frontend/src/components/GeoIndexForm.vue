<template>
  <q-card>
    <q-card-section class="q-py-none" :style="'opacity: ' + cardOpacity">
      <q-form>
        <div class="q-py-sm">
          <q-file
            ref="fileRef"
            v-model="fileModel"
            @update:model-value="loadFile"
            style="display: none"
          />
          <q-input v-model="geometry" :label='"Geometry (" + supportedGeometries.join("/") + ")"' spellcheck="false" >
            <template v-slot:append>
              <q-btn
                dense
                flat
                icon="edit"
                @click="
                  editDialog = true;
                  editDialogText = geometry;
                "
              />
              <q-btn dense flat icon="upload" @click="selectFile" />
            </template>
          </q-input>
        </div>
      </q-form>
      <q-card-actions align="right" class="text-primary">
        <q-btn flat label="Reset" @click="geometry = ''; clearDisplay()" />
        <q-btn flat label="Load" @click="submitGeometry" />
      </q-card-actions>
      <q-separator inset />
      <div class="q-py-md">
        <div class="row q-pt-md">
          <q-btn icon="content_copy" flat round size="8px" @click="copyWkt()" /><strong>Well-Known Text (WKT):</strong>
        </div>
        <q-scroll-area :content-style="contentStyle" :content-active-style="contentActiveStyle" style="height: 100px; max-width: 466px;">
          <div class=q-pa-xs style="max-width: 466px;" >{{ wkt }}</div>
        </q-scroll-area>
        <div class="row q-pt-md">
          <q-btn icon="content_copy" flat round size="8px" @click="copyGeoPointIndex()" /><strong>Geo Point Index:</strong>
        </div>
        <q-scroll-area :content-style="contentStyle" :content-active-style="contentActiveStyle" style="height: 25px; max-width: 466px;">
          <div class=q-pa-xs>{{ geoPointIndex }}</div>
        </q-scroll-area>
        <div class="row q-pt-md">
          <q-btn icon="content_copy" flat round size="8px" @click="copyGeoWavePointIndex()" /><strong>GeoWave Point Index:</strong>
        </div>
        <q-scroll-area :content-style="contentStyle" :content-active-style="contentActiveStyle" style="height: 25px; max-width: 466px;">
          <div class=q-pa-xs>{{ geoWavePointIndex }}</div>
        </q-scroll-area>
        <div class="row q-pt-md">
          <q-btn icon="content_copy" flat round size="8px" @click="copyGeoWaveGeometryIndices()" /><strong>GeoWave Geometry Indices:</strong>
        </div>
        <q-scroll-area :content-style="contentStyle" :content-active-style="contentActiveStyle" style="height: 100px; max-width: 466px;">
          <div class=q-pa-xs>{{ geoWaveGeometryIndices }}</div>
        </q-scroll-area>
      </div>
    </q-card-section>

    <CardLoading ref="cardLoading" @doneClick="cardOpacity=1.0; if (success) { geometry = ''; };" />
  </q-card>
  <q-dialog v-model="editDialog">
    <q-card style="min-width: 66%">
      <q-card-section>
        <div class="text-h6">Geometry</div>
      </q-card-section>

      <q-card-section>
        <q-input
          filled
          type="textarea"
          :rows="30"
          v-model="editDialogText"
          autofocus
        />
      </q-card-section>

      <q-card-actions align="right" class="text-primary">
        <q-btn flat label="Reset" @click="editDialogText = ''; clearDisplay()" />
        <q-btn flat label="Cancel" v-close-popup @click="editDialogText = ''" />
        <q-btn
          flat
          label="Save"
          v-close-popup
          @click="
            geometry = editDialogText;
            editDialogText = '';
          "
        />
      </q-card-actions>
    </q-card>
  </q-dialog>
</template>

<script setup lang="ts">
import { useQuasar } from 'quasar';
import axios from 'axios';
import { api } from 'boot/axios';
import { QFile } from 'quasar';
import { ref } from 'vue';
import CardLoading, { CardLoadingMethods } from 'components/CardLoading.vue';
import { GeoIndices } from 'components/models'

export interface GeoIndexFormProps {
  supportedGeometries: string[];
}
defineProps<GeoIndexFormProps>();

const cardLoading = ref<CardLoadingMethods>();
const cardOpacity = ref<number>(1.0);

const editDialog = ref<boolean>(false);
const editDialogText = ref<string>('');

const fileModel = ref<File>();
const fileRef = ref<QFile>();
const fileURL = ref<string>();

const geometry = ref<string>('');

const wkt = ref<string>('');
const geoPointIndex = ref<string>('');
const geoWavePointIndex = ref<string>('');
const geoWaveGeometryIndices = ref<string>('');

const contentStyle = {
  backgroundColor: 'rgba(0,0,0,0.02)',
  color: '#555'
};

const contentActiveStyle = {
  backgroundColor: '#eee',
  color: 'black'
};

function clearDisplay() {
  wkt.value = ''
  geoPointIndex.value = '';
  geoWavePointIndex.value = '';
  geoWaveGeometryIndices.value = '';
}

function selectFile() {
  if (fileRef.value) {
    fileRef.value.pickFiles();
  }
}

function loadFile(selectedFile: File) {
  if (selectedFile) {
    if (fileURL.value) {
      URL.revokeObjectURL(fileURL.value);
    }
    fileURL.value = URL.createObjectURL(selectedFile);

    // load the file from the user's local filesystem
    axios
      .create({ baseURL: fileURL.value })
      .get('')
      .then((response) => {
        if (fileURL.value) {
          URL.revokeObjectURL(fileURL.value);
          if (fileModel.value && fileRef.value) {
            fileRef.value.removeFile(fileModel.value);
            fileModel.value = undefined;
          }
          fileURL.value = undefined;
          geometry.value = response.data;
        }
      })
      .catch((reason) => {
        console.log('Something went wrong? ' + reason);
      });
  }
}

const success = ref(false);

function submitGeometry() {
  cardOpacity.value = 0.05;
  cardLoading.value?.loading('Loading indices.  Please wait...');
  success.value = false;

  const formData = new FormData();
  formData.append('geometry', geometry.value);

  api
    .post('/map/v1/geoIndicesForGeometry', formData, undefined)
    .then((response) => {
      cardLoading.value?.success('Indices loaded successfully!');
      success.value = true;

      const geoIndices = response.data as GeoIndices;

      console.log(geoIndices);

      if (geoIndices.wkt != undefined){
        wkt.value = geoIndices.wkt;
      }
      if (geoIndices.geoPointIndex != undefined) {
        geoPointIndex.value = geoIndices.geoPointIndex;
      }
      if (geoIndices.geoWavePointIndex != undefined) {
        geoWavePointIndex.value = geoIndices.geoWavePointIndex;
      }
      if (geoIndices.geoWaveGeometryIndex != undefined) {
        geoWaveGeometryIndices.value = geoIndices.geoWaveGeometryIndex.join(', ');
      }
    })
    .catch(() => {
      cardLoading.value?.failure('Failed to load indices.');
    });
}


const $q = useQuasar();
async function copyWkt() {
    await navigator.clipboard.writeText(wkt.value);
    $q.notify('Text Copied to Clipboard')
}

async function copyGeoPointIndex() {
    await navigator.clipboard.writeText(geoPointIndex.value);
    $q.notify('Text Copied to Clipboard')
}

async function copyGeoWavePointIndex() {
    await navigator.clipboard.writeText(geoWavePointIndex.value);
    $q.notify('Text Copied to Clipboard')
}

async function copyGeoWaveGeometryIndices() {
    await navigator.clipboard.writeText(geoWaveGeometryIndices.value);
    $q.notify('Text Copied to Clipboard')
}
</script>

<style>
/* override quasar's styling to remove the textarea resize button */
.q-textarea .q-field__native {
  resize: none;
}
</style>
