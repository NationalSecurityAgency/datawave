<template>
    <q-card>
        <q-card-section class="q-py-none">
            <q-form>
                <div class="q-py-sm">
                    <q-file ref="fileRef" v-model="fileModel" @update:model-value="loadFile" style="display: none;" />
                    <q-input v-model="manualGeometryForm.geometry" label="Geometry">
                        <template v-slot:append>
                            <q-btn dense flat icon="edit" @click="editDialog=true; editDialogText=manualGeometryForm.geometry" />
                            <q-btn dense flat icon="upload" @click="selectFile" />
                        </template>
                    </q-input>
                </div>
                <div class="q-py-sm">
                    Geometry Type:
                    <q-option-group name="geometryType" v-model="manualGeometryForm.geometryType" :options="GEOMETRY_TYPE_OPTIONS" inline />
                </div>
                <div class="q-py-sm">
                    <q-toggle v-model="manualGeometryForm.createRanges" label="Create Query Ranges" />
                </div>
                <q-separator inset v-if="manualGeometryForm.createRanges" />
                <div v-if="manualGeometryForm.createRanges">
                    <div class="q-py-sm">
                        <q-btn-toggle v-model="manualGeometryForm.rangeType" :options="RANGE_TYPE_OPTIONS" spread no-caps padding="xs" />
                    </div>
                    <div>
                        <div class="row q-pt-md">
                            <strong>Basic Settings:</strong>
                        </div>
                        <div class="row">
                            <div class="col">
                                <q-input v-model="manualGeometryForm.rangeSettings[manualGeometryForm.rangeType].maxEnvelopes" style="max-width: 200px" label="Max Envelopes" />
                            </div>
                            <div class="col">
                                <q-input v-model="manualGeometryForm.rangeSettings[manualGeometryForm.rangeType].maxExpansion" style="max-width: 200px" label="Max Expansion" />
                            </div>
                        </div>
                        <div class="row q-pt-md">
                            <div class="col">
                                <strong>Advanced Settings:</strong>
                            </div>
                            <div class="col">
                                <q-toggle v-model="manualGeometryForm.rangeSettings[manualGeometryForm.rangeType].optimizeRanges" label="Optimize Ranges" />
                            </div>
                        </div>
                        <div v-if="manualGeometryForm.rangeType != GEO_POINT" class="row">
                            <div class="col">
                                <q-input v-model="manualGeometryForm.rangeSettings[manualGeometryForm.rangeType].rangeSplitThreshold" :disable="!manualGeometryForm.rangeSettings[manualGeometryForm.rangeType].optimizeRanges" style="max-width: 200px" label="Range Split Threshold" />
                            </div>
                            <div class="col">
                                <q-input v-model="manualGeometryForm.rangeSettings[manualGeometryForm.rangeType].maxRangeOverlap" :disable="!manualGeometryForm.rangeSettings[manualGeometryForm.rangeType].optimizeRanges" style="max-width: 200px" label="Max Range Overlap" />
                            </div>
                        </div>
                    </div>
                </div>
            </q-form>
            <q-card-actions align="right" class="text-primary">
                <q-btn flat label="Reset" @click="manualGeometryForm.$reset()" />
                <q-btn flat label="Save" @click="submitGeometry" />
            </q-card-actions>
        </q-card-section>
    </q-card>
    <q-dialog v-model="editDialog">
        <q-card style="min-width: 66%;">
            <q-card-section>
                <div class="text-h6">Geometry</div>
            </q-card-section>

            <q-card-section >
                <q-input filled type="textarea" :rows=30 v-model="editDialogText" autofocus />
            </q-card-section>

            <q-card-actions align="right" class="text-primary">
                <q-btn flat label="Reset" @click="editDialogText=''" />
                <q-btn flat label="Cancel" v-close-popup @click="editDialogText=''" />
                <q-btn flat label="Save" v-close-popup @click="manualGeometryForm.geometry=editDialogText; editDialogText=''" />
            </q-card-actions>
        </q-card>
    </q-dialog>
</template>

<!-- TODO: ADD FORM VALIDATION -->

<script setup lang="ts">
import { getActivePinia } from 'pinia';
import axios from 'axios';
import { QFile } from 'quasar';
import { onUnmounted, ref } from 'vue';
import { manualGeometryFormStore, GEO_POINT, GEOMETRY_TYPE_OPTIONS, RANGE_TYPE_OPTIONS } from 'stores/manual-geometry-store';
import { geoFeaturesStore } from 'stores/geo-features-store';

const manualGeometryForm = manualGeometryFormStore('abc-123');
const geoQueryFeatures = geoFeaturesStore();

const editDialog = ref<boolean>(false);
const editDialogText = ref<string>('');

const fileModel = ref<File>();
const fileRef = ref<QFile>();
const fileURL = ref<string>();

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
        axios.create({ baseURL: fileURL.value }).get('')
        .then((response) => {
            if (fileURL.value){
                URL.revokeObjectURL(fileURL.value);
                if (fileModel.value && fileRef.value) {
                    fileRef.value.removeFile(fileModel.value);
                    fileModel.value = undefined;
                }
                fileURL.value = undefined;
                manualGeometryForm.geometry = response.data;
            }
        })
        .catch((reason) => {
            console.log('Something went wrong? ' + reason);
        })
    }
}

function submitGeometry() {
    console.log('not empty anymore am i');
    geoQueryFeatures.loadGeoFeaturesFromGeometry(manualGeometryForm.$state);
}

onUnmounted(() => {
    // remove store from pinia registry
    const id = manualGeometryForm.$id;
    manualGeometryForm.$dispose();

    // delete the data stored in pinia
    const pinia = getActivePinia();
    pinia && delete pinia.state.value[id];
});
</script>

<style>
/* override quasar's styling to remove the textarea resize button */
.q-textarea .q-field__native {
    resize: none;
}</style>
