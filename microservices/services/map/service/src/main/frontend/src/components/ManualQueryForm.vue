<template>
  <q-card>
    <q-card-section class="q-py-none">
      <q-form>
        <div class="q-py-sm">
          <q-file
            ref="fileRef"
            v-model="fileModel"
            @update:model-value="loadFile"
            style="display: none"
          />
          <q-input v-model="manualQueryForm.query" label="Query">
            <template v-slot:append>
              <q-btn
                dense
                flat
                icon="edit"
                @click="
                  editDialog = true;
                  editDialogText = manualQueryForm.query;
                "
              />
              <q-btn dense flat icon="upload" @click="selectFile" />
            </template>
          </q-input>
        </div>
        <div class="q-py-sm">
          Geo Fields:
          <q-list>
            <FieldTypeInput
              v-for="fieldType in manualQueryForm.fieldTypes"
              :key="fieldType.id"
              v-bind="fieldType"
              :id="fieldType.id"
              :onDelete="(id: number) => {manualQueryForm.fieldTypes.splice(manualQueryForm.fieldTypes.findIndex((element) => element.id == id), 1)}"
              @update="(emittedFieldType: FieldType) => {fieldType.field = emittedFieldType.field; fieldType.type = emittedFieldType.type}"
            />
          </q-list>
          <div
            class="q-pt-md"
            style="display: flex; flex-direction: column; align-items: center"
          >
            <q-btn
              icon="add_circle"
              label="Add Geo Field"
              @click="
                manualQueryForm.fieldTypes.push({
                  id: fieldNum++,
                  field: '',
                  type: '',
                } as FieldType)
              "
            />
          </div>
        </div>
      </q-form>
      <q-card-actions align="right" class="text-primary">
        <q-btn
          flat
          label="Reset"
          @click="
            manualQueryForm.$reset();
            fieldNum = 0;
          "
        />
        <q-btn flat label="Save" @click="submitQuery" />
      </q-card-actions>
    </q-card-section>
  </q-card>
  <q-dialog v-model="editDialog">
    <q-card style="min-width: 66%">
      <q-card-section>
        <div class="text-h6">Query</div>
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
        <q-btn flat label="Reset" @click="editDialogText = ''" />
        <q-btn flat label="Cancel" v-close-popup @click="editDialogText = ''" />
        <q-btn
          flat
          label="Save"
          v-close-popup
          @click="
            manualQueryForm.query = editDialogText;
            editDialogText = '';
          "
        />
      </q-card-actions>
    </q-card>
  </q-dialog>
</template>

<script setup lang="ts">
import { getActivePinia } from 'pinia';
import axios from 'axios';
import { QFile } from 'quasar';
import { onUnmounted, ref } from 'vue';
import { manualQueryFormStore } from 'stores/manual-query-store';
import { geoFeaturesStore } from 'stores/geo-features-store';
import FieldTypeInput from 'components/FieldTypeInput.vue';
import { FieldType } from 'components/models';

const manualQueryForm = manualQueryFormStore('abc-123');
const geoQueryFeatures = geoFeaturesStore();

const editDialog = ref<boolean>(false);
const editDialogText = ref<string>('');

const fileModel = ref<File>();
const fileRef = ref<QFile>();
const fileURL = ref<string>();

const fieldNum = ref(0);

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
          manualQueryForm.query = response.data;
        }
      })
      .catch((reason) => {
        console.log('Something went wrong? ' + reason);
      });
  }
}

function getFieldTypeMap() {
  const fieldTypeMap = new Map<string, string[]>();
  manualQueryForm.fieldTypes.forEach((element) => {
    if (element.field !== '') {
      let typeArray = [] as string[];
      if (fieldTypeMap.has(element.field)) {
        typeArray = fieldTypeMap.get(element.field) || typeArray;
      } else {
        fieldTypeMap.set(element.field, typeArray);
      }

      typeArray.push(element.type);
    }
  });

  return fieldTypeMap;
}

function submitQuery() {
  geoQueryFeatures.loadGeoFeaturesForQuery(
    manualQueryForm.query,
    getFieldTypeMap()
  );
}

onUnmounted(() => {
  // remove store from pinia registry
  const id = manualQueryForm.$id;
  manualQueryForm.$dispose();

  // delete the data stored in pinia
  const pinia = getActivePinia();
  pinia && delete pinia.state.value[id];
});
</script>

<style>
/* override quasar's styling to remove the textarea resize button */
.q-textarea .q-field__native {
  resize: none;
}
</style>
