<template>
  <div style="height: 100%; width: 500px; background: white">
    <q-card square class="bg-secondary text-white q-px-md q-py-sm">
      <q-card-section horizontal>
        <div class="text-h6" style="flex-grow: 1">Add Content</div>
        <q-btn
          flat
          round
          size="10px"
          icon="close"
          @click="appState.disableConfigPanel"
        />
      </q-card-section>
    </q-card>
    <q-list bordered class="q-pa-none">
      <!-- Manually add geometry to the map -->
      <q-expansion-item
        group="contentgroup"
        icon="explore"
        label="Geometry"
        header-class="text-secondary"
      >
        <ManualGeometryForm />
      </q-expansion-item>

      <q-separator />

      <!-- Manually add a query to the map -->
      <q-expansion-item
        group="contentgroup"
        icon="manage_search"
        label="Query"
        header-class="text-secondary"
      >
        <ManualQueryForm />
      </q-expansion-item>

      <q-separator />

      <!-- Lookup a query by ID and add its geometry to the map -->
      <q-expansion-item
        group="contentgroup"
        icon="travel_explore"
        label="Query Lookup"
        header-class="text-secondary"
      >
        <q-card>
          <LookupQueryForm />
        </q-card>
      </q-expansion-item>
    </q-list>
  </div>
  <q-dialog v-model="appState.content.editDialog">
    <q-card style="min-width: 66%">
      <q-card-section>
        <div class="text-h6">Geometry</div>
      </q-card-section>

      <q-card-section>
        <q-input
          filled
          type="textarea"
          :rows="30"
          v-model="appState.content.editDialogText"
          autofocus
        />
      </q-card-section>

      <q-card-actions align="right" class="text-primary">
        <q-btn
          flat
          label="Reset"
          @click="appState.content.editDialogText = ''"
        />
        <q-btn
          flat
          label="Cancel"
          v-close-popup
          @click="appState.content.editDialogText = ''"
        />
        <q-btn
          flat
          label="Save"
          v-close-popup
          @click="
            appState.content.geometry = appState.content.editDialogText;
            appState.content.editDialogText = '';
          "
        />
      </q-card-actions>
    </q-card>
  </q-dialog>
</template>

<script setup lang="ts">
import { appStateStore } from 'stores/state-store';
import ManualGeometryForm from 'components/ManualGeometryForm.vue';
import ManualQueryForm from 'components/ManualQueryForm.vue';
import LookupQueryForm from 'components/LookupQueryForm.vue';

const appState = appStateStore();
</script>

<style>
/* override quasar's styling to remove the textarea resize button */
.q-textarea .q-field__native {
  resize: none;
}
</style>
