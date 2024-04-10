<template>
  <div style="height: 100%; width: 500px; background: white">
    <q-card square class="bg-secondary text-white q-px-md q-py-sm">
      <q-card-section horizontal>
        <div class="text-h6" style="flex-grow: 1">Layers</div>
        <q-btn
          flat
          round
          size="10px"
          icon="close"
          @click="appState.disableConfigPanel"
        />
      </q-card-section>
    </q-card>
  </div>
</template>

<script setup lang="ts">
import { appStateStore } from 'stores/state-store';

const appState = appStateStore();
</script>
