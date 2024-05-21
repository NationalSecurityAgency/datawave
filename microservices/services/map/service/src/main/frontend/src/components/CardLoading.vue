<template>
  <q-card v-if="loadingState!=DONE" class="absolute-full column flex-center" style="background-color: transparent;">
    <!-- if loading, show the spinner -->
    <q-spinner v-if="loadingState==LOADING" size="6em" color="secondary"/>

    <!-- if success, show the success dialog -->
    <q-icon v-if="loadingState==SUCCESS" name="check_circle" size="6em" color="green"/>
    
    <!-- if failure, show the failure dialog -->
    <q-icon v-if="loadingState==FAILURE" name="warning" size="6em" color="red"/>
    
    <!-- display the loading message -->
    <p style="font-size:18px;">{{ loadingMessage }}</p>

    <!-- if not loading, show the done button -->
    <q-btn v-if="loadingState!=LOADING" label="Done" color="white" text-color="black" @click="loadingState=DONE; loadingMessage=''; $emit('doneClick');"/>
  </q-card>
</template>

<script setup lang="ts">
import { ref } from 'vue';

export interface CardLoadingMethods {
  loading: (message: string) => void;
  success: (message: string) => void;
  failure: (message: string) => void;
}

const LOADING = 'loading';
const SUCCESS = 'success';
const FAILURE = 'failure';
const DONE = 'done'

const loadingState = ref<string>(DONE);
const loadingMessage = ref<string>('');

const cardLoadingMethods: CardLoadingMethods = {
  loading(message: string) {
    loadingState.value = LOADING;
    loadingMessage.value = message;
  },

  success(message: string){
    loadingState.value = SUCCESS;
    loadingMessage.value = message;
  },

  failure(message: string) {
    loadingState.value = FAILURE;
    loadingMessage.value = message;
  }
};

defineEmits(['doneClick']);
defineExpose(cardLoadingMethods);
</script>

<style>
</style>