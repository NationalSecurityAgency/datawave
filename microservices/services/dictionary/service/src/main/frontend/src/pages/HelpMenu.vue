<template>
  <div>
    <q-btn
      ref="helpBtn"
      round
      color="cyan-8"
      icon="help"
      style="position: fixed; top: 15px; right: 15px;"
      aria-label="Help Menu"
    />
    <q-menu
      :target="helpBtn?.$el"
      anchor="bottom right"
      self="top right"
      :offset="[-8, 8]"
    >
      <q-list style="min-width: 200px;">
        <q-item clickable v-ripple @click="openSkill">
          <q-item-section>{{ props.menu.menuOne ?? "Menu One" }}</q-item-section>
        </q-item>
        <q-item clickable v-ripple @click="openTrain">
          <q-item-section>{{ props.menu.menuTwo ?? "Menu Two" }}</q-item-section>
        </q-item>
        <q-item clickable v-ripple @click="openHelp">
          <q-item-section>{{ props.menu.menuThree ?? "Menu Three" }}</q-item-section>
        </q-item>
      </q-list>
    </q-menu>
  </div>
</template>

<script setup lang="ts">
import { ref } from 'vue';
import { defineProps } from 'vue';
import { Menu } from '../functions/components';

const props = defineProps<{ menu: Menu }>();
const helpBtn = ref();

function openSkill() { window.open(props.menu.menuOneLink ?? 'test', '_blank'); }
function openTrain() { window.open(props.menu.menuTwoLink ?? 'test', '_blank'); }
function openHelp()  { window.open(props.menu.menuThreeLink ?? 'test', '_blank'); }
</script>
