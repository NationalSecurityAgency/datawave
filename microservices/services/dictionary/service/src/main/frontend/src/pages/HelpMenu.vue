<template>
  <div>
    <q-btn
      ref="helpBtn"
      round
      color="cyan-8"
      icon="help"
      :style="{ position: 'fixed', top: btnTop + 'px', right: '15px', transition: 'top 0.3s ease' }"
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
import { ref, onMounted, onBeforeUnmount } from 'vue';
import { defineProps } from 'vue';
import { Menu } from '../functions/components';

const props = defineProps<{ menu: Menu }>();
const helpBtn = ref();
const btnTop = ref(70); // Initial position of the button at 70px from the top.

// Function to adjust button position based on scroll position of 50px. This is to account for the banner at the top of the page.
function onScroll() { btnTop.value = window.scrollY > 50 ? 15 : 70;}

// Reset button position on scroll, and removes event listener on unmount (garbage collection).
onMounted(() => { window.addEventListener('scroll', onScroll); });
onBeforeUnmount(() => { window.removeEventListener('scroll', onScroll); });

function openSkill() { window.open(props.menu.menuOneLink ?? 'test', '_blank'); }
function openTrain() { window.open(props.menu.menuTwoLink ?? 'test', '_blank'); }
function openHelp()  { window.open(props.menu.menuThreeLink ?? 'test', '_blank'); }
</script>
