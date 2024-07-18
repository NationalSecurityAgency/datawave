<template>
  <q-layout view="hHh Lpr lFf" style="height: 100vh">
    <q-header elevated>
      <q-toolbar class="bg-grey-9 text-white">
        <q-btn
          flat
          dense
          round
          icon="menu"
          aria-label="Menu"
          @click="toggleDrawer"
        />

        <q-toolbar-title>DataWave Map Service</q-toolbar-title>

        <div>v{{ app.version }}</div>
      </q-toolbar>
    </q-header>

    <q-drawer
      v-model="drawer"
      show-if-above
      :mini="!drawer || mini"
      bordered
      style="display: flex; flex-direction: column"
    >
      <q-list style="flex-grow: 1">
        <EssentialLink
          v-for="link in essentialLinks"
          :key="link.title"
          v-bind="link"
        />
      </q-list>

      <q-list>
        <q-separator />
        <q-item clickable @click="toggleMini">
          <q-item-section avatar>
            <q-icon :name="expandIcon" />
          </q-item-section>
          <q-item-section>
            <q-item-label>Collapse</q-item-label>
          </q-item-section>
        </q-item>
      </q-list>
    </q-drawer>

    <q-page-container style="height: 100%">
      <router-view />
    </q-page-container>
  </q-layout>
  <q-ajax-bar position="bottom" color="accent" size="10px" />
</template>

<script setup lang="ts">
import { ref } from 'vue';
import EssentialLink, {
  EssentialLinkProps,
} from 'components/EssentialLink.vue';
import { appStateStore } from 'stores/state-store';
import app from '../../package.json';

const appState = appStateStore();

const essentialLinks: EssentialLinkProps[] = [
  {
    title: 'Add Content',
    icon: 'add',
    myFunction: () => {
      if (appState.getConfigPanelSelection === 'Add') {
        appState.disableConfigPanel();
        appState.setConfigPanelSelection('');
      } else {
        appState.enableConfigPanel();
        appState.setConfigPanelSelection('Add');
      }
    },
  },
  {
    title: 'Layers',
    icon: 'layers',
    myFunction: () => {
      if (appState.getConfigPanelSelection === 'Layers') {
        appState.disableConfigPanel();
        appState.setConfigPanelSelection('');
      } else {
        appState.enableConfigPanel();
        appState.setConfigPanelSelection('Layers');
      }
    },
  },
  {
    title: 'Tools',
    icon: 'handyman',
  },
];

const drawer = ref(false);
function toggleDrawer() {
  drawer.value = !drawer.value;
}

const mini = ref(true);
const expandIcon = ref('chevron_right');
function toggleMini() {
  mini.value = !mini.value;
  expandIcon.value = mini.value ? 'chevron_right' : 'chevron_left';
}
</script>
