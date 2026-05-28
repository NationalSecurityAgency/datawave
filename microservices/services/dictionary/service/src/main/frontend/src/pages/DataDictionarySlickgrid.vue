<template>
  <div v-if="banner?.enabled" :style="banner?.styleTop">
      {{ banner?.messageTop }}
  </div>
  <div v-if="banner?.enabled" :style="banner?.styleBottom" style="margin-bottom: 0.50vh;">
      {{ banner?.messageBottom }}
  </div>
  
  <main class="main col">
    <div
      class="row"
      style="
        width: 60%;
        height: 4%;
        justify-content: center;
        align-self: center;
        margin-bottom:5px;"
    >
      <label class="title">Data Dictionary</label>
      <q-img
      class="icon"
      :src="'icons/favicon-32x32.png'"
      spinner-color="white"
      />
      <HelpMenu v-if="helpMenu" :menu="helpMenu" />
    </div>
    <div class="row" style="width: 100%; height: 80%">
      <p class="information">
        <br />
        Cluster: {{ system?.systemName }} <br />
        When a value is present in the forward index types, this means that a
        field is indexed and informs you how your query terms will be treated
        (e.g. text, number, IPv4 address, etc). The same applies for the reverse
        index types with the caveat that you can also query these fields using
        leading wildcards. Fields that are marked as 'Index only' will not
        appear in a result set unless explicitly queried on. Index only fields
        are typically composite fields, derived from actual data, created by the
        software to make querying easier.
      </p>
    </div>
  </main>

  <div id="dd-container" style="resize:width; overflow:auto; height:100%">
  <SlickgridVue
    grid-id="grid1"
    v-model:options="gridOptions"
    v-model:columns="col"
    v-model:dataset="dataset"
    @onVueGridCreated="vueGridReady($event.detail)" />
  </div>

  <div v-if="banner?.enabled" :style="banner?.styleTop" style="margin-top: 0.50vh;">
      {{ banner?.messageTop }}
  </div>
  <div v-if="banner?.enabled" :style="banner?.styleBottom">
      {{ banner?.messageBottom }}
  </div>
</template>

<script setup lang="ts">
import { onBeforeMount, ref, watch, type Ref } from 'vue';
import { useRoute, useRouter } from 'vue-router';
import { type Column, GridState, SlickgridVue, SlickgridVueInstance, GridOption } from 'slickgrid-vue';
import { useToggle, useDark } from '@vueuse/core';
import { api } from '../boot/axios';
import { Banner, Menu, columns, System } from '../functions/components';
import * as Wrapper from '../functions/csvWrapper';
import * as Feature from '../functions/features';
import HelpMenu from './HelpMenu.vue';
import '@slickgrid-universal/common/dist/styles/sass/slickgrid-theme-bootstrap.scss';

const gridOptions = ref<GridOption>();
const col: Ref<Column[]> = ref([]);
const dataset = ref<any[]>([]);

const banner = ref<Banner>();
const system = ref<System>();
const helpMenu = ref<Menu>();
const search = ref('');

let vueGrid!: SlickgridVueInstance;

onBeforeMount(() => {
  let endpointData = '';
  let bannerData = 'banner';
  let systemData = 'system';
  let helpMenuData = 'menu';
  if (process.env.DEV) {
    endpointData = 'data/v2/'
    bannerData = 'data/v2/banner/'
    systemData = 'data/v2/system/'
    helpMenuData = 'data/v2/menu/'
  }

  api
  .get(bannerData, undefined)
  .then((response) => {
    banner.value = response.data as Banner;
  })
  .catch((reason) => {
    console.error('Could not fetch banner: ' + reason);
  });

  api.get(helpMenuData)
  .then((response) => {
    helpMenu.value = response.data as Menu;
  })
  .catch((reason) => {
    console.error('Could not fetch help menu: ' + reason);
  });

  api
  .get(systemData)
  .then((response) => {
    system.value = response.data as System;
  })
  .catch((reason) => {
    console.error('Could not fetch system name: ' + reason);
  });

  const responseData: any[] = [];

  api
  .get(endpointData)
  .then((response) => {
    // Mini Filter to sort collapsable Rows
    let position = 0;

    for (const row of response.data.MetadataFields) {
      responseData[position] = {
        id: position,
        fieldName:row.fieldName,
        internalFieldName:row.internalFieldName,
        dataType:row.dataType,
        indexOnly:row.indexOnly,
        forwardIndex:row.forwardIndexed,
        reverseIndex:row.reverseIndexed,
        normalized:row.normalized,
        types:row.Types,
        tokenized:row.tokenized,
        description:row.Descriptions,
        lastUpdated:row.lastUpdated,
      }

      position++;
    }

    console.table(responseData)
  })
  .catch((reason) => {
    console.log('Error fetching and formatting rows: ' + reason);
  });

  defineGrids();
  dataset.value = responseData;
});

function defineGrids() {
  col.value = [
    {
      id: 'fieldName',
      name: 'Field Name',
      field: 'fieldName',
      sortable: true,
      minWidth: 150
    },
    {
      id: 'internalFieldName',
      name: 'Internal FieldName',
      field: 'internalFieldName',
      sortable: true,
      minWidth: 150
    },
    {
      id: 'dataType',
      name: 'Data Type',
      field: 'dataType',
      sortable: true,
      minWidth: 150
    },
    {
      id: 'indexOnly',
      name: 'Index Only',
      field: 'indexOnly',
      sortable: true,
      minWidth: 150
    },
    {
      id: 'forwardIndex',
      name: 'Forward Index',
      field: 'forwardIndex',
      sortable: true,
      minWidth: 150
    },
    {
      id: 'reverseIndex',
      name: 'Reverse Index',
      field: 'reverseIndex',
      sortable: true,
      minWidth: 150
    },
    {
      id: 'normalized',
      name: 'Normalized',
      field: 'normalized',
      sortable: true,
      minWidth: 150
    },
    {
      id: 'types',
      name: 'Types',
      field: 'types',
      sortable: true,
      minWidth: 150
    },
    {
      id: 'tokenized',
      name: 'Tokenized',
      field: 'tokenized',
      sortable: true,
      minWidth: 150
    },
    {
      id: 'description',
      name: 'Description',
      field: 'description',
      sortable: true,
      minWidth: 150
    },
    {
      id: 'lastUpdated',
      name: 'Last Updated',
      field: 'lastUpdated',
      sortable: true,
      minWidth: 150
    },
  ];

  gridOptions.value = {
    enableAutoResize: true,
    autoResize: {
      container: '#dd-container',
      resizeDetection: 'container',
      calculateAvailableSizeBy: 'container',
    }
  };
}

function vueGridReady(grid: SlickgridVueInstance) {
  vueGrid = grid;
}
</script>
