<template>
  <div v-if="banner?.enabled" :style="banner?.styleTop">
      {{ banner?.messageTop }}
  </div>
  <div v-if="banner?.enabled" :style="banner?.styleBottom" style="margin-bottom: 0.50vh;">
      {{ banner?.messageBottom }}
  </div>
  <main class="main col" style="height: 100vh">
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
    </div>
    <div class="row" style="width: 100%; height: 80%">
      <p class="information">
        When a value is present in the forward index types, this means that a
        field is indexed and informs you how your query terms will be treated
        (e.g. text, number, IPv4 address, etc). The same applies for the reverse
        index types with the caveat that you can also query these fields using
        leading wildcards. Fields that are marked as 'Index only' will not
        appear in a result set unless explicitly queried on. Index only fields
        are typically composite fields, derived from actual data, created by the
        software to make querying easier.
      </p>
      <q-table
        ref="table"
        :loading="loading"
        :rows="rows"
        :columns="columns"
        :filter="filter"
        v-model:pagination="paginationFront"
        row-key="internalFieldName"
        dense
        style="font-size: smaller; height: 100%; width: 100%;"
        class="datawave-dictionary-sticky-sass dark"
        :rows-per-page-options="[]"
      >
        <template v-slot:top-left>
          <q-btn
            style="margin-right: 2px;"
            size="12px"
            color="cyan-8"
            icon-right="archive"
            label="Export"
            no-caps
            @click="exportTable()"
          />
          <q-btn
            style="margin-left: 2px;"
            size="12px"
            padding="5px 5px"
            color="cyan-8"
            :icon="isDark ? 'bi-sun-fill' : 'bi-moon-fill'"
            no-caps
            @click="toggleDark(); $q.dark.toggle();"
          />
        </template>
        <template v-slot:top-right>
          <q-input
            borderless
            dense
            debounce="300"
            v-model="changeFilter"
            placeholder="Search"
            @keydown.enter.prevent="queryTable"
            style="margin-right: 1em;"
          >
          </q-input>
          <q-btn
                size="12px"
                color="cyan-8"
                icon="search"
                round
                dense
                @click="queryTable"
              />
        </template>

        <template v-slot:header="props">
          <q-tr :props="props">
            <q-th />
            <q-th style="font-size: 13.7px;" v-for="col in props.cols" :key="col.name" :props="props">
              {{ col.label }}
            </q-th>
          </q-tr>
        </template>

        <template v-slot:body="props">
          <q-tr
            :props="props"
            v-if="Formatters.isVisible(props.row)"
          >
            <q-td style="width: 60px; min-width: 60px">
              <q-btn
                size="9px"
                color="cyan-8"
                round
                dense
                @click="
                  {
                    props.expand = !props.expand;
                    Formatters.toggleVisibility(props.row);
                  }
                "
                :icon="props.row.isVisible.value ? 'remove' : 'add'"
                v-if="Formatters.buttonParse(props.cols, props.row)"
              />
              <q-icon
                  style="margin-left: 4px;"
                  size="1rem"
                  :name="'bi-arrow-right'"
                  color="cyan-8"
                  v-if="props.row.duplicate == 1"
              />
            </q-td>
            <q-td
              v-for="col in props.cols"
              :key="col.name"
              :props="props"
              style="font-size: 13px;"
              :title="Formatters.parseVal(col.name, col.value)"
              @click="Feature.copyLabel(col.value)"
            >
              <label style="cursor: pointer;">
                {{
                Formatters.maxSubstring(
                  Formatters.parseVal(col.name, col.value), col.name
                )
              }}
              </label>
            </q-td>
          </q-tr>
        </template>
      </q-table>
    </div>
  </main>
  <div v-if="banner?.enabled" :style="banner?.styleTop" style="margin-top: 0.50vh;">
      {{ banner?.messageTop }}
  </div>
  <div v-if="banner?.enabled" :style="banner?.styleBottom">
      {{ banner?.messageBottom }}
  </div>
</template>

<script setup lang="ts">
import { onMounted, ref } from 'vue';
import { QTable, QTableProps, exportFile, useQuasar, Notify } from 'quasar';
import { useToggle, useDark } from '@vueuse/core';
import { api } from '../boot/axios';
import { Banner, columns } from '../functions/components';
import * as Formatters from '../functions/formatters';
import * as Wrapper from '../functions/csvWrapper';
import * as Feature from '../functions/features';

// Defines the Table References, loading for axios, search filter, and pagination to sort.
const $q = useQuasar();
const table = ref();
const loading = ref(true);
const filter = ref('');
const changeFilter = ref('');
const banner = ref<Banner>();
let rows: QTableProps['rows'] = [];
const paginationFront = ref({
  rowsPerPage: 200,
  sortBy: 'fieldName',
});

// API - Defines all the Nececssary API calls for the user, and filters.
// Note that to run the endpoint in DEV mode, you must build the project at least once first.
onMounted(() => {
  let endpointData = '';
  let bannerData = 'banner';
  if (process.env.DEV) {
    endpointData = 'data/v2/'
    bannerData = 'data/v2/banner/'
  }

  api
  .get(bannerData, undefined)
  .then((response) => {
    banner.value = response.data as Banner;
  })
  .catch((reason) => {
    console.error('Could not fetch banner: ' + reason);
  });

  api
  .get(endpointData)
  .then((response) => {
    // Mini Filter to sort collapsable Rows
    rows = response.data.MetadataFields.sort((a: any, b: any) => {
      // Create a combined key for both fieldname and internalFieldName
      const keyA = `${a.fieldname}_${a.internalFieldName}`;
      const keyB = `${b.fieldname}_${b.internalFieldName}`;

      // First compare the combined key alphabetically
      if (keyA < keyB) {
        return -1;
      } else if (keyA > keyB) {
        return 1;
      } else {
        // If the combined key is the same, compare lastUpdated in descending order
        return b.lastUpdated - a.lastUpdated;
      }
    });
    rows = Formatters.setVisibility(rows);
    loading.value = false;
  })
  .catch((reason) => {
    console.log('Error fetching and formatting rows: ' + reason);
  });
});

// Export - Attempts to Wrap the CSV and Download.
function exportTable(this: any) {
  const rowsToExport = table.value?.filteredSortedRows.filter(
    Formatters.isVisible
  );
  const content = [columns!.map((col) => Wrapper.wrapCsvValue(col.label))]
    .concat(
      rowsToExport.map((row: any) =>
        columns!
          .map((col: any) =>
            Wrapper.wrapCsvValue(
              typeof col.field === 'function'
                ? col.field(row)
                : row[col.field === undefined ? col.name : col.field],
              col.format,
              row
            )
          )
          .join(',')
      )
    )
    .join('\r\n');

  const status = exportFile('table-export.csv', content, 'text/csv');
  if (status !== true) {
    $q.notify({
      message: 'Browser denied file download...',
      color: 'negative',
      icon: 'warning',
    });
  }
}

// Query - Runs through a Search Process as it waits for the user.
async function queryTable(this: any) {
  // Wait Until User Enters...
  await waitUp();

  // 1 - Filter the Rows
  const rowsToExport = table.value?.filteredSortedRows.filter(() => true);

  // 2 - Define Refresh Trigger (By Pagination) and Orginial Rows Stored
  const originalRows = rows;
  const triggerRefresh = paginationFront.value.rowsPerPage;

  // 3 - Set the Current Rows to Filtered Value
  rows = Formatters.setVisibility(rowsToExport);

  // 4 - Trigger the Refresh
  paginationFront.value.rowsPerPage = 100;
  paginationFront.value.rowsPerPage = triggerRefresh;

  // 5 - Restore Original Rows for Next Query
  rows = originalRows;
}

// Query 2 - Awaits for the user to change or add something.
function waitUp() {
  filter.value = changeFilter.value;
}

// Customization - Sets the Dark Mode Toggle for the User.
const isDark = useDark();
const toggleDark = useToggle(isDark);
if (isDark.value) {
  $q.dark.set(true);
} else {
  $q.dark.set(false);
}

</script>
