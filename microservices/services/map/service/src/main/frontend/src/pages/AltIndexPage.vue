<template>
    <main class="main">
      <div class="header">
        <label class="title">Data Dictionary</label>
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
        >
          <template v-slot:top-left>
            <q-btn
              color="primary"
              icon-right="archive"
              label="Export to csv"
              no-caps
              @click="exportTable"
            />
          </template>
          <template v-slot:top-right>
            <q-input
              borderless
              dense
              debounce="300"
              v-model="filter"
              placeholder="Search"
            >
              <template v-slot:append>
                <q-icon name="search" />
              </template>
            </q-input> </template
        ></q-table>
      </div>
    </main>
  </template>
  <script setup lang="ts">
  import { ref } from 'vue';
  import { QTable, QTableProps, exportFile, useQuasar } from 'quasar';
  import axios from 'axios';
  const columns: QTableProps['columns'] = [
    {
      label: 'Field Name',
      name: 'fieldName',
      field: 'fieldName',
      align: 'left',
      sortable: true,
    },
    {
      label: 'Internal FieldName',
      name: 'internalFieldName',
      field: 'internalFieldName',
      align: 'left',
      sortable: true,
    },
    {
      label: 'Data Type',
      name: 'dataType',
      field: 'dataType',
      align: 'left',
      sortable: true,
    },
    {
      label: 'Index Only',
      name: 'indexOnly',
      field: 'indexOnly',
      align: 'left',
      sortable: true,
    },
    {
      label: 'Forward Index',
      name: 'forwardIndexed',
      field: 'forwardIndexed',
      align: 'left',
      sortable: true,
    },
    {
      label: 'Reverse Index',
      name: 'revereseIndexed',
      field: 'fowardIndexed',
      align: 'left',
      sortable: true,
    },
    {
      label: 'Normalized',
      name: 'normalized',
      field: 'normalized',
      align: 'left',
      sortable: true,
    },
    {
      label: 'Types',
      name: 'Types',
      field: 'Types',
      align: 'left',
      sortable: true,
    },
    {
      label: 'Tokenized',
      name: 'tokenized',
      field: 'tokenized',
      align: 'left',
      sortable: true,
    },
    {
      label: 'Description',
      name: 'Descriptions',
      field: 'Descriptions',
      align: 'left',
      sortable: true,
    },
    {
      label: 'Last Updated',
      name: 'lastUpdated',
      field: 'lastUpdated',
      align: 'left',
      sortable: true,
    },
  ];
  const loading = ref(true);
  const filter = ref('');
  let rows: QTableProps['rows'] = [];
  // define pagination for standard table and table collapse
  const paginationFront = ref({
    rowsPerPage: 23,
    sortBy: 'fieldName',
  });
  // load the data from the dictionary
  axios
    .get('https://localhost:8643/dictionary/data/v1/')
    .then((response) => {
      rows = response.data.MetadataFields;
      loading.value = false;
    })
    .catch((reason) => {
      console.log('Something went wrong? ' + reason);
    });
  // function to export the table to a csv -> Default Functionality and can be modified
  const $q = useQuasar();
  function wrapCsvValue(val?: any, formatFn?: any, row?: any) {
    let formatted = formatFn !== void 0 ? formatFn(val, row) : val;
    formatted =
      formatted === void 0 || formatted === null ? '' : String(formatted);
    formatted = formatted.split('"').join('""');
    /**
     * Excel accepts \n and \r in strings, but some other CSV parsers do not
     * Uncomment the next two lines to escape new lines
     */
    // .split('\n').join('\\n')
    // .split('\r').join('\\r')
    return `"${formatted}"`;
  }

  const table = ref();

  function exportTable(this: any) {
    // naive encoding to csv format
    if (loading.value === false) {
      if (table.value?.filteredSortedRows) {
        const content = [columns!.map((col) => wrapCsvValue(col.label))]
            .concat(
            table.value?.filteredSortedRows.map((row: any) =>
                columns!
                .map((col: any) =>
                    wrapCsvValue(
                    typeof col.field === 'function'
                        ? col.field(row)
                        : row[col.field === void 0 ? col.name : col.field],
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
    }
  }
  </script>
  <style scoped>
  .main {
    display: flex;
    flex-direction: column;
    flex-wrap: wrap;
    margin-left: 5%;
    margin-right: 5%;
  }
  .header {
    font-family: 'Courier New', Courier, monospace;
    display: flex;
    flex-direction: column;
    align-items: center;
    font-size: 12px;
    margin-bottom: 5em;
  }
  .title {
    font-size: large;
    margin-bottom: 0.5em;
  }
  .information {
    font-size: 10px;
    text-align: center;
  }
  .qtable {
  }
  </style>