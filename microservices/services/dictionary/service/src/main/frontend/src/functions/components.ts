import { QTableProps } from 'quasar';

export interface Banner {
  enabled: boolean;
  messageTop?: string;
  messageBottom?: string;
  styleTop?: string;
  styleBottom?: string;
}

export const columns: QTableProps['columns'] = [
  {
    label: 'Field Name',
    name: 'fieldName',
    field: 'fieldName',
    align: 'left',
    sortable: false,
    style: 'max-width: 275px; min-width: 275px',
  },
  {
    label: 'Internal FieldName',
    name: 'internalFieldName',
    field: 'internalFieldName',
    align: 'left',
    sortable: false,
    style: 'max-width: 275px; min-width: 275px',
  },
  {
    label: 'Data Type',
    name: 'dataType',
    field: 'dataType',
    align: 'left',
    sortable: false,
    style: 'max-width: 100px; min-width: 100px',
  },
  {
    label: 'Index Only',
    name: 'indexOnly',
    field: 'indexOnly',
    align: 'left',
    sortable: false,
    style: 'max-width: 100px; min-width: 100px',
  },
  {
    label: 'Forward Index',
    name: 'forwardIndexed',
    field: 'forwardIndexed',
    align: 'left',
    sortable: false,
    style: 'max-width: 100px; min-width: 100px',
  },
  {
    label: 'Reverse Index',
    name: 'reverseIndexed',
    field: 'reverseIndexed',
    align: 'left',
    sortable: false,
    style: 'max-width: 100px; min-width: 100px',
  },
  {
    label: 'Normalized',
    name: 'normalized',
    field: 'normalized',
    align: 'left',
    sortable: false,
    style: 'max-width: 100px; min-width: 100px',
  },
  {
    label: 'Types',
    name: 'Types',
    field: 'Types',
    align: 'left',
    sortable: false,
    style: 'max-width: 100px; min-width: 100px',
  },
  {
    label: 'Tokenized',
    name: 'tokenized',
    field: 'tokenized',
    align: 'left',
    sortable: false,
    style: 'max-width: 100px; min-width: 100px',
  },
  {
    label: 'Description',
    name: 'Descriptions',
    field: 'Descriptions',
    align: 'center',
    sortable: false,
    style: 'max-width: 200px; min-width: 200px',
  },
  {
    label: 'Last Updated',
    name: 'lastUpdated',
    field: 'lastUpdated',
    align: 'left',
    sortable: false,
    style: 'max-width: 125px; min-width: 125px',
  },
];
