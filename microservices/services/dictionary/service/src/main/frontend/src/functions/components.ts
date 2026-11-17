import { QTableProps } from 'quasar';

export interface Banner {
  enabled: boolean;
  messageTop?: string;
  messageBottom?: string;
  styleTop?: string;
  styleBottom?: string;
}

export interface Menu {
  enabled: boolean;
  menuOne?: string;
  menuTwo?: string;
  menuThree?: string;
  menuOneLink?: string;
  menuTwoLink?: string;
  menuThreeLink?: string;
}

export interface System {
  systemName: string;
}

export const columns: QTableProps['columns'] = [
  {
    label: 'Field Name',
    name: 'fieldName',
    field: 'fieldName',
    align: 'left',
    sortable: false,
    style: 'max-width: 0px;'
  },
  {
    label: 'Internal FieldName',
    name: 'internalFieldName',
    field: 'internalFieldName',
    align: 'left',
    sortable: false,
    style: 'max-width: 0px;'
  },
  {
    label: 'Data Type',
    name: 'dataType',
    field: 'dataType',
    align: 'left',
    sortable: false,
    style: 'max-width: 0px;'
  },
  {
    label: 'Index Only',
    name: 'indexOnly',
    field: 'indexOnly',
    align: 'left',
    sortable: false,
    style: 'max-width: 0px;'
  },
  {
    label: 'Forward Index',
    name: 'forwardIndexed',
    field: 'forwardIndexed',
    align: 'left',
    sortable: false,
    style: 'max-width: 0px;'
  },
  {
    label: 'Reverse Index',
    name: 'reverseIndexed',
    field: 'reverseIndexed',
    align: 'left',
    sortable: false,
    style: 'max-width: 0px;'
  },
  {
    label: 'Normalized',
    name: 'normalized',
    field: 'normalized',
    align: 'left',
    sortable: false,
    style: 'max-width: 0px;'
  },
  {
    label: 'Types',
    name: 'Types',
    field: 'Types',
    align: 'left',
    sortable: false,
    style: 'max-width: 0px;'
  },
  {
    label: 'Tokenized',
    name: 'tokenized',
    field: 'tokenized',
    align: 'left',
    sortable: false,
    style: 'max-width: 0px;'
  },
  {
    label: 'Description',
    name: 'Descriptions',
    field: 'Descriptions',
    align: 'left',
    sortable: false,
    style: 'max-width: 0px;'
  },
  {
    label: 'Last Updated',
    name: 'lastUpdated',
    field: 'lastUpdated',
    align: 'left',
    sortable: false,
    style: 'max-width: 0px;'
  },
];
