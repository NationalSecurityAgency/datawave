import { QTableProps } from 'quasar';
import { computed, reactive } from 'vue';

let resizingCol = ''
let startX = 0
let startWidth = 0
const MIN_WIDTH = 60

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
  },
  {
    label: 'Internal FieldName',
    name: 'internalFieldName',
    field: 'internalFieldName',
    align: 'left',
    sortable: false,
  },
  {
    label: 'Data Type',
    name: 'dataType',
    field: 'dataType',
    align: 'left',
    sortable: false,
  },
  {
    label: 'Index Only',
    name: 'indexOnly',
    field: 'indexOnly',
    align: 'left',
    sortable: false,
  },
  {
    label: 'Forward Index',
    name: 'forwardIndexed',
    field: 'forwardIndexed',
    align: 'left',
    sortable: false,
  },
  {
    label: 'Reverse Index',
    name: 'reverseIndexed',
    field: 'reverseIndexed',
    align: 'left',
    sortable: false,
  },
  {
    label: 'Normalized',
    name: 'normalized',
    field: 'normalized',
    align: 'left',
    sortable: false,
  },
  {
    label: 'Types',
    name: 'Types',
    field: 'Types',
    align: 'left',
    sortable: false,
  },
  {
    label: 'Tokenized',
    name: 'tokenized',
    field: 'tokenized',
    align: 'left',
    sortable: false,
  },
  {
    label: 'Description',
    name: 'Descriptions',
    field: 'Descriptions',
    align: 'center',
    sortable: false,
  },
  {
    label: 'Last Updated',
    name: 'lastUpdated',
    field: 'lastUpdated',
    align: 'left',
    sortable: false,
  },
];
const columnWidths = reactive<Record<string, number>>({
  fieldName: 275,
  internalFieldName: 275,
  dataType: 100,
  indexOnly: 100,
  forwardIndexed: 100,
  reverseIndexed: 100,
  normalized: 100,
  Types: 100,
  tokenized: 100,
  Descriptions: 200,
  lastUpdated: 125,
})

export const baseColumns = computed(() =>
columns.map(col => ({
  ...col,
  style: 'width: ${columnWidths[col.name]}px',
}))
)

export function startResize(name: string, e: MouseEvent){
  resizingCol = name
  startX = e.clientX
  startWidth = columnWidths[name]

  document.addEventListener('mousemove', onMouseMove)
  document.addEventListener('mouseup', stopResize)
}

export function onMouseMove(e: MouseEvent){
  if (!resizingCol) return
  const delta = e.clientX - startX
  columnWidths[resizingCol] = Math.max(MIN_WIDTH, startWidth + delta)
}

export function stopResize(){
  resizingCol = ''
  document.removeEventListener('mousemove', onMouseMove)
  document.removeEventListener('mouseup',stopResize)
}