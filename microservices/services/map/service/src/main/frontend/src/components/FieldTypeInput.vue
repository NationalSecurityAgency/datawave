<template>
  <q-item class="q-py-none">
    <q-item-section>
        <div class="row">
            <div class="col">
                <q-input v-model="fieldType.field" style="max-width: 175px" label="Field Name" @update:model-value="$emit('update', fieldType)" />
            </div>
            <div class="col">
                <q-select emit-value v-model="fieldType.type" style="max-width: 175px" :options="options" label="Geo Type" @update:model-value="$emit('update', fieldType)" />
            </div>
        </div>
    </q-item-section>
    <q-item-section side >
      <q-btn class="gt-xs" size="12px" flat dense round icon="cancel" @click="onDelete(id)" />
    </q-item-section>
  </q-item>
</template>

<script setup lang="ts">
import { ref } from 'vue';
import { FieldType } from './models';

export interface Props {
    id: number;
    onDelete: (id: number) => void
}
const props = defineProps<Props>();

const fieldType = ref<FieldType>({
  id: props.id,
  field: '',
  type: ''
});

const options = [
  {
    label: 'Geo Point',
    value: 'datawave.data.type.GeoType'
  },
  {
    label: 'GeoWave Point',
    value: 'datawave.data.type.PointType'
  },
  {
    label: 'GeoWave Geometry',
    value: 'datawave.data.type.GeometryType'
  }
];

defineEmits(['update']);
</script>