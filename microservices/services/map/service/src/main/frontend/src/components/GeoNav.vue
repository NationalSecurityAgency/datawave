<template>
  <div class="cursor-pointer gt-md">{{ props.label }}</div>
</template>

<script setup lang="ts">
import { TypedFeature } from './models';

const props = defineProps<TypedFeature>();

</script>
