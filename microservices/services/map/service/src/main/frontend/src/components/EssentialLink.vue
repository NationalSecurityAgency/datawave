<template>
  <q-item
    clickable
    @click="myFunction"
  >
    <q-item-section
      v-if="icon"
      avatar
    >
      <q-icon :name="icon" />
    </q-item-section>

    <q-item-section>
      <q-item-label>{{ title }}</q-item-label>
      <q-item-label v-if="caption" caption>{{ caption }}</q-item-label>
    </q-item-section>
  </q-item>
</template>

<script setup lang="ts">
export interface EssentialLinkProps {
  title: string;
  caption?: string;
  icon?: string;
  myFunction?: () => void;
}
withDefaults(defineProps<EssentialLinkProps>(), {
  caption: '',
  icon: '',
});
</script>
