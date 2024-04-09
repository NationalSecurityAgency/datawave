import { defineStore } from 'pinia';
import { ManualQueryForm, FieldType } from 'components/models';

export const manualQueryFormStore = (id: string) => 
    defineStore('manualQueryForm-' + id, {
        state: () => ({
            query: '',
            fieldTypes: [] as FieldType[]
        } as ManualQueryForm),
        getters: {

        },
        actions: {
            
        },
    })();
