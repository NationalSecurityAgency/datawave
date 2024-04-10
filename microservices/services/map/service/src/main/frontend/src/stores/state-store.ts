import { defineStore } from 'pinia';
import { AppState } from 'src/components/models';

export const appStateStore = defineStore('appState', {
  state: () =>
    ({
      configPanel: {
        enabled: false,
        selection: '',
      },
      content: {
        geometry: '',
        editDialog: false,
        editDialogText: '',
      },
    } as AppState),
  getters: {
    isConfigPanelEnabled: (state) => state.configPanel.enabled,
    getConfigPanelSelection: (state) => state.configPanel.selection,
    isEditDialogEnabled: (state) => state.content.editDialog,
    getGeometry: (state) => state.content.geometry,
  },
  actions: {
    setConfigPanelSelection(configPanelSelection: string) {
      this.configPanel.selection = configPanelSelection;
    },
    toggleConfigPanelEnabled() {
      this.configPanel.enabled = !this.configPanel.enabled;
    },
    enableConfigPanel() {
      this.configPanel.enabled = true;
    },
    disableConfigPanel() {
      this.configPanel.enabled = false;
      this.configPanel.selection = '';
    },
    enableEditDialog() {
      this.content.editDialog = true;
    },
    disableEditDialog() {
      this.content.editDialog = false;
    },
    setGeometry(geometry: string) {
      this.content.geometry = geometry;
    },
    resetContent() {
      this.content.geometry = '';
      this.content.editDialog = false;
    },
  },
});
