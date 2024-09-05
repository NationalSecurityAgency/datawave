import { api } from 'boot/axios';
import L from 'leaflet';
import { defineStore } from 'pinia';

export const basemapsStore = defineStore('basemaps', {
  state: () => ({
    basemap: {} as Basemap,
    basemaps: [] as Basemap[],
  }),
  getters: {
    getBasemap(): Basemap {
      return this.basemap as Basemap;
    },
    getBasemaps(): Basemap[] {
      return this.basemaps as Basemap[];
    }
  },
  actions: {
    async initialize() {
      if (this.basemaps.length == 0) {
        return new Promise((resolve, reject) => {
          api
            .get('/map/v1/basemaps')
            .then((response) => {
              const basemapsConfig = response.data as {
                title: string,
                urlTemplate: string,
                maxZoom?: number,
                maxNativeZoom?: number,
                attribution: string,
                default?: boolean
              }[];

              let defaultSet = false;
              for (const basemapConfig of basemapsConfig) {
                const basemap = {
                  title: basemapConfig.title,
                  urlTemplate: basemapConfig.urlTemplate,
                  tileLayer: L.tileLayer(basemapConfig.urlTemplate, {
                    maxZoom: basemapConfig.maxZoom,
                    maxNativeZoom: basemapConfig.maxNativeZoom,
                    attribution: basemapConfig.attribution
                  }),
                  default: basemapConfig.default
                } as Basemap

                this.basemaps.push(basemap);
                if (!defaultSet) {
                  this.basemap = basemap;
                  defaultSet = true;
                } else {
                  
                }
              }
              resolve(this.basemaps);
            })
            .catch((reason) => {
              console.log('Something went wrong? ' + reason);
              reject(reason);
            });
        });
      }
    },
    setBasemap(basemap: Basemap) {
      this.basemap = basemap;
    }
  },
});

export interface Basemap {
  title: string;
  urlTemplate: string;
  tileLayer: L.TileLayer;
  default: boolean;
  enabled?: boolean;
}
