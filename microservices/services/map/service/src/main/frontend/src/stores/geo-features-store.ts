import { defineStore } from 'pinia';
import { GeoFeatures, GeoQueryFeatures, ManualGeometryForm } from 'src/components/models';
import { api } from 'boot/axios';

export const geoFeaturesStore = defineStore('geoQueryFeatures', {
  state: () => ({
    geoQueryFeatures: {} as GeoFeaturesMap,
  }),
  getters: {
    getGeoFeatures: (state) => state.geoQueryFeatures,
    getGeoFeaturesById: (state) => {
      return (queryId: string) => state.geoQueryFeatures[queryId];
    },
  },
  actions: {
    async loadGeoFeaturesForQuery(
      query: string,
      fieldTypes?: Map<string, string[]>,
      expand?: boolean
    ) {
      return new Promise((resolve, reject) => {

        let fieldTypesString = '';
        if (fieldTypes != null) {
          fieldTypes.forEach((value, key) => {
            if (fieldTypesString.length > 0) {
              fieldTypesString += ',';
            }
            fieldTypesString += key + ':' + value;
          });
        }

        const formData = new FormData();
        formData.append('plan', query);
        formData.append('fieldTypes', fieldTypesString);
        formData.append('expand', (expand || false).toString())

        api
          .post('/map/v1/getGeoFeaturesForQuery', formData, undefined)
          .then((response) => {
            this.geoQueryFeatures[query] = response.data;
            resolve(query);
          })
          .catch((reason) => {
            console.log('Something went wrong? ' + reason);
            reject(reason);
          });
      })
    },
    async loadGeoFeaturesForQueryId(queryId: string) {
      return new Promise((resolve, reject) => {
        const params: { queryId: string } = {
          queryId: queryId
        };

        api
          .post('/map/v1/getGeoFeaturesForQueryId', null, {
            params: params,
          })
          .then((response) => {
            this.geoQueryFeatures[queryId] = response.data;
            resolve(queryId);
          })
          .catch((reason) => {
            console.log('Something went wrong? ' + reason);
            reject(reason);
          });
      })
    },
    async loadGeoFeaturesForGeometry(geometryFormData: ManualGeometryForm) {
      return new Promise((resolve, reject) => {
        const formData = new FormData();
        formData.append('geometry', geometryFormData.geometry);
        formData.append('geometryType', geometryFormData.geometryType);

        if (geometryFormData.createRanges){
          formData.append('createRanges', geometryFormData.createRanges.toString());
          formData.append('rangeType', geometryFormData.rangeType);

          const rangeSettings = geometryFormData.rangeSettings[geometryFormData.rangeType];
          formData.append('maxEnvelopes', rangeSettings.maxEnvelopes.toString());
          formData.append('maxExpansion', rangeSettings.maxExpansion.toString());

          if (rangeSettings.optimizeRanges) {
            formData.append('optimizeRanges', rangeSettings.optimizeRanges.toString());

            if (rangeSettings.rangeSplitThreshold){
              formData.append('rangeSplitThreshold', rangeSettings.rangeSplitThreshold.toString());
            }
            if (rangeSettings.maxRangeOverlap) {
              formData.append('maxRangeOverlap', rangeSettings.maxRangeOverlap.toString());
            }
          }
        }

        api
          .post('/map/v1/geoFeaturesForGeometry', formData, undefined)
          .then((response) => {
            const id = new Date().toLocaleString();
            this.geoQueryFeatures[id] = response.data;
            resolve(id);
          })
          .catch((reason) => {
            console.log('Something went wrong? ' + reason);
            reject(reason);
          });
      })
    },
  },
});

interface GeoFeaturesMap {
  [queryId: string]: GeoQueryFeatures | GeoFeatures;
}
