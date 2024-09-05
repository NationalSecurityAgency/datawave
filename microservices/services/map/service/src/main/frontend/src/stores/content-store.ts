import { defineStore } from 'pinia';
import { geoFeaturesStore, getChildren } from './geo-features-store';
import { TypedFeature } from 'src/components/models';
import { Feature } from 'geojson';

const geoFeatures = geoFeaturesStore();

export const contentStore = defineStore('content', {
  state: () => ({
    // initialized: false,
    content: [] as (TypedFeature|Feature)[],
    featureChain: [] as (TypedFeature|Feature)[],
  }),
  getters: {
    getContent: (state) => state.content,
    getFeatureChain: (state) => state.featureChain,
    getRootFeature: (state) => state.featureChain[0],
    getCurrentFeature: (state) => state.featureChain[state.featureChain.length - 1],
  },
  actions: {
    initialize() {
      this.content.length = 0;
      this.featureChain.length = 0;        
      for (const id in geoFeatures.geoQueryFeatures) {
          const feature = geoFeatures.geoQueryFeatures[id];
          this.content.push(feature);
      }
    },
    updateContent(typedFeatures: (TypedFeature|Feature)[]) {
        this.content = typedFeatures;
    },
    navigateBack() {
      const newLength = this.featureChain.length - 1;
      if (newLength >= 0) {
        this.featureChain.length = newLength; 
      } 
      
      if (newLength > 0){
        this.content = getChildren(this.featureChain[newLength-1]);
      } else {
        this.initialize();
      }
    },
    setCurrentFeature(typedFeature: TypedFeature|Feature) {
      const index = this.featureChain.indexOf(typedFeature);
      if (index > 0) {
        this.featureChain.length = index+1;
      } else {
        this.featureChain.push(typedFeature);
      }

      // now update the content
      this.content = getChildren(typedFeature);
    },
    deleteFeature(typedFeature: TypedFeature|Feature) {
      const index = this.content.indexOf(typedFeature);
      if (index >= 0){
        this.content.splice(index,1);
      }
    }
  },
});

const content = contentStore();
geoFeatures.$subscribe((mutation, state) => {
  if (content.getFeatureChain.length == 0 && content.getContent.length != Object.keys(state.geoQueryFeatures).length) {
    content.initialize();
  }
});

