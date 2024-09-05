import { defineStore } from 'pinia';
import { DelegateTypedFeature, Geo, GeoByField, GeoByTier, GeoFeatures, GeoFunction, GeoQueryFeatures, GeoTerms, ManualGeometryForm, TypedFeature } from 'src/components/models';
import { api } from 'boot/axios';
import { GeoJsonObject, FeatureCollection, Feature, GeoJsonProperties } from 'geojson';
import { simpleMapStore } from './simple-map-store';
import { markRaw } from 'vue';
import L from 'leaflet';
import { onEachFeature } from './feature-info-store';

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
            let id = 'Query ';
            let i = 1;
            while (id + i in this.geoQueryFeatures) {
              i++;
            }
            id = id + i;

            const geoQueryFeatures = response.data as GeoQueryFeatures;
            geoQueryFeatures.id = id;
            geoQueryFeatures.label = id;
            geoQueryFeatures.typeName = GEO_QUERY_FEATURES;
            geoQueryFeatures.query = query;
            assignTypesAndLabelsVisitor(geoQueryFeatures);
            createLayers(geoQueryFeatures);
            for (const geoFunction of geoQueryFeatures.functions){
              enableLayers(geoFunction);
            }

            this.geoQueryFeatures[id] = geoQueryFeatures;
            resolve(id);
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
            let id = 'Query Lookup ';
            let i = 1;
            while (id + i in this.geoQueryFeatures) {
              i++;
            }
            id = id + i;

            const geoQueryFeatures = response.data as GeoQueryFeatures;
            geoQueryFeatures.id = id;
            geoQueryFeatures.label = id;
            geoQueryFeatures.typeName = GEO_QUERY_FEATURES;
            geoQueryFeatures.queryId = queryId;
            assignTypesAndLabelsVisitor(geoQueryFeatures);
            createLayers(geoQueryFeatures);
            for (const geoFunction of geoQueryFeatures.functions){
              enableLayers(geoFunction);
            }

            this.geoQueryFeatures[id] = geoQueryFeatures;
            resolve(id);
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
            let id = 'Geometry ';
            let i = 1;
            while (id + i in this.geoQueryFeatures) {
              i++;
            }
            id = id + i;
            
            const geoFeatures = response.data as GeoFeatures;
            geoFeatures.id = id;
            geoFeatures.label = id;
            geoFeatures.typeName = GEO_FEATURES;
            assignTypesAndLabelsVisitor(geoFeatures);
            createLayers(geoFeatures);
            enableLayers(geoFeatures.geometry);
      
            this.geoQueryFeatures[id] = geoFeatures;
            resolve(id);
          })
          .catch((reason) => {
            console.log('Something went wrong? ' + reason);
            reject(reason);
          });
      })
    },
    deleteGeoFeatures(id: string) {
      const geoFeatures = this.geoQueryFeatures[id];

      if (geoFeatures != undefined) {
        // first remove all of the layers from the map
        disableLayers(geoFeatures);

        // finally delete the features
        delete this.geoQueryFeatures[id];
      }
    },
    renameGeoFeatures(id: string, newId: string) {
      // first, find the feature
      const geoFeatures = this.geoQueryFeatures[id];

      // if the feature exists, change the label and rekey it
      if (geoFeatures != undefined) {
        geoFeatures.label = newId;
        delete this.geoQueryFeatures[id];
        this.geoQueryFeatures[newId] = geoFeatures;
      }
    }
  },
});

const leafletMap = simpleMapStore;

export interface GeoFeaturesMap {
  [queryId: string]: GeoQueryFeatures | GeoFeatures;
}

export const GEO_FEATURES = 'GeoFeatures';
export const GEO_QUERY_FEATURES = 'GeoQueryFeatures';
export const GEO_BY_FIELD = 'GeoByField';
export const GEO_TERMS = 'GeoTerms';
export const GEO_BY_TIER = 'GeoByTier';
export const GEO = 'Geo';
export const GEO_FUNCTION = 'GeoFunction';
export const GEO_FUNCTION_ARRAY = 'GeoFunctionArray';
export const FEATURE = 'Feature';
export const FEATURE_COLLECTION = 'FeatureCollection';


export function getTypeName(feature: TypedFeature|Feature): string | undefined {
  let typeName = '' as string | undefined;
  if('typeName' in feature) {
    typeName = (feature as TypedFeature).typeName;
  } else if ('type' in feature) {
    typeName = (feature as Feature).type;
  }
  return typeName;
}

export function getLabel(feature: TypedFeature | Feature): string {
  let label = '';
  if ('label' in feature && feature.label != undefined) {
    label = feature.label;
  } else if ('properties' in feature && feature.properties != undefined) {
    const properties = feature.properties as GeoJsonProperties;
    if (properties != null && properties.label != undefined) {
      label = properties.label;
    }
  }
  return label;
}

export function setLabel(feature: TypedFeature | Feature, label: string) {
  if ('label' in feature && feature.label != undefined) {
    feature.label = label;
  } else if ('properties' in feature && feature.properties != undefined) {
    const properties = feature.properties as GeoJsonProperties;
    if (properties != null && properties.label != undefined) {
      properties.label = label;
    }
  }
}

export function getLayer(feature: TypedFeature|Feature): L.Layer | undefined {
  let layer = undefined;
  if ('layer' in feature && feature.layer != undefined) {
    layer = feature.layer;
  } else if ('properties' in feature && feature.properties != undefined) {
    const properties = feature.properties as GeoJsonProperties;
    if (properties != null && properties.layer != undefined) {
      layer = properties.layer;
    }
  }
  return layer;
}

function geoByFieldTypedFeature(geoByField: GeoByField): TypedFeature {
  return {
    delegate: true,
    label: 'Geo By Field',
    typeName: GEO_BY_FIELD,
    feature: geoByField
  } as DelegateTypedFeature;
}

function geoFunctionsTypedFeature(functions: GeoFunction[]): TypedFeature {
  return {
    delegate: true,
    label: 'Geo Functions',
    typeName: GEO_FUNCTION_ARRAY,
    feature: functions
  } as DelegateTypedFeature;
}

function geoByTierTypedFeature(geoByTier: GeoByTier): TypedFeature {
  return {
    delegate: true,
    label: 'Geo By Tier',
    typeName: GEO_BY_TIER,
    feature: geoByTier
  } as DelegateTypedFeature;
}

function visitChildren(feature: TypedFeature|Feature, visit: (feature: TypedFeature|Feature) => void) {
  const typeName = getTypeName(feature);
  
  switch(typeName) {
    case GEO_QUERY_FEATURES: {
      const geoQueryFeatures = feature as GeoQueryFeatures;
      if (geoQueryFeatures.geoByField != undefined) {
        visit(geoByFieldTypedFeature(geoQueryFeatures.geoByField));
      }
      if (geoQueryFeatures.functions != undefined) {
        visit(geoFunctionsTypedFeature(geoQueryFeatures.functions));
      }
      break;
    }

    case GEO_BY_FIELD: {
      const geoByField = (feature as DelegateTypedFeature).feature as GeoByField;
      for (const field in geoByField) {
        visit(geoByField[field]);
      }
      break;
    }

    case GEO_TERMS: {
      const geoTerms = (feature as GeoTerms);
      if (geoTerms.geo != undefined) {
        if (geoTerms.geo != undefined && geoTerms.geo.geoJson != undefined) {
          const geoJson = geoTerms.geo.geoJson as GeoJsonObject;
          if (geoJson.type == FEATURE_COLLECTION) {
            for (const geoJsonFeature of (geoJson as FeatureCollection).features) {
              visit(geoJsonFeature);
            }
          } else if (geoJson.type == FEATURE) {
            const geoJsonFeature = geoJson as Feature;
            visit(geoJsonFeature);
          }
        }
      }
      if (geoTerms.geoByTier != undefined) {
        visit(geoByTierTypedFeature(geoTerms.geoByTier));
      }
      break;
    }

    case GEO_BY_TIER: {
      const geoByTier = (feature as DelegateTypedFeature).feature as GeoByTier;
      for (const field in geoByTier) {
        visit(geoByTier[field]);
      }
      break;
    }

    case GEO_FUNCTION_ARRAY: {
      const functions = (feature as DelegateTypedFeature).feature as GeoFunction[];
      for (const geoFunction of functions) {
        visit(geoFunction);
      }
      break;
    }

    case GEO: {
      const geoJson = (feature as Geo).geoJson as GeoJsonObject;
      if (geoJson.type == FEATURE_COLLECTION) {
        for (const childFeature of (geoJson as FeatureCollection).features) {
          visit(childFeature as TypedFeature);
        }
      } else if (geoJson.type == FEATURE) {
        visit(geoJson as TypedFeature);
      }
      break;
    }

    case GEO_FUNCTION: {
      const geoJson = (feature as GeoFunction).geoJson as GeoJsonObject;
      if (geoJson.type == FEATURE_COLLECTION) {
        for (const childFeature of (geoJson as FeatureCollection).features) {
          visit(childFeature as TypedFeature);
        }
      } else if (geoJson.type == FEATURE) {
        visit(geoJson as TypedFeature);
      }
      break;
    }

    case GEO_FEATURES: {
      const geoFeatures = feature as GeoFeatures;
      if (geoFeatures.geometry != undefined) {
        visit(geoFeatures.geometry);
      }
      if (geoFeatures.queryRanges != undefined) {
        visit(geoFeatures.queryRanges);
      }
      break;
    }

    case FEATURE: {
      // for our purposes features don't have children
      break;
    }

    default:
      console.log('Unknown type: ' + typeName);
  }
}

export function getChildren(feature: TypedFeature|Feature): (TypedFeature|Feature)[] {
  const children = [] as (TypedFeature | Feature)[];

  const typeName = getTypeName(feature);

  switch(typeName) {
    case GEO_QUERY_FEATURES: {
      const geoQueryFeatures = (feature as GeoQueryFeatures);
      if (geoQueryFeatures.geoByField != undefined && Object.keys(geoQueryFeatures.geoByField).length > 0) {
        children.push({
          delegate: true,
          label: 'Geo By Field',
          typeName: GEO_BY_FIELD,
          feature: (feature as GeoQueryFeatures).geoByField
        } as DelegateTypedFeature);
      }
      if (geoQueryFeatures.functions!= undefined && geoQueryFeatures.functions.length > 0) {
        children.push({
          delegate: true,
          label: 'Geo Functions',
          typeName: GEO_FUNCTION_ARRAY,
          feature: (feature as GeoQueryFeatures).functions
        } as DelegateTypedFeature);
      }
      break;
    }

    case GEO_BY_FIELD: {
      const geoByField = (feature as DelegateTypedFeature).feature as GeoByField;
      for (const field in geoByField) {
        children.push(geoByField[field]);
      }
      break;
    }

    case GEO_TERMS: {
      const geoTerms = (feature as GeoTerms);
      if (geoTerms.geo != undefined) {
        if (geoTerms.geo != undefined && geoTerms.geo.geoJson != undefined) {
          const geoJson = geoTerms.geo.geoJson as GeoJsonObject;
          if (geoJson.type == FEATURE_COLLECTION) {
            for (const geoJsonFeature of (geoJson as FeatureCollection).features) {
              children.push(geoJsonFeature);
            }
          } else if (geoJson.type == FEATURE) {
            const geoJsonFeature = geoJson as Feature;
            children.push(geoJsonFeature);
          }
        }
      }
      if (geoTerms.geoByTier != undefined) {
        children.push({
          delegate: true,
          label: 'Geo By Tier',
          typeName: GEO_BY_TIER,
          feature: geoTerms.geoByTier
        } as DelegateTypedFeature);
      }
      break;
    }

    case GEO_BY_TIER: {
      const geoByTier = (feature as DelegateTypedFeature).feature as GeoByTier;
      for (const field in geoByTier) {
        children.push(geoByTier[field]);
      }
      break;
    }

    case GEO_FUNCTION_ARRAY: {
      const functions = (feature as DelegateTypedFeature).feature as GeoFunction[];
      for (const geoFunction of functions) {
        children.push(geoFunction);
      }
      break;
    }

    case GEO: {
      const geoJson = (feature as Geo).geoJson as GeoJsonObject;
      if (geoJson.type == FEATURE_COLLECTION) {
        for (const childFeature of (geoJson as FeatureCollection).features) {
          children.push(childFeature as TypedFeature);
        }
      } else if (geoJson.type == FEATURE) {
        children.push(geoJson as TypedFeature);
      }
      break;
    }

    case GEO_FUNCTION: {
      const geoJson = (feature as GeoFunction).geoJson as GeoJsonObject;
      if (geoJson.type == FEATURE_COLLECTION) {
        for (const childFeature of (geoJson as FeatureCollection).features) {
          children.push(childFeature as TypedFeature);
        }
      } else if (geoJson.type == FEATURE) {
        children.push(geoJson as TypedFeature);
      }
      break;
    }

    case GEO_FEATURES: {
      const geoFeatures = feature as GeoFeatures;
      if (geoFeatures.geometry != undefined) {
        children.push(geoFeatures.geometry);
      }
      if (geoFeatures.queryRanges != undefined) {
        children.push(geoFeatures.queryRanges);
      }
      break;
    }

    default:
      console.log('Unknown type: ' + typeName);
  }
  return children;
}

function assignTypesAndLabelsVisitor(feature: TypedFeature|Feature) {
  const typeName = getTypeName(feature);

  switch(typeName) {
    case GEO_QUERY_FEATURES: {
      // geoByField and geoFunctions handles by visitChildren
      visitChildren(feature, assignTypesAndLabelsVisitor);
      break;
    }

    case GEO_BY_FIELD: {
      const geoByField = (feature as DelegateTypedFeature).feature as GeoByField;
      for (const field in geoByField) {
        geoByField[field].label = field;
        geoByField[field].typeName = GEO_TERMS;
      }
      visitChildren(feature, assignTypesAndLabelsVisitor);
      break;
    }

    case GEO_TERMS: {
      const geoTerms = feature as GeoTerms;
      if (geoTerms.geo != undefined && geoTerms.geo.geoJson != undefined) {
        const geoJson = geoTerms.geo.geoJson as GeoJsonObject;
        if (geoJson.type == FEATURE_COLLECTION) {
          for (const geoJsonFeature of (geoJson as FeatureCollection).features) {
            if (geoJsonFeature.properties != undefined) {
              geoJsonFeature.properties.label = geoJsonFeature.id;
            }
          }
        } else if (geoJson.type == FEATURE) {
          const geoJsonFeature = geoJson as Feature;
          if (geoJsonFeature.properties != undefined) {
            geoJsonFeature.properties.label = geoJsonFeature.id;
          }
        }
      }
      // geoByTier handled by visitChildren
      visitChildren(feature, assignTypesAndLabelsVisitor);
      break;
    }

    case GEO_BY_TIER: {
      const geoByTier = (feature as DelegateTypedFeature).feature as GeoByTier;
      for (const field in geoByTier) {
        geoByTier[field].label = field;
        geoByTier[field].typeName = GEO;
      }
      visitChildren(feature, assignTypesAndLabelsVisitor);
      break;
    }

    case GEO_FUNCTION_ARRAY: {
      for (const geoFunction of ((feature as DelegateTypedFeature).feature as GeoFunction[])) {
        geoFunction.label = geoFunction.function;
        geoFunction.typeName = GEO_FUNCTION;
      }
      visitChildren(feature, assignTypesAndLabelsVisitor);
      break;
    }

    case GEO: {
      const geoJson = (feature as Geo).geoJson as GeoJsonObject;
      if (geoJson.type == FEATURE_COLLECTION) {
        for (const geoJsonFeature of (geoJson as FeatureCollection).features) {
          if (geoJsonFeature.properties != undefined) {
            geoJsonFeature.properties.label = geoJsonFeature.id;
          }
        }
      } else if (geoJson.type == FEATURE) {
        const geoJsonFeature = geoJson as Feature;
        if (geoJsonFeature.properties != undefined) {
          geoJsonFeature.properties.label = geoJsonFeature.id;
        }
      }
      // no children to visit
      break;
    }

    case GEO_FUNCTION: {
      const geoJson = (feature as GeoFunction).geoJson as GeoJsonObject;
      if (geoJson.type == FEATURE_COLLECTION) {
        for (const geoJsonFeature of (geoJson as FeatureCollection).features) {
          if (geoJsonFeature.properties != undefined) {
            geoJsonFeature.properties.label = geoJsonFeature.id;
          }
        }
      } else if (geoJson.type == FEATURE) {
        const geoJsonFeature = geoJson as Feature;
        if (geoJsonFeature.properties != undefined) {
          geoJsonFeature.properties.label = geoJsonFeature.id;
        }
      }
      break;
    }

    case GEO_FEATURES: {
      const geoFeatures = feature as GeoFeatures;
      if (geoFeatures.geometry != undefined) {
        geoFeatures.geometry.label = 'Geometry';
        geoFeatures.geometry.typeName= GEO;
      }
      if (geoFeatures.queryRanges != undefined) {
        geoFeatures.queryRanges.label = 'Query Ranges';
        geoFeatures.queryRanges.typeName= GEO_TERMS;
      }
      visitChildren(feature, assignTypesAndLabelsVisitor);
      break;
    }

    case FEATURE_COLLECTION:
    case FEATURE:
      // do nothing
      break;

    default:
      console.log('Unknown type: ' + typeName);
  }
}

let visibleBounds: L.LatLngBounds | undefined;
export function getVisibleBounds(feature: TypedFeature|Feature): L.LatLngBounds | undefined {
  visibleBounds = undefined;
  return getVisibleBoundsInternal(feature);
}

function getVisibleBoundsInternal(feature: TypedFeature|Feature): L.LatLngBounds | undefined {
  const typeName = getTypeName(feature);

  switch(typeName) {
    case GEO_QUERY_FEATURES:
    case GEO_BY_FIELD:
    case GEO_TERMS: 
    case GEO_BY_TIER: 
    case GEO_FUNCTION_ARRAY: 
    case GEO: 
    case GEO_FUNCTION: 
    case GEO_FEATURES: 
    case FEATURE_COLLECTION:
      visitChildren(feature, getVisibleBoundsInternal);
      break;

    case FEATURE:
      let layer = undefined;
      const geoJsonFeature = feature as Feature;
      if (geoJsonFeature.properties != undefined) {
        if (geoJsonFeature.properties.layer != undefined) {
          layer = geoJsonFeature.properties.layer;
        }
      }

      if (layer != undefined && leafletMap.hasLayer(layer)) {
        const bounds = layer.getBounds()
        if (visibleBounds == undefined) {
          visibleBounds = bounds;
        } else {
          visibleBounds.extend(bounds);
        }
      }
      break;

    default:
      console.log('Unknown type: ' + typeName);
  }

  return visibleBounds;
}

let addToMapGlobal = true;
export function enableLayers(feature: TypedFeature|Feature, addToMap?: boolean) {
  addToMapGlobal = (addToMap == undefined) ? true : addToMap;

  enableLayersInternal(feature);
}

export function createLayers(feature: TypedFeature|Feature) {
  enableLayers(feature, false);
}

function enableLayersInternal(feature: TypedFeature|Feature) {
  const typeName = getTypeName(feature);

  switch(typeName) {
    case GEO_QUERY_FEATURES:
    case GEO_BY_FIELD:
    case GEO_TERMS: 
    case GEO_BY_TIER: 
    case GEO_FUNCTION_ARRAY: 
    case GEO_FEATURES: 
    case FEATURE_COLLECTION:
    case GEO:
    case GEO_FUNCTION:
      visitChildren(feature, enableLayersInternal);
      break;

    case FEATURE: {
      let layer = undefined;
      const geoJsonFeature = feature as Feature;
      if (geoJsonFeature.properties != undefined) {
        if (geoJsonFeature.properties.layer == undefined) {
          geoJsonFeature.properties.layer = markRaw(L.geoJSON(geoJsonFeature, {onEachFeature: onEachFeature}));
          geoJsonFeature.properties.color = '#1976d2';
          geoJsonFeature.properties.layer.setStyle({
            color: geoJsonFeature.properties.color,
            fillOpacity: 0.4,
            weight: 2
          });
        }

        layer = geoJsonFeature.properties.layer;
      }

      if (layer != undefined) {
        if (addToMapGlobal && !leafletMap.hasLayer(layer)) {
          leafletMap.addLayer(layer);
        }
      }
      break;
    }

    default:
      console.log('Unknown type: ' + typeName);
  }
}

export function disableLayers(feature: TypedFeature|Feature) {
  const typeName = getTypeName(feature);

  switch(typeName) {
    case GEO_QUERY_FEATURES:
    case GEO_BY_FIELD:
    case GEO_TERMS: 
    case GEO_BY_TIER: 
    case GEO_FUNCTION_ARRAY: 
    case GEO_FEATURES: 
    case FEATURE_COLLECTION:
    case GEO:
    case GEO_FUNCTION:
      visitChildren(feature, disableLayers);
      break;

    case FEATURE: {
      let layer = undefined;
      const geoJsonFeature = feature as Feature;
      if (geoJsonFeature.properties != undefined) {
        if (geoJsonFeature.properties.layer != undefined) {
          layer = geoJsonFeature.properties.layer;
        }
      }

      if (layer != undefined) {
        if (leafletMap.hasLayer(layer)) {
          leafletMap.removeLayer(layer);
        }
      }
      break;
    }

    default:
      console.log('Unknown type: ' + typeName + ', for feature: ', feature);
  }
}

let isVisibleGlobal = false;
export function layersVisible(feature: TypedFeature|Feature): boolean {
  isVisibleGlobal = false;
  return layersVisibleInternal(feature);
}

function layersVisibleInternal(feature: TypedFeature|Feature): boolean {
  const typeName = getTypeName(feature);

  switch(typeName) {
    case GEO_QUERY_FEATURES:
    case GEO_BY_FIELD:
    case GEO_TERMS: 
    case GEO_BY_TIER: 
    case GEO_FUNCTION_ARRAY: 
    case GEO_FEATURES: 
    case FEATURE_COLLECTION:
    case GEO:
    case GEO_FUNCTION:
      visitChildren(feature, layersVisibleInternal);
      break;

    case FEATURE:
      let layer = undefined;
      const geoJsonFeature = feature as Feature;
      if (geoJsonFeature.properties != undefined) {
        if (geoJsonFeature.properties.layer != undefined) {
          layer = geoJsonFeature.properties.layer;
        }
      }

      if (layer != undefined) {
        if (leafletMap.hasLayer(layer)) {
          isVisibleGlobal = true;
        }
      }
      break;

    default:
      console.log('Unknown type: ' + typeName);
  }

  return isVisibleGlobal;
}

let colors = [] as string[];
export function layersColor(feature: TypedFeature|Feature): string | undefined {
  colors = [];
  layersColorInternal(feature);

  if (colors.length == 1) {
    return colors[0];
  }
  
  return undefined;
}

function layersColorInternal(feature: TypedFeature|Feature) {
  const typeName = getTypeName(feature);

  switch(typeName) {
    case GEO_QUERY_FEATURES:
    case GEO_BY_FIELD:
    case GEO_TERMS: 
    case GEO_BY_TIER: 
    case GEO_FUNCTION_ARRAY: 
    case GEO_FEATURES: 
    case FEATURE_COLLECTION:
    case GEO:
    case GEO_FUNCTION:
      visitChildren(feature, layersColorInternal);
      break;

    case FEATURE:
      let layer = undefined;
      let color = undefined;
      const geoJsonFeature = feature as Feature;
      if (geoJsonFeature.properties != undefined) {
        if (geoJsonFeature.properties.layer != undefined) {
          layer = geoJsonFeature.properties.layer;
          color = geoJsonFeature.properties.color;
        }
      }

      if (layer != undefined && color != undefined) {
        if (colors.indexOf(color) < 0) {
          colors.push(color);
        }
      }
      break;

    default:
      console.log('Unknown type: ' + typeName);
  }
}

let selectedColor = ''
export function setLayerColor(feature: TypedFeature|Feature, color: string) {
  selectedColor = color;
  setLayerColorInternal(feature);
}

function setLayerColorInternal(feature: TypedFeature|Feature) {
  const typeName = getTypeName(feature);

  switch(typeName) {
    case GEO_QUERY_FEATURES:
    case GEO_BY_FIELD:
    case GEO_TERMS: 
    case GEO_BY_TIER: 
    case GEO_FUNCTION_ARRAY: 
    case GEO_FEATURES: 
    case FEATURE_COLLECTION:
    case GEO:
    case GEO_FUNCTION:
      visitChildren(feature, setLayerColorInternal);
      break;

    case FEATURE:
      let layer = undefined;
      const geoJsonFeature = feature as Feature;
      if (geoJsonFeature.properties != undefined) {
        if (geoJsonFeature.properties.layer != undefined) {
          layer = geoJsonFeature.properties.layer;
          geoJsonFeature.properties.color = selectedColor;
        }
      }

      if (layer != undefined) {
        layer.setStyle({
          color: selectedColor
        })
      }
      break;

    default:
      console.log('Unknown type: ' + typeName);
  }
}
