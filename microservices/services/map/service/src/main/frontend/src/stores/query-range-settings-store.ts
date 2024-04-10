import { defineStore } from 'pinia';

export const GEO_POINT = 'geopoint';
export const GEOWAVE_POINT = 'geowavepoint';
export const GEOWAVE_GEOMETRY = 'geowavegeometry';

export const queryRangeSettingsStore = (id: number) =>
  defineStore('queryRangeSettings-' + id, {
    state: () =>
      ({
        [GEO_POINT]: {
          maxEnvelopes: 4,
          maxExpansion: 400,
          optimizeRanges: false,
        } as QueryRangeSettings,
        [GEOWAVE_POINT]: {
          maxEnvelopes: 4,
          maxExpansion: 400,
          optimizeRanges: false,
          rangeSplitThreshold: 16,
          maxRangeOverlap: 0.25,
        } as QueryRangeSettings,
        [GEOWAVE_GEOMETRY]: {
          maxEnvelopes: 4,
          maxExpansion: 400,
          optimizeRanges: false,
          rangeSplitThreshold: 16,
          maxRangeOverlap: 0.25,
        } as QueryRangeSettings,
      } as QueryRangeSettingsMap),
    //   getters: {

    //   },
    //   actions: {

    //   },
  })();

interface QueryRangeSettingsMap {
  [rangeType: string]: QueryRangeSettings;
}

export interface QueryRangeSettings {
  maxEnvelopes: number;
  maxExpansion: number;
  optimizeRanges: boolean;
  rangeSplitThreshold?: number;
  maxRangeOverlap?: number;
}
