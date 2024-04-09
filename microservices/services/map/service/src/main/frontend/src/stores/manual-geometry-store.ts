import { defineStore } from 'pinia';
import { QueryRangeSettings, QueryRangeSettingsMap, ManualGeometryForm } from 'components/models';

export const WKT = 'wkt';
export const GEOJSON = 'geojson';

export const WKT_LABEL = 'Well-Known Text';
export const GEOJSON_LABEL = 'GeoJSON';

export const GEOMETRY_TYPE_OPTIONS = [
    {
        label: WKT_LABEL,
        value: WKT
    },
    {
        label: GEOJSON_LABEL,
        value: GEOJSON
    }
];

export const GEO_POINT = 'GeoType';
export const GEOWAVE_POINT = 'PointType';
export const GEOWAVE_GEOMETRY = 'GeometryType';

export const GEO_POINT_LABEL = 'Geo Point';
export const GEOWAVE_POINT_LABEL = 'GeoWave Point';
export const GEOWAVE_GEOMETRY_LABEL = 'GeoWave Geometry';

export const RANGE_TYPE_OPTIONS = [
    {
        label: GEO_POINT_LABEL,
        value: GEO_POINT
    },
    {
        label: GEOWAVE_POINT_LABEL,
        value: GEOWAVE_POINT
    },
    {
        label: GEOWAVE_GEOMETRY_LABEL,
        value: GEOWAVE_GEOMETRY
    }
];

export const manualGeometryFormStore = (id: string) => 
    defineStore('manualGeometryForm-' + id, {
        state: () => ({
            geometry: '',
            geometryType: WKT,
            createRanges: false,
            rangeType: GEO_POINT,
            rangeSettings: {
                [GEO_POINT]: {
                    maxEnvelopes: 4,
                    maxExpansion: 400,
                    optimizeRanges: false
                } as QueryRangeSettings,
                [GEOWAVE_POINT]: {
                    maxEnvelopes: 4,
                    maxExpansion: 400,
                    optimizeRanges: false,
                    rangeSplitThreshold: 16,
                    maxRangeOverlap: 0.25
                } as QueryRangeSettings,
                [GEOWAVE_GEOMETRY]: {
                    maxEnvelopes: 4,
                    maxExpansion: 400,
                    optimizeRanges: false,
                    rangeSplitThreshold: 16,
                    maxRangeOverlap: 0.25
                } as QueryRangeSettings,
            } as QueryRangeSettingsMap
        } as ManualGeometryForm),
        getters: {

        },
        actions: {
            
        },
    })();
