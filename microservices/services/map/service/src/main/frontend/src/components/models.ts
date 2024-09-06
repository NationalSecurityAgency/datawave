export interface Todo {
  id: number;
  content: string;
}

export interface Meta {
  totalCount: number;
}

export interface TypedFeature {
  typeName?: string;
  label?: string;
  id?: string;
}

export interface DelegateTypedFeature extends TypedFeature{
  delegate: boolean;
  feature: TypedFeature;
}

export interface GeoFeatures extends TypedFeature {
  geometry: Geo;
  queryRanges?: GeoTerms;
}

export interface GeoQueryFeatures extends TypedFeature {
  geoByField: GeoByField;
  functions: GeoFunction[];

  // fields added by us
  queryId?: string;
  query?: string;
}

export interface GeoByField {
  [key: string]: GeoTerms;
}

export interface GeoTerms extends TypedFeature {
  type: string;
  geo?: Geo;
  geoByTier?: GeoByTier;
}

export interface GeoByTier {
  [key: string]: Geo;
}

export interface Geo extends TypedFeature {
  wkt: string;
  geoJson: object;
}

export interface GeoFunction extends TypedFeature {
  function: string;
  fields: string[];
  geoJson: object;
}

export interface AppState {
  configPanel: ConfigPanel;
  content: Content;
}

export interface ConfigPanel {
  enabled: boolean;
  selection: string;
}

export interface Content {
  geometry: string;
  editDialog: boolean;
  editDialogText: string;
}

export interface ManualGeometryForm {
  geometry: string;
  createRanges: boolean;
  rangeType: string;
  rangeSettings: QueryRangeSettingsMap;
}

export interface QueryRangeSettingsMap {
  [rangeType: string]: QueryRangeSettings;
}

export interface QueryRangeSettings {
  maxEnvelopes: number;
  maxExpansion: number;
  optimizeRanges: boolean;
  rangeSplitThreshold?: number;
  maxRangeOverlap?: number;
}

export interface ManualQueryForm {
  query: string;
  fieldTypes: FieldType[];
  expand: boolean;
}

export interface FieldType {
  id: number;
  field: string;
  type: {
    label: string,
    value: string
  };
}

export interface Banner {
  enabled: boolean;
  message?: string;
  style?: string;
}
