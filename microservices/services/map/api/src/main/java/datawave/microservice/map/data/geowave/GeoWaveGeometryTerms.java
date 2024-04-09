package datawave.microservice.map.data.geowave;

import java.util.Map;

import datawave.microservice.map.data.AbstractGeoTerms;
import datawave.microservice.map.data.GeoFeature;

public class GeoWaveGeometryTerms extends AbstractGeoTerms {
    // terms and ranges are encoded as features in the geojson
    private Map<Integer,GeoFeature> geoByTier;
    
    public GeoWaveGeometryTerms(String type) {
        super(type);
    }
    
    public GeoWaveGeometryTerms(Map<Integer,GeoFeature> geoByTier) {
        super("GeoWave Geometry");
        this.geoByTier = geoByTier;
    }
    
    public Map<Integer,GeoFeature> getGeoByTier() {
        return geoByTier;
    }
    
    public void setGeoByTier(Map<Integer,GeoFeature> geoByTier) {
        this.geoByTier = geoByTier;
    }
}
