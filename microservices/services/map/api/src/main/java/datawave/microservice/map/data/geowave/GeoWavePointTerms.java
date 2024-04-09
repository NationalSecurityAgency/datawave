package datawave.microservice.map.data.geowave;

import datawave.microservice.map.data.AbstractGeoTerms;
import datawave.microservice.map.data.GeoFeature;

public class GeoWavePointTerms extends AbstractGeoTerms {
    // terms and ranges are encoded as features in the geojson
    private GeoFeature geo;
    
    public GeoWavePointTerms(String type) {
        super(type);
    }
    
    public GeoWavePointTerms(GeoFeature geo) {
        super("GeoWave Point");
        this.geo = geo;
    }
    
    public GeoFeature getGeo() {
        return geo;
    }
    
    public void setGeo(GeoFeature geo) {
        this.geo = geo;
    }
}
