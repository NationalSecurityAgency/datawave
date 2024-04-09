package datawave.microservice.map.data.geo;

import datawave.microservice.map.data.AbstractGeoTerms;
import datawave.microservice.map.data.GeoFeature;

public class GeoPointTerms extends AbstractGeoTerms {
    // terms and ranges are encoded as features in the geojson
    private GeoFeature geo;
    
    public GeoPointTerms(String type) {
        super(type);
    }
    
    public GeoPointTerms(GeoFeature geo) {
        super("Geo Point");
        this.geo = geo;
    }
    
    public GeoFeature getGeo() {
        return geo;
    }
    
    public void setGeo(GeoFeature geo) {
        this.geo = geo;
    }
}
