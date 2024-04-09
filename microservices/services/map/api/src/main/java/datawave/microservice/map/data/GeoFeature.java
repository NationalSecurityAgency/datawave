package datawave.microservice.map.data;

import org.geotools.feature.FeatureCollection;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import com.fasterxml.jackson.databind.annotation.JsonSerialize;

import datawave.microservice.map.data.serialization.FeatureCollectionDeserializer;
import datawave.microservice.map.data.serialization.FeatureCollectionSerializer;

public class GeoFeature {
    private String wkt;
    @JsonSerialize(using = FeatureCollectionSerializer.class)
    @JsonDeserialize(using = FeatureCollectionDeserializer.class)
    private FeatureCollection geoJson;
    
    public GeoFeature() {}
    
    public GeoFeature(String wkt, FeatureCollection geoJson) {
        this.wkt = wkt;
        this.geoJson = geoJson;
    }
    
    public String getWkt() {
        return wkt;
    }
    
    public void setWkt(String wkt) {
        this.wkt = wkt;
    }
    
    public FeatureCollection getGeoJson() {
        return geoJson;
    }
    
    public void setGeoJson(FeatureCollection geoJson) {
        this.geoJson = geoJson;
    }
}
