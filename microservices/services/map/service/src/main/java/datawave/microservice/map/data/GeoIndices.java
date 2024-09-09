package datawave.microservice.map.data;

import java.util.List;

public class GeoIndices {
    private String wkt;
    private String geoPointIndex;
    private String geoWavePointIndex;
    private List<String> geoWaveGeometryIndex;
    
    public String getWkt() {
        return wkt;
    }
    
    public void setWkt(String wkt) {
        this.wkt = wkt;
    }
    
    public String getGeoPointIndex() {
        return geoPointIndex;
    }
    
    public void setGeoPointIndex(String geoPointIndex) {
        this.geoPointIndex = geoPointIndex;
    }
    
    public String getGeoWavePointIndex() {
        return geoWavePointIndex;
    }
    
    public void setGeoWavePointIndex(String geoWavePointIndex) {
        this.geoWavePointIndex = geoWavePointIndex;
    }
    
    public List<String> getGeoWaveGeometryIndex() {
        return geoWaveGeometryIndex;
    }
    
    public void setGeoWaveGeometryIndex(List<String> geoWaveGeometryIndex) {
        this.geoWaveGeometryIndex = geoWaveGeometryIndex;
    }
}
