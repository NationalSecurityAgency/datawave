package datawave.microservice.map.data;

public class GeoFeatures {
    
    private GeoFeature geometry;
    
    private AbstractGeoTerms queryRanges;
    
    public GeoFeature getGeometry() {
        return geometry;
    }
    
    public void setGeometry(GeoFeature geometry) {
        this.geometry = geometry;
    }
    
    public AbstractGeoTerms getQueryRanges() {
        return queryRanges;
    }
    
    public void setQueryRanges(AbstractGeoTerms queryRanges) {
        this.queryRanges = queryRanges;
    }
}
