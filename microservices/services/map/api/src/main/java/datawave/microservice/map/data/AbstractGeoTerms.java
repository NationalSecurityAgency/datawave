package datawave.microservice.map.data;

public abstract class AbstractGeoTerms {
    private String type;
    
    public AbstractGeoTerms(String type) {
        this.type = type;
    }
    
    public String getType() {
        return type;
    }
}
