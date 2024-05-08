package datawave.microservice.map.config;

import java.util.Set;

import org.springframework.boot.context.properties.ConfigurationProperties;

@ConfigurationProperties(prefix = "datawave.map")
public class MapServiceProperties {
    private String authorizationUri = "https://authorization:8443/authorization/v1/authorize";
    private String dictionaryUri = "https://dictionary:8443/dictionary/data/v1/";
    private String metricsUri = "https://metrics:8443/querymetric/v1/id/";
    private String metadataTableName;
    private Set<String> geoFields;
    private Set<String> geoWaveFields;
    private Set<String> geoTypes;
    private Set<String> geoWaveTypes;
    
    public String getAuthorizationUri() {
        return authorizationUri;
    }
    
    public void setAuthorizationUri(String authorizationUri) {
        this.authorizationUri = authorizationUri;
    }
    
    public String getDictionaryUri() {
        return dictionaryUri;
    }
    
    public void setDictionaryUri(String dictionaryUri) {
        this.dictionaryUri = dictionaryUri;
    }
    
    public String getMetricsUri() {
        return metricsUri;
    }
    
    public void setMetricsUri(String metricsUri) {
        this.metricsUri = metricsUri;
    }
    
    public String getMetadataTableName() {
        return metadataTableName;
    }
    
    public void setMetadataTableName(String metadataTableName) {
        this.metadataTableName = metadataTableName;
    }
    
    public Set<String> getGeoFields() {
        return geoFields;
    }
    
    public void setGeoFields(Set<String> geoFields) {
        this.geoFields = geoFields;
    }
    
    public Set<String> getGeoWaveFields() {
        return geoWaveFields;
    }
    
    public void setGeoWaveFields(Set<String> geoWaveFields) {
        this.geoWaveFields = geoWaveFields;
    }
    
    public Set<String> getGeoTypes() {
        return geoTypes;
    }
    
    public void setGeoTypes(Set<String> geoTypes) {
        this.geoTypes = geoTypes;
    }
    
    public Set<String> getGeoWaveTypes() {
        return geoWaveTypes;
    }
    
    public void setGeoWaveTypes(Set<String> geoWaveTypes) {
        this.geoWaveTypes = geoWaveTypes;
    }
}
