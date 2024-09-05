package datawave.microservice.map.config;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;

import org.springframework.boot.context.properties.ConfigurationProperties;

import com.fasterxml.jackson.annotation.JsonInclude;
import com.fasterxml.jackson.annotation.JsonProperty;

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
    private List<Basemap> basemaps = new ArrayList<>();
    
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
    
    public List<Basemap> getBasemaps() {
        return basemaps;
    }
    
    public void setBasemaps(List<Basemap> basemaps) {
        this.basemaps = basemaps;
    }
    
    public void setGeoWaveTypes(Set<String> geoWaveTypes) {
        this.geoWaveTypes = geoWaveTypes;
    }
    
    @JsonInclude(JsonInclude.Include.NON_NULL)
    public static class Basemap {
        private String title;
        private String urlTemplate;
        @JsonProperty()
        private Integer maxZoom;
        private Integer maxNativeZoom;
        private String attribution;
        @JsonProperty("default")
        private Boolean defaultEnabled;
        
        public String getTitle() {
            return title;
        }
        
        public void setTitle(String title) {
            this.title = title;
        }
        
        public String getUrlTemplate() {
            return urlTemplate;
        }
        
        public void setUrlTemplate(String urlTemplate) {
            this.urlTemplate = urlTemplate;
        }
        
        public Integer getMaxZoom() {
            return maxZoom;
        }
        
        public void setMaxZoom(Integer maxZoom) {
            this.maxZoom = maxZoom;
        }
        
        public Integer getMaxNativeZoom() {
            return maxNativeZoom;
        }
        
        public void setMaxNativeZoom(Integer maxNativeZoom) {
            this.maxNativeZoom = maxNativeZoom;
        }
        
        public String getAttribution() {
            return attribution;
        }
        
        public void setAttribution(String attribution) {
            this.attribution = attribution;
        }
        
        public Boolean isDefaultEnabled() {
            return defaultEnabled;
        }
        
        public void setDefaultEnabled(Boolean defaultEnabled) {
            this.defaultEnabled = defaultEnabled;
        }
    }
}
