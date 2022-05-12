package datawave.microservice.query.edge.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import javax.validation.constraints.NotEmpty;

@Validated
@ConfigurationProperties(prefix = "datawave.edge-dictionary-provider")
public class EdgeDictionaryProviderProperties {
    @NotEmpty
    private String uri = "https://dictionary:8443/dictionary/edge/v1/";
    
    public String getUri() {
        return uri;
    }
    
    public void setUri(String uri) {
        this.uri = uri;
    }
}
