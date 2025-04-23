package datawave.microservice.query.edge.config;

import javax.validation.constraints.NotEmpty;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

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
