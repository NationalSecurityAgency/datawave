package datawave.microservice.metadata.config;

import java.util.HashMap;
import java.util.Map;
import java.util.Set;

import javax.validation.constraints.NotEmpty;
import javax.validation.constraints.NotNull;

import org.apache.accumulo.core.security.Authorizations;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.validation.annotation.Validated;

@Validated
@ConfigurationProperties(prefix = "datawave.metadata")
@EnableConfigurationProperties(MetadataHelperProperties.class)
public class MetadataHelperProperties {
    @NotEmpty
    private Set<Authorizations> allMetadataAuths;
    @NotNull
    private Map<String,String> typeSubstitutions;
    
    public MetadataHelperProperties() {
        typeSubstitutions = new HashMap<>();
        typeSubstitutions.put("datawave.data.type.DateType", "datawave.data.type.RawDateType");
    }
    
    public Set<Authorizations> getAllMetadataAuths() {
        return allMetadataAuths;
    }
    
    public void setAllMetadataAuths(Set<Authorizations> allMetadataAuths) {
        this.allMetadataAuths = allMetadataAuths;
    }
    
    public Map<String,String> getTypeSubstitutions() {
        return typeSubstitutions;
    }
    
    public void setTypeSubstitutions(Map<String,String> typeSubstitutions) {
        this.typeSubstitutions = typeSubstitutions;
    }
}
