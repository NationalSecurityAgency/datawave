package datawave.microservice.dictionary.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import datawave.microservice.config.accumulo.AccumuloProperties;
import jakarta.validation.Valid;
import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@ConfigurationProperties(prefix = "datawave.dictionary")
@Validated
public class DictionaryServiceProperties {
    @Valid
    private AccumuloProperties accumuloProperties = new AccumuloProperties();
    private System system = new System();
    
    public AccumuloProperties getAccumuloProperties() {
        return accumuloProperties;
    }
    
    @Getter
    @Setter
    public static class System {
        public String systemName;
    }
}
