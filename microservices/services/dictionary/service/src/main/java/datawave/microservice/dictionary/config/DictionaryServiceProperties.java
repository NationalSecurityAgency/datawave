package datawave.microservice.dictionary.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import datawave.microservice.config.accumulo.AccumuloProperties;
import jakarta.validation.Valid;

@ConfigurationProperties(prefix = "datawave.dictionary")
@Validated
public class DictionaryServiceProperties {
    @Valid
    private AccumuloProperties accumuloProperties = new AccumuloProperties();
    
    public AccumuloProperties getAccumuloProperties() {
        return accumuloProperties;
    }
}
