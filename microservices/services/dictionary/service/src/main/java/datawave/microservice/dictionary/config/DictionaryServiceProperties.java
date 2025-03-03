package datawave.microservice.dictionary.config;

import javax.validation.Valid;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import datawave.microservice.config.accumulo.AccumuloProperties;

@ConfigurationProperties(prefix = "datawave.dictionary")
@Validated
public class DictionaryServiceProperties {
    @Valid
    private AccumuloProperties accumuloProperties = new AccumuloProperties();
    
    public AccumuloProperties getAccumuloProperties() {
        return accumuloProperties;
    }
}
