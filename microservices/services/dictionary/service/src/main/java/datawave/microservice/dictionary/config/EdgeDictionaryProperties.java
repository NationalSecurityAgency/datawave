package datawave.microservice.dictionary.config;

import javax.validation.constraints.NotBlank;
import javax.validation.constraints.Positive;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@ConfigurationProperties(prefix = "datawave.dictionary.edge")
@Validated
public class EdgeDictionaryProperties {
    
    @NotBlank
    private String metadataTableName;
    @Positive
    private int numThreads;
}
