package datawave.microservice.model.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@Validated
@ConfigurationProperties(prefix = "datawave.model")
public class ModelProperties {
    
    private String defaultTableName;
    private String jqueryUri;
    private String dataTablesUri;
}
