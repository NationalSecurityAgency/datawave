package datawave.microservice.modification.config;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.validation.annotation.Validated;

import lombok.Getter;
import lombok.Setter;

@Getter
@Setter
@ConfigurationProperties(prefix = "datawave.modification.data")
@Validated
public class ModificationDataProperties {
    private String xmlBeansPath = "classpath:ModificationServices.xml";
}
