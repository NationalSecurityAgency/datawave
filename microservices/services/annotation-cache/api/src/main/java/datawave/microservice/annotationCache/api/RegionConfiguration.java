package datawave.microservice.annotationCache.api;

import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Configuration;

import lombok.Data;

@Configuration
@Data
@ConfigurationProperties(prefix = "region")
public class RegionConfiguration {
    String name;
}
