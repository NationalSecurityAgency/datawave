package datawave.microservice.query.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@EnableConfigurationProperties(QueryProperties.class)
@Configuration
public class QueryServiceConfig {}
