package datawave.microservice.query.config;

import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(QueryProperties.class)
public class QueryStarterConfig {}
