package datawave.microservice.query.config;

import datawave.microservice.query.stream.StreamingProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties({QueryProperties.class, StreamingProperties.class})
public class QueryStarterConfig {}
