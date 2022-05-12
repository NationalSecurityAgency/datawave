package datawave.microservice.query.config;

import datawave.microservice.query.edge.config.EdgeDictionaryProviderProperties;
import datawave.microservice.query.stream.StreamingProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties({QueryProperties.class, StreamingProperties.class, EdgeDictionaryProviderProperties.class})
public class QueryStarterConfig {}
