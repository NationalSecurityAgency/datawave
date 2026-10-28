package datawave.configuration.spring;

import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Configuration;

@Configuration
@EnableConfigurationProperties(SpringContextProperties.class)
@ConditionalOnProperty(name = "datawave.configuration.spring.configure-from-properties", havingValue = "true")
public class SpringContextConfiguration {

}
