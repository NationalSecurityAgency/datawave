package datawave.microservice.map.config;

import static org.springframework.beans.factory.config.ConfigurableBeanFactory.SCOPE_PROTOTYPE;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Scope;

import datawave.core.query.language.parser.jexl.LuceneToJexlQueryParser;
import datawave.microservice.config.accumulo.AccumuloProperties;

@EnableConfigurationProperties({MapServiceProperties.class})
@Configuration
public class MapServiceConfiguration {
    @Bean
    @Qualifier("warehouse")
    @ConfigurationProperties("datawave.map.accumulo")
    public AccumuloProperties accumuloProperties() {
        return new AccumuloProperties();
    }
    
    @Bean
    @Scope(SCOPE_PROTOTYPE)
    public LuceneToJexlQueryParser luceneToJexlQueryParser() {
        return new LuceneToJexlQueryParser();
    }
}
