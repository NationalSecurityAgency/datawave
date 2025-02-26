package datawave.microservice.accumulo.lookup.config;

import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Lazy;
import org.springframework.web.context.annotation.RequestScope;

import datawave.marking.ColumnVisibilitySecurityMarking;
import datawave.marking.SecurityMarking;

@Lazy
@Configuration
@EnableConfigurationProperties({LookupProperties.class, LookupAuditProperties.class})
@ConditionalOnProperty(name = "accumulo.lookup.enabled", havingValue = "true", matchIfMissing = true)
public class LookupConfiguration {
    
    @Bean
    @ConditionalOnMissingBean
    public ResponseObjectFactory responseObjectFactory() {
        return new ResponseObjectFactory() {};
    }
    
    @Bean
    @RefreshScope
    @RequestScope
    @ConditionalOnMissingBean
    public SecurityMarking auditLookupSecurityMarking(LookupAuditProperties lookupAuditProperties) {
        return new ColumnVisibilitySecurityMarking();
    }
}
