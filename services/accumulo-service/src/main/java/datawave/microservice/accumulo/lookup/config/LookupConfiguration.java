package datawave.microservice.accumulo.lookup.config;

import datawave.marking.ColumnVisibilitySecurityMarking;
import datawave.marking.SecurityMarking;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.context.config.annotation.RefreshScope;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.web.context.annotation.RequestScope;

@Configuration
@EnableConfigurationProperties({LookupProperties.class, LookupAuditProperties.class})
@ConditionalOnProperty(name = "accumulo.lookup.enabled", havingValue = "true", matchIfMissing = true)
public class LookupConfiguration {
    
    @Bean
    @RefreshScope
    @RequestScope
    @ConditionalOnMissingBean
    public SecurityMarking auditLookupSecurityMarking(LookupAuditProperties lookupAuditProperties) {
        ColumnVisibilitySecurityMarking auditCVSM = new ColumnVisibilitySecurityMarking();
        auditCVSM.setColumnVisibility(lookupAuditProperties.getDefaultColumnVisibility());
        return auditCVSM;
    }
}
