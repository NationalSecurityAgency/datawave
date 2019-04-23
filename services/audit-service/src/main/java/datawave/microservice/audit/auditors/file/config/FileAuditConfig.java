package datawave.microservice.audit.auditors.file.config;

import datawave.microservice.audit.auditors.file.FileAuditor;
import datawave.microservice.audit.config.AuditProperties;
import datawave.webservice.common.audit.Auditor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import javax.validation.Valid;
import java.util.List;

/**
 * Configures an FileAuditor to process messages received by the audit service in the case that our messaging infrastructure has failed. This configuration is
 * activated via the 'audit.auditors.file.enabled' property.
 *
 */
@Configuration
@ConditionalOnProperty(name = "audit.auditors.file.enabled", havingValue = "true")
public class FileAuditConfig {
    
    @Bean("fileAuditProperties")
    @Valid
    @ConfigurationProperties("audit.auditors.file")
    public FileAuditProperties fileAuditProperties() {
        return new FileAuditProperties();
    }
    
    @Bean(name = "fileAuditor")
    public Auditor fileAuditor(AuditProperties auditProperties, @Qualifier("fileAuditProperties") FileAuditProperties fileAuditProperties) throws Exception {
        List<String> configResources = (fileAuditProperties.getFs().getConfigResources() != null) ? fileAuditProperties.getFs().getConfigResources()
                        : auditProperties.getFs().getConfigResources();
        
        String subpath = fileAuditProperties.getSubpath();
        if (subpath == null && fileAuditProperties.getSubpathEnvVar() != null)
            subpath = System.getenv(fileAuditProperties.getSubpathEnvVar());
        
        // @formatter:off
        return new FileAuditor.Builder()
                .setPath(fileAuditProperties.getPathUri())
                .setSubpath(subpath)
                .setMaxFileAgeMillis(fileAuditProperties.getMaxFileAgeMillis())
                .setMaxFileLenBytes(fileAuditProperties.getMaxFileLenBytes())
                .setConfigResources(configResources)
                .setPrefix(fileAuditProperties.getPrefix())
                .build();
        // @formatter:on
    }
}
