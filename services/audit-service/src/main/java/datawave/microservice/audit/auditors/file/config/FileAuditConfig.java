package datawave.microservice.audit.auditors.file.config;

import datawave.microservice.audit.auditors.file.FileAuditor;
import datawave.microservice.audit.config.AuditProperties;
import datawave.webservice.common.audit.Auditor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

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
    @ConfigurationProperties("audit.auditors.file")
    public FileAuditProperties fileAuditProperties() {
        return new FileAuditProperties();
    }
    
    @Bean(name = "fileAuditor")
    public Auditor fileAuditor(AuditProperties auditProperties, @Qualifier("fileAuditProperties") FileAuditProperties fileAuditProperties) throws Exception {
        String fileUri = (fileAuditProperties.getFs().getFileUri() != null) ? fileAuditProperties.getFs().getFileUri() : auditProperties.getFs().getFileUri();
        List<String> configResources = (fileAuditProperties.getFs().getConfigResources() != null) ? fileAuditProperties.getFs().getConfigResources()
                        : auditProperties.getFs().getConfigResources();
        
        // @formatter:off
        return new FileAuditor.Builder()
                .setFileUri(fileUri)
                .setPath(fileAuditProperties.getPath())
                .setMaxFileAgeMillis(fileAuditProperties.getMaxFileAgeMillis())
                .setMaxFileLenBytes(fileAuditProperties.getMaxFileLenBytes())
                .setConfigResources(configResources)
                .setPrefix(fileAuditProperties.getPrefix())
                .build();
        // @formatter:on
    }
}
