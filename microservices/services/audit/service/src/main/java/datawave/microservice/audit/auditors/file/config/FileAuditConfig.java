package datawave.microservice.audit.auditors.file.config;

import java.util.List;

import javax.validation.Valid;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import datawave.microservice.audit.auditors.file.FileAuditor;
import datawave.microservice.audit.config.AuditProperties;
import datawave.webservice.common.audit.Auditor;

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
        List<String> fsConfigResources = (fileAuditProperties.getFsConfigResources() != null) ? fileAuditProperties.getFsConfigResources()
                        : auditProperties.getFsConfigResources();
        
        String subPath = fileAuditProperties.getSubPath();
        if (subPath == null && fileAuditProperties.getSubPathEnvVar() != null)
            subPath = System.getenv(fileAuditProperties.getSubPathEnvVar());
        
        // @formatter:off
        return new FileAuditor.Builder()
                .setUser(fileAuditProperties.getUser())
                .setPath(fileAuditProperties.getPathUri())
                .setSubPath(subPath)
                .setFsConfigResources(fsConfigResources)
                .setMaxFileAgeSeconds(fileAuditProperties.getMaxFileAgeSeconds())
                .setMaxFileLengthMB(fileAuditProperties.getMaxFileLengthMB())
                .setPrefix(fileAuditProperties.getPrefix())
                .build();
        // @formatter:on
    }
}
