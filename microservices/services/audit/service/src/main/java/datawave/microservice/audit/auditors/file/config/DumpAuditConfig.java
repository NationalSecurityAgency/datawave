package datawave.microservice.audit.auditors.file.config;

import java.util.List;

import javax.annotation.Resource;
import javax.validation.Valid;

import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;

import datawave.microservice.audit.auditors.file.FileAuditor;
import datawave.microservice.audit.common.AuditMessageConsumer;
import datawave.microservice.audit.config.AuditProperties;
import datawave.webservice.common.audit.AuditParameters;
import datawave.webservice.common.audit.Auditor;

/**
 * Configures a FileAuditor to dump messages to the filesystem by request. This configuration is activated via the 'audit.auditors.dump.enabled' property. When
 * enabled, this configuration will also enable the appropriate Spring Cloud Stream configuration for the audit dump binding, as specified in the audit config.
 */
@Configuration
@ConditionalOnProperty(name = "audit.auditors.dump.enabled", havingValue = "true")
public class DumpAuditConfig {
    
    @Bean("dumpAuditProperties")
    @Valid
    @ConfigurationProperties("audit.auditors.dump")
    public FileAuditProperties dumpAuditProperties() {
        return new FileAuditProperties();
    }
    
    @Resource(name = "msgHandlerAuditParams")
    private AuditParameters msgHandlerAuditParams;
    
    @Bean
    public AuditMessageConsumer dumpAuditSink(Auditor dumpAuditor) {
        return new AuditMessageConsumer(msgHandlerAuditParams, dumpAuditor);
    }
    
    @Bean
    public Auditor dumpAuditor(AuditProperties auditProperties, @Qualifier("dumpAuditProperties") FileAuditProperties dumpAuditProperties) throws Exception {
        List<String> fsConfigResources = (dumpAuditProperties.getFsConfigResources() != null) ? dumpAuditProperties.getFsConfigResources()
                        : auditProperties.getFsConfigResources();
        
        String subPath = dumpAuditProperties.getSubPath();
        if (subPath == null && dumpAuditProperties.getSubPathEnvVar() != null)
            subPath = System.getenv(dumpAuditProperties.getSubPathEnvVar());
        
        // @formatter:off
        return new FileAuditor.Builder()
                .setPath(dumpAuditProperties.getPathUri())
                .setSubPath(subPath)
                .setFsConfigResources(fsConfigResources)
                .setMaxFileAgeSeconds(dumpAuditProperties.getMaxFileAgeSeconds())
                .setMaxFileLengthMB(dumpAuditProperties.getMaxFileLengthMB())
                .setPrefix((dumpAuditProperties.getPrefix() != null) ? dumpAuditProperties.getPrefix() : "dump")
                .build();
        // @formatter:on
    }
}
