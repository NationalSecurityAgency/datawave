package datawave.microservice.audit.auditors.file.config;

import datawave.microservice.audit.common.AuditMessage;
import datawave.microservice.audit.common.AuditMessageHandler;
import datawave.microservice.audit.auditors.file.FileAuditor;
import datawave.microservice.audit.config.AuditProperties;
import datawave.webservice.common.audit.AuditParameters;
import datawave.webservice.common.audit.Auditor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.ConfigurationProperties;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.SubscribableChannel;

import javax.annotation.Resource;
import java.util.List;

/**
 * Configures a FileAuditor to dump messages to the filesystem by request. This configuration is activated via the 'audit.auditors.dump.enabled' property. When
 * enabled, this configuration will also enable the appropriate Spring Cloud Stream configuration for the audit dump binding, as specified in the audit config.
 */
@Configuration
@EnableBinding(DumpAuditConfig.AuditDumpBinding.class)
@ConditionalOnProperty(name = "audit.auditors.dump.enabled", havingValue = "true")
public class DumpAuditConfig {
    
    @Bean("dumpAuditProperties")
    @ConfigurationProperties("audit.auditors.dump")
    public FileAuditProperties dumpAuditProperties() {
        return new FileAuditProperties();
    }
    
    @Resource(name = "msgHandlerAuditParams")
    private AuditParameters msgHandlerAuditParams;
    
    @Bean
    public AuditMessageHandler auditDumpMessageHandler(Auditor dumpAuditor) {
        return new AuditMessageHandler(msgHandlerAuditParams, dumpAuditor) {
            @Override
            @StreamListener(DumpAuditConfig.AuditDumpBinding.NAME)
            public void onMessage(AuditMessage msg) throws Exception {
                super.onMessage(msg);
            }
        };
    }
    
    @Bean
    public Auditor dumpAuditor(AuditProperties auditProperties, @Qualifier("dumpAuditProperties") FileAuditProperties dumpAuditProperties) throws Exception {
        String fileUri = (dumpAuditProperties.getFs().getFileUri() != null) ? dumpAuditProperties.getFs().getFileUri() : auditProperties.getFs().getFileUri();
        List<String> configResources = (dumpAuditProperties.getFs().getConfigResources() != null) ? dumpAuditProperties.getFs().getConfigResources()
                        : auditProperties.getFs().getConfigResources();
        
        // @formatter:off
        return new FileAuditor.Builder()
                .setFileUri(fileUri)
                .setPath(dumpAuditProperties.getPath())
                .setMaxFileAgeMillis(dumpAuditProperties.getMaxFileAgeMillis())
                .setMaxFileLenBytes(dumpAuditProperties.getMaxFileLenBytes())
                .setConfigResources(configResources)
                .setPrefix((dumpAuditProperties.getPrefix() != null) ? dumpAuditProperties.getPrefix() : "dump")
                .build();
        // @formatter:on
    }
    
    public interface AuditDumpBinding {
        String NAME = "dumpAuditSink";
        
        @Input(NAME)
        SubscribableChannel auditDumpSink();
    }
}
