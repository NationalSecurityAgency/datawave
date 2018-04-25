package datawave.microservice.audit.accumulo.config;

import datawave.microservice.audit.accumulo.AccumuloAuditor;
import datawave.microservice.audit.accumulo.config.AccumuloAuditProperties.Accumulo;
import datawave.microservice.audit.common.AuditMessageHandler;
import datawave.webservice.common.audit.AuditParameters;
import datawave.webservice.common.audit.Auditor;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.ClientConfiguration;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.commons.configuration.BaseConfiguration;
import org.apache.log4j.Logger;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.annotation.EnableBinding;
import org.springframework.cloud.stream.annotation.Input;
import org.springframework.cloud.stream.annotation.StreamListener;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.messaging.SubscribableChannel;

import javax.annotation.Resource;
import java.util.Map;

@Configuration
@EnableConfigurationProperties(AccumuloAuditProperties.class)
@EnableBinding(AccumuloAuditConfig.AccumuloAuditBinding.class)
@ConditionalOnProperty(name = "audit.accumulo.enabled", havingValue = "true")
public class AccumuloAuditConfig {
    
    private Logger log = Logger.getLogger(this.getClass());
    
    @Resource(name = "msgHandlerAuditParams")
    private AuditParameters msgHandlerAuditParams;
    
    @Bean
    public AuditMessageHandler accumuloAuditMessageHandler(Auditor accumuloAuditor) {
        return new AuditMessageHandler(msgHandlerAuditParams, accumuloAuditor) {
            @Override
            @StreamListener(AccumuloAuditBinding.NAME)
            public void onMessage(Map<String,String> msg) throws Exception {
                super.onMessage(msg);
            }
        };
    }
    
    @Bean
    public Auditor accumuloAuditor(AccumuloAuditProperties accumuloAuditProperties, Connector connector) {
        return new AccumuloAuditor(accumuloAuditProperties.getTableName(), connector);
    }
    
    @Bean
    @ConditionalOnMissingBean
    public Connector connector(AccumuloAuditProperties accumuloAuditProperties) {
        Accumulo accumulo = accumuloAuditProperties.getAccumuloConfig();
        final BaseConfiguration baseConfiguration = new BaseConfiguration();
        baseConfiguration.setDelimiterParsingDisabled(true); // Silence warnings about multi-value properties
        baseConfiguration.setProperty("instance.name", accumulo.getInstanceName());
        baseConfiguration.setProperty("instance.zookeeper.host", accumulo.getZookeepers());
        final ClientConfiguration clientConfiguration = new ClientConfiguration(baseConfiguration);
        final Instance instance = new ZooKeeperInstance(clientConfiguration);
        Connector connector = null;
        try {
            connector = instance.getConnector(accumulo.getUsername(), new PasswordToken(accumulo.getPassword()));
        } catch (AccumuloException | AccumuloSecurityException e) {
            log.error("Unable to contact Accumulo.", e);
        }
        return connector;
    }
    
    public interface AccumuloAuditBinding {
        String NAME = "accumuloAuditSink";
        
        @Input(NAME)
        SubscribableChannel accumuloAuditSink();
    }
}
