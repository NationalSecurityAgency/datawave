package datawave.microservice.audit.config;

import org.springframework.amqp.core.Binding;
import org.springframework.beans.DirectFieldAccessor;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.stream.annotation.Output;
import org.springframework.cloud.stream.binding.BindingService;
import org.springframework.context.SmartLifecycle;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.integration.amqp.outbound.AmqpOutboundEndpoint;
import org.springframework.integration.channel.PublishSubscribeChannel;
import org.springframework.messaging.MessageChannel;

import java.util.Map;

/**
 * Configuration for the audit service.
 * <p>
 * This configuration is used to specify the binding for the audit producer, and to establish a confirm ack channel which is used to confirm that audit messages
 * have been successfully received by our messaging infrastructure.
 */
@Configuration
@EnableConfigurationProperties(AuditProperties.class)
public class AuditServiceConfig {
    public static final String CONFIRM_ACK_CHANNEL = "confirmAckChannel";
    
    public interface AuditSourceBinding {
        String NAME = "auditSource";
        
        @Output(NAME)
        MessageChannel auditSource();
    }
    
    @Bean(name = CONFIRM_ACK_CHANNEL)
    @ConditionalOnProperty(value = "audit.confirmAckEnabled", havingValue = "true", matchIfMissing = true)
    public MessageChannel confirmAckChannel() {
        return new PublishSubscribeChannel();
    }
    
    /**
     * Note: This is a workaround suggested by the Spring Cloud Stream developers in order to establish a confirm ack channel. We should remove this once Spring
     * Cloud Stream is updated to allow for a configurable confirm ack channel.
     */
    @Bean
    @ConditionalOnProperty(value = "audit.confirmAckEnabled", havingValue = "true", matchIfMissing = true)
    public SmartLifecycle confirmAckEnabler(BindingService bindingService, @Qualifier(CONFIRM_ACK_CHANNEL) MessageChannel confirmAckChannel) {
        return new SmartLifecycle() {
            
            @Override
            public int getPhase() {
                return Integer.MAX_VALUE;
            }
            
            @Override
            public void stop() {
                // no op
            }
            
            @Override
            public void start() {
                @SuppressWarnings("unchecked")
                Map<String,Binding> producers = (Map<String,Binding>) new DirectFieldAccessor(bindingService).getPropertyValue("producerBindings");
                ((AmqpOutboundEndpoint) new DirectFieldAccessor(producers.get(AuditSourceBinding.NAME)).getPropertyValue("lifecycle"))
                                .setConfirmAckChannel(confirmAckChannel);
            }
            
            @Override
            public boolean isRunning() {
                return false;
            }
            
            @Override
            public void stop(Runnable callback) {
                callback.run();
            }
            
            @Override
            public boolean isAutoStartup() {
                return true;
            }
        };
    }
}
