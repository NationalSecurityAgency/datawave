package datawave.microservice.config.discovery.rabbit;

import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.aop.AopAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.cloud.client.discovery.event.HeartbeatEvent;
import org.springframework.cloud.client.discovery.event.HeartbeatMonitor;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.retry.annotation.Retryable;
import org.springframework.retry.interceptor.RetryInterceptorBuilder;
import org.springframework.retry.interceptor.RetryOperationsInterceptor;

@ConditionalOnProperty(value = "spring.rabbitmq.discovery.enabled")
@Configuration
@EnableDiscoveryClient
public class RabbitDiscoveryConfiguration {
    private static Logger logger = LoggerFactory.getLogger(RabbitDiscoveryConfiguration.class);
    
    @Autowired
    private RabbitDiscoveryProperties rabbitProperties;
    
    @Autowired
    private RabbitDiscoveryInstanceProvider instanceProvider;
    
    @Autowired
    private CachingConnectionFactory connectionFactory;
    
    private HeartbeatMonitor monitor = new HeartbeatMonitor();
    
    @Bean
    public RabbitDiscoveryInstanceProvider rabbitDiscoveryInstanceProvider(DiscoveryClient discoveryClient) {
        return new RabbitDiscoveryInstanceProvider(discoveryClient);
    }
    
    @EventListener(ContextRefreshedEvent.class)
    public void startup() {
        refresh();
    }
    
    @EventListener(HeartbeatEvent.class)
    public void heartbeat(HeartbeatEvent event) {
        if (monitor.update(event.getValue())) {
            refresh();
        }
    }
    
    private void refresh() {
        try {
            String serviceId = rabbitProperties.getServiceId();
            ServiceInstance server = instanceProvider.getRabbitMQServerInstance(serviceId);
            connectionFactory.setHost(server.getHost());
            connectionFactory.setPort(server.getPort());
            connectionFactory.setAddresses(server.getHost() + ":" + server.getPort());
        } catch (Exception e) {
            if (rabbitProperties.isFailFast()) {
                throw e;
            } else {
                logger.warn("Could not locate rabbitmq server via discovery", e);
            }
        }
    }
    
    @ConditionalOnProperty(value = "spring.rabbitmq.discovery.failFast")
    @ConditionalOnClass({Retryable.class, Aspect.class, AopAutoConfiguration.class})
    @Configuration
    @EnableRetry(proxyTargetClass = true)
    @Import(AopAutoConfiguration.class)
    @EnableConfigurationProperties(RetryProperties.class)
    protected static class RetryConfiguration {
        
        @Bean
        @ConditionalOnMissingBean(name = "rabbitDiscoveryRetryInterceptor")
        public RetryOperationsInterceptor rabbitDiscoveryRetryInterceptor(RetryProperties properties) {
            return RetryInterceptorBuilder.stateless().backOffOptions(properties.getInitialInterval(), properties.getMultiplier(), properties.getMaxInterval())
                            .maxAttempts(properties.getMaxAttempts()).build();
        }
    }
    
}
