package datawave.microservice.audit.config.discovery;

import org.aspectj.lang.annotation.Aspect;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.aop.AopAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
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

import datawave.microservice.audit.AuditServiceProvider;
import datawave.microservice.audit.config.AuditServiceProperties;

@Configuration
@ConditionalOnBean(DiscoveryClient.class)
@ConditionalOnProperty(name = "audit-client.discovery.enabled", havingValue = "true")
@EnableConfigurationProperties(AuditServiceDiscoveryProperties.class)
@EnableDiscoveryClient
public class AuditServiceDiscoveryConfiguration {
    
    private static Logger logger = LoggerFactory.getLogger(AuditServiceDiscoveryConfiguration.class);
    
    private final AuditServiceProperties serviceProperties;
    private final AuditServiceDiscoveryProperties discoveryProperties;
    private final AuditServiceProvider instanceProvider;
    private final HeartbeatMonitor monitor;
    
    //@formatter:off
    @Autowired
    public AuditServiceDiscoveryConfiguration(
            AuditServiceProperties serviceProperties,
            AuditServiceDiscoveryProperties discoveryProperties,
            AuditServiceProvider instanceProvider) {
        this.serviceProperties = serviceProperties;
        this.discoveryProperties = discoveryProperties;
        this.instanceProvider = instanceProvider;
        this.monitor = new HeartbeatMonitor();
    }
    //@formatter:on
    
    @Bean
    public AuditServiceDiscoveryProperties auditDiscoveryProperties() {
        return new AuditServiceDiscoveryProperties();
    }
    
    @Bean
    public AuditServiceProvider auditDiscoveryInstanceProvider(DiscoveryClient discoveryClient) {
        return new RetryableServiceProvider(serviceProperties, discoveryClient);
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
        logger.debug("Refreshing audit service instance");
        try {
            ServiceInstance si = instanceProvider.getServiceInstance();
            logger.debug("Audit server located. URI [{}]", si.getUri());
        } catch (Exception e) {
            if (discoveryProperties.isFailFast()) {
                throw e;
            } else {
                logger.warn("Audit service discovery failed [serviceId: " + serviceProperties.getServiceId() + "]", e);
            }
        }
    }
    
    @ConditionalOnProperty(value = "audit.discovery.failFast")
    @ConditionalOnClass({Retryable.class, Aspect.class, AopAutoConfiguration.class})
    @Configuration
    @EnableRetry(proxyTargetClass = true)
    @Import(AopAutoConfiguration.class)
    @EnableConfigurationProperties(RetryProperties.class)
    protected static class RetryConfiguration {
        @Bean
        @ConditionalOnMissingBean(name = "auditDiscoveryRetryInterceptor")
        public RetryOperationsInterceptor auditDiscoveryRetryInterceptor(RetryProperties properties) {
            return RetryInterceptorBuilder.stateless().backOffOptions(properties.getInitialInterval(), properties.getMultiplier(), properties.getMaxInterval())
                            .maxAttempts(properties.getMaxAttempts()).build();
        }
    }
}
