package datawave.microservice.config.discovery.rabbit;

import org.aspectj.lang.annotation.Aspect;
import org.springframework.boot.autoconfigure.aop.AopAutoConfiguration;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.cloud.client.discovery.EnableDiscoveryClient;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.context.annotation.Import;
import org.springframework.retry.annotation.EnableRetry;
import org.springframework.retry.annotation.Retryable;
import org.springframework.retry.interceptor.RetryInterceptorBuilder;
import org.springframework.retry.interceptor.RetryOperationsInterceptor;

@ConditionalOnProperty(value = "spring.rabbitmq.discovery.enabled")
@Configuration
@EnableDiscoveryClient
public class RabbitDiscoveryConfiguration {
    @Bean
    public RabbitDiscoveryInstanceProvider rabbitDiscoveryInstanceProvider(DiscoveryClient discoveryClient) {
        return new RabbitDiscoveryInstanceProvider(discoveryClient);
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
