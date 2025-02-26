package datawave.microservice.audit.config.discovery;

import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.retry.annotation.Retryable;

import datawave.microservice.audit.AuditServiceProvider;
import datawave.microservice.audit.config.AuditServiceProperties;

public class RetryableServiceProvider extends AuditServiceProvider {
    
    public RetryableServiceProvider(AuditServiceProperties properties, DiscoveryClient client) {
        super(properties, client);
    }
    
    @Override
    @Retryable(interceptor = "auditDiscoveryRetryInterceptor")
    public ServiceInstance getServiceInstance() {
        return super.getServiceInstance();
    }
}
