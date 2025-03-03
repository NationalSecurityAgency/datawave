package datawave.microservice.audit;

import java.net.URI;
import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.client.DefaultServiceInstance;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;

import com.google.common.base.Preconditions;
import com.google.common.base.Strings;

import datawave.microservice.audit.config.AuditServiceProperties;

/**
 * Provides a {@link ServiceInstance} representing the remote audit service
 */
public class AuditServiceProvider {
    
    private static Logger logger = LoggerFactory.getLogger(AuditServiceProvider.class);
    
    protected final AuditServiceProperties properties;
    protected final DiscoveryClient discoveryClient;
    
    public AuditServiceProvider(AuditServiceProperties properties) {
        this(properties, null);
    }
    
    public AuditServiceProvider(AuditServiceProperties properties, DiscoveryClient client) {
        Preconditions.checkNotNull(properties, "AuditServiceProperties argument is null");
        this.properties = properties;
        this.discoveryClient = client;
    }
    
    /**
     * If internal {@link DiscoveryClient} is null, returns the configured default service instance, otherwise the audit service will be discovered
     * automatically
     * 
     * @return {@link ServiceInstance} representing the remote audit service
     */
    public ServiceInstance getServiceInstance() {
        if (null == this.discoveryClient) {
            return getDefaultServiceInstance();
        }
        return discoverInstance(properties.getServiceId());
    }
    
    protected ServiceInstance discoverInstance(String serviceId) {
        
        Preconditions.checkState(!Strings.isNullOrEmpty(serviceId), "service id must not be null/empty");
        Preconditions.checkNotNull(this.discoveryClient, "discovery client must not be null");
        
        logger.debug("Locating audit server by id ({}) via discovery", serviceId);
        
        List<ServiceInstance> instances = this.discoveryClient.getInstances(serviceId);
        if (instances.isEmpty()) {
            throw new IllegalStateException("No instances found of audit service (id: " + serviceId + ")");
        }
        if (instances.size() > 1) {
            logger.info("More than one audit service is available, but I only know how to select the first in the list");
        }
        ServiceInstance instance = instances.get(0);
        
        logger.debug("Located audit service (id: {}) via discovery. URI: {}", serviceId, instance.getUri());
        
        return instance;
    }
    
    protected ServiceInstance getDefaultServiceInstance() {
        logger.debug("Returning default ServiceInstance for auditing: {}", properties.getUri());
        final URI uri = URI.create(properties.getUri());
        return new DefaultServiceInstance(null, properties.getServiceId(), uri.getHost(), uri.getPort(), uri.getScheme().equals("https"));
    }
    
    AuditServiceProperties getProperties() {
        return properties;
    }
}
