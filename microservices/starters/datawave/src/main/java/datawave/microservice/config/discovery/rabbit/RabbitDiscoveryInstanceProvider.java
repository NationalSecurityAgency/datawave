package datawave.microservice.config.discovery.rabbit;

import java.util.List;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;
import org.springframework.retry.annotation.Retryable;

public class RabbitDiscoveryInstanceProvider {
    private static Logger logger = LoggerFactory.getLogger(RabbitDiscoveryInstanceProvider.class);
    private final DiscoveryClient client;
    
    public RabbitDiscoveryInstanceProvider(DiscoveryClient client) {
        this.client = client;
    }
    
    @Retryable(interceptor = "rabbitDiscoveryRetryInterceptor")
    public ServiceInstance getRabbitMQServerInstance(String serviceId) {
        logger.debug("Locating rabbitmq server (" + serviceId + ") via discovery");
        List<ServiceInstance> instances = this.client.getInstances(serviceId);
        if (instances.isEmpty()) {
            throw new IllegalStateException("No instances found of rabbitmq server (" + serviceId + ")");
        }
        ServiceInstance instance = instances.get(0);
        logger.debug("Located rabbitmq server (" + serviceId + ") via discovery: " + instance);
        return instance;
    }
}
