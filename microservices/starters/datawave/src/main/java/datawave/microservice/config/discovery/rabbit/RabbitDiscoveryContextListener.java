package datawave.microservice.config.discovery.rabbit;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.amqp.rabbit.connection.CachingConnectionFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.event.HeartbeatEvent;
import org.springframework.cloud.client.discovery.event.HeartbeatMonitor;
import org.springframework.context.event.ContextRefreshedEvent;
import org.springframework.context.event.EventListener;
import org.springframework.stereotype.Component;

@ConditionalOnProperty(value = "spring.rabbitmq.discovery.enabled")
@Component
@EnableConfigurationProperties(RabbitDiscoveryProperties.class)
public class RabbitDiscoveryContextListener {
    private static Logger logger = LoggerFactory.getLogger(RabbitDiscoveryContextListener.class);
    
    private final RabbitDiscoveryProperties rabbitProperties;
    private final RabbitDiscoveryInstanceProvider instanceProvider;
    private final CachingConnectionFactory connectionFactory;
    private final HeartbeatMonitor monitor;
    
    @Autowired
    public RabbitDiscoveryContextListener(RabbitDiscoveryProperties rabbitProperties, RabbitDiscoveryInstanceProvider instanceProvider,
                    CachingConnectionFactory connectionFactory) {
        this.rabbitProperties = rabbitProperties;
        this.instanceProvider = instanceProvider;
        this.connectionFactory = connectionFactory;
        this.monitor = new HeartbeatMonitor();
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
}
