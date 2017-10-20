package datawave.microservice.cached;

import com.hazelcast.nio.Address;
import com.hazelcast.spi.discovery.DiscoveryNode;
import com.hazelcast.spi.discovery.SimpleDiscoveryNode;
import com.hazelcast.spi.discovery.integration.DiscoveryService;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.cloud.client.ServiceInstance;
import org.springframework.cloud.client.discovery.DiscoveryClient;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * A {@link DiscoveryService} for Hazelcast clients located in the spring boot discovery service. We expect that, when registering, the service will include
 * metadata indicating the host and port to use when adding the node to the Hazelcast cluster.
 */
public class HazelcastDiscoveryService implements DiscoveryService {
    private final Logger logger = LoggerFactory.getLogger(getClass());
    private final DiscoveryClient discoveryClient;
    
    @Value("${hazelcast.clusterName:cache}")
    private String hazelcastCluster;
    
    public HazelcastDiscoveryService(DiscoveryClient discoveryClient) {
        this.discoveryClient = discoveryClient;
    }
    
    @Override
    public Iterable<DiscoveryNode> discoverNodes() {
        List<DiscoveryNode> nodes = new ArrayList<>();
        for (ServiceInstance serviceInstance : discoveryClient.getInstances(hazelcastCluster)) {
            try {
                String host = serviceInstance.getMetadata().get("hzHost");
                int port = Integer.parseInt(serviceInstance.getMetadata().get("hzPort"));
                Address address = new Address(host, port);
                DiscoveryNode discoveryNode = new SimpleDiscoveryNode(address);
                nodes.add(discoveryNode);
            } catch (Exception e) {
                logger.warn("Unable to add discovered instance {}: {}", serviceInstance, e.getMessage(), e);
            }
        }
        return nodes;
    }
    
    @Override
    public Map<String,Object> discoverLocalMetadata() {
        // Not used
        return new HashMap<>();
    }
    
    @Override
    public void start() {
        // Not used
    }
    
    @Override
    public void destroy() {
        // Not used
    }
}
