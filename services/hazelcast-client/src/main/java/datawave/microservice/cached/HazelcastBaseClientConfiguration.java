package datawave.microservice.cached;

import com.hazelcast.client.config.ClientConfig;
import com.hazelcast.client.config.ClientConnectionStrategyConfig;
import com.hazelcast.client.config.XmlClientConfigBuilder;

import java.io.ByteArrayInputStream;

public class HazelcastBaseClientConfiguration {
    public ClientConfig createClientConfig(HazelcastClientProperties clientProperties) {
        ClientConfig clientConfig;
        if (clientProperties.getXmlConfig() == null) {
            clientConfig = new ClientConfig();
        } else {
            XmlClientConfigBuilder xmlBuilder = new XmlClientConfigBuilder(new ByteArrayInputStream(clientProperties.getXmlConfig().getBytes()));
            clientConfig = xmlBuilder.build();
        }
        
        if (!clientProperties.isSkipDefaultConfiguration()) {
            // Configure hazelcast properties
            clientConfig.setProperty("hazelcast.logging.type", "slf4j");
            clientConfig.setProperty("hazelcast.phone.home.enabled", Boolean.FALSE.toString());
            
            // Support non-blocking initial and re-connect to the cluster
            clientConfig.getConnectionStrategyConfig().setAsyncStart(true);
            clientConfig.getConnectionStrategyConfig().setReconnectMode(ClientConnectionStrategyConfig.ReconnectMode.ASYNC);
            
            // Set our cluster name
            clientConfig.getGroupConfig().setName(clientProperties.getClusterName());
        }
        
        return clientConfig;
    }
}
