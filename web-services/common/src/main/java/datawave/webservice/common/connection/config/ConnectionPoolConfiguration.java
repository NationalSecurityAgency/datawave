package datawave.webservice.common.connection.config;

import datawave.core.common.result.ConnectionPoolProperties;
import org.apache.deltaspike.core.api.config.ConfigResolver;

public class ConnectionPoolConfiguration extends ConnectionPoolProperties {
    
    public ConnectionPoolConfiguration(String poolName) {
        username = ConfigResolver.getPropertyValue("dw." + poolName + ".accumulo.userName");
        password = ConfigResolver.getPropertyValue("dw." + poolName + ".accumulo.password");
        instance = ConfigResolver.getPropertyValue("dw." + poolName + ".instanceName");
        zookeepers = ConfigResolver.getPropertyValue("dw." + poolName + ".zookeepers");
        lowPriorityPoolSize = Integer.parseInt(ConfigResolver.getPropertyValue("dw." + poolName + ".pool.low.size", "25"));
        normalPriorityPoolSize = Integer.parseInt(ConfigResolver.getPropertyValue("dw." + poolName + ".pool.normal.size", "50"));
        highPriorityPoolSize = Integer.parseInt(ConfigResolver.getPropertyValue("dw." + poolName + ".pool.high.size", "100"));
        adminPriorityPoolSize = Integer.parseInt(ConfigResolver.getPropertyValue("dw." + poolName + ".pool.admin.size", "200"));
    }
}
