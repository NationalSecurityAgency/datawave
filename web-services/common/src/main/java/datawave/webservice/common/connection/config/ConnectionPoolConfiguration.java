package datawave.webservice.common.connection.config;

import org.apache.deltaspike.core.api.config.ConfigResolver;

import datawave.core.common.result.ConnectionPoolProperties;
import datawave.core.common.util.EnvProvider;

public class ConnectionPoolConfiguration extends ConnectionPoolProperties {
    public ConnectionPoolConfiguration(String poolName) {
        username = ConfigResolver.getPropertyValue("dw." + poolName + ".accumulo.userName");
        password = resolvePassword(poolName);
        instance = ConfigResolver.getPropertyValue("dw." + poolName + ".instanceName");
        zookeepers = ConfigResolver.getPropertyValue("dw." + poolName + ".zookeepers");
        lowPriorityPoolSize = Integer.parseInt(ConfigResolver.getPropertyValue("dw." + poolName + ".pool.low.size", "25"));
        normalPriorityPoolSize = Integer.parseInt(ConfigResolver.getPropertyValue("dw." + poolName + ".pool.normal.size", "50"));
        highPriorityPoolSize = Integer.parseInt(ConfigResolver.getPropertyValue("dw." + poolName + ".pool.high.size", "100"));
        adminPriorityPoolSize = Integer.parseInt(ConfigResolver.getPropertyValue("dw." + poolName + ".pool.admin.size", "200"));
    }

    /**
     * Resolve the accumulo password from either the system properties or the environment. The environment takes precedence if both are configured.
     *
     * @param poolName
     *            the name of the connection pool
     * @return the resolved password
     */
    protected String resolvePassword(String poolName) {
        String value = ConfigResolver.getPropertyValue("dw." + poolName + ".accumulo.password");
        return EnvProvider.resolve(value);
    }
}
