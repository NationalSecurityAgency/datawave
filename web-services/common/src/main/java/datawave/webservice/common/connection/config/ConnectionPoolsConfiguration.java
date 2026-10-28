package datawave.webservice.common.connection.config;

import java.util.Map;

import datawave.core.common.result.ConnectionPoolProperties;
import datawave.core.common.result.ConnectionPoolsProperties;

public class ConnectionPoolsConfiguration extends ConnectionPoolsProperties {
    private ConnectionPoolsProperties connectionPoolsProperties;

    public ConnectionPoolsConfiguration build() {
        this.defaultPool = this.connectionPoolsProperties.getDefaultPool();
        for (Map.Entry<String,ConnectionPoolProperties> entry : connectionPoolsProperties.getPools().entrySet()) {
            String poolName = entry.getKey();
            pools.put(poolName, entry.getValue());
            configs.put(poolName, new ConnectionPoolClientConfiguration(poolName.toLowerCase()));
        }
        return this;
    }

    public ConnectionPoolsConfiguration withConnectionPoolsProperties(ConnectionPoolsProperties connectionPoolsProperties) {
        this.connectionPoolsProperties = connectionPoolsProperties;
        return this;
    }
}
