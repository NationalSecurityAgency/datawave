package datawave.webservice.common.connection.config;

import datawave.core.common.result.ConnectionPoolsProperties;

import java.util.List;

public class ConnectionPoolsConfiguration extends ConnectionPoolsProperties {
    private List<String> poolNames;

    public ConnectionPoolsConfiguration build() {
        for (String poolName : poolNames) {
            pools.put(poolName, new ConnectionPoolConfiguration(poolName.toLowerCase()));
        }
        return this;
    }

    public ConnectionPoolsConfiguration withPoolNames(List<String> poolNames) {
        this.poolNames = poolNames;
        return this;
    }

    public ConnectionPoolsConfiguration withDefaultPool(String defaultPool) {
        this.defaultPool = defaultPool;
        return this;
    }

}
