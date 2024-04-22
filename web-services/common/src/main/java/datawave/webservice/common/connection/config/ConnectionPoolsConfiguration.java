package datawave.webservice.common.connection.config;

import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import javax.annotation.PostConstruct;
import javax.inject.Inject;

import org.apache.deltaspike.core.api.config.ConfigProperty;

public class ConnectionPoolsConfiguration {

    @Inject
    @ConfigProperty(name = "dw.connectionPool.default", defaultValue = "WAREHOUSE")
    private String defaultPool = null;

    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    @Inject
    @ConfigProperty(name = "dw.connectionPool.pools", defaultValue = "WAREHOUSE,METRICS")
    private List<String> poolNames;

    private Map<String,ConnectionPoolConfiguration> pools = new HashMap<>();
    private Map<String,ConnectionPoolClientConfiguration> configs = new HashMap<>();

    @PostConstruct
    private void initializePools() {
        for (String poolName : poolNames) {
            pools.put(poolName, new ConnectionPoolConfiguration(poolName.toLowerCase()));
            configs.put(poolName, new ConnectionPoolClientConfiguration(poolName.toLowerCase()));
        }
    }

    public String getDefaultPool() {
        return defaultPool;
    }

    public Map<String,ConnectionPoolConfiguration> getPools() {
        return Collections.unmodifiableMap(pools);
    }

    public ConnectionPoolConfiguration getConfiguration(String pool) {
        return pools.get(pool);
    }

    public Map<String,ConnectionPoolClientConfiguration> getClientConfiguration() {
        return Collections.unmodifiableMap(configs);
    }

    public ConnectionPoolClientConfiguration getClientConfiguration(String pool) {
        return configs.get(pool);
    }
}
