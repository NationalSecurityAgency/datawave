package datawave.core.common.result;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class ConnectionPoolsProperties {
    protected String defaultPool;
    protected Map<String,ConnectionPoolProperties> pools = new HashMap<>();
    protected Map<String,ConnectionPoolClientProperties> configs = new HashMap<>();

    public String getDefaultPool() {
        return defaultPool;
    }

    public Map<String,ConnectionPoolProperties> getPools() {
        return Collections.unmodifiableMap(pools);
    }

    public ConnectionPoolProperties getConfiguration(String pool) {
        return pools.get(pool);
    }

    public List<String> getPoolNames() {
        return Collections.unmodifiableList(new ArrayList<>(pools.keySet()));
    }

    public Map<String,ConnectionPoolClientProperties> getClientConfiguration() {
        return Collections.unmodifiableMap(configs);
    }

    public ConnectionPoolClientProperties getClientConfiguration(String pool) {
        return configs.get(pool);
    }

    public void setDefaultPool(String defaultPool) {
        this.defaultPool = defaultPool;
    }

    public void setPools(Map<String,ConnectionPoolProperties> pools) {
        this.pools = pools;
    }

    public void setClientConfiguration(Map<String,ConnectionPoolClientProperties> configs) {
        this.configs = configs;
    }
}
