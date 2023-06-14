package datawave.webservice.common.cache;

import org.apache.deltaspike.core.api.config.ConfigProperty;

import javax.annotation.PostConstruct;
import javax.inject.Inject;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AccumuloTableCacheConfiguration {

    @Inject
    @ConfigProperty(name = "dw.warehouse.zookeepers")
    private String zookeepers = null;
    @SuppressWarnings("MismatchedQueryAndUpdateOfCollection")
    @Inject
    @ConfigProperty(name = "dw.cache.tableNames", defaultValue = "DatawaveMetadata,QueryMetrics_m,errorMetadata")
    private List<String> tableNames;
    @Inject
    @ConfigProperty(name = "dw.cache.pool", defaultValue = "WAREHOUSE")
    private String poolName;
    @Inject
    @ConfigProperty(name = "dw.cache.reloadInterval", defaultValue = "86400000")
    private long reloadInterval;

    private Map<String,TableCache> caches = new HashMap<>();

    @PostConstruct
    private void initializeCaches() {
        for (String tableName : tableNames) {
            BaseTableCache cache = new BaseTableCache();
            cache.setTableName(tableName);
            cache.setConnectionPoolName(poolName);
            cache.setReloadInterval(reloadInterval);
            caches.put(tableName, cache);
        }
    }

    public String getZookeepers() {
        return zookeepers;
    }

    public void setZookeepers(String zookeepers) {
        this.zookeepers = zookeepers;
    }

    public Map<String,TableCache> getCaches() {
        return Collections.unmodifiableMap(caches);
    }
}
