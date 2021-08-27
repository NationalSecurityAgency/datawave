package datawave.webservice.common.cache;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

public class AccumuloTableCacheConfiguration {
    private String zookeepers = null;
    private List<String> tableNames = new ArrayList<>();
    private String poolName;
    private long reloadInterval;
    private int evictionReaperIntervalInSeconds;
    private int numLocks;
    private int maxRetries;
    
    private Map<String,TableCache> caches = new HashMap<>();
    
    public AccumuloTableCacheConfiguration build() {
        for (String tableName : tableNames) {
            BaseTableCache cache = new BaseTableCache();
            cache.setTableName(tableName);
            cache.setConnectionPoolName(poolName);
            cache.setReloadInterval(reloadInterval);
            caches.put(tableName, cache);
        }
        return this;
    }
    
    public String getZookeepers() {
        return zookeepers;
    }
    
    public AccumuloTableCacheConfiguration withZookeepers(String zookeepers) {
        this.zookeepers = zookeepers;
        return this;
    }
    
    public List<String> getTableNames() {
        return tableNames;
    }
    
    public AccumuloTableCacheConfiguration withTableNames(List<String> tableNames) {
        this.tableNames = tableNames;
        return this;
    }
    
    public String getPoolName() {
        return poolName;
    }
    
    public AccumuloTableCacheConfiguration withPoolName(String poolName) {
        this.poolName = poolName;
        return this;
    }
    
    public long getReloadInterval() {
        return reloadInterval;
    }
    
    public AccumuloTableCacheConfiguration withReloadInterval(long reloadInterval) {
        this.reloadInterval = reloadInterval;
        return this;
    }
    
    public int getEvictionReaperIntervalInSeconds() {
        return evictionReaperIntervalInSeconds;
    }
    
    public AccumuloTableCacheConfiguration withEvictionReaperIntervalInSeconds(int evictionReaperIntervalInSeconds) {
        this.evictionReaperIntervalInSeconds = evictionReaperIntervalInSeconds;
        return this;
    }
    
    public int getNumLocks() {
        return numLocks;
    }
    
    public AccumuloTableCacheConfiguration withNumLocks(int numLocks) {
        this.numLocks = numLocks;
        return this;
    }
    
    public int getMaxRetries() {
        return maxRetries;
    }
    
    public AccumuloTableCacheConfiguration withMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
        return this;
    }
    
    public Map<String,TableCache> getCaches() {
        return Collections.unmodifiableMap(caches);
    }
    
    public void setZookeepers(String zookeepers) {
        this.zookeepers = zookeepers;
    }
    
    public void setTableNames(List<String> tableNames) {
        this.tableNames = tableNames;
    }
    
    public void setPoolName(String poolName) {
        this.poolName = poolName;
    }
    
    public void setReloadInterval(long reloadInterval) {
        this.reloadInterval = reloadInterval;
    }
    
    public void setEvictionReaperIntervalInSeconds(int evictionReaperIntervalInSeconds) {
        this.evictionReaperIntervalInSeconds = evictionReaperIntervalInSeconds;
    }
    
    public void setNumLocks(int numLocks) {
        this.numLocks = numLocks;
    }
    
    public void setMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
    }
    
}
