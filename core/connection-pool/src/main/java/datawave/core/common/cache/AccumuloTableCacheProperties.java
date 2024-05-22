package datawave.core.common.cache;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

public class AccumuloTableCacheProperties {
    private String zookeepers = null;
    private List<String> tableNames = new ArrayList<>();
    private String poolName;
    private long reloadInterval;
    private int evictionReaperIntervalInSeconds;
    private int numLocks;
    private int maxRetries;
    private long tableCacheReloadTaskLease = TimeUnit.MINUTES.toMillis(10);
    private TimeUnit tableCacheReloadTaskLeaseTimeUnit = TimeUnit.MILLISECONDS;

    public String getZookeepers() {
        return zookeepers;
    }

    public AccumuloTableCacheProperties withZookeepers(String zookeepers) {
        this.zookeepers = zookeepers;
        return this;
    }

    public List<String> getTableNames() {
        return tableNames;
    }

    public AccumuloTableCacheProperties withTableNames(List<String> tableNames) {
        this.tableNames = tableNames;
        return this;
    }

    public String getPoolName() {
        return poolName;
    }

    public AccumuloTableCacheProperties withPoolName(String poolName) {
        this.poolName = poolName;
        return this;
    }

    public long getReloadInterval() {
        return reloadInterval;
    }

    public AccumuloTableCacheProperties withReloadInterval(long reloadInterval) {
        this.reloadInterval = reloadInterval;
        return this;
    }

    public int getEvictionReaperIntervalInSeconds() {
        return evictionReaperIntervalInSeconds;
    }

    public AccumuloTableCacheProperties withEvictionReaperIntervalInSeconds(int evictionReaperIntervalInSeconds) {
        this.evictionReaperIntervalInSeconds = evictionReaperIntervalInSeconds;
        return this;
    }

    public int getNumLocks() {
        return numLocks;
    }

    public AccumuloTableCacheProperties withNumLocks(int numLocks) {
        this.numLocks = numLocks;
        return this;
    }

    public int getMaxRetries() {
        return maxRetries;
    }

    public AccumuloTableCacheProperties withMaxRetries(int maxRetries) {
        this.maxRetries = maxRetries;
        return this;
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

    public long getTableCacheReloadTaskLease() {
        return tableCacheReloadTaskLease;
    }

    public void setTableCacheReloadTaskLease(long tableCacheReloadTaskLease) {
        this.tableCacheReloadTaskLease = tableCacheReloadTaskLease;
    }

    public long getTableCacheReloadTaskLeaseMillis() {
        return tableCacheReloadTaskLeaseTimeUnit.toMillis(tableCacheReloadTaskLease);
    }

    public TimeUnit getTableCacheReloadTaskLeaseTimeUnit() {
        return tableCacheReloadTaskLeaseTimeUnit;
    }

    public void setTableCacheReloadTaskLeaseTimeUnit(TimeUnit tableCacheReloadTaskLeaseTimeUnit) {
        this.tableCacheReloadTaskLeaseTimeUnit = tableCacheReloadTaskLeaseTimeUnit;
    }

}
