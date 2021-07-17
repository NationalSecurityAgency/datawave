package datawave.ingest.util.cache.lease;

import com.beust.jcommander.Parameter;
import org.apache.hadoop.conf.Configuration;

import static datawave.ingest.util.cache.lease.JobCacheZkLockFactory.DW_JOB_CACHE_ZOOKEEPER_NAMESPACE;
import static datawave.ingest.util.cache.lease.JobCacheZkLockFactory.DW_JOB_CACHE_ZOOKEEPER_NODES;
import static datawave.ingest.util.cache.lease.JobCacheZkLockFactory.DW_JOB_CACHE_ZOOKEEPER_RETRY_CNT;
import static datawave.ingest.util.cache.lease.JobCacheZkLockFactory.DW_JOB_CACHE_ZOOKEEPER_RETRY_TIMEOUT;
import static datawave.ingest.util.cache.lease.JobCacheZkLockFactory.DW_JOB_CACHE_ZOOKEEPER_TIMEOUT;

public class LockFactoryModeOptions {
    
    @Parameter(names = {"--cache-namespace"}, description = "Zookeeper namespace to check for active jobs")
    String cacheNamespace = "datawave/jobCache";
    
    @Parameter(names = {"--lock-timeout-in-ms"}, description = "Zookeeper timeout for acquiring lock")
    int lockTimeoutInMs = 30;
    
    @Parameter(names = {"--lock-retry-timeout-in-ms"}, description = "Zookeeper timeout for retrying connection ")
    int lockRetryTimeoutInMs = 30;
    
    @Parameter(names = {"--lock-retry-count"}, description = "Zookeeper retry connection attempts ")
    int lockRetryCount = 30;
    
    @Parameter(names = {"--zookeepers"}, description = "Zookeeper instances.", required = true)
    String zookeepers;
    
    public String getNamespace() {
        return cacheNamespace;
    }
    
    public int getLockTime() {
        return lockTimeoutInMs;
    }
    
    public int getLockRetryTimeout() {
        return lockRetryTimeoutInMs;
    }
    
    public int getLockRetryCount() {
        return lockRetryCount;
    }
    
    public String getZookeepers() {
        return zookeepers;
    }
    
    public Configuration getConf() {
        Configuration conf = new Configuration();
        conf.setInt(DW_JOB_CACHE_ZOOKEEPER_TIMEOUT, lockTimeoutInMs);
        conf.setInt(DW_JOB_CACHE_ZOOKEEPER_RETRY_CNT, lockRetryCount);
        conf.setInt(DW_JOB_CACHE_ZOOKEEPER_RETRY_TIMEOUT, lockRetryTimeoutInMs);
        conf.set(DW_JOB_CACHE_ZOOKEEPER_NAMESPACE, cacheNamespace);
        conf.set(DW_JOB_CACHE_ZOOKEEPER_NODES, zookeepers);
        return conf;
    }
}
