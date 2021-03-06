package datawave.ingest.util.cache.lease;

import com.google.common.collect.Lists;
import com.google.common.collect.Maps;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.utils.CloseableUtils;
import org.apache.hadoop.conf.Configuration;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Collection;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;
import java.util.stream.Collectors;

/** Factory class to handle job cache leases. */
public class JobCacheZkLockFactory implements JobCacheLockFactory {
    private static final Logger LOGGER = LoggerFactory.getLogger(JobCacheZkLockFactory.class);
    
    public static final String DW_JOB_CACHE_ZOOKEEPER_TIMEOUT = "datawave.job.cache.zk.timeout.ms";
    public static final String DW_JOB_CACHE_ZOOKEEPER_RETRY_CNT = "datawave.job.cache.zk.retry.cnt";
    public static final String DW_JOB_CACHE_ZOOKEEPER_RETRY_TIMEOUT = "datawave.job.cache.zk.retry.timeout.ms";
    public static final String DW_JOB_CACHE_ZOOKEEPER_NAMESPACE = "datawave.job.cache.zk.namespace";
    public static final String DW_JOB_CACHE_ZOOKEEPER_NODES = "datawave.job.cache.zk.nodes";
    private static final int ZOOKEEPER_TIME_BEFORE_RETRY_MS = 30;
    private static final int ZOOKEEPER_ACQUIRE_LOCK_TIMEOUT = 30;
    private static final int ZOOKEEPER_RETRY_CONNECTION_CNT = 3;
    
    private static final String ZOOKEEPER_LEASE_DIR = "/leases";
    
    private CuratorFramework client;
    private final Map<String,InterProcessSemaphoreMutex> jobIdToLeases = Maps.newHashMap();
    private int lockTimeoutInMs;
    
    public JobCacheZkLockFactory() {}
    
    public JobCacheZkLockFactory(String namespace, String zookeepers, int lockTimeoutInMs, int retryCnt, int retryTimeoutInMS) {
        init(namespace, zookeepers, lockTimeoutInMs, retryCnt, retryTimeoutInMS);
    }
    
    @Override
    public void init(Configuration conf) {
        int lockTimeout = conf.getInt(DW_JOB_CACHE_ZOOKEEPER_TIMEOUT, ZOOKEEPER_TIME_BEFORE_RETRY_MS);
        int retryCnt = conf.getInt(DW_JOB_CACHE_ZOOKEEPER_RETRY_CNT, ZOOKEEPER_RETRY_CONNECTION_CNT);
        int retryTimeout = conf.getInt(DW_JOB_CACHE_ZOOKEEPER_RETRY_TIMEOUT, ZOOKEEPER_ACQUIRE_LOCK_TIMEOUT);
        String namespace = conf.get(DW_JOB_CACHE_ZOOKEEPER_NAMESPACE);
        String zookeepers = conf.get(DW_JOB_CACHE_ZOOKEEPER_NODES);
        init(namespace, zookeepers, lockTimeout, retryCnt, retryTimeout);
    }
    
    @Override
    public boolean acquireLock(String id) {
        return acquireLock(id, this::isCacheLocked);
    }
    
    @Override
    public boolean acquireLock(String id, Predicate<String> cacheCheck) {
        if (cacheCheck.test(getCacheId(id))) {
            LOGGER.warn("Unable to acquire job lease for {} since cache is locked or active if deleting", id);
            return false;
        }
        
        InterProcessSemaphoreMutex lock = new InterProcessSemaphoreMutex(client, getLockPath(id));
        try {
            if (lock.acquire(lockTimeoutInMs, TimeUnit.MILLISECONDS)) {
                jobIdToLeases.put(id, lock);
                return true;
            }
        } catch (Exception e) {
            LOGGER.warn("Unable to acquire lock for " + id, e);
        }
        
        return false;
    }
    
    @Override
    public boolean releaseLock(String id) {
        if (jobIdToLeases.containsKey(id) && releaseLock(id, jobIdToLeases.get(id))) {
            jobIdToLeases.remove(id);
            return true;
        }
        return false;
    }
    
    @Override
    public Mode getMode() {
        return Mode.ZOOKEEPER;
    }
    
    @Override
    public void close() {
        jobIdToLeases.forEach(this::releaseLock);
        jobIdToLeases.clear();
        CloseableUtils.closeQuietly(client);
    }
    
    @Override
    public Predicate<String> getCacheAvailablePredicate() {
        return id -> isCacheLocked(id) || isCacheActive(id);
    }
    
    @Override
    public LockCacheStatus getCacheStatus(String id) {
        Collection<String> jobIds = getJobIds(id);
        return new LockCacheStatus(isCacheLocked(id), jobIds);
    }
    
    /**
     * Get the curator framework.
     *
     * @return The curator framework object
     */
    CuratorFramework getCurator() {
        return client;
    }
    
    /**
     * Convert id to zookeeper node path
     *
     * @param id
     *            A cache id.
     * @return A string representing the a zookeeper path
     */
    String getLockPath(String id) {
        return id.startsWith(File.separator) ? id : File.separator + id;
    }
    
    /**
     * Initializes Zookeeper curator framework.
     *
     * @param namespace
     *            The namespace that will appended to each curator framework call.
     * @param zookeepers
     *            The zookeepers.
     * @param lockTimeoutInMs
     *            The timeout to use when acquiring a lock.
     * @param retryCnt
     *            The number of attempts to retry to reconnect.
     * @param retryTimeoutInMS
     *            The timeout while attempting to reconnect.
     */
    private void init(String namespace, String zookeepers, int lockTimeoutInMs, int retryCnt, int retryTimeoutInMS) {
        // @formatter:off
        client = CuratorFrameworkFactory
                .builder()
                .connectString(zookeepers)
                .retryPolicy(new RetryNTimes(retryCnt, retryTimeoutInMS))
                .namespace(namespace)
                .build();
        // @formatter:on
        client.start();
        this.lockTimeoutInMs = lockTimeoutInMs;
    }
    
    /**
     * Find any jobIds that are active for this cacheId
     *
     * @param cacheId
     *            A cache id.
     * @return A collection of active job ids.
     */
    private Collection<String> getJobIds(String cacheId) {
        Collection<String> jobIds = Lists.newArrayList();
        try {
            // @formatter:off
            jobIds = client.getChildren().forPath(getLockPath(cacheId))
                    .stream()
                    .map(childId -> cacheId + File.separator + childId)
                    .filter(this::doesLeaseExist)
                    .collect(Collectors.toList());
            // @formatter:on
        } catch (Exception e) {
            LOGGER.debug("Could not find zookeeper children for " + cacheId, e);
        }
        return jobIds;
    }
    
    /**
     * Tests to see if there are still active leases under this cache id.
     *
     * @param cacheId
     *            A cache id.
     * @return True if the cache is still active.
     */
    private boolean isCacheActive(String cacheId) {
        return !getJobIds(cacheId).isEmpty();
    }
    
    /**
     * Convert jobId to cacheId.
     *
     * @param id
     *            An id.
     * @return The parent(cache) id.
     */
    private String getCacheId(String id) {
        String lockPath = getLockPath(id);
        int endIndex = lockPath.lastIndexOf("/");
        return (endIndex == 0) ? lockPath : lockPath.substring(0, endIndex);
    }
    
    /**
     * Tests to see if the timestamp cache(not the job id) is locked by another process.
     *
     * @param cacheId
     *            A cache id.
     * @return True if the parent(cache) id is locked.
     */
    private boolean isCacheLocked(String cacheId) {
        return doesLeaseExist(cacheId);
    }
    
    /**
     * Get the zookeeper node for the lease directory if it exists.
     *
     * @param zkNodePath
     *            The zookeeper node path.
     * @return Returns null if the path does not exist or an Stat object.
     */
    private Stat getZNodeStat(String zkNodePath) {
        try {
            return client.checkExists().forPath(getLockPath(zkNodePath) + ZOOKEEPER_LEASE_DIR);
        } catch (Exception e) {
            throw new RuntimeException("Unable to check existence for " + zkNodePath, e);
        }
    }
    
    /**
     * Determines if a lease exists for the zookeeper node path
     *
     * @param zkNodePath
     *            The zookeeper node path.
     * @return True if a lease exists for this node path
     */
    private boolean doesLeaseExist(String zkNodePath) {
        Stat zkNodeStat = getZNodeStat(zkNodePath);
        return zkNodeStat != null && zkNodeStat.getNumChildren() > 0;
    }
    
    /**
     * Manual cleanup of unlocked nodes is only needed before Zookeeper 3.5. This is automatically done in 3.5
     *
     * @param path
     *            Zookeeper node path to delete.
     * @throws Exception
     *             if unable to delete zookeeper nodes
     */
    private void manuallyCleanupZKnode(String path) throws Exception {
        client.delete().guaranteed().deletingChildrenIfNeeded().forPath(getLockPath(path));
    }
    
    /**
     * Release lock for id if possible
     *
     * @param id
     *            A cache id.
     * @param lease
     *            The lease associated with the id.
     * @return True if the lock was released.
     */
    private boolean releaseLock(String id, InterProcessSemaphoreMutex lease) {
        try {
            lease.release();
            manuallyCleanupZKnode(id);
        } catch (Exception e) {
            LOGGER.warn("Unable to release lock for " + id, e);
            return false;
        }
        return true;
    }
}
