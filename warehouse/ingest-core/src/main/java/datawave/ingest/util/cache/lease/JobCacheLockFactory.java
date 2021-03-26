package datawave.ingest.util.cache.lease;

import com.google.common.collect.Maps;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreMutex;
import org.apache.curator.retry.RetryNTimes;
import org.apache.curator.utils.CloseableUtils;
import org.apache.zookeeper.data.Stat;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.File;
import java.util.Map;
import java.util.concurrent.TimeUnit;
import java.util.function.Predicate;

/** Factory class to handle job cache leases. */
public class JobCacheLockFactory implements LockFactory {
    
    private static final int ZOOKEEPER_TIME_BEFORE_RETRY_MS = 30;
    private static final int ZOOKEEPER_ACQUIRE_LOCK_TIMEOUT = 30;
    private static final int ZOOKEEPER_RETRY_CONNECTION_CNT = 3;
    private static final Logger LOGGER = LoggerFactory.getLogger(JobCacheLockFactory.class);
    
    private static final String ZOOKEEPER_LEASE_DIR = "/leases";
    
    private final CuratorFramework client;
    private final Map<String,InterProcessSemaphoreMutex> jobIdToLeases = Maps.newHashMap();
    private int lockTimeoutInMs;
    
    public JobCacheLockFactory(String namespace, String zookeepers, int lockTimeoutInMs, int retryCnt, int retryTimeoutInMS) {
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
    
    @Override
    public boolean acquireLock(String id) {
        return acquireLock(id, this::isCacheLocked);
    }
    
    /**
     * Acquire a lock based on the id.
     *
     * @param id
     *            A cache or job id to attempt to acquire a lock
     * @param cacheCheck
     *            A predicate function that will determine if lock can be acquired.
     * @return True if the lock was acquired.
     */
    public boolean acquireLock(String id, Predicate<String> cacheCheck) {
        if (cacheCheck.test(getCacheId(id))) {
            LOGGER.warn("Unable to acquire job lease for {} since cache is locked or active if deleting", id);
            return false;
        }
        
        InterProcessSemaphoreMutex lock = new InterProcessSemaphoreMutex(client, id);
        try {
            if (lock.acquire(lockTimeoutInMs, TimeUnit.MILLISECONDS)) {
                jobIdToLeases.put(id, lock);
                return true;
            }
        } catch (Exception e) {
            LOGGER.warn("Unable to acquire lock for {} ", id, e);
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
    public void close() {
        jobIdToLeases.forEach(this::releaseLock);
        jobIdToLeases.clear();
        CloseableUtils.closeQuietly(client);
    }
    
    /**
     * Get a predicate that will check if an id is locked or is active
     *
     * @return A predicate that will test the given condition.
     */
    public Predicate<String> getCacheAvailablePredicate() {
        return id -> isCacheLocked(id) || isCacheActive(id);
    }
    
    /**
     * Tests to see if there are still active leases under this cache id.
     *
     * @param cacheId
     *            A cache id.
     * @return True if the cache is still active.
     */
    private boolean isCacheActive(String cacheId) {
        try {
            return client.getChildren().forPath(cacheId).stream().map(childId -> cacheId + File.separator + childId).anyMatch(this::doesLeaseExist);
        } catch (Exception e) {
            LOGGER.warn("Could not find zookeeper children for {}", cacheId, e);
            return false;
        }
    }
    
    /**
     * Convert jobId to cacheId.
     *
     * @param id
     *            An id.
     * @return The parent(cache) id.
     */
    private String getCacheId(String id) {
        int endIndex = id.lastIndexOf("/");
        return (endIndex == 0) ? id : id.substring(0, endIndex);
    }
    
    /**
     * Tests to see if the timestamp cache(not the job id) is locked by another process.
     *
     * @param cacheId
     *            A cache id.
     * @return True if the parent(cache) id is locked.
     */
    private boolean isCacheLocked(String cacheId) {
        return doesLeaseExist((cacheId));
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
            return client.checkExists().forPath(zkNodePath + ZOOKEEPER_LEASE_DIR);
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
     * Get the curator framework.
     *
     * @return The curator framework object
     */
    CuratorFramework getCurator() {
        return client;
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
        client.delete().guaranteed().deletingChildrenIfNeeded().forPath(path);
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
