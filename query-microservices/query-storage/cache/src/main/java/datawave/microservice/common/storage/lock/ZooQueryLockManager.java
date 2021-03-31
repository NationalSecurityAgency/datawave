package datawave.microservice.common.storage.lock;

import datawave.microservice.common.storage.QueryLockManager;
import datawave.microservice.common.storage.TaskKey;
import datawave.microservice.common.storage.TaskLockException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.framework.recipes.locks.InterProcessMutex;
import org.apache.curator.framework.recipes.locks.InterProcessSemaphoreV2;
import org.apache.curator.framework.recipes.locks.Lease;
import org.apache.curator.framework.recipes.locks.Reaper;
import org.apache.curator.framework.recipes.shared.SharedCount;
import org.apache.curator.retry.ExponentialBackoffRetry;
import org.apache.zookeeper.CreateMode;

import java.io.IOException;
import java.util.Collections;
import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

public class ZooQueryLockManager implements QueryLockManager {
    
    // The curator client
    private CuratorFramework client;
    
    // The base zookeeper path for all of the locks
    private static final String BASE_PATH = "/QueryStorageCache/";
    
    // The set of query lock objects being used by this JVM
    private Map<UUID,QueryLock> semaphores = Collections.synchronizedMap(new HashMap<>());
    
    public ZooQueryLockManager(String zookeeperConnectionString) {
        client = CuratorFrameworkFactory.builder().connectString(zookeeperConnectionString).retryPolicy(new ExponentialBackoffRetry(1000, 3)).build();
        client.start();
    }
    
    /**
     * This will setup a semaphore for a query with a specified count. This can be called multiple times for the same query if the number of available locks
     * needs to be adjusted over time. Specifying a count of 0 will effectively delete the semaphore.
     *
     * @param queryId
     *            The query id
     * @param count
     */
    @Override
    public void createSemaphore(UUID queryId, int count) throws IOException {
        if (count == 0) {
            QueryLock lock = semaphores.remove(queryId);
            if (lock != null) {
                lock.delete();
            }
        } else {
            QueryLock lock = semaphores.get(queryId);
            if (lock != null) {
                lock.updatePermits(count);
            } else {
                semaphores.put(queryId, new QueryLock(queryId, count));
            }
        }
    }
    
    /**
     * This will drop any cached data for this queryid, leaving the query locks in the underlying cluster. Any local task locks will be released.
     *
     * @param queryId
     */
    @Override
    public void closeSemaphore(UUID queryId) throws IOException {
        QueryLock lock = semaphores.remove(queryId);
        if (lock != null) {
            lock.close();
        }
    }
    
    /**
     * Acquire a lock for a task. This will wait the specified waitMs for a semaphore slot to be available.
     *
     * @param task
     *            The task to lock
     * @param waitMs
     *            How long to wait for semaphore availability and then how long to wait for the lock
     * @return true if able to lock the task
     * @throws TaskLockException
     *             if the task is already locked or the query semaphore does not exist
     */
    @Override
    public boolean acquireLock(TaskKey task, long waitMs) throws TaskLockException, IOException {
        return getQueryLock(task.getQueryId()).acquireLock(task, waitMs);
    }
    
    /**
     * Release the lock for a task. This will also decrement the semaphore allowing another task to be locked.
     *
     * @param task
     *            The task to unlock
     * @throws TaskLockException
     *             if the task is not locked.
     */
    @Override
    public void releaseLock(TaskKey task) throws TaskLockException, IOException {
        getQueryLock(task.getQueryId()).releaseLock(task);
    }
    
    /**
     * Determine if a task is locked.
     *
     * @param task
     *            The task
     * @return True if locked
     */
    @Override
    public boolean isLocked(TaskKey task) {
        return getQueryLock(task.getQueryId()).isLocked(task);
    }
    
    /**
     * Get the list of tasks that currently have locks (in this JVM)
     *
     * @param queryId
     *            The query id
     * @return the list of tasks
     */
    @Override
    public Set<TaskKey> getLockedTasks(UUID queryId) {
        return getQueryLock(queryId).getLockedTasks();
    }
    
    /**
     * Get the list of queries that currently have semaphores (in this JVM)
     */
    @Override
    public Set<UUID> getQueries() {
        synchronized (semaphores) {
            return new HashSet<>(semaphores.keySet());
        }
    }
    
    /**
     * Get the task ids for all locks that the zookeeper cluster knows about
     *
     * @param queryId
     * @return A set of task UUIDs
     * @throws IOException
     */
    @Override
    public Set<UUID> getDistributedLockedTasks(UUID queryId) throws IOException {
        try {
            List<String> paths = client.getChildren().forPath(getBaseTaskLockPath(queryId));
            return paths.stream().map(u -> UUID.fromString(u)).collect(Collectors.toSet());
        } catch (Exception e) {
            throw new IOException("Failed to examine zookeeper path for " + getBaseTaskLockPath(queryId), e);
        }
    }
    
    /**
     * Get the query ids for all semaphores that the zookeeper cluster knows about
     *
     * @return The set of query UUIDs
     * @throws IOException
     */
    @Override
    public Set<UUID> getDistributedQueries() throws IOException {
        try {
            List<String> paths = client.getChildren().forPath(BASE_PATH);
            return paths.stream().map(u -> UUID.fromString(u)).collect(Collectors.toSet());
        } catch (Exception e) {
            throw new IOException("Failed to examine zookeeper path for " + BASE_PATH, e);
        }
    }
    
    /**
     * Get a query lock that is expected to exist.
     * 
     * @param queryId
     * @return the query lock
     * @throws IllegalStateException
     *             if the query lock has not already been created
     */
    private QueryLock getQueryLock(UUID queryId) {
        QueryLock lock = semaphores.get(queryId);
        if (lock == null) {
            throw new IllegalStateException("One must create the query semaphore first");
        }
        return lock;
    }
    
    /**
     * Get the base path for all locks, semaphores, etc for a query
     * 
     * @param queryId
     *            The queryId
     * @return the base path
     */
    private static String getBasePath(UUID queryId) {
        return BASE_PATH + queryId;
    }
    
    /**
     * Get the semaphore count path for a query which holds the max number of permits.
     * 
     * @param queryId
     *            The queryId
     * @return the semaphore count path
     */
    private static String getSemaphoreCountPath(UUID queryId) {
        return getBasePath(queryId) + "/semaphoreCount";
    }
    
    /**
     * Get the semaphore path for a query
     * 
     * @param queryId
     *            The queryid
     * @return the semaphore path
     */
    private static String getSemaphorePath(UUID queryId) {
        return getBasePath(queryId) + "/semaphore";
    }
    
    /**
     * Get the base task lock path for a query
     * 
     * @param queryId
     * @return the base task lock path
     */
    private static String getBaseTaskLockPath(UUID queryId) {
        return getBasePath(queryId) + "/locks";
    }
    
    /**
     * Get the task lock path for a task
     * 
     * @param task
     *            The task key
     * @return the task lock path
     */
    private static String getTaskLockPath(TaskKey task) {
        return getBaseTaskLockPath(task.getQueryId()) + '/' + task.getTaskId();
    }
    
    /**
     * The query lock class which handles creating and deleting query semaphores and locks
     */
    private class QueryLock {
        private final UUID queryId;
        private final SharedCount maxPermits;
        private final InterProcessSemaphoreV2 semaphore;
        private final Map<TaskKey,Lease> leases = new HashMap<>();
        
        QueryLock(UUID queryId, int count) throws IOException {
            this.queryId = queryId;
            // first lets setup the shared count if needed
            maxPermits = getSemaphoreCount(queryId, count);
            try {
                maxPermits.start();
            } catch (Exception e) {
                throw new IOException("Unable to start zookeeper counter", e);
            }
            
            // set the number of permits
            updatePermits(count);
            
            // now lets create the semaphore
            semaphore = getSemaphore(queryId, maxPermits);
        }
        
        synchronized void updatePermits(int count) throws IOException {
            try {
                maxPermits.setCount(count);
            } catch (Exception e) {
                throw new IOException("Unable to set zookeeper counter", e);
            }
        }
        
        synchronized void close() throws IOException {
            for (Lease lease : leases.values()) {
                lease.close();
            }
            leases.clear();
            maxPermits.close();
        }
        
        synchronized void delete() throws IOException {
            try {
                close();
            } finally {
                try {
                    client.delete().forPath(getBasePath(queryId));
                } catch (Exception e) {
                    throw new IOException("Failed to delete query lock for " + queryId);
                }
            }
        }
        
        synchronized boolean acquireLock(TaskKey task, long waitMs) throws IOException, TaskLockException {
            if (leases.containsKey(task)) {
                throw new TaskLockException("Task already locked locally: " + task);
            }
            Lease lease = null;
            boolean acquiredLock = false;
            try {
                // first get a semaphore lease
                lease = semaphore.acquire(waitMs, TimeUnit.MILLISECONDS);
                if (lease == null) {
                    return false;
                }
                
                // acquire the lock
                acquiredLock = getTaskLock(task).acquire(waitMs, TimeUnit.MILLISECONDS);
            } catch (Exception e) {
                throw new IOException("Unable to acquire a task lock", e);
            } finally {
                if (acquiredLock) {
                    leases.put(task, lease);
                    return true;
                } else {
                    if (lease != null)
                        lease.close();
                    throw new TaskLockException("Task is already locked: " + task);
                }
            }
        }
        
        synchronized void releaseLock(TaskKey task) throws TaskLockException, IOException {
            Lease lease = leases.remove(task);
            if (lease == null) {
                throw new TaskLockException("Task not locked: " + task);
            }
            lease.close();
        }
        
        synchronized boolean isLocked(TaskKey task) {
            return leases.containsKey(task);
        }
        
        synchronized Set<TaskKey> getLockedTasks() {
            return new HashSet<>(leases.keySet());
        }
        
        private SharedCount getSemaphoreCount(UUID queryId, int count) {
            return new SharedCount(client, getSemaphoreCountPath(queryId), count);
        }
        
        private InterProcessSemaphoreV2 getSemaphore(UUID queryId, SharedCount count) {
            return new InterProcessSemaphoreV2(client, getSemaphorePath(queryId), count);
        }
        
        private InterProcessMutex getTaskLock(TaskKey task) {
            return new InterProcessMutex(client, getTaskLockPath(task));
        }
    }
    
}
