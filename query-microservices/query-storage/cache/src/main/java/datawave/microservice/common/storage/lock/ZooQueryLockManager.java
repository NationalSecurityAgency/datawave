package datawave.microservice.common.storage.lock;

import datawave.microservice.common.storage.QueryLockManager;
import datawave.microservice.common.storage.TaskKey;
import datawave.microservice.common.storage.TaskLockException;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.ExponentialBackoffRetry;

import java.util.Collections;
import java.util.Set;
import java.util.UUID;

public class ZooQueryLockManager implements QueryLockManager {
    
    private CuratorFramework framework;
    
    public ZooQueryLockManager(String zookeeperConnectionString) {
        framework = CuratorFrameworkFactory.newClient(zookeeperConnectionString, new ExponentialBackoffRetry(1000, 3));
        framework.start();
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
    public void createSemaphore(UUID queryId, int count) {
        // TBD
    }
    
    /**
     * Acquire a lock for a task. This will wait the specified waitMs for a semaphore slot to be available.
     *
     * @param task
     *            The task to lock
     * @param waitMs
     *            How long to wait for semaphore availability
     * @return true if able to lock the task
     * @throws TaskLockException
     *             if the task is already locked or the query semaphore does not exist
     */
    @Override
    public boolean acquireLock(TaskKey task, long waitMs) throws TaskLockException {
        // TBD
        return false;
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
    public void releaseLock(TaskKey task) throws TaskLockException {
        // TBD
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
        // TBD
        return false;
    }
    
    /**
     * Get the list of tasks that currently have locks
     *
     * @param queryId
     *            The query id
     * @return the list of tasks
     */
    @Override
    public Set<TaskKey> getLockedTasks(UUID queryId) {
        // TBD
        return Collections.emptySet();
    }
    
    /**
     * Get the list of queries that currently have semaphores
     */
    public Set<UUID> getQueries() {
        // TBD
        return Collections.emptySet();
    }
}
