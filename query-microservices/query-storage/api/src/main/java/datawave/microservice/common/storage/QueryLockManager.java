package datawave.microservice.common.storage;

import java.util.Set;
import java.util.UUID;

public interface QueryLockManager {
    
    /**
     * This will setup a semaphore for a query with a specified count. This can be called multiple times for the same query if the number of available locks
     * needs to be adjusted over time. Specifying a count of 0 will effectively delete the semaphore.
     *
     * @param queryId
     *            The query id
     * @param count
     *            The max count for the semaphore
     */
    public void createSemaphore(UUID queryId, int count);
    
    /**
     * Delete a semaphore.
     *
     * @param queryId
     *            The query id
     */
    default void deleteSemaphore(UUID queryId) {
        createSemaphore(queryId, 0);
        for (TaskKey task : getLockedTasks(queryId)) {
            try {
                releaseLock(task);
            } catch (TaskLockException e) {
                // ignore
            }
        }
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
    public boolean acquireLock(TaskKey task, long waitMs) throws TaskLockException;
    
    /**
     * Release the lock for a task. This will also decrement the semaphore allowing another task to be locked.
     *
     * @param task
     *            The task to unlock
     * @throws TaskLockException
     *             if the task is not locked.
     */
    public void releaseLock(TaskKey task) throws TaskLockException;
    
    /**
     * Determine if a task is locked.
     *
     * @param task
     *            The task
     * @return True if locked
     */
    public boolean isLocked(TaskKey task);
    
    /**
     * Get the set of tasks that currently have locks
     *
     * @param queryId
     *            The query id
     * @return the list of tasks
     */
    public Set<TaskKey> getLockedTasks(UUID queryId);
    
    /**
     * Get the list of queries that currently have semaphores
     *
     */
    public Set<UUID> getQueries();
}
