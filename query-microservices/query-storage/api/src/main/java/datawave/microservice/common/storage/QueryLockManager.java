package datawave.microservice.common.storage;

import java.io.IOException;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.locks.Lock;

public interface QueryLockManager {
    
    /**
     * This will setup a semaphore for a query with a specified count. This can be called multiple times for the same query if the number of available locks
     * needs to be adjusted over time. Specifying a count of 0 will effectively delete the semaphore.
     *
     * @param queryId
     *            The query id
     * @param count
     *            The max count for the semaphore
     * @throws IOException
     *             if there was a lock system access failure
     */
    void createSemaphore(UUID queryId, int count) throws IOException;
    
    /**
     * This will drop any cached data for this queryid, leaving the query locks in the underlying cluster. Any local task locks will be released.
     * 
     * @param queryId
     *            The query id
     */
    void closeSemaphore(UUID queryId) throws IOException;
    
    /**
     * Delete a semaphore. This will delete the query in the underlying cluster.
     *
     * @param queryId
     *            The query id
     * @throws IOException
     *             if there was a lock system access failure
     */
    default void deleteSemaphore(UUID queryId) throws IOException {
        createSemaphore(queryId, 0);
    }

    /**
     * Get a lock object for a task
     * @param task
     * @return The Lock object
     */
    Lock getLock(TaskKey task);

    /**
     * Get a lock object for a query
     * @param queryId
     * @return The lock object
     */
    Lock getLock(UUID queryId);

    /**
     * Geeet a lock object for a arbitrary string
     * @param lockId
     * @return The lock object
     */
    Lock getLock(String lockId);

    /**
     * Determine if a task is locked.
     *
     * @param task
     *            The task
     * @return True if locked
     * @throws IOException
     *             if there was a lock system access failure
     */
    boolean isLocked(TaskKey task) throws IOException;
    
    /**
     * Get the set of tasks that currently have locks in this JVM
     *
     * @param queryId
     *            The query id
     * @return the list of tasks
     * @throws IOException
     *             if there was a lock system access failure
     */
    Set<TaskKey> getLockedTasks(UUID queryId) throws IOException;
    
    /**
     * Get the list of queries that currently have semaphores in this JVM
     *
     * @throws IOException
     *             if there was a lock system access failure
     */
    Set<UUID> getQueries() throws IOException;
    
    /**
     * Get the task ids for all locks that the zookeeper cluster knows about
     * 
     * @param queryId
     *            The query id
     * @return A set of task UUIDs
     * @throws IOException
     */
    Set<UUID> getDistributedLockedTasks(UUID queryId) throws IOException;
    
    /**
     * Get the query ids for all semaphores that the zookeeper cluster knows about
     * 
     * @return The set of query UUIDs
     * @throws IOException
     */
    Set<UUID> getDistributedQueries() throws IOException;
}
