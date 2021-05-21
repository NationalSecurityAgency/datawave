package datawave.microservice.common.storage;

import datawave.webservice.query.Query;
import org.apache.accumulo.core.security.Authorizations;

import java.io.IOException;
import java.util.List;
import java.util.Set;
import java.util.UUID;

/**
 * This is an interface to the query storage service
 */
public interface QueryStorageCache {
    /**
     * Store/cache a new query. This will create a query task containing the query with a DEFINE query action and send out a task notification.
     * 
     * @param queryPool
     *            The query pool
     * @param query
     *            The query parameters
     * @param calculatedAuths
     *            The intersection of the user's auths with the requested auths
     * @param count
     *            The number of available locks which equates to the number of concurrent executors that can act on this query
     * @return The create task key
     */
    TaskKey defineQuery(QueryPool queryPool, Query query, Set<Authorizations> calculatedAuths, int count) throws IOException;
    
    /**
     * Store/cache a new query. This will create a query task containing the query with a CREATE query action and send out a task notification.
     *
     * @param queryPool
     *            The query pool
     * @param query
     *            The query parameters
     * @param calculatedAuths
     *            The intersection of the user's auths with the requested auths
     * @param count
     *            The number of available locks which equates to the number of concurrent executors that can act on this query
     * @return The create task key
     */
    TaskKey createQuery(QueryPool queryPool, Query query, Set<Authorizations> calculatedAuths, int count) throws IOException;
    
    /**
     * Get the current query state. This includes the query status and the task statuses
     * 
     * @param queryId
     *            the query id
     * @return query stats
     */
    QueryState getQueryState(UUID queryId);
    
    /**
     * Get the current query status.
     * 
     * @param queryId
     *            the query id
     * @return the query status
     */
    QueryStatus getQueryStatus(UUID queryId);
    
    /**
     * Get all of the query status
     * 
     * @return a list of query status
     */
    List<QueryStatus> getQueryStatus();
    
    /**
     * update the query status
     * 
     * @param queryStatus
     *            the query status
     */
    void updateQueryStatus(QueryStatus queryStatus);
    
    /**
     * Update the query status state
     * 
     * @param queryId
     *            The query id
     * @param state
     *            The updated state
     */
    void updateQueryStatus(UUID queryId, QueryStatus.QUERY_STATE state);
    
    /**
     * Update a task state
     * 
     * @param taskKey
     *            The task key
     * @param state
     *            The updated state
     */
    boolean updateTaskState(TaskKey taskKey, TaskStates.TASK_STATE state);
    
    /**
     * Get a lock for the query status
     *
     * @param queryId
     *            the query id
     * @return a lock object
     */
    QueryStorageLock getQueryStatusLock(UUID queryId);
    
    /**
     * Get a task states lock
     *
     * @param queryId
     *            the query id
     * @return a lock object
     */
    QueryStorageLock getTaskStatesLock(UUID queryId);
    
    /**
     * Get a task lock
     *
     * @param task
     *            The task key
     * @return a lock object
     */
    QueryStorageLock getTaskLock(TaskKey task);
    
    /**
     * Get the current task states.
     * 
     * @param queryId
     *            the query id
     * @return the task states
     */
    TaskStates getTaskStates(UUID queryId);
    
    /**
     * update the query status
     * 
     * @param taskStates
     *            the task states
     */
    void updateTaskStates(TaskStates taskStates);
    
    /**
     * Create a new query task. This will create a new query task, store it, and send out a task notification.
     * 
     * @param action
     *            The query action
     * @param checkpoint
     *            The query checkpoint
     * @return The new query task
     */
    QueryTask createTask(QueryTask.QUERY_ACTION action, QueryCheckpoint checkpoint) throws IOException;
    
    /**
     * Post a task notification
     * 
     * @param taskNotification
     */
    public void post(QueryTaskNotification taskNotification);
    
    /**
     * Get a task for a given task key and lock it for processing. This return null if the task no longer exists. This will throw an exception if the task is
     * already locked.
     *
     * @param taskKey
     *            The task key
     * @param waitMs
     *            How long to wait to get a task lock
     * @return The query task, null if deleted
     * @throws TaskLockException
     *             if the task is already locked
     */
    QueryTask getTask(TaskKey taskKey, long waitMs) throws TaskLockException, IOException, InterruptedException;
    
    /**
     * Update a stored query task with an updated checkpoint. This will also release the lock. This will throw an exception is the task is not locked.
     * 
     * @param taskKey
     *            The task key to update
     * @param checkpoint
     *            The new query checkpoint
     * @return The updated query task
     * @throws TaskLockException
     *             if the task is not locked
     */
    QueryTask checkpointTask(TaskKey taskKey, QueryCheckpoint checkpoint) throws TaskLockException, IOException;
    
    /**
     * Delete a query task. This will also release the lock. This will throw an exception if the task is not locked.
     * 
     * @param taskKey
     *            The task key
     * @throws TaskLockException
     *             if the task is not locked
     */
    void deleteTask(TaskKey taskKey) throws TaskLockException, IOException;
    
    /**
     * Delete a query
     *
     * @param queryId
     *            the query id
     * @return true if deleted
     */
    public boolean deleteQuery(UUID queryId) throws IOException;
    
    /**
     * Clear the cache
     */
    public void clear() throws IOException;
    
    /**
     * Get the tasks that are stored for a specified query
     *
     * @param queryId
     *            The query id
     * @return The list of task keys
     */
    public List<TaskKey> getTasks(UUID queryId) throws IOException;
    
    /************** Some convenience methods for query status locking ************/
    
    /**
     * Acquires the lock for the specified query status.
     *
     * @param queryId
     *            The query id
     * @deprecated try getQueryStatusLock(queryId).lock()
     */
    @Deprecated
    default void lockQueryStatus(UUID queryId) {
        getQueryStatusLock(queryId).lock();
    }
    
    /**
     * Acquires the lock for the specified query status for the specified lease time.
     *
     * @param queryId
     *            The query id
     * @param leaseTimeMillis
     *            The lease time in millis
     * @deprecated try getQueryStatusLock(queryId).lock(leaseTimeMillis)
     */
    @Deprecated
    default void lockQueryStatus(UUID queryId, long leaseTimeMillis) {
        getQueryStatusLock(queryId).lock(leaseTimeMillis);
    }
    
    /**
     * Acquires the lock for the specified query status.
     *
     * @param queryId
     *            The query id
     * @return true if the lock was acquired, false if the waiting time elapsed before the lock was acquired
     * @deprecated try getQueryStatusLock(queryId).tryLock()
     */
    @Deprecated
    default boolean tryLockQueryStatus(UUID queryId) {
        return getQueryStatusLock(queryId).tryLock();
    }
    
    /**
     * Tries to acquires the lock for the specified query status for the specified wait time.
     *
     * @param queryId
     *            The query id
     * @param waitTimeMillis
     *            The wait time in millis
     * @return true if the lock was acquired, false if the waiting time elapsed before the lock was acquired
     * @deprecated try getQueryStatusLock(queryId).tryLock(waitTimeMillis)
     */
    @Deprecated
    default boolean tryLockQueryStatus(UUID queryId, long waitTimeMillis) throws InterruptedException {
        return getQueryStatusLock(queryId).tryLock(waitTimeMillis);
    }
    
    /**
     * Try to acquire the lock for the specified query status for the specified wait time and lease time
     *
     * @param queryId
     *            The query id
     * @param waitTimeMillis
     *            The wait time in millis
     * @param leaseTimeMillis
     *            Time to wait before releasing the lock
     * @return true if the lock was acquired, false if the waiting time elapsed before the lock was acquired
     * @deprecated try getQueryStatusLock(queryId).tryLock(waitTimeMillis, leaseTimeMillis)
     */
    @Deprecated
    default boolean tryLockQueryStatus(UUID queryId, long waitTimeMillis, long leaseTimeMillis) throws InterruptedException {
        return getQueryStatusLock(queryId).tryLock(waitTimeMillis, leaseTimeMillis);
    }
    
    /**
     * Releases the lock for the specified query status
     *
     * @param queryId
     *            The query id
     * @deprecated try getQueryStatusLock(queryId).unlock()
     */
    @Deprecated
    default void unlockQueryStatus(UUID queryId) {
        getQueryStatusLock(queryId).unlock();
    }
    
    /**
     * Releases the lock for the specified query status regardless of the lock owner. It always successfully unlocks the key, never blocks, and returns
     * immediately.
     *
     * @param queryId
     *            The query id
     * @deprecated try getQueryStatusLock(queryId).forceUnlock()
     */
    @Deprecated
    default void forceUnlockQueryStatus(UUID queryId) {
        getQueryStatusLock(queryId).forceUnlock();
    }
    
}
