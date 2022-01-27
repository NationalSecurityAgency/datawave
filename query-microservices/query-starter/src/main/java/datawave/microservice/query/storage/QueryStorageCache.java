package datawave.microservice.query.storage;

import datawave.microservice.query.remote.QueryRequest;
import datawave.services.query.logic.QueryCheckpoint;
import datawave.webservice.query.Query;
import org.apache.accumulo.core.security.Authorizations;

import java.io.IOException;
import java.util.List;
import java.util.Set;

/**
 * This is an interface to the query storage service
 */
public interface QueryStorageCache {
    /**
     * Store/cache a new query. This will create a query task containing the query with a DEFINE query action.
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
     * @throws IOException
     *             underlying storage error
     */
    TaskKey defineQuery(String queryPool, Query query, Set<Authorizations> calculatedAuths, int count) throws IOException;
    
    /**
     * Store/cache a new query. This will create a query task containing the query with a CREATE query action.
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
     * @throws IOException
     *             underlying storage error
     */
    TaskKey createQuery(String queryPool, Query query, Set<Authorizations> calculatedAuths, int count) throws IOException;
    
    /**
     * Store/cache a new query. This will create a query task containing the query with a PLAN query action.
     *
     * @param queryPool
     *            The query pool
     * @param query
     *            The query parameters
     * @param calculatedAuths
     *            The intersection of the user's auths with the requested auths
     * @return The plan task key
     * @throws IOException
     *             underlying storage error
     */
    TaskKey planQuery(String queryPool, Query query, Set<Authorizations> calculatedAuths) throws IOException;
    
    /**
     * Store/cache a new query. This will create a query task containing the query with a PREDICT query action.
     *
     * @param queryPool
     *            The query pool
     * @param query
     *            The query parameters
     * @param calculatedAuths
     *            The intersection of the user's auths with the requested auths
     * @return The predict task key
     * @throws IOException
     *             underlying storage error
     */
    TaskKey predictQuery(String queryPool, Query query, Set<Authorizations> calculatedAuths) throws IOException;
    
    /**
     * Get the current query state. This includes the query status and the task statuses
     * 
     * @param queryId
     *            the query id
     * @return query stats
     */
    QueryState getQueryState(String queryId);
    
    /**
     * Get the current query status.
     * 
     * @param queryId
     *            the query id
     * @return the query status
     */
    QueryStatus getQueryStatus(String queryId);
    
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
    void updateQueryStatus(String queryId, QueryStatus.QUERY_STATE state);
    
    /**
     * Update the query status state
     *
     * @param queryId
     *            The query id
     * @param e
     *            The exception
     */
    void updateFailedQueryStatus(String queryId, Exception e);
    
    /**
     * Update a task state
     *
     * @param taskKey
     *            The task key
     * @param state
     *            The updated state
     * @return false if no more running state slots available, true otherwise.
     */
    boolean updateTaskState(TaskKey taskKey, TaskStates.TASK_STATE state);
    
    /**
     * Get a lock for the query status
     *
     * @param queryId
     *            the query id
     * @return a lock object
     */
    QueryStorageLock getQueryStatusLock(String queryId);
    
    /**
     * Get a task states lock
     *
     * @param queryId
     *            the query id
     * @return a lock object
     */
    QueryStorageLock getTaskStatesLock(String queryId);
    
    /**
     * Get the current task states.
     * 
     * @param queryId
     *            the query id
     * @return the task states
     */
    TaskStates getTaskStates(String queryId);
    
    /**
     * update the query status
     * 
     * @param taskStates
     *            the task states
     */
    void updateTaskStates(TaskStates taskStates);
    
    /**
     * Create a new query task. This will create a new query task, store it.
     * 
     * @param action
     *            The query action
     * @param checkpoint
     *            The query checkpoint
     * @return The new query task
     * @throws IOException
     *             underlying storage error
     */
    QueryTask createTask(QueryRequest.Method action, QueryCheckpoint checkpoint) throws IOException;
    
    /**
     * Get a task for a given task key. This return null if the task no longer exists.
     *
     * @param taskKey
     *            The task key
     * @return The query task, null if deleted
     */
    QueryTask getTask(TaskKey taskKey);
    
    /**
     * Update a stored query task with an updated checkpoint.
     *
     * @param taskKey
     *            The task key to update
     * @param checkpoint
     *            The new query checkpoint
     * @return The updated query task
     */
    QueryTask checkpointTask(TaskKey taskKey, QueryCheckpoint checkpoint);
    
    /**
     * Delete a query task.
     * 
     * @param taskKey
     *            The task key
     * @throws IOException
     *             underlying storage error
     */
    void deleteTask(TaskKey taskKey) throws IOException;
    
    /**
     * Delete a query
     *
     * @param queryId
     *            the query id
     * @return true if deleted
     * @throws IOException
     *             underlying storage error
     */
    boolean deleteQuery(String queryId) throws IOException;
    
    /**
     * Clear the cache
     * 
     * @throws IOException
     *             underlying storage error
     */
    void clear() throws IOException;
    
    /**
     * Get the tasks that are stored for a specified query
     *
     * @param queryId
     *            The query id
     * @return The list of task keys
     * @throws IOException
     *             underlying storage error
     */
    List<TaskKey> getTasks(String queryId) throws IOException;
    
    /************** Some convenience methods for query status locking ************/
    
    /**
     * Acquires the lock for the specified query status.
     *
     * @param queryId
     *            The query id
     * @deprecated try getQueryStatusLock(queryId).lock()
     */
    @Deprecated
    default void lockQueryStatus(String queryId) {
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
    default void lockQueryStatus(String queryId, long leaseTimeMillis) {
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
    default boolean tryLockQueryStatus(String queryId) {
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
     * @throws InterruptedException
     *             if interrupted
     */
    @Deprecated
    default boolean tryLockQueryStatus(String queryId, long waitTimeMillis) throws InterruptedException {
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
     * @throws InterruptedException
     *             if interrupted
     */
    @Deprecated
    default boolean tryLockQueryStatus(String queryId, long waitTimeMillis, long leaseTimeMillis) throws InterruptedException {
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
    default void unlockQueryStatus(String queryId) {
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
    default void forceUnlockQueryStatus(String queryId) {
        getQueryStatusLock(queryId).forceUnlock();
    }
    
}
