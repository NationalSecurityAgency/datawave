package datawave.microservice.query.storage;

import java.io.IOException;
import java.util.List;
import java.util.Set;

import org.apache.accumulo.core.security.Authorizations;

import datawave.core.query.logic.QueryCheckpoint;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.query.Query;
import datawave.microservice.query.remote.QueryRequest;

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
     * @param currentUser
     *            The current user
     * @param calculatedAuths
     *            The intersection of the user's auths with the requested auths
     * @param count
     *            The number of available locks which equates to the number of concurrent executors that can act on this query
     * @return The create task key
     * @throws IOException
     *             underlying storage error
     */
    TaskKey defineQuery(String queryPool, Query query, DatawaveUserDetails currentUser, Set<Authorizations> calculatedAuths, int count) throws IOException;
    
    /**
     * Store/cache a new query. This will create a query task containing the query with a CREATE query action.
     *
     * @param queryPool
     *            The query pool
     * @param query
     *            The query parameters
     * @param currentUser
     *            The current user
     * @param calculatedAuths
     *            The intersection of the user's auths with the requested auths
     * @param count
     *            The number of available locks which equates to the number of concurrent executors that can act on this query
     * @return The create task key
     * @throws IOException
     *             underlying storage error
     */
    TaskKey createQuery(String queryPool, Query query, DatawaveUserDetails currentUser, Set<Authorizations> calculatedAuths, int count) throws IOException;
    
    /**
     * Store/cache a new query. This will create a query task containing the query with a PLAN query action.
     *
     * @param queryPool
     *            The query pool
     * @param query
     *            The query parameters
     * @param currentUser
     *            The current user
     * @param calculatedAuths
     *            The intersection of the user's auths with the requested auths
     * @return The plan task key
     * @throws IOException
     *             underlying storage error
     */
    TaskKey planQuery(String queryPool, Query query, DatawaveUserDetails currentUser, Set<Authorizations> calculatedAuths) throws IOException;
    
    /**
     * Store/cache a new query. This will create a query task containing the query with a PREDICT query action.
     *
     * @param queryPool
     *            The query pool
     * @param query
     *            The query parameters
     * @param currentUser
     *            The current user
     * @param calculatedAuths
     *            The intersection of the user's auths with the requested auths
     * @return The predict task key
     * @throws IOException
     *             underlying storage error
     */
    TaskKey predictQuery(String queryPool, Query query, DatawaveUserDetails currentUser, Set<Authorizations> calculatedAuths) throws IOException;
    
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
     * @param stage
     *            The updated state
     */
    void updateCreateStage(String queryId, QueryStatus.CREATE_STAGE stage);
    
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
     * Update a stored query task with an updated checkpoint. This will also update the last updated time in the task.
     *
     * @param taskKey
     *            The task key to update
     * @param checkpoint
     *            The new query checkpoint
     * @return The updated query task
     */
    QueryTask checkpointTask(TaskKey taskKey, QueryCheckpoint checkpoint);
    
    /**
     * Update the stored query task last updated time
     *
     * @param task
     *            The task to updatge
     *            
     * @return The updated query task
     */
    default QueryTask updateTask(QueryTask task) {
        return checkpointTask(task.getTaskKey(), task.getQueryCheckpoint());
    }
    
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
    
}
