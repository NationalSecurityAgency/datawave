package datawave.microservice.query.storage;

import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

import org.apache.accumulo.core.security.Authorizations;
import org.apache.log4j.Logger;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.stereotype.Service;

import datawave.core.query.logic.QueryCheckpoint;
import datawave.core.query.logic.QueryKey;
import datawave.microservice.authorization.user.DatawaveUserDetails;
import datawave.microservice.query.Query;
import datawave.microservice.query.messaging.QueryResultsManager;
import datawave.microservice.query.remote.QueryRequest;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;

@Service
@ConditionalOnProperty(name = "datawave.query.storage.cache.enabled", havingValue = "true", matchIfMissing = true)
public class QueryStorageCacheImpl implements QueryStorageCache {
    
    private static final Logger log = Logger.getLogger(QueryStorageCacheImpl.class);
    
    private final QueryStatusCache queryStatusCache;
    private final TaskStatesCache taskStatesCache;
    private final TaskCache taskCache;
    private final QueryResultsManager queue;
    
    public QueryStorageCacheImpl(QueryStatusCache queryStatusCache, TaskStatesCache taskStatesCache, TaskCache taskCache,
                    @Autowired(required = false) QueryResultsManager queue) {
        this.queryStatusCache = queryStatusCache;
        this.taskStatesCache = taskStatesCache;
        this.taskCache = taskCache;
        this.queue = queue;
    }
    
    /**
     * Store/cache a new query. This will create a query task containing the query with a DEFINE query action.
     *
     * @param queryPool
     *            The query pool
     * @param query
     *            The query parameters
     * @param currentUser
     *            The current user
     * @param calculatedAuthorizations
     *            The intersection of the user's auths with the requested auths
     * @param count
     *            The number of available locks which equates to the number of concurrent executors that can act on this query
     * @return The task key
     */
    @Override
    public TaskKey defineQuery(String queryPool, Query query, DatawaveUserDetails currentUser, Set<Authorizations> calculatedAuthorizations, int count) {
        return storeQuery(queryPool, query, currentUser, calculatedAuthorizations, count, QueryStatus.QUERY_STATE.DEFINE);
    }
    
    /**
     * Store/cache a new query. This will create a query task containing the query with a CREATE query action.
     *
     * @param queryPool
     *            The query pool
     * @param query
     *            The query parameters
     * @param currentUser
     *            The current user
     * @param calculatedAuthorizations
     *            The intersection of the user's auths with the requested auths
     * @param count
     *            The number of available locks which equates to the number of concurrent executors that can act on this query
     * @return The task key
     */
    @Override
    public TaskKey createQuery(String queryPool, Query query, DatawaveUserDetails currentUser, Set<Authorizations> calculatedAuthorizations, int count) {
        return storeQuery(queryPool, query, currentUser, calculatedAuthorizations, count, QueryStatus.QUERY_STATE.CREATE);
    }
    
    /**
     * Store/cache a new query. This will create a query task containing the query with a PLAN query action.
     *
     * @param queryPool
     *            The query pool
     * @param query
     *            The query parameters
     * @param currentUser
     *            The current user
     * @param calculatedAuthorizations
     *            The intersection of the user's auths with the requested auths
     * @return The plan task key
     */
    @Override
    public TaskKey planQuery(String queryPool, Query query, DatawaveUserDetails currentUser, Set<Authorizations> calculatedAuthorizations) {
        return storeQuery(queryPool, query, currentUser, calculatedAuthorizations, 1, QueryStatus.QUERY_STATE.PLAN);
    }
    
    /**
     * Store/cache a new query. This will create a query task containing the query with a PREDICT query action.
     *
     * @param queryPool
     *            The query pool
     * @param query
     *            The query parameters
     * @param currentUser
     *            The current user
     * @param calculatedAuthorizations
     *            The intersection of the user's auths with the requested auths
     * @return The predict task key
     */
    @Override
    public TaskKey predictQuery(String queryPool, Query query, DatawaveUserDetails currentUser, Set<Authorizations> calculatedAuthorizations) {
        return storeQuery(queryPool, query, currentUser, calculatedAuthorizations, 1, QueryStatus.QUERY_STATE.PREDICT);
    }
    
    private TaskKey storeQuery(String queryPool, Query query, DatawaveUserDetails currentUser, Set<Authorizations> calculatedAuthorizations, int count,
                    QueryStatus.QUERY_STATE queryState) {
        UUID queryUuid = query.getId();
        if (queryUuid == null) {
            // create the query String
            queryUuid = UUID.randomUUID();
            query.setId(queryUuid);
        }
        String queryId = queryUuid.toString();
        
        QueryKey queryKey = new QueryKey(queryPool, queryId, query.getQueryLogicName());
        
        // create the initial query checkpoint
        QueryCheckpoint checkpoint = new QueryCheckpoint(queryKey);
        
        // store the initial query properties
        QueryStatus queryStatus = new QueryStatus(checkpoint.getQueryKey());
        queryStatus.setCurrentUser(currentUser);
        queryStatus.setQueryState(queryState);
        queryStatus.setQuery(query);
        queryStatus.setCalculatedAuthorizations(calculatedAuthorizations);
        queryStatus.setLastUsedMillis(System.currentTimeMillis());
        queryStatus.setLastUpdatedMillis(queryStatus.getLastUsedMillis());
        queryStatusCache.updateQueryStatus(queryStatus);
        
        // only create tasks if we are creating a query
        if (queryState == QueryStatus.QUERY_STATE.CREATE || queryState == QueryStatus.QUERY_STATE.PLAN || queryState == QueryStatus.QUERY_STATE.PREDICT) {
            // store the initial tasks states
            TaskStates taskStates = new TaskStates(queryKey, count);
            taskStatesCache.updateTaskStates(taskStates);
            
            // create and store the initial task with the checkpoint
            QueryTask task = createTask(stateToMethod(queryState), checkpoint);
            return task.getTaskKey();
        }
        
        // return the an empty task key
        return new TaskKey(-1, new QueryKey(queryPool, queryId, query.getQueryLogicName()));
    }
    
    private QueryRequest.Method stateToMethod(QueryStatus.QUERY_STATE queryState) {
        switch (queryState) {
            case CREATE:
                return QueryRequest.Method.CREATE;
            case PLAN:
                return QueryRequest.Method.PLAN;
            case PREDICT:
                return QueryRequest.Method.PREDICT;
        }
        return null;
    }
    
    /**
     * Get the current query state. This includes the query properties, stats, and task summary
     * 
     * @param queryId
     *            the query id
     * @return query stats
     */
    public QueryState getQueryState(String queryId) {
        return new QueryState(getQueryStatus(queryId), getTaskStates(queryId));
    }
    
    /**
     * Get the current query properties.
     * 
     * @param queryId
     *            the query id
     * @return the query properties
     */
    public QueryStatus getQueryStatus(String queryId) {
        return queryStatusCache.getQueryStatus(queryId);
    }
    
    /**
     * Get all query properties
     * 
     * @return the query properties
     */
    public List<QueryStatus> getQueryStatus() {
        return queryStatusCache.getQueryStatus();
    }
    
    /**
     * update the query properties
     * 
     * @param queryStatus
     *            the query properties
     */
    public void updateQueryStatus(QueryStatus queryStatus) {
        queryStatus.setLastUpdatedMillis(System.currentTimeMillis());
        queryStatusCache.updateQueryStatus(queryStatus);
    }
    
    /**
     * Update the query status state
     *
     * @param queryId
     *            The query id
     * @param state
     *            the query state
     */
    @Override
    public void updateQueryStatus(String queryId, QueryStatus.QUERY_STATE state) {
        QueryStorageLock lock = queryStatusCache.getQueryStatusLock(queryId);
        lock.lock();
        try {
            QueryStatus status = queryStatusCache.getQueryStatus(queryId);
            status.setQueryState(state);
            updateQueryStatus(status);
        } finally {
            lock.unlock();
        }
    }
    
    /**
     * Update the query status state
     *
     * @param queryId
     *            The query id
     * @param stage
     *            the query create stage
     */
    @Override
    public void updateCreateStage(String queryId, QueryStatus.CREATE_STAGE stage) {
        QueryStorageLock lock = queryStatusCache.getQueryStatusLock(queryId);
        lock.lock();
        try {
            QueryStatus status = queryStatusCache.getQueryStatus(queryId);
            status.setCreateStage(stage);
            updateQueryStatus(status);
        } finally {
            lock.unlock();
        }
    }
    
    /**
     * Update the query status state
     *
     * @param queryId
     *            The query id
     * @param e
     *            the exception
     */
    @Override
    public void updateFailedQueryStatus(String queryId, Exception e) {
        QueryStorageLock lock = queryStatusCache.getQueryStatusLock(queryId);
        lock.lock();
        try {
            QueryStatus status = queryStatusCache.getQueryStatus(queryId);
            status.setQueryState(QueryStatus.QUERY_STATE.FAIL);
            status.setFailure(getErrorCode(e), e);
            updateQueryStatus(status);
        } finally {
            lock.unlock();
        }
    }
    
    protected DatawaveErrorCode getErrorCode(Exception e) {
        DatawaveErrorCode code = DatawaveErrorCode.QUERY_EXECUTION_ERROR;
        if (e instanceof QueryException) {
            code = DatawaveErrorCode.findCode(((QueryException) e).getErrorCode());
        }
        return code;
    }
    
    /**
     * Update a task state
     *
     * @param taskKey
     *            The task key
     * @param state
     *            The task state
     */
    @Override
    public boolean updateTaskState(TaskKey taskKey, TaskStates.TASK_STATE state) {
        QueryStorageLock lock = taskStatesCache.getTaskStatesLock(taskKey.getQueryId());
        lock.lock();
        try {
            TaskStates states = taskStatesCache.getTaskStates(taskKey.getQueryId());
            if (states != null && states.setState(taskKey.getTaskId(), state)) {
                updateTaskStates(states);
                return true;
            }
        } finally {
            lock.unlock();
        }
        return false;
    }
    
    /**
     * Get a lock for the query status
     *
     * @param queryId
     *            the query id
     * @return a lock object
     */
    @Override
    public QueryStorageLock getQueryStatusLock(String queryId) {
        return queryStatusCache.getQueryStatusLock(queryId);
    }
    
    /**
     * Get a task states lock
     *
     * @param queryId
     *            the query id
     * @return a lock object
     */
    @Override
    public QueryStorageLock getTaskStatesLock(String queryId) {
        return taskStatesCache.getTaskStatesLock(queryId);
    }
    
    /**
     * Get the current task states.
     *
     * @param queryId
     *            the query id
     * @return the task states
     */
    @Override
    public TaskStates getTaskStates(String queryId) {
        return taskStatesCache.getTaskStates(queryId);
    }
    
    /**
     * update the query status
     *
     * @param taskStates
     *            the task states
     */
    @Override
    public void updateTaskStates(TaskStates taskStates) {
        taskStatesCache.updateTaskStates(taskStates);
    }
    
    /**
     * Create a new query task. This will create a new query task, store it.
     * 
     * @param action
     *            The query action
     * @param checkpoint
     *            The query checkpoint
     * @return The new query task
     */
    @Override
    public QueryTask createTask(QueryRequest.Method action, QueryCheckpoint checkpoint) {
        QueryTask task;
        
        String queryId = checkpoint.getQueryKey().getQueryId();
        
        QueryStorageLock lock = taskStatesCache.getTaskStatesLock(queryId);
        lock.lock();
        try {
            // Set the initial ready state in the task states
            TaskStates states = taskStatesCache.getTaskStates(queryId);
            
            int taskId = states.getAndIncrementNextTaskId();
            
            // create a query task in the cache
            task = taskCache.addQueryTask(taskId, action, checkpoint);
            
            states.setState(taskId, TaskStates.TASK_STATE.READY);
            
            taskStatesCache.updateTaskStates(states);
        } catch (Exception e) {
            log.error("Failed to add query task", e);
            throw new RuntimeException("Failed to add query task", e);
        } finally {
            lock.unlock();
        }
        
        // return the task
        return task;
    }
    
    /**
     * Get a task for a given task key and lock it for processing. This return null if the task no longer exists. This will throw an exception if the task is
     * already locked.
     *
     * @param taskKey
     *            The task key
     * @return The query task, null if deleted
     */
    @Override
    public QueryTask getTask(TaskKey taskKey) {
        return taskCache.getTask(taskKey);
    }
    
    /**
     * Update a stored query task with an updated checkpoint.
     *
     * @param taskKey
     *            The task key to update
     * @param checkpoint
     *            The new query checkpoint
     * @return The updated query task
     */
    @Override
    public QueryTask checkpointTask(TaskKey taskKey, QueryCheckpoint checkpoint) {
        return taskCache.updateQueryTask(taskKey, checkpoint);
    }
    
    /**
     * Delete a query task.
     *
     * @param taskKey
     *            The task key
     */
    @Override
    public void deleteTask(TaskKey taskKey) {
        taskCache.deleteTask(taskKey);
    }
    
    /**
     * Delete a query
     *
     * @param queryId
     *            the query id
     * @return true if deleted
     */
    @Override
    public boolean deleteQuery(String queryId) {
        QueryStorageLock lock = queryStatusCache.getQueryStatusLock(queryId);
        lock.lock();
        try {
            boolean existed = (queryStatusCache.getQueryStatus(queryId) != null);
            queryStatusCache.deleteQueryStatus(queryId);
            taskStatesCache.deleteTaskStates(queryId);
            taskCache.deleteTasks(queryId);
            
            if (queue != null) {
                queue.deleteQuery(queryId);
            }
            return existed;
        } finally {
            lock.unlock();
        }
    }
    
    /**
     * Clear the cache
     */
    @Override
    public void clear() {
        if (queue != null) {
            for (QueryStatus query : queryStatusCache.getQueryStatus()) {
                queue.emptyQuery(query.getQueryKey().getQueryId());
            }
        }
        
        queryStatusCache.clear();
        taskStatesCache.clear();
        taskCache.clear();
    }
    
    /**
     * Get the tasks that are stored for a specified query
     *
     * @param queryId
     *            The query id
     * @return The list of task keys
     */
    @Override
    public List<TaskKey> getTasks(String queryId) {
        List<TaskKey> tasks = new ArrayList<>();
        for (QueryTask task : taskCache.getTasks(queryId)) {
            tasks.add(task.getTaskKey());
        }
        return tasks;
    }
    
}
