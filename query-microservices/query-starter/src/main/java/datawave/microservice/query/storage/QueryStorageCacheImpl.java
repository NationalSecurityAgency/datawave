package datawave.microservice.query.storage;

import com.ecwid.consul.v1.query.model.QueryExecution;
import datawave.microservice.query.config.QueryProperties;
import datawave.microservice.query.remote.QueryRequest;
import datawave.microservice.query.storage.config.QueryStorageProperties;
import datawave.services.query.logic.QueryCheckpoint;
import datawave.services.query.logic.QueryKey;
import datawave.webservice.query.Query;
import datawave.webservice.query.exception.DatawaveErrorCode;
import datawave.webservice.query.exception.QueryException;
import org.apache.accumulo.core.security.Authorizations;
import org.springframework.cloud.bus.BusProperties;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Set;
import java.util.UUID;

@Service
public class QueryStorageCacheImpl implements QueryStorageCache {
    
    private final QueryCache cache;
    private final QueryQueueManager queue;
    private final QueryStorageProperties properties;
    
    private final QueryProperties queryProperties;
    private final ApplicationEventPublisher publisher;
    private final BusProperties busProperties;
    
    public QueryStorageCacheImpl(QueryCache cache, QueryQueueManager queue, QueryStorageProperties properties, QueryProperties queryProperties,
                    ApplicationEventPublisher publisher, BusProperties busProperties) {
        this.cache = cache;
        this.queue = queue;
        this.properties = properties;
        this.queryProperties = queryProperties;
        this.publisher = publisher;
        this.busProperties = busProperties;
    }
    
    /**
     * Store/cache a new query. This will create a query task containing the query with a DEFINE query action.
     *
     * @param queryPool
     *            The query pool
     * @param query
     *            The query parameters
     * @param calculatedAuthorizations
     *            The intersection of the user's auths with the requested auths
     * @param count
     *            The number of available locks which equates to the number of concurrent executors that can act on this query
     * @return The task key
     */
    @Override
    public TaskKey defineQuery(String queryPool, Query query, Set<Authorizations> calculatedAuthorizations, int count) throws IOException {
        return storeQuery(queryPool, query, null, calculatedAuthorizations, count, QueryStatus.QUERY_STATE.DEFINED);
    }
    
    /**
     * Store/cache a new query. This will create a query task containing the query with a CREATE query action.
     *
     * @param queryPool
     *            The query pool
     * @param query
     *            The query parameters
     * @param calculatedAuthorizations
     *            The intersection of the user's auths with the requested auths
     * @param count
     *            The number of available locks which equates to the number of concurrent executors that can act on this query
     * @return The task key
     */
    @Override
    public TaskKey createQuery(String queryPool, Query query, String originService, Set<Authorizations> calculatedAuthorizations, int count)
                    throws IOException {
        return storeQuery(queryPool, query, originService, calculatedAuthorizations, count, QueryStatus.QUERY_STATE.CREATED);
    }
    
    private TaskKey storeQuery(String queryPool, Query query, String originService, Set<Authorizations> calculatedAuthorizations, int count,
                    QueryStatus.QUERY_STATE queryState) throws IOException {
        UUID queryUuid = query.getId();
        if (queryUuid == null) {
            // create the query String
            queryUuid = UUID.randomUUID();
            query.setId(queryUuid);
        }
        String queryId = queryUuid.toString();
        
        QueryKey queryKey = new QueryKey(queryPool, queryId, query.getQueryLogicName());
        
        // create the initial query checkpoint
        QueryCheckpoint checkpoint = new QueryCheckpoint(queryKey, query);
        
        // store the initial query properties
        QueryStatus queryStatus = new QueryStatus(checkpoint.getQueryKey());
        queryStatus.setQueryState(queryState);
        queryStatus.setQuery(query);
        queryStatus.setOriginService(originService);
        queryStatus.setCalculatedAuthorizations(calculatedAuthorizations);
        queryStatus.setLastUsedMillis(System.currentTimeMillis());
        queryStatus.setLastUpdatedMillis(queryStatus.getLastUsedMillis());
        cache.updateQueryStatus(queryStatus);
        
        // only create tasks if we are creating a query
        if (queryState == QueryStatus.QUERY_STATE.CREATED) {
            // store the initial tasks states
            TaskStates taskStates = new TaskStates(queryKey, count);
            cache.updateTaskStates(taskStates);
            
            // create the results queue
            queue.ensureQueueCreated(queryId);
            
            // create and store the initial create task with the checkpoint
            QueryTask task = createTask(QueryRequest.Method.CREATE, checkpoint);
            return task.getTaskKey();
        }
        
        // return the an empty task key
        return new TaskKey(-1, new QueryKey(queryPool, queryId, query.getQueryLogicName()));
    }
    
    /**
     * Get the current query state. This includes the query properties, stats, and task summary
     * 
     * @param queryId
     *            the query id
     * @return query stats
     */
    public QueryState getQueryState(String queryId) {
        return cache.getQuery(queryId);
    }
    
    /**
     * Get the current query properties.
     * 
     * @param queryId
     *            the query id
     * @return the query properties
     */
    public QueryStatus getQueryStatus(String queryId) {
        return cache.getQueryStatus(queryId);
    }
    
    /**
     * Get all query propertis
     * 
     * @return the query properties
     */
    public List<QueryStatus> getQueryStatus() {
        return cache.getQueryStatus();
    }
    
    /**
     * update the query properties
     * 
     * @param queryStatus
     *            the query properties
     */
    public void updateQueryStatus(QueryStatus queryStatus) {
        queryStatus.setLastUpdatedMillis(System.currentTimeMillis());
        cache.updateQueryStatus(queryStatus);
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
        QueryStorageLock lock = cache.getQueryStatusLock(queryId);
        lock.lock();
        try {
            QueryStatus status = cache.getQueryStatus(queryId);
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
     * @param e
     *            the exception
     */
    @Override
    public void updateFailedQueryStatus(String queryId, Exception e) {
        QueryStorageLock lock = cache.getQueryStatusLock(queryId);
        lock.lock();
        try {
            QueryStatus status = cache.getQueryStatus(queryId);
            status.setQueryState(QueryStatus.QUERY_STATE.FAILED);
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
        QueryStorageLock lock = cache.getTaskStatesLock(taskKey.getQueryId());
        lock.lock();
        try {
            TaskStates states = cache.getTaskStates(taskKey.getQueryId());
            if (states.setState(taskKey.getTaskId(), state)) {
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
        return cache.getQueryStatusLock(queryId);
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
        return cache.getTaskStatesLock(queryId);
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
        return cache.getTaskStates(queryId);
    }
    
    /**
     * update the query status
     *
     * @param taskStates
     *            the task states
     */
    @Override
    public void updateTaskStates(TaskStates taskStates) {
        cache.updateTaskStates(taskStates);
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
        QueryTask task = null;
        
        String queryId = checkpoint.getQueryKey().getQueryId();
        
        QueryStorageLock lock = cache.getTaskStatesLock(queryId);
        lock.lock();
        try {
            // Set the initial ready state in the task states
            TaskStates states = cache.getTaskStates(queryId);
            
            int taskId = states.getAndIncrementNextTaskId();
            
            // create a query task in the cache
            task = cache.addQueryTask(taskId, action, checkpoint);
            
            states.setState(taskId, TaskStates.TASK_STATE.READY);
            
            cache.updateTaskStates(states);
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
     * @throws TaskLockException
     *             if the task is already locked
     */
    @Override
    public QueryTask getTask(TaskKey taskKey) {
        return cache.getTask(taskKey);
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
        return cache.updateQueryTask(taskKey, checkpoint);
    }
    
    /**
     * Delete a query task.
     *
     * @param taskKey
     *            The task key
     */
    @Override
    public void deleteTask(TaskKey taskKey) {
        cache.deleteTask(taskKey);
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
        int deleted = cache.deleteQuery(queryId);
        queue.deleteQueue(queryId);
        return (deleted > 0);
    }
    
    /**
     * Clear the cache
     */
    @Override
    public void clear() {
        for (QueryState queries : cache.getQueries()) {
            queue.emptyQueue(queries.getQueryStatus().getQueryKey().getQueryId());
        }
        cache.clear();
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
        for (QueryTask task : cache.getTasks(queryId)) {
            tasks.add(task.getTaskKey());
        }
        return tasks;
    }
    
}
