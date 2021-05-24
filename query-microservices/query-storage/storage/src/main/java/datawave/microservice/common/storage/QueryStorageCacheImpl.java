package datawave.microservice.common.storage;

import datawave.microservice.common.storage.config.QueryStorageProperties;
import datawave.microservice.query.config.QueryProperties;
import datawave.webservice.query.Query;
import org.apache.accumulo.core.security.Authorizations;
import org.springframework.cloud.bus.BusProperties;
import org.springframework.cloud.bus.event.RemoteQueryTaskNotificationEvent;
import org.springframework.context.ApplicationEventPublisher;
import org.springframework.stereotype.Service;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Date;
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
     * Store/cache a new query. This will create a query task containing the query with a DEFINE query action and send out a task notification on a channel
     * using the name of the query logic.
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
    public TaskKey defineQuery(QueryPool queryPool, Query query, Set<Authorizations> calculatedAuthorizations, int count) throws IOException {
        return storeQuery(queryPool, query, calculatedAuthorizations, count, QueryStatus.QUERY_STATE.DEFINED);
    }
    
    /**
     * Store/cache a new query. This will create a query task containing the query with a CREATE query action and send out a task notification on a channel
     * using the name of the query logic.
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
    public TaskKey createQuery(QueryPool queryPool, Query query, Set<Authorizations> calculatedAuthorizations, int count) throws IOException {
        return storeQuery(queryPool, query, calculatedAuthorizations, count, QueryStatus.QUERY_STATE.CREATED);
    }
    
    private TaskKey storeQuery(QueryPool queryPool, Query query, Set<Authorizations> calculatedAuthorizations, int count, QueryStatus.QUERY_STATE queryState)
                    throws IOException {
        UUID queryUuid = query.getId();
        if (queryUuid == null) {
            // create the query UUID
            queryUuid = UUID.randomUUID();
            query.setId(queryUuid);
        }
        
        QueryKey queryKey = new QueryKey(queryPool, queryUuid, query.getQueryLogicName());
        
        // create the initial query checkpoint
        QueryCheckpoint checkpoint = new QueryCheckpoint(queryKey, query);
        
        // store the initial query properties
        QueryStatus queryStatus = new QueryStatus(checkpoint.getQueryKey());
        queryStatus.setQueryState(queryState);
        queryStatus.setQuery(query);
        queryStatus.setCalculatedAuthorizations(calculatedAuthorizations);
        queryStatus.setLastUpdated(new Date());
        cache.updateQueryStatus(queryStatus);
        
        // store the initial tasks states
        TaskStates taskStates = new TaskStates(queryKey, count);
        cache.updateTaskStates(taskStates);
        
        // create the results queue
        queue.ensureQueueCreated(queryKey.getQueryId());
        
        // create and store the initial create task with the checkpoint. This will send out the task notification.
        QueryTask.QUERY_ACTION queryAction = null;
        if (queryState == QueryStatus.QUERY_STATE.DEFINED) {
            queryAction = QueryTask.QUERY_ACTION.DEFINE;
        } else if (queryState == QueryStatus.QUERY_STATE.CREATED) {
            queryAction = QueryTask.QUERY_ACTION.CREATE;
        }
        QueryTask task = createTask(queryAction, checkpoint);
        
        // return the task key
        return task.getTaskKey();
    }
    
    /**
     * Get the current query state. This includes the query properties, stats, and task summary
     * 
     * @param queryId
     *            the query id
     * @return query stats
     */
    public QueryState getQueryState(UUID queryId) {
        return cache.getQuery(queryId);
    }
    
    /**
     * Get the current query properties.
     * 
     * @param queryId
     *            the query id
     * @return the query properties
     */
    public QueryStatus getQueryStatus(UUID queryId) {
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
        queryStatus.setLastUpdated(new Date());
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
    public void updateQueryStatus(UUID queryId, QueryStatus.QUERY_STATE state) {
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
            if (states.setState(taskKey, state)) {
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
    public QueryStorageLock getQueryStatusLock(UUID queryId) {
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
    public QueryStorageLock getTaskStatesLock(UUID queryId) {
        return cache.getTaskStatesLock(queryId);
    }
    
    /**
     * Get a task lock
     *
     * @param task
     *            The task key
     * @return a lock object
     */
    @Override
    public QueryStorageLock getTaskLock(TaskKey task) {
        return cache.getTaskLock(task);
    }
    
    /**
     * Get the current task states.
     *
     * @param queryId
     *            the query id
     * @return the task states
     */
    @Override
    public TaskStates getTaskStates(UUID queryId) {
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
     * Create a new query task. This will create a new query task, store it, and send out a task notification.
     * 
     * @param action
     *            The query action
     * @param checkpoint
     *            The query checkpoint
     * @return The new query task
     */
    @Override
    public QueryTask createTask(QueryTask.QUERY_ACTION action, QueryCheckpoint checkpoint) {
        // create a query task in the cache
        QueryTask task = cache.addQueryTask(action, checkpoint);
        
        // Set the initial ready state in the task states
        TaskStates states = cache.getTaskStates(task.getTaskKey().getQueryId());
        states.setState(task.getTaskKey(), TaskStates.TASK_STATE.READY);
        cache.updateTaskStates(states);
        
        // publish a task notification event
        post(task.getNotification());
        
        // return the task
        return task;
    }
    
    /**
     * Post a task notification
     * 
     * @param taskNotification
     */
    @Override
    public void post(QueryTaskNotification taskNotification) {
        // publish a task notification event
        if (properties.isSendNotifications()) {
            // broadcast the notification to the executor services
            publisher.publishEvent(
                            new RemoteQueryTaskNotificationEvent(this, busProperties.getId(), queryProperties.getExecutorServiceName(), taskNotification));
        }
    }
    
    /**
     * Get a task for a given task key and lock it for processing. This return null if the task no longer exists. This will throw an exception if the task is
     * already locked.
     *
     * @param taskKey
     *            The task key
     * @param waitMs
     *            How long to wait to get a task lock
     * @param leaseMs
     *            How long to hold onto lock
     * @return The query task, null if deleted
     * @throws TaskLockException
     *             if the task is already locked
     */
    @Override
    public QueryTask getTask(TaskKey taskKey, long waitMs, long leaseMs) throws TaskLockException, InterruptedException {
        if (cache.getTaskLock(taskKey).tryLock(waitMs, leaseMs)) {
            return cache.getTask(taskKey);
        } else {
            throw new TaskLockException("Unable to get task lock for " + taskKey);
        }
    }
    
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
    @Override
    public QueryTask checkpointTask(TaskKey taskKey, QueryCheckpoint checkpoint) throws TaskLockException {
        QueryStorageLock taskLock = cache.getTaskLock(taskKey);
        if (taskLock.isLocked()) {
            QueryTask queryTask = cache.updateQueryTask(taskKey, checkpoint);
            taskLock.unlock();
            return queryTask;
        } else {
            throw new TaskLockException("Attempting to checkpoint a task that is not locked: " + taskKey);
        }
    }
    
    /**
     * Delete a query task. This will also release the lock. This will throw an exception if the task is not locked.
     *
     * @param taskKey
     *            The task key
     * @throws TaskLockException
     *             if the task is not locked
     */
    @Override
    public void deleteTask(TaskKey taskKey) throws TaskLockException {
        QueryStorageLock taskLock = cache.getTaskLock(taskKey);
        if (taskLock.isLocked()) {
            cache.deleteTask(taskKey);
            taskLock.unlock();
        } else {
            throw new TaskLockException("Attempting to delete a task that is not locked: " + taskKey);
        }
    }
    
    /**
     * Delete a query
     *
     * @param queryId
     *            the query id
     * @return true if deleted
     */
    @Override
    public boolean deleteQuery(UUID queryId) {
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
            queue.emptyQueue(queries.getQueryId());
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
    public List<TaskKey> getTasks(UUID queryId) {
        List<TaskKey> tasks = new ArrayList<>();
        for (QueryTask task : cache.getTasks(queryId)) {
            tasks.add(task.getTaskKey());
        }
        return tasks;
    }
    
}
