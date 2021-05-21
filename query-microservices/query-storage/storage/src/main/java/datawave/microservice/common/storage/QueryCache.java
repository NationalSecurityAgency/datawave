package datawave.microservice.common.storage;

import datawave.microservice.cached.LockableCacheInspector;
import org.apache.commons.lang3.mutable.MutableInt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.CachePut;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

@CacheConfig(cacheNames = QueryCache.CACHE_NAME)
public class QueryCache {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    public static final String CACHE_NAME = "QueryCache";
    
    public static final String QUERY = "QUERY:";
    public static final String TASKS = "TASKS:";
    public static final String TASK = "TASK:";
    
    private final LockableCacheInspector cacheInspector;
    
    public QueryCache(LockableCacheInspector cacheInspector) {
        this.cacheInspector = cacheInspector;
    }
    
    /**
     * Store the query status for a query.
     * 
     * @param queryStatus
     *            the query status
     * @return the stored query status
     */
    @CachePut(key = "T(datawave.microservice.common.storage.QueryCache).QUERY + #queryStatus.getQueryKey().toUUIDKey()")
    public QueryStatus updateQueryStatus(QueryStatus queryStatus) {
        logStatus("Storing", queryStatus, queryStatus.getQueryKey().getQueryId());
        return queryStatus;
    }
    
    /**
     * Delete the query status for a query
     * 
     * @param queryId
     *            The query id
     */
    @CacheEvict(key = "T(datawave.microservice.common.storage.QueryCache).QUERY + T(datawave.microservice.common.storage.QueryKey).toUUIDKey(#queryId)")
    public void deleteQueryStatus(UUID queryId) {
        String key = QUERY + QueryKey.toUUIDKey(queryId);
        if (log.isDebugEnabled()) {
            log.debug("Deleted query status for " + queryId);
        }
    }
    
    /**
     * Return the query status for a query
     *
     * @param queryId
     *            The query id
     * @return The query status
     */
    public QueryStatus getQueryStatus(UUID queryId) {
        QueryStatus props = cacheInspector.list(CACHE_NAME, QueryStatus.class, QUERY + QueryKey.toUUIDKey(queryId));
        logStatus("Retrieved", props, queryId);
        return props;
    }
    
    /**
     * Get all of the existing query status
     * 
     * @return A list of query status
     */
    public List<QueryStatus> getQueryStatus() {
        return (List<QueryStatus>) cacheInspector.listMatching(CACHE_NAME, QueryStatus.class, QUERY);
    }
    
    /**
     * Store the task states for a query.
     * 
     * @param taskStates
     *            the task states
     * @return the stored task states
     */
    @CachePut(key = "T(datawave.microservice.common.storage.QueryCache).TASKS + #taskStates.getQueryKey().toUUIDKey()")
    public TaskStates updateTaskStates(TaskStates taskStates) {
        logStatus("Storing", taskStates, taskStates.getQueryKey().getQueryId());
        return taskStates;
    }
    
    /**
     * Delete the task states for a query
     * 
     * @param queryId
     *            The query id
     */
    @CacheEvict(key = "T(datawave.microservice.common.storage.QueryCache).TASKS + T(datawave.microservice.common.storage.QueryKey).toUUIDKey(#queryId)")
    public void deleteTaskStates(UUID queryId) {
        if (log.isDebugEnabled()) {
            log.debug("Deleted task statuses for " + queryId);
        }
    }
    
    /**
     * Return the task states for a query
     *
     * @param queryId
     *            The query id
     * @return The task states
     */
    public TaskStates getTaskStates(UUID queryId) {
        TaskStates states = cacheInspector.list(CACHE_NAME, TaskStates.class, TASKS + QueryKey.toUUIDKey(queryId));
        logStatus("Retrieved", states, queryId);
        return states;
    }
    
    /**
     * This will create and store a new query task.
     * 
     * @param action
     *            The query action
     * @param checkpoint
     *            The query checkpoint
     * @return The new query task
     */
    @CachePut(key = "T(datawave.microservice.common.storage.QueryCache).TASK + #result.getTaskKey().toKey() ")
    public QueryTask addQueryTask(QueryTask.QUERY_ACTION action, QueryCheckpoint checkpoint) {
        QueryTask task = new QueryTask(action, checkpoint);
        logTask("Adding task", task);
        return task;
    }
    
    /**
     * Update a stored query task with an updated checkpoint
     * 
     * @param taskKey
     *            The task key to update
     * @param checkpoint
     *            The new query checkpoint
     * @return The updated query task
     */
    @CachePut(key = "T(datawave.microservice.common.storage.QueryCache).TASK + #result.getTaskKey().toKey()")
    public QueryTask updateQueryTask(TaskKey taskKey, QueryCheckpoint checkpoint) {
        if (!checkpoint.getQueryKey().equals(taskKey)) {
            throw new IllegalArgumentException("Checkpoint query key " + checkpoint.getQueryKey() + " does not match " + taskKey);
        }
        QueryTask task = getTask(taskKey);
        if (task == null) {
            throw new NullPointerException("Could not find a query task for " + taskKey);
        }
        task = new QueryTask(taskKey.getTaskId(), task.getAction(), checkpoint);
        logTask("Updating task", task);
        return task;
    }
    
    /**
     * Delete a task
     * 
     * @param taskKey
     *            The task key
     */
    @CacheEvict(key = "T(datawave.microservice.common.storage.QueryCache).TASK + #taskKey.toKey()")
    public void deleteTask(TaskKey taskKey) {
        if (log.isDebugEnabled()) {
            log.debug("Deleted task " + taskKey);
        }
    }
    
    /**
     * Return the task for a given task id and query key
     * 
     * @param taskKey
     *            The task key
     * @return The query task
     */
    public QueryTask getTask(TaskKey taskKey) {
        QueryTask task = cacheInspector.list(CACHE_NAME, QueryTask.class, TASK + taskKey.toKey());
        logTask("Retrieved", task, taskKey);
        return task;
    }
    
    /**
     * Get the tasks for a query
     *
     * @param queryId
     *            The query id
     * @return A list of tasks
     */
    public List<QueryTask> getTasks(UUID queryId) {
        List<QueryTask> tasks = (List<QueryTask>) cacheInspector.listMatching(CACHE_NAME, QueryTask.class, TASK + QueryKey.toUUIDKey(queryId));
        if (tasks == null) {
            tasks = Collections.EMPTY_LIST;
        }
        if (log.isDebugEnabled()) {
            log.debug("Retrieved " + tasks.size() + " tasks for queryId " + queryId);
        }
        return tasks;
    }
    
    /**
     * Get the tasks for a query
     *
     * @param queryKey
     *            The query key
     * @return A list of tasks
     */
    public List<QueryTask> getTasks(QueryKey queryKey) {
        List<QueryTask> tasks = (List<QueryTask>) cacheInspector.listMatching(CACHE_NAME, QueryTask.class, TASK + queryKey.toKey());
        if (tasks == null) {
            tasks = Collections.EMPTY_LIST;
        }
        if (log.isDebugEnabled()) {
            log.debug("Retrieved " + tasks.size() + " tasks for queryId " + queryKey);
        }
        return tasks;
    }
    
    /**
     * Get the tasks counts for a query
     *
     * @param queryKey
     *            The query key
     * @return A list of tasks
     */
    public Map<QueryTask.QUERY_ACTION,Integer> getTaskCounts(QueryKey queryKey) {
        Map<QueryTask.QUERY_ACTION,MutableInt> taskCounts = new HashMap<>();
        for (QueryTask.QUERY_ACTION action : QueryTask.QUERY_ACTION.values()) {
            taskCounts.put(action, new MutableInt());
        }
        List<QueryTask> tasks = getTasks(queryKey);
        for (QueryTask task : tasks) {
            taskCounts.get(task.getAction()).increment();
        }
        return taskCounts.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getValue()));
    }
    
    /**
     * Get the query tasks for a query pool
     *
     * @param queryPool
     *            The query pool
     * @return A list of tasks
     */
    public List<QueryTask> getTasks(QueryPool queryPool) {
        List<QueryTask> tasks = (List<QueryTask>) cacheInspector.listMatching(CACHE_NAME, QueryTask.class, QueryKey.POOL_PREFIX + queryPool.getName());
        if (tasks == null) {
            tasks = Collections.EMPTY_LIST;
        }
        if (log.isDebugEnabled()) {
            log.debug("Retrieved " + tasks.size() + "tasks for query pool " + queryPool);
        }
        return tasks;
    }
    
    /**
     * Get all tasks
     *
     * @return A list of tasks
     */
    public List<QueryTask> getTasks() {
        List<QueryTask> tasks = (List<QueryTask>) cacheInspector.listMatching(CACHE_NAME, QueryTask.class, TASK);
        if (tasks == null) {
            tasks = Collections.EMPTY_LIST;
        }
        if (log.isDebugEnabled()) {
            log.debug("Retrieved " + tasks.size() + " tasks");
        }
        return tasks;
    }
    
    /**
     * Get a list of query states from the cache
     * 
     * @return A list of query states
     */
    public List<QueryState> getQueries() {
        List<QueryState> queries = new ArrayList<>();
        List<QueryStatus> status = (List<QueryStatus>) cacheInspector.listMatching(CACHE_NAME, QueryStatus.class, QUERY);
        for (QueryStatus query : status) {
            queries.add(getQuery(query));
        }
        if (log.isDebugEnabled()) {
            log.debug("Retrieved " + queries.size() + " queries");
        }
        return queries;
    }
    
    /**
     * Get the query state for a specified query
     * 
     * @param queryId
     *            A query id
     * @return the query state
     */
    public QueryState getQuery(UUID queryId) {
        QueryStatus props = getQueryStatus(queryId);
        return getQuery(props);
    }
    
    /**
     * Get a query state
     * 
     * @param status
     *            A query status
     * @return A query stat
     */
    private QueryState getQuery(QueryStatus status) {
        return new QueryState(status.getQueryKey(), status, getTaskStates(status.getQueryKey().getQueryId()));
    }
    
    /**
     * Get the task descriptions for a specified query
     *
     * @param queryId
     *            The query id
     * @return a list of task descriptions
     */
    public List<TaskDescription> getTaskDescriptions(UUID queryId) {
        List<TaskDescription> tasks = getTaskDescriptions(getTasks(queryId));
        if (log.isDebugEnabled()) {
            log.debug("Retrieved " + tasks.size() + " task descriptions for queryId " + queryId);
        }
        return tasks;
    }
    
    /**
     * For a list of tasks, return a list of task descriptions
     * 
     * @param tasks
     *            A list of query tasks
     * @return A list of TaskDescription
     */
    private List<TaskDescription> getTaskDescriptions(List<QueryTask> tasks) {
        List<TaskDescription> descriptions = new ArrayList<>();
        for (QueryTask task : tasks) {
            descriptions.add(new TaskDescription(task.getTaskKey(), task.getAction(), task.getQueryCheckpoint().getProperties().entrySet().stream()
                            .collect(Collectors.toMap(Map.Entry::getKey, e -> String.valueOf(e.getValue())))));
        }
        return descriptions;
    }
    
    /**
     * Delete everything for a query
     *
     * @param queryId
     *            the query id
     * @return the number of items deleted
     */
    public int deleteQuery(UUID queryId) {
        int deleted = cacheInspector.evictMatching(CACHE_NAME, QueryTask.class, QueryKey.toUUIDKey(queryId));
        if (log.isDebugEnabled()) {
            log.debug("Deleted all ( " + deleted + ") tasks for query " + queryId);
        }
        return deleted;
    }
    
    /**
     * Clear out the cache
     *
     * @return a clear message
     */
    @CacheEvict(allEntries = true, beforeInvocation = true)
    public String clear() {
        log.debug("Clearing all queries and tasks");
        
        // make sure we deleted everything
        cacheInspector.evictMatching(CACHE_NAME, QueryStatus.class, QUERY);
        cacheInspector.evictMatching(CACHE_NAME, TaskStates.class, TASKS);
        cacheInspector.evictMatching(CACHE_NAME, QueryTask.class, TASK);
        
        return "Cleared " + CACHE_NAME + " cache";
    }
    
    /**
     * A convenience method for logging a task
     * 
     * @param msg
     *            The message
     * @param task
     *            The task
     */
    private void logTask(String msg, QueryTask task) {
        if (log.isTraceEnabled()) {
            log.trace(msg + ' ' + (task == null ? "null task" : task.toString()));
        } else if (log.isDebugEnabled()) {
            log.debug(msg + ' ' + (task == null ? "null task" : task.toDebug()));
        }
    }
    
    /**
     * A convenience method for logging a task
     *
     * @param msg
     *            The message
     * @param task
     *            The task
     * @param key
     *            The task key
     */
    private void logTask(String msg, QueryTask task, TaskKey key) {
        if (log.isTraceEnabled()) {
            log.trace(msg + ' ' + (task == null ? "null task for " + key : task.toString()));
        } else if (log.isDebugEnabled()) {
            log.debug(msg + ' ' + (task == null ? "null task for " + key : task.toDebug()));
        }
    }
    
    /**
     * A convience method for logging query status
     *
     * @param msg
     *            A message to prepend
     * @param status
     *            the query status
     * @param key
     *            the query id
     */
    private void logStatus(String msg, QueryStatus status, UUID key) {
        if (log.isTraceEnabled()) {
            log.trace(msg + ' ' + (status == null ? "null query for " + key : status.toString()));
        } else if (log.isDebugEnabled()) {
            log.debug(msg + ' ' + (status == null ? "null query for " + key : "query for " + key));
        }
    }
    
    /**
     * A convenience method for logging task statuses
     *
     * @param msg
     *            A message to prepend
     * @param status
     *            the task statueses
     * @param key
     *            the query id
     */
    private void logStatus(String msg, TaskStates status, UUID key) {
        if (log.isTraceEnabled()) {
            log.trace(msg + ' ' + (status == null ? "null tasks for " + key : status.toString()));
        } else if (log.isDebugEnabled()) {
            log.debug(msg + ' ' + (status == null ? "null tasks for " + key : "tasks for " + key));
        }
    }
    
    /**
     * Get a query status lock for a given query id.
     * 
     * @param queryId
     *            the query id
     * @return a query status lock
     */
    public QueryStorageLock getQueryStatusLock(UUID queryId) {
        return new QueryStatusLock(queryId);
    }
    
    /**
     * Get a query task states lock for a given query id
     * 
     * @param queryId
     *            the query id
     * @return a task states lock
     */
    public QueryStorageLock getTaskStatesLock(UUID queryId) {
        return new TaskStatesLock(queryId);
    }
    
    /**
     * Get a task lock for a given task key
     * 
     * @param task
     *            the task key
     * @return a task lock
     */
    public QueryStorageLock getTaskLock(TaskKey task) {
        return new TaskLock(task);
    }
    
    /**
     * A lock object for a query status
     */
    public class QueryStorageLockImpl implements QueryStorageLock {
        private final String storageKey;
        
        public QueryStorageLockImpl(String storageKey) {
            this.storageKey = storageKey;
        }
        
        /**
         * Acquires the lock.
         */
        @Override
        public void lock() {
            cacheInspector.lock(CACHE_NAME, storageKey);
        }
        
        /**
         * Acquires the lock for the specified lease time.
         *
         * @param leaseTimeMillis
         *            The lease time in millis
         */
        @Override
        public void lock(long leaseTimeMillis) {
            cacheInspector.lock(CACHE_NAME, storageKey, leaseTimeMillis, TimeUnit.MILLISECONDS);
        }
        
        /**
         * Acquires the lock for the specified query status.
         *
         * @return true if the lock was acquired, false if the waiting time elapsed before the lock was acquired
         */
        @Override
        public boolean tryLock() {
            return cacheInspector.tryLock(CACHE_NAME, storageKey);
        }
        
        /**
         * Determine if the lock is already acquired
         *
         * @return true if the lock is already acquired, false otherwise
         */
        @Override
        public boolean isLocked() {
            return cacheInspector.isLocked(CACHE_NAME, storageKey);
        }
        
        /**
         * Acquires the lock within a specified amount of time.
         *
         * @param waitTimeMillis
         *            The wait time in millis
         * @return true if the lock was acquired, false if the waiting time elapsed before the lock was acquired
         */
        @Override
        public boolean tryLock(long waitTimeMillis) throws InterruptedException {
            return cacheInspector.tryLock(CACHE_NAME, storageKey, waitTimeMillis, TimeUnit.MILLISECONDS);
        }
        
        /**
         * Acquires the lock for the specified lease time.
         *
         * @param waitTimeMillis
         *            The wait time in millis
         * @param leaseTimeMillis
         *            Time to wait before automatically releasing the lock
         * @return true if the lock was acquired, false if the waiting time elapsed before the lock was acquired
         */
        @Override
        public boolean tryLock(long waitTimeMillis, long leaseTimeMillis) throws InterruptedException {
            return cacheInspector.tryLock(CACHE_NAME, storageKey, waitTimeMillis, TimeUnit.MILLISECONDS, leaseTimeMillis, TimeUnit.MILLISECONDS);
        }
        
        /**
         * Releases the lock for the specified query status
         */
        @Override
        public void unlock() {
            cacheInspector.unlock(CACHE_NAME, storageKey);
        }
        
        /**
         * Releases the lock for the specified query status regardless of the lock owner. It always successfully unlocks the key, never blocks, and returns
         * immediately.
         */
        @Override
        public void forceUnlock() {
            cacheInspector.forceUnlock(CACHE_NAME, storageKey);
        }
    }
    
    /**
     * A lock object for a query status
     */
    public class QueryStatusLock extends QueryStorageLockImpl {
        public QueryStatusLock(UUID queryId) {
            super(QUERY + QueryKey.toUUIDKey(queryId));
        }
    }
    
    /**
     * A lock object for the task states
     */
    public class TaskStatesLock extends QueryStorageLockImpl {
        public TaskStatesLock(UUID queryId) {
            super(TASKS + QueryKey.toUUIDKey(queryId));
        }
    }
    
    /**
     * A lock object for a task
     */
    public class TaskLock extends QueryStorageLockImpl {
        public TaskLock(TaskKey task) {
            super(TASK + task.toKey());
        }
    }
}
