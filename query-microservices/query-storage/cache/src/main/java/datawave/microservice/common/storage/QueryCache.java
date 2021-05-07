package datawave.microservice.common.storage;

import datawave.microservice.cached.LockableCacheInspector;
import org.apache.commons.lang3.mutable.MutableInt;
import org.apache.log4j.Logger;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.CachePut;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.UUID;
import java.util.stream.Collectors;

@CacheConfig(cacheNames = QueryCache.CACHE_NAME, cacheManager = "queryStorageCacheManager")
public class QueryCache {
    private final static Logger log = Logger.getLogger(QueryCache.class);
    
    public static final String CACHE_NAME = "QueryCache";

    public static final String PROPS = "PROPS:";
    public static final String STATS = "STATS:";
    public static final String TASK = "TASK:";

    private final LockableCacheInspector cacheInspector;
    
    public QueryCache(LockableCacheInspector cacheInspector) {
        this.cacheInspector = cacheInspector;
    }

    /**
     * Store the query properties for a query.
     * @param queryProperties
     *     the query properties
     * @return the stored query properties
     */
    @CachePut(key = "T(datawave.microservice.common.storage.QueryCache).PROPS + #queryProperties.getQueryKey().toUUIDKey()", cacheManager = "queryStorageCacheManager")
    public QueryProperties updateQueryProperties(QueryProperties queryProperties) {
        logProperties("Storing", queryProperties, queryProperties.getQueryKey().getQueryId());
        return queryProperties;
    }

    /**
     * Delete the query properties for a query
     * @param queryId
     *      The query id
     */
    @CacheEvict(key = "T(datawave.microservice.common.storage.QueryCache).PROPS + T(datawave.microservice.common.storage.QueryKey).toUUIDKey(#queryId)", cacheManager = "queryStorageCacheManager")
    public void deleteQueryProperties(UUID queryId) {
        if (log.isDebugEnabled()) {
            log.debug("Deleted query properties for " + queryId);
        }
    }

    /**
     * Return the query properties for a query
     *
     * @param queryId
     *            The query id
     * @return The query properties
     */
    public QueryProperties getQueryProperties(UUID queryId) {
        QueryProperties props = cacheInspector.list(CACHE_NAME, QueryProperties.class, PROPS + QueryKey.toUUIDKey(queryId));
        logProperties("Retrieved", props, queryId);
        return props;
    }

    /**
     * Get all of the existing query properties
     * @return A list of query properties
     */
    public List<QueryProperties> getQueryProperties() {
        return (List<QueryProperties>)cacheInspector.listMatching(CACHE_NAME, QueryProperties.class, PROPS);
    }

    /**
     * Store the query stats for a query.
     * @param queryId
     *     the query id
     * @param queryStats
     *     the query stats
     * @return the stored query stats
     */
    @CachePut(key = "T(datawave.microservice.common.storage.QueryCache).STATS + QueryKey.getUUIDKey(#queryId)", cacheManager = "queryStorageCacheManager")
    public QueryStats updateQueryStats(UUID queryId, QueryStats queryStats) {
        logStats("Storing", queryStats, queryId);
        return queryStats;
    }

    /**
     * Delete the query stats for a query
     * @param queryId
     *      The query id
     */
    @CacheEvict(key = "T(datawave.microservice.common.storage.QueryCache).STATS + QueryKey.getUUIDKey(#queryId)", cacheManager = "queryStorageCacheManager")
    public void deleteQueryStats(UUID queryId) {
        if (log.isDebugEnabled()) {
            log.debug("Deleted query stats for " + queryId);
        }
    }

    /**
     * Return the query stats for a query
     *
     * @param queryId
     *            The query id
     * @return The query stats
     */
    public QueryStats getQueryStats(UUID queryId) {
        QueryStats queryStats = cacheInspector.list(CACHE_NAME, QueryStats.class, STATS + QueryKey.toUUIDKey(queryId));
        logStats("Retrieved", queryStats, queryId);
        return queryStats;
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
    @CachePut(key = "T(datawave.microservice.common.storage.QueryCache).TASK + #result.getTaskKey().toKey() ", cacheManager = "queryStorageCacheManager")
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
    @CachePut(key = "T(datawave.microservice.common.storage.QueryCache).TASK + #result.getTaskKey().toKey()", cacheManager = "queryStorageCacheManager")
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
    @CacheEvict(key = "T(datawave.microservice.common.storage.QueryCache).TASK + #taskKey.toKey()", cacheManager = "queryStorageCacheManager")
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
    public Map<QueryTask.QUERY_ACTION, Integer> getTaskCounts(QueryKey queryKey) {
        Map<QueryTask.QUERY_ACTION, MutableInt> taskCounts = new HashMap<>();
        for (QueryTask.QUERY_ACTION action : QueryTask.QUERY_ACTION.values()) {
            taskCounts.put(action, new MutableInt());
        }
        List<QueryTask> tasks = getTasks(queryKey);
        for (QueryTask task : tasks) {
            taskCounts.get(task.getAction()).increment();
        }
        return taskCounts.entrySet().stream()
                .collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getValue()));
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
        List<QueryProperties> properties = (List<QueryProperties>) cacheInspector.listMatching(CACHE_NAME, QueryProperties.class, PROPS);
        for (QueryProperties query : properties) {
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
        QueryProperties props = getQueryProperties(queryId);
        return getQuery(props);
    }
    
    /**
     * Get a query state
     * 
     * @param properties
     *            A query properties
     * @return A query stat
     */
    private QueryState getQuery(QueryProperties properties) {
        return new QueryState(properties.getQueryKey(),
                properties,
                getQueryStats(properties.getQueryKey().getQueryId()),
                getTaskCounts(properties.getQueryKey()));
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
    @CacheEvict(allEntries = true, cacheManager = "queryStorageCacheManager")
    public String clear() {
        log.debug("Clearing all tasks");
        return "Cleared " + CACHE_NAME + " cache";
    }

    /**
     * A convience method for logging a task
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
     * A convience method for logging a task
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
     * A convience method for logging query properties
     * @param props
     *     the query properties
     * @param key
     *     the query id
     */
    private void logProperties(String msg, QueryProperties props, UUID key) {
        if (log.isTraceEnabled()) {
            log.trace(msg + ' ' + (props == null ? "null props for " + key : props.toString()));
        } else if (log.isDebugEnabled()) {
            log.debug(msg + ' ' + (props == null ? "null props for " + key : "props for " + key));
        }
    }

    /**
     * A convience method for logging query stats
     * @param stats
     *     the query stats
     * @param key
     *     the query id
     */
    private void logStats(String msg, QueryStats stats, UUID key) {
        if (log.isTraceEnabled()) {
            log.trace(msg + ' ' + (stats == null ? "null stats for " + key : stats.toString()));
        } else if (log.isDebugEnabled()) {
            log.debug(msg + ' ' + (stats == null ? "null stats for " + key : "stats for " + key));
        }
    }


}
