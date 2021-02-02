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

@CacheConfig(cacheNames = QueryStorageCache.CACHE_NAME)
public class QueryStorageCache {
    private final static Logger log = Logger.getLogger(QueryStorageCache.class);
    
    public static final String CACHE_NAME = "QueryCache";
    
    private final LockableCacheInspector cacheInspector;
    
    public QueryStorageCache(LockableCacheInspector cacheInspector) {
        this.cacheInspector = cacheInspector;
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
    @CachePut(key = "#result.toKey()")
    public QueryTask addQueryTask(QueryTask.QUERY_ACTION action, QueryCheckpoint checkpoint) {
        QueryTask task = new QueryTask(action, checkpoint);
        logTask("Adding task", task);
        return task;
    }
    
    /**
     * Update a stored query task with an updated checkpoint
     * 
     * @param taskId
     *            The task id to update
     * @param checkpoint
     *            The new query checkpoint
     * @return The updated query task
     */
    @CachePut(key = "T(datawave.microservice.common.storage.QueryTask).toKey(#taskId, #checkpoint.queryKey)")
    public QueryTask updateQueryTask(UUID taskId, QueryCheckpoint checkpoint) {
        QueryTask task = getTask(taskId, checkpoint.getQueryKey());
        if (task == null) {
            throw new NullPointerException("Could not find a query task for " + taskId);
        }
        task = new QueryTask(task.getTaskId(), task.getAction(), checkpoint);
        logTask("Updating task", task);
        return task;
    }
    
    /**
     * Delete a task. For efficiency it is recommended to use deleteTask(taskId, queryId) instead.
     *
     * @param taskId
     *            The task to delete
     * @return True if found and deleted
     */
    public boolean deleteTask(UUID taskId) {
        if (log.isDebugEnabled()) {
            log.debug("Deleting task " + taskId);
        }
        return (cacheInspector.evictMatching(CACHE_NAME, QueryTask.class, taskId.toString()) > 0);
    }
    
    /**
     * Delete a task
     * 
     * @param taskId
     *            The taskid
     * @param queryKey
     *            The queryid
     */
    @CacheEvict(key = "T(datawave.microservice.common.storage.QueryTask).toKey(#taskId, #queryKey)")
    public void deleteTask(UUID taskId, QueryKey queryKey) {
        String key = QueryTask.toKey(taskId, queryKey);
        if (log.isDebugEnabled()) {
            log.debug("Deleted task " + key);
        }
    }
    
    /**
     * Delete all tasks for a query
     * 
     * @param queryId
     *            the query id
     * @return the number of tasks deleted
     */
    public int deleteTasks(UUID queryId) {
        int deleted = cacheInspector.evictMatching(CACHE_NAME, QueryTask.class, queryId.toString());
        if (log.isDebugEnabled()) {
            log.debug("Deleted all ( " + deleted + ") tasks for query " + queryId);
        }
        return deleted;
    }
    
    /**
     * Delete all tasks for a query type
     *
     * @param type
     *            the query type
     * @return the number of tasks deleted
     */
    public int deleteTasks(QueryType type) {
        int deleted = cacheInspector.evictMatching(CACHE_NAME, QueryTask.class, type.getType());
        if (log.isDebugEnabled()) {
            log.debug("Deleted all ( " + deleted + ") tasks for query type " + type);
        }
        return deleted;
    }
    
    /**
     * Clear out the cache
     * 
     * @return a clear message
     */
    @CacheEvict(allEntries = true)
    public String clear() {
        log.debug("Clearing all tasks");
        return "Cleared " + CACHE_NAME + " cache";
    }
    
    /**
     * Get a task for a given task id
     *
     * @param taskId
     *            The task id
     * @return The query task
     */
    public QueryTask getTask(UUID taskId) {
        List<? extends QueryTask> tasks = cacheInspector.listMatching(CACHE_NAME, QueryTask.class, taskId.toString());
        if (tasks == null || tasks.isEmpty()) {
            log.debug("Retrieved no tasks for taskId " + taskId);
            return null;
        } else if (tasks.size() == 1) {
            QueryTask task = tasks.get(0);
            logTask("Retrieved 1 task for taskId", task);
            return task;
        } else {
            log.error("Retrieved too many (" + tasks.size() + ") tasks for taskId " + taskId);
            throw new IllegalStateException("Found " + tasks.size() + " tasks matching the specified taskId : " + taskId);
        }
    }
    
    /**
     * Return the task for a given task id and query key
     * 
     * @param taskId
     *            The task id
     * @param queryKey
     *            The query key
     * @return The query task
     */
    public QueryTask getTask(UUID taskId, QueryKey queryKey) {
        QueryTask task = cacheInspector.list(CACHE_NAME, QueryTask.class, QueryTask.toKey(taskId, queryKey));
        logTask("Retrieved", task);
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
        List<QueryTask> tasks = (List<QueryTask>) cacheInspector.listMatching(CACHE_NAME, QueryTask.class, queryId.toString());
        if (tasks == null) {
            tasks = Collections.EMPTY_LIST;
        }
        if (log.isDebugEnabled()) {
            log.debug("Retrieved " + tasks.size() + " tasks for queryId " + queryId);
        }
        return tasks;
    }
    
    /**
     * Get the query tasks for a query type
     *
     * @param type
     *            The query type
     * @return A list of tasks
     */
    public List<QueryTask> getTasks(QueryType type) {
        List<QueryTask> tasks = (List<QueryTask>) cacheInspector.listMatching(CACHE_NAME, QueryTask.class, type.getType());
        if (tasks == null) {
            tasks = Collections.EMPTY_LIST;
        }
        if (log.isDebugEnabled()) {
            log.debug("Retrieved " + tasks.size() + "tasks for query type " + type);
        }
        return tasks;
    }
    
    /**
     * Get all tasks
     *
     * @return A list of tasks
     */
    public List<QueryTask> getTasks() {
        List<QueryTask> tasks = (List<QueryTask>) cacheInspector.listAll(CACHE_NAME, QueryTask.class);
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
        List<QueryState> queries = getQueries(getTasks());
        if (log.isDebugEnabled()) {
            log.debug("Retrieved " + queries.size() + " queries");
        }
        return queries;
    }
    
    /**
     * Get a list of query states from the cache for a specified query type
     * 
     * @param type
     * @return a list of query states
     */
    public List<QueryState> getQueries(QueryType type) {
        List<QueryState> queries = getQueries(getTasks(type));
        if (log.isDebugEnabled()) {
            log.debug("Retrieved " + queries.size() + " queries for type " + type);
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
        List<QueryState> states = getQueries(getTasks(queryId));
        if (states.isEmpty()) {
            if (log.isDebugEnabled()) {
                log.debug("Returning null query state for queryId " + queryId);
            }
            return null;
        } else if (states.size() == 1) {
            QueryState state = states.get(0);
            if (log.isTraceEnabled()) {
                log.trace("Returning query state " + state);
            } else if (log.isDebugEnabled()) {
                log.debug("Returning query state for queryId " + queryId);
            }
            return state;
        } else {
            log.error("Retrieved too many (" + states.size() + ") query states for queryId " + queryId);
            throw new IllegalStateException("Found " + states.size() + " query states matching query id " + queryId);
        }
    }
    
    /**
     * Get the task descriptions for a specified query
     * 
     * @param queryId
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
     * For a list of tasks, return a list of QueryState
     * 
     * @param tasks
     *            A list of query tasks
     * @return A list of QueryState
     */
    private List<QueryState> getQueries(List<QueryTask> tasks) {
        Map<QueryKey,Map<QueryTask.QUERY_ACTION,MutableInt>> queries = new HashMap<>();
        for (QueryTask task : tasks) {
            QueryKey key = task.getQueryCheckpoint().getQueryKey();
            Map<QueryTask.QUERY_ACTION,MutableInt> actionCounts = queries.get(key);
            if (actionCounts == null) {
                actionCounts = new HashMap<>();
                queries.put(key, actionCounts);
            }
            MutableInt count = actionCounts.get(task.getAction());
            if (count == null) {
                count = new MutableInt(0);
                actionCounts.put(task.getAction(), count);
            }
            count.increment();
        }
        List<QueryState> states = new ArrayList<>();
        for (Map.Entry<QueryKey,Map<QueryTask.QUERY_ACTION,MutableInt>> entry : queries.entrySet()) {
            QueryState state = new QueryState(entry.getKey(),
                            entry.getValue().entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getValue())));
            states.add(state);
        }
        return states;
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
            descriptions.add(new TaskDescription(task.getTaskId(), task.getAction(), task.getQueryCheckpoint().getProperties().entrySet().stream()
                            .collect(Collectors.toMap(Map.Entry::getKey, e -> String.valueOf(e.getValue())))));
        }
        return descriptions;
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
            log.debug(msg + ' ' + task.toDebug());
        }
    }
}
