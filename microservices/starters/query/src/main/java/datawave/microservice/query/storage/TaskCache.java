package datawave.microservice.query.storage;

import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.stream.Collectors;

import org.apache.commons.lang3.mutable.MutableInt;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.cache.annotation.CacheConfig;
import org.springframework.cache.annotation.CacheEvict;
import org.springframework.cache.annotation.CachePut;

import datawave.core.query.logic.QueryCheckpoint;
import datawave.core.query.logic.QueryKey;
import datawave.microservice.cached.LockableCacheInspector;
import datawave.microservice.query.remote.QueryRequest;

@CacheConfig(cacheNames = TaskCache.CACHE_NAME)
public class TaskCache {
    private final Logger log = LoggerFactory.getLogger(this.getClass());
    
    public static final String CACHE_NAME = "TaskCache";
    
    private final LockableCacheInspector cacheInspector;
    
    public TaskCache(LockableCacheInspector cacheInspector) {
        this.cacheInspector = cacheInspector;
    }
    
    /**
     * This will create and store a new query task.
     *
     * @param taskId
     *            The task id
     * @param action
     *            The query action
     * @param checkpoint
     *            The query checkpoint
     * @return The new query task
     */
    @CachePut(key = "#result.getTaskKey().toKey() ")
    public QueryTask addQueryTask(int taskId, QueryRequest.Method action, QueryCheckpoint checkpoint) {
        QueryTask task = new QueryTask(taskId, action, checkpoint);
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
    @CachePut(key = "#result.getTaskKey().toKey()")
    public QueryTask updateQueryTask(TaskKey taskKey, QueryCheckpoint checkpoint) {
        if (!checkpoint.getQueryKey().equals(taskKey.getQueryKey())) {
            throw new IllegalArgumentException("Checkpoint query key " + checkpoint.getQueryKey() + " does not match " + taskKey.getQueryKey());
        }
        QueryTask task = getTask(taskKey);
        if (task == null) {
            throw new IllegalStateException("Could not find a query task for " + taskKey);
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
    @CacheEvict(key = "#taskKey.toKey()")
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
        QueryTask task = cacheInspector.list(CACHE_NAME, QueryTask.class, taskKey.toKey());
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
    public List<QueryTask> getTasks(String queryId) {
        List<QueryTask> tasks = (List<QueryTask>) cacheInspector.listMatching(CACHE_NAME, QueryTask.class, QueryKey.toUUIDKey(queryId));
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
        List<QueryTask> tasks = (List<QueryTask>) cacheInspector.listMatching(CACHE_NAME, QueryTask.class, queryKey.toKey());
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
    public Map<QueryRequest.Method,Integer> getTaskCounts(QueryKey queryKey) {
        Map<QueryRequest.Method,MutableInt> taskCounts = new HashMap<>();
        for (QueryRequest.Method action : QueryRequest.Method.values()) {
            taskCounts.put(action, new MutableInt());
        }
        List<QueryTask> tasks = getTasks(queryKey);
        for (QueryTask task : tasks) {
            taskCounts.get(task.getAction()).increment();
        }
        return taskCounts.entrySet().stream().collect(Collectors.toMap(Map.Entry::getKey, e -> e.getValue().getValue()));
    }
    
    /**
     * Get all tasks
     *
     * @return A list of tasks
     */
    public List<QueryTask> getTasks() {
        List<? extends Object> tasks = cacheInspector.listAll(CACHE_NAME, Object.class);
        if (tasks == null) {
            return Collections.emptyList();
        }
        return tasks.stream().filter(o -> o instanceof QueryTask).map(QueryTask.class::cast).collect(Collectors.toList());
    }
    
    /**
     * Get the task descriptions for a specified query
     *
     * @param queryId
     *            The query id
     * @return a list of task descriptions
     */
    public List<TaskDescription> getTaskDescriptions(String queryId) {
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
            descriptions.add(new TaskDescription(task.getTaskKey(), task.getQueryCheckpoint().getQueries()));
        }
        return descriptions;
    }
    
    /**
     * Delete all tasks for a query
     *
     * @param queryId
     *            the query id
     * @return the number of items deleted
     */
    public int deleteTasks(String queryId) {
        int deleted = cacheInspector.evictMatching(CACHE_NAME, QueryTask.class, QueryKey.toUUIDKey(queryId));
        if (log.isDebugEnabled()) {
            log.debug("Deleted all (" + deleted + ") tasks for query " + queryId);
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
    
}
