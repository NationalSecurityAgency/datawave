package datawave.microservice.common.storage;

import datawave.microservice.common.storage.config.QueryStorageConfig;
import datawave.webservice.query.Query;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.MessageChannel;
import org.springframework.stereotype.Service;

import java.util.List;
import java.util.UUID;

@Service
public class QueryStorageServiceImpl implements QueryStorageService {
    
    @Autowired
    private QueryStorageCache cache;
    
    @Autowired
    @Qualifier(QueryStorageConfig.TaskNotificationSourceBinding.NAME)
    private MessageChannel taskNotificationChannel;
    
    /**
     * Store/cache a new query. This will create a query task containing the query with a CREATE query action and send out a task notification.
     * 
     * @param queryType
     *            The query type
     * @param query
     *            The query parameters
     * @return The task key
     */
    @Override
    public TaskKey storeQuery(QueryType queryType, Query query) {
        UUID queryUuid = query.getId();
        if (queryUuid == null) {
            // create the query UUID
            queryUuid = UUID.randomUUID();
            query.setId(queryUuid);
        }
        
        // create the initial query checkpoint
        QueryCheckpoint checkpoint = new QueryCheckpoint(queryUuid, queryType, query);
        
        // create and store the initial create task with the checkpoint
        QueryTask task = createTask(QueryTask.QUERY_ACTION.CREATE, checkpoint);
        
        // return the task key
        return task.getTaskKey();
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
        
        // send a task notification
        sendMessage(task.getNotification());
        
        // return the task
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
    @Override
    public QueryTask checkpointTask(TaskKey taskKey, QueryCheckpoint checkpoint) {
        return cache.updateQueryTask(taskKey, checkpoint);
    }
    
    /**
     * Get a task for a given task key
     * 
     * @param taskKey
     *            The task key
     * @return The query task
     */
    @Override
    public QueryTask getTask(TaskKey taskKey) {
        return cache.getTask(taskKey);
    }
    
    /**
     * Delete a query task
     * 
     * @param taskKey
     *            The task key
     */
    @Override
    public void deleteTask(TaskKey taskKey) {
        cache.deleteTask(taskKey);
    }
    
    /**
     * Get the tasks for a query
     *
     * @param queryId
     *            The query id
     * @return A list of tasks
     */
    @Override
    public List<QueryTask> getTasks(UUID queryId) {
        return cache.getTasks(queryId);
    }
    
    /**
     * Get the tasks for a query
     *
     * @param type
     *            The query type
     * @return A list of tasks
     */
    @Override
    public List<QueryTask> getTasks(QueryType type) {
        return cache.getTasks(type);
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
        return (cache.deleteTasks(queryId) > 0);
    }
    
    /**
     * Delete all queries for a query type
     *
     * @param type
     *            The query type
     * @return true if anything deleted
     */
    @Override
    public boolean deleteQueryType(QueryType type) {
        return (cache.deleteTasks(type) > 0);
    }
    
    /**
     * Clear the cache
     */
    @Override
    public void clear() {
        cache.clear();
    }
    
    /**
     * Passes task notifications to the messaging infrastructure.
     *
     * @param taskNotification
     *            The task notification to be sent
     */
    private boolean sendMessage(QueryTaskNotification taskNotification) {
        return taskNotificationChannel.send(MessageBuilder.withPayload(taskNotification).setCorrelationId(taskNotification.getTaskKey().toKey()).build());
    }
    
}
