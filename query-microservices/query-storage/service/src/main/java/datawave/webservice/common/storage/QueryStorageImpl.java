package datawave.webservice.common.storage;

import datawave.webservice.common.storage.config.QueryStorageConfig;
import datawave.webservice.query.Query;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.integration.support.MessageBuilder;
import org.springframework.messaging.MessageChannel;
import org.springframework.stereotype.Service;

import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

@Service
public class QueryStorageImpl implements QueryStorage {
    
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
     * @return The query UUID
     */
    @Override
    public UUID storeQuery(QueryType queryType, Query query) {
        // create the query UUID
        UUID queryUuid = UUID.randomUUID();
        
        // create the initial query checkpoint
        Map<String,Object> props = new HashMap<>();
        props.put(QueryCheckpoint.INITIAL_QUERY_PROPERTY, query);
        QueryCheckpoint checkpoint = new QueryCheckpoint(queryUuid, queryType, props);
        
        // create and store the initial create task with the checkpoint
        createTask(QueryTask.QUERY_ACTION.CREATE, checkpoint);
        
        // return the query id
        return queryUuid;
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
     * @param taskId
     *            The task id to update
     * @param checkpoint
     *            The new query checkpoint
     * @return The updated query task
     */
    @Override
    public QueryTask checkpointTask(UUID taskId, QueryCheckpoint checkpoint) {
        return cache.updateQueryTask(taskId, checkpoint);
    }
    
    /**
     * Get a task for a given task id
     * 
     * @param taskNotification
     *            The task notification
     * @return The query task
     */
    @Override
    public QueryTask getTask(QueryTaskNotification taskNotification) {
        return cache.getTask(taskNotification.getTaskId());
    }
    
    /**
     * Delete a query task
     * 
     * @param taskId
     *            The task id
     * @return true if found and deleted
     */
    @Override
    public boolean deleteTask(UUID taskId) {
        return cache.deleteTask(taskId);
    }
    
    /**
     * Passes task notifications to the messaging infrastructure.
     *
     * @param taskNotification
     *            The task notification to be sent
     */
    private boolean sendMessage(QueryTaskNotification taskNotification) {
        return taskNotificationChannel.send(MessageBuilder.withPayload(taskNotification).setCorrelationId(taskNotification.getTaskId()).build());
    }
    
}
