package datawave.microservice.query.storage;

import com.fasterxml.jackson.annotation.JsonIgnore;
import datawave.microservice.query.remote.QueryRequest;
import datawave.services.query.logic.QueryCheckpoint;
import datawave.services.query.logic.QueryKey;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.Serializable;
import java.util.UUID;

/**
 * A query task is an action to perform for a specified query.
 */
public class QueryTask implements Serializable {
    private static final long serialVersionUID = 579211458890999398L;
    
    /**
     * Parameter names used when getting a plan prior to creating a query
     */
    public static final String EXPAND_VALUES = "expand.values";
    public static final String EXPAND_FIELDS = "expand.fields";
    
    private final String taskId;
    private final QueryRequest.Method action;
    private final QueryCheckpoint queryCheckpoint;
    
    public QueryTask(QueryRequest.Method action, QueryCheckpoint queryCheckpoint) {
        this(UUID.randomUUID().toString(), action, queryCheckpoint);
    }
    
    public QueryTask(String taskId, QueryRequest.Method action, QueryCheckpoint queryCheckpoint) {
        this.taskId = taskId;
        this.action = action;
        this.queryCheckpoint = queryCheckpoint;
    }
    
    @JsonIgnore
    public TaskKey getTaskKey() {
        return new TaskKey(taskId, action, queryCheckpoint.getQueryKey());
    }
    
    /**
     * The action to perform
     * 
     * @return the action
     */
    public QueryRequest.Method getAction() {
        return action;
    }
    
    /**
     * Get the query checkpoint on which to perform the next task
     * 
     * @return A query checkpoint
     */
    public QueryCheckpoint getQueryCheckpoint() {
        return queryCheckpoint;
    }
    
    /**
     * Get the task id
     *
     * @return The task id
     */
    public String getTaskId() {
        return taskId;
    }
    
    @Override
    public String toString() {
        return getTaskKey() + " with " + getQueryCheckpoint().getProperties();
    }
    
    /**
     * Get a somewhat simpler message for debugging purposes
     * 
     * @return A debug string
     */
    public String toDebug() {
        return getTaskKey().toString();
    }
    
    @Override
    public boolean equals(Object o) {
        if (o instanceof QueryTask) {
            QueryTask other = (QueryTask) o;
            return new EqualsBuilder().append(getTaskId(), other.getTaskId()).append(getAction(), other.getAction())
                            .append(getQueryCheckpoint(), other.getQueryCheckpoint()).isEquals();
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(getTaskId()).toHashCode();
    }
    
    /**
     * Get the key used to store a query task containing the specified components
     * 
     * @param taskId
     *            The task id
     * @param queryKey
     *            The query key
     * @return a key
     */
    public static String toKey(String taskId, QueryKey queryKey) {
        return queryKey.toKey() + ':' + taskId.toString();
    }
}
