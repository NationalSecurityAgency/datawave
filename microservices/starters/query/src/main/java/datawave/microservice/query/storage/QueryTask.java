package datawave.microservice.query.storage;

import java.io.Serializable;
import java.util.Date;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import com.fasterxml.jackson.annotation.JsonIgnore;

import datawave.core.query.logic.QueryCheckpoint;
import datawave.core.query.logic.QueryKey;
import datawave.microservice.query.remote.QueryRequest;

/**
 * A query task is an action to perform for a specified query.
 */
public class QueryTask implements Serializable {
    private static final long serialVersionUID = 579211458890999398L;
    
    private final int taskId;
    private final QueryRequest.Method action;
    private final QueryCheckpoint queryCheckpoint;
    // datetime of last service interaction
    private long lastUpdatedMillis = System.currentTimeMillis();
    
    public QueryTask(int taskId, QueryRequest.Method action, QueryCheckpoint queryCheckpoint) {
        this.taskId = taskId;
        this.action = action;
        this.queryCheckpoint = queryCheckpoint;
    }
    
    @JsonIgnore
    public TaskKey getTaskKey() {
        return new TaskKey(taskId, queryCheckpoint.getQueryKey());
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
     * Get the last updated time for this query task
     * 
     * @return the last updated time
     */
    public long getLastUpdatedMillis() {
        return lastUpdatedMillis;
    }
    
    /**
     * Update the last updated time for this query task
     * 
     * @param lastUpdatedMillis
     *            The last updated date/time
     */
    public void setLastUpdatedMillis(long lastUpdatedMillis) {
        this.lastUpdatedMillis = lastUpdatedMillis;
    }
    
    /**
     * Get the task id
     *
     * @return The task id
     */
    public int getTaskId() {
        return taskId;
    }
    
    @Override
    public String toString() {
        return getTaskKey() + " with " + getQueryCheckpoint().getQueries() + "; lastUpdated : " + new Date(lastUpdatedMillis);
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
