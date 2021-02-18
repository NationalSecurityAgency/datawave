package datawave.microservice.common.storage;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.Serializable;
import java.util.UUID;

/**
 * A query task is an action to perform for a specified query.
 */
public class QueryTask implements Serializable {
    private static final long serialVersionUID = 579211458890999398L;
    
    public enum QUERY_ACTION implements Serializable {
        DEFINE, CREATE, NEXT, CLOSE, TEST
    }
    
    private final TaskKey taskKey;
    private final QUERY_ACTION action;
    private final QueryCheckpoint queryCheckpoint;
    
    public QueryTask(QUERY_ACTION action, QueryCheckpoint queryCheckpoint) {
        this(UUID.randomUUID(), action, queryCheckpoint);
    }
    
    public QueryTask(UUID taskId, QUERY_ACTION action, QueryCheckpoint queryCheckpoint) {
        this.taskKey = new TaskKey(taskId, queryCheckpoint.getQueryKey());
        this.action = action;
        this.queryCheckpoint = queryCheckpoint;
    }
    
    /**
     * The action to perform
     * 
     * @return the action
     */
    public QUERY_ACTION getAction() {
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
     * Get the task key
     *
     * @return The task key
     */
    public TaskKey getTaskKey() {
        return taskKey;
    }
    
    /**
     * Get a task notification for this task
     * 
     * @return a query task notification
     */
    public QueryTaskNotification getNotification() {
        return new QueryTaskNotification(getTaskKey(), getAction());
    }
    
    @Override
    public String toString() {
        return getTaskKey() + ":" + getAction() + " on " + getQueryCheckpoint().getProperties();
    }
    
    /**
     * Get a somewhat simpler message for debugging purposes
     * 
     * @return A debug string
     */
    public String toDebug() {
        return getTaskKey() + ":" + getAction();
    }
    
    @Override
    public boolean equals(Object o) {
        if (o instanceof QueryTask) {
            QueryTask other = (QueryTask) o;
            return new EqualsBuilder().append(getTaskKey(), other.getTaskKey()).append(getAction(), other.getAction())
                            .append(getQueryCheckpoint(), other.getQueryCheckpoint()).isEquals();
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(getTaskKey()).toHashCode();
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
    public static String toKey(UUID taskId, QueryKey queryKey) {
        return taskId.toString() + ':' + queryKey.toKey();
    }
}
