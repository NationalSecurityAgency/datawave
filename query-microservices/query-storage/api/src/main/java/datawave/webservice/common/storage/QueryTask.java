package datawave.webservice.common.storage;

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
        CREATE, NEXT, CLOSE
    }
    
    private final UUID taskId;
    private final QUERY_ACTION action;
    private final QueryCheckpoint queryCheckpoint;
    
    public QueryTask(QUERY_ACTION action, QueryCheckpoint queryCheckpoint) {
        this(UUID.randomUUID(), action, queryCheckpoint);
    }
    
    public QueryTask(UUID taskId, QUERY_ACTION action, QueryCheckpoint queryCheckpoint) {
        this.taskId = taskId;
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
     * Get the task id
     *
     * @return The task id
     */
    public UUID getTaskId() {
        return taskId;
    }
    
    /**
     * Get a task notification for this task
     * 
     * @return a query task notification
     */
    public QueryTaskNotification getNotification() {
        return new QueryTaskNotification(getQueryCheckpoint().getQueryId(), getQueryCheckpoint().getQueryType(), getTaskId());
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
}
