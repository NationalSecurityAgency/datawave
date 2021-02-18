package datawave.microservice.common.storage;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.Serializable;
import java.util.UUID;

/**
 * This is a message for the control queue to denote a pending task to perform.
 */
public class QueryTaskNotification implements Serializable {
    private static final long serialVersionUID = 364194052797912452L;
    
    private TaskKey taskKey;
    private QueryTask.QUERY_ACTION action;

    /**
     * Default constructor for deserialization
     */
    public QueryTaskNotification() {}
    
    public QueryTaskNotification(TaskKey taskKey, QueryTask.QUERY_ACTION action) {
        this.taskKey = taskKey;
        this.action = action;
    }
    
    public TaskKey getTaskKey() {
        return taskKey;
    }
    
    public QueryTask.QUERY_ACTION getAction() {
        return action;
    }
    
    @Override
    public String toString() {
        return taskKey.toString() + ':' + action;
    }
    
    @Override
    public boolean equals(Object o) {
        if (o instanceof QueryTaskNotification) {
            QueryTaskNotification other = (QueryTaskNotification) o;
            return new EqualsBuilder().append(getAction(), other.getAction()).append(getTaskKey(), other.getTaskKey()).isEquals();
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(getAction()).append(getTaskKey()).toHashCode();
    }
    
}
