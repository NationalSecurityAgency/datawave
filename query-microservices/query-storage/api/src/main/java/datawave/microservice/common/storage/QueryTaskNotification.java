package datawave.microservice.common.storage;

import com.fasterxml.jackson.annotation.JsonIgnore;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.Serializable;

/**
 * This is a message for the control queue to denote a pending task to perform.
 */
public class QueryTaskNotification implements Serializable {
    private static final long serialVersionUID = 364194052797912452L;
    
    private TaskKey taskKey;
    
    /**
     * Default constructor for deserialization
     */
    public QueryTaskNotification() {}
    
    public QueryTaskNotification(TaskKey taskKey) {
        this.taskKey = taskKey;
    }
    
    public TaskKey getTaskKey() {
        return taskKey;
    }
    
    @JsonIgnore
    public QueryTask.QUERY_ACTION getAction() {
        return taskKey.getAction();
    }
    
    @Override
    public String toString() {
        return taskKey.toString();
    }
    
    @Override
    public boolean equals(Object o) {
        if (o instanceof QueryTaskNotification) {
            QueryTaskNotification other = (QueryTaskNotification) o;
            return new EqualsBuilder().append(getTaskKey(), other.getTaskKey()).isEquals();
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(getTaskKey()).toHashCode();
    }
    
}
