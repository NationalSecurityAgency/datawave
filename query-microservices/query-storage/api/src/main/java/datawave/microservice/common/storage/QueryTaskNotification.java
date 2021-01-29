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
    
    private final UUID taskId;
    private final QueryKey queryKey;
    
    public QueryTaskNotification(UUID taskId, UUID queryId, QueryType queryType) {
        this(taskId, new QueryKey(queryType, queryId));
    }
    
    public QueryTaskNotification(UUID taskId, QueryKey queryKey) {
        this.taskId = taskId;
        this.queryKey = queryKey;
    }
    
    public QueryKey getQueryKey() {
        return queryKey;
    }
    
    public UUID getTaskId() {
        return taskId;
    }
    
    @Override
    public String toString() {
        return taskId.toString() + ":" + queryKey.toString();
    }
    
    @Override
    public boolean equals(Object o) {
        if (o instanceof QueryTaskNotification) {
            QueryTaskNotification other = (QueryTaskNotification) o;
            return new EqualsBuilder().append(getQueryKey(), other.getQueryKey()).append(getTaskId(), other.getTaskId()).isEquals();
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(getQueryKey()).append(getTaskId()).toHashCode();
    }
    
}
