package datawave.webservice.common.storage;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.Serializable;
import java.util.UUID;

/**
 * This is a message for the control queue to denote a pending task to perform.
 */
public class QueryTaskNotification implements Serializable {
    private static final long serialVersionUID = 364194052797912452L;
    
    private final UUID queryId;
    private final QueryType queryType;
    private final UUID taskId;
    
    public QueryTaskNotification(UUID queryId, QueryType queryType, UUID taskId) {
        this.queryId = queryId;
        this.queryType = queryType;
        this.taskId = taskId;
    }
    
    public UUID getQueryId() {
        return queryId;
    }
    
    public UUID getTaskId() {
        return taskId;
    }
    
    public QueryType getQueryType() {
        return queryType;
    }
    
    @Override
    public boolean equals(Object o) {
        if (o instanceof QueryTaskNotification) {
            QueryTaskNotification other = (QueryTaskNotification) o;
            return new EqualsBuilder().append(getQueryId(), other.getQueryId()).append(getTaskId(), other.getTaskId())
                            .append(getQueryType(), other.getQueryType()).isEquals();
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(getQueryId()).append(getTaskId()).append(getQueryType()).toHashCode();
    }
    
}
