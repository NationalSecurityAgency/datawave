package datawave.microservice.query.storage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import datawave.microservice.query.remote.QueryRequest;
import datawave.services.query.logic.QueryKey;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import java.io.Serializable;

public class TaskKey extends QueryKey implements Serializable {
    private static final long serialVersionUID = -2589618312956104322L;
    
    public static final String TASK_ID_PREFIX = "T-";
    public static final String TASK_ACTION_PREFIX = "A-";
    
    @JsonProperty("taskId")
    private String taskId;
    @JsonProperty("action")
    private QueryRequest.Method action;
    
    /**
     * Default constructor for deserialization
     */
    public TaskKey() {}
    
    /**
     * This method id to allow deserialization of the toKey() or toString() value used when this is in a map
     * 
     * @param value
     *            The toString() from a task key
     */
    public TaskKey(String value) {
        super(value);
    }
    
    /**
     * Get the query key for this task
     * 
     * @return the query key
     */
    @JsonIgnore
    public QueryKey getQueryKey() {
        return new QueryKey(getQueryPool(), getQueryId(), getQueryLogic());
    }
    
    @Override
    public void setPart(String part) {
        if (part.startsWith(TASK_ID_PREFIX)) {
            taskId = part.substring(TASK_ID_PREFIX.length());
        } else {
            super.setPart(part);
        }
    }
    
    @JsonCreator
    public TaskKey(@JsonProperty("taskId") String taskId, @JsonProperty("action") QueryRequest.Method action, @JsonProperty("queryPool") String queryPool,
                    @JsonProperty("queryId") String queryId, @JsonProperty("queryLogic") String queryLogic) {
        super(queryPool, queryId, queryLogic);
        this.taskId = taskId;
        this.action = action;
    }
    
    public TaskKey(String taskId, QueryRequest.Method action, QueryKey queryKey) {
        this(taskId, action, queryKey.getQueryPool(), queryKey.getQueryId(), queryKey.getQueryLogic());
    }
    
    public String getTaskId() {
        return taskId;
    }
    
    public QueryRequest.Method getAction() {
        return action;
    }
    
    public String toKey() {
        return super.toKey() + '.' + TASK_ID_PREFIX + taskId.toString() + '.' + TASK_ACTION_PREFIX + action.name();
    }
    
    public String toRoutingKey() {
        return super.toRoutingKey() + ".*";
    }
    
    @Override
    public String toString() {
        return toKey();
    }
    
    @Override
    public boolean equals(Object o) {
        if (o instanceof TaskKey) {
            TaskKey other = (TaskKey) o;
            return new EqualsBuilder().appendSuper(super.equals(o)).append(getTaskId(), other.getTaskId()).append(getAction(), other.getAction()).isEquals();
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder().appendSuper(super.hashCode()).append(getTaskId()).append(getAction()).toHashCode();
    }
}
