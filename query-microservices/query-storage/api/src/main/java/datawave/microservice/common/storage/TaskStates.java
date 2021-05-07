package datawave.microservice.common.storage;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class TaskStates implements Serializable {
    public enum TASK_STATE {
        READY, RUNNING, COMPLETED
    }
    
    private QueryKey queryKey;
    private Map<TaskKey,String> taskStates = new HashMap<>();
    
    public TaskStates() {}
    
    public TaskStates(QueryKey queryKey) {
        setQueryKey(queryKey);
    }
    
    public void setQueryKey(QueryKey key) {
        this.queryKey = key;
    }
    
    public QueryKey getQueryKey() {
        return queryKey;
    }
    
    public Map<TaskKey,String> getTaskStates() {
        return taskStates;
    }
    
    public void setTaskStates(Map<TaskKey,String> taskStates) {
        this.taskStates = taskStates;
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(queryKey).append(taskStates).build();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TaskStates) {
            TaskStates other = (TaskStates) obj;
            return new EqualsBuilder().append(queryKey, other.queryKey).append(taskStates, other.taskStates).build();
        }
        return false;
    }
    
    @Override
    public String toString() {
        return new ToStringBuilder(this).append("queryKey", queryKey).append("taskStates", taskStates).build();
    }
}
