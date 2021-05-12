package datawave.microservice.common.storage;

import com.fasterxml.jackson.databind.annotation.JsonDeserialize;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;
import org.springframework.util.LinkedMultiValueMap;
import org.springframework.util.MultiValueMap;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;

public class TaskStates implements Serializable {
    public enum TASK_STATE implements Serializable {
        READY, RUNNING, COMPLETED
    }
    
    private QueryKey queryKey;
    private int maxRunning = 1;
    
    @JsonDeserialize(as = LinkedMultiValueMap.class)
    private MultiValueMap<TASK_STATE,TaskKey> taskStates = new LinkedMultiValueMap<>();
    
    public TaskStates() {}
    
    public TaskStates(QueryKey queryKey, int maxRunning) {
        setQueryKey(queryKey);
        setMaxRunning(maxRunning);
    }
    
    public void setQueryKey(QueryKey key) {
        this.queryKey = key;
    }
    
    public QueryKey getQueryKey() {
        return queryKey;
    }
    
    public int getMaxRunning() {
        return maxRunning;
    }
    
    public void setMaxRunning(int maxRunning) {
        this.maxRunning = maxRunning;
    }
    
    public MultiValueMap<TASK_STATE,TaskKey> getTaskStates() {
        return taskStates;
    }
    
    public void setTaskStates(MultiValueMap<TASK_STATE,TaskKey> taskStates) {
        this.taskStates = taskStates;
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(queryKey).append(maxRunning).append(taskStates).build();
    }
    
    @Override
    public boolean equals(Object obj) {
        if (obj instanceof TaskStates) {
            TaskStates other = (TaskStates) obj;
            return new EqualsBuilder().append(queryKey, other.queryKey).append(maxRunning, other.maxRunning).append(taskStates, other.taskStates).build();
        }
        return false;
    }
    
    @Override
    public String toString() {
        return new ToStringBuilder(this).append("queryKey", queryKey).append("maxRunning", maxRunning).append("taskStates", taskStates).build();
    }
}
