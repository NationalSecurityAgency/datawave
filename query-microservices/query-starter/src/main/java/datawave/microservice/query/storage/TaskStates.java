package datawave.microservice.query.storage;

import datawave.microservice.query.logic.QueryKey;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;
import java.util.HashMap;
import java.util.HashSet;
import java.util.Map;
import java.util.Set;

public class TaskStates implements Serializable {
    public enum TASK_STATE implements Serializable {
        READY, RUNNING, COMPLETED, FAILED
    }
    
    private QueryKey queryKey;
    private int maxRunning = 1;
    private Map<TASK_STATE,Set<TaskKey>> taskStates = new HashMap<>();
    
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
    
    public Map<TASK_STATE,Set<TaskKey>> getTaskStates() {
        return taskStates;
    }
    
    public void setTaskStates(Map<TASK_STATE,Set<TaskKey>> taskStates) {
        this.taskStates = taskStates;
    }
    
    public TASK_STATE getState(TaskKey task) {
        for (TASK_STATE state : TASK_STATE.values()) {
            if (taskStates.containsKey(state) && taskStates.get(state).contains(task)) {
                return state;
            }
        }
        return null;
    }
    
    public boolean setState(TaskKey task, TASK_STATE taskState) {
        TASK_STATE currentState = getState(task);
        if (currentState == taskState) {
            return true;
        }
        if (taskState == TASK_STATE.RUNNING) {
            // if we already have the max number of running tasks, then we cannot change state
            if (taskStates.containsKey(taskState) && taskStates.get(taskState).size() >= maxRunning) {
                return false;
            }
        }
        if (currentState != null) {
            taskStates.get(currentState).remove(task);
        }
        if (taskState != null) {
            if (taskStates.get(taskState) == null) {
                taskStates.put(taskState, new HashSet<>());
            }
            taskStates.get(taskState).add(task);
        }
        return true;
    }
    
    public boolean hasUnfinishedTasks() {
        return taskStates.keySet().stream().anyMatch(state -> state == TASK_STATE.READY || state == TASK_STATE.RUNNING);
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
