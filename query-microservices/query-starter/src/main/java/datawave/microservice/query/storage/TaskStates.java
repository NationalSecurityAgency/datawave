package datawave.microservice.query.storage;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.zaxxer.sparsebits.SparseBitSet;
import datawave.services.query.logic.QueryKey;
import datawave.util.StringUtils;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import java.io.Serializable;
import java.util.HashMap;
import java.util.Map;
import java.util.stream.Collectors;

public class TaskStates implements Serializable {
    public enum TASK_STATE implements Serializable {
        READY, RUNNING, COMPLETED, FAILED
    }
    
    private QueryKey queryKey;
    private int maxRunning = 1;
    private int nextTaskId = 1;
    @JsonIgnore
    private Map<TASK_STATE,SparseBitSet> taskStates = new HashMap<>();
    
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
    
    public int getNextTaskId() {
        return nextTaskId;
    }
    
    @JsonIgnore
    public int getAndIncrementNextTaskId() {
        int taskId = nextTaskId;
        nextTaskId++;
        return taskId;
    }
    
    /**
     * Get task states in a form that is JSON serializable
     * 
     * @return taskStates
     */
    @JsonProperty("taskStates")
    public Map<TASK_STATE,String> getTaskStates2() {
        return taskStates.entrySet().stream().collect(Collectors.toMap(e -> e.getKey(), e -> bitSetToString(e.getValue())));
    }
    
    /**
     * Set task states in a form that was JSON serializable
     * 
     * @param taskStates2
     */
    @JsonProperty("taskStates")
    public void setTaskStates2(Map<TASK_STATE,String> taskStates2) {
        taskStates = taskStates2.entrySet().stream().collect(Collectors.toMap(e -> e.getKey(), e -> stringToBitSet(e.getValue())));
    }
    
    private String bitSetToString(SparseBitSet bitSet) {
        StringBuilder builder = new StringBuilder();
        for (int i = bitSet.nextSetBit(0); i >= 0; i = bitSet.nextSetBit(i + 1)) {
            builder.append(Integer.toString(i)).append(',');
        }
        builder.setLength(builder.length() - 1);
        return builder.toString();
    }
    
    private SparseBitSet stringToBitSet(String bitSetStr) {
        SparseBitSet bitSet = new SparseBitSet();
        for (String taskId : StringUtils.splitIterable(bitSetStr, ',')) {
            bitSet.set(Integer.parseInt(taskId));
        }
        return bitSet;
    }
    
    public void setNextTaskId(int nextTaskId) {
        this.nextTaskId = nextTaskId;
    }
    
    public int getMaxRunning() {
        return maxRunning;
    }
    
    public void setMaxRunning(int maxRunning) {
        this.maxRunning = maxRunning;
    }
    
    public Map<TASK_STATE,SparseBitSet> getTaskStates() {
        return taskStates;
    }
    
    public void setTaskStates(Map<TASK_STATE,SparseBitSet> taskStates) {
        this.taskStates = taskStates;
    }
    
    public TASK_STATE getState(int taskId) {
        for (TASK_STATE state : TASK_STATE.values()) {
            if (taskStates.containsKey(state) && taskStates.get(state).get(taskId)) {
                return state;
            }
        }
        return null;
    }
    
    public boolean setState(int taskId, TASK_STATE taskState) {
        TASK_STATE currentState = getState(taskId);
        if (currentState == taskState) {
            return true;
        }
        if (taskState == TASK_STATE.RUNNING) {
            // if we already have the max number of running tasks, then we cannot change state
            if (taskStates.containsKey(taskState) && taskStates.get(taskState).cardinality() >= maxRunning) {
                return false;
            }
        }
        if (currentState != null) {
            taskStates.get(currentState).clear(taskId);
        }
        if (taskState != null) {
            if (taskStates.get(taskState) == null) {
                taskStates.put(taskState, new SparseBitSet());
            }
            taskStates.get(taskState).set(taskId);
        }
        return true;
    }
    
    public boolean hasTasksForState(TASK_STATE state) {
        return taskStates.get(state) != null && !taskStates.get(state).isEmpty();
    }
    
    public boolean hasReadyTasks() {
        return hasTasksForState(TASK_STATE.READY);
    }
    
    public boolean hasRunningTasks() {
        return hasTasksForState(TASK_STATE.RUNNING);
    }
    
    public boolean hasUnfinishedTasks() {
        return hasReadyTasks() || hasRunningTasks();
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
