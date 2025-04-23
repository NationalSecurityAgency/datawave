package datawave.microservice.query.storage;

import java.io.Serializable;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.stream.Collectors;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

import datawave.core.query.logic.QueryKey;
import datawave.util.StringUtils;

public class TaskStates implements Serializable {
    private static final long serialVersionUID = 1361359960334155427L;
    
    /**
     * The possible task states: <br>
     * READY: ready to run <br>
     * RUNNING: currently running <br>
     * COMPLETED: completed successfully <br>
     * FAILED: failed to execute successfully <br>
     */
    public enum TASK_STATE {
        READY, RUNNING, COMPLETED, FAILED
    }
    
    private QueryKey queryKey;
    private int maxRunning = 1;
    private int nextTaskId = 1;
    
    @JsonIgnore
    private Map<TASK_STATE,SortedSet<Integer>> taskStates = new HashMap<>();
    
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
    public Map<TASK_STATE,String> getTaskStatesAsStrings() {
        return taskStates.entrySet().stream().collect(Collectors.toMap(e -> e.getKey(), e -> taskIdsToString(e.getValue())));
    }
    
    /**
     * Set task states in a form that was JSON serializable
     * 
     * @param taskStatesStrings
     */
    @JsonProperty("taskStates")
    public void setTaskStatesAsStrings(Map<TASK_STATE,String> taskStatesStrings) {
        taskStates = taskStatesStrings.entrySet().stream().collect(Collectors.toMap(e -> e.getKey(), e -> stringToTaskIds(e.getValue())));
    }
    
    private String taskIdsToString(Set<Integer> taskIds) {
        StringBuilder builder = new StringBuilder();
        if (!taskIds.isEmpty()) {
            List<Integer> sortedIds = new ArrayList<>(taskIds);
            Collections.sort(sortedIds);
            for (Integer i : sortedIds) {
                builder.append(i).append(',');
            }
            builder.setLength(builder.length() - 1);
        }
        return builder.toString();
    }
    
    private SortedSet<Integer> stringToTaskIds(String bitSetStr) {
        SortedSet<Integer> ids = new TreeSet<>();
        for (String taskId : StringUtils.splitIterable(bitSetStr, ',')) {
            ids.add(Integer.parseInt(taskId));
        }
        return ids;
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
    
    /**
     * This will get the number of tasks we can start running now (concurrently) by subtracting the number of runnning tasks from the max concurrent running
     */
    @JsonIgnore
    public int getAvailableRunningSlots() {
        return getMaxRunning() - getRunningTaskCount();
    }
    
    /**
     * This will get the number of tasks we can start running now (@see getAvailableRunningSlots) out of the tasks that are currently in a READY state. This
     * would be a minimum of the ready tasks and the available running slots
     * 
     * @return
     */
    @JsonIgnore
    public int getAvailableReadyTasksToRun() {
        return Math.min(getAvailableRunningSlots(), getReadyTaskCount());
    }
    
    public Map<TASK_STATE,SortedSet<Integer>> getTaskStates() {
        return taskStates;
    }
    
    public void setTaskStates(Map<TASK_STATE,SortedSet<Integer>> taskStates) {
        this.taskStates = taskStates;
    }
    
    public TASK_STATE getState(int taskId) {
        for (TASK_STATE state : TASK_STATE.values()) {
            if (taskStates.containsKey(state) && taskStates.get(state).contains(taskId)) {
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
            if (getAvailableRunningSlots() <= 0) {
                return false;
            }
        }
        if (currentState != null) {
            taskStates.get(currentState).remove(taskId);
        }
        if (taskState != null) {
            if (taskStates.get(taskState) == null) {
                taskStates.put(taskState, new TreeSet<>());
            }
            taskStates.get(taskState).add(taskId);
        }
        return true;
    }
    
    public int getTaskCountForState(TASK_STATE state) {
        return taskStates.containsKey(state) ? taskStates.get(state).size() : 0;
    }
    
    @JsonIgnore
    public int getReadyTaskCount() {
        return getTaskCountForState(TASK_STATE.READY);
    }
    
    @JsonIgnore
    public int getRunningTaskCount() {
        return getTaskCountForState(TASK_STATE.RUNNING);
    }
    
    @JsonIgnore
    public int getFailedTaskCount() {
        return getTaskCountForState(TASK_STATE.FAILED);
    }
    
    @JsonIgnore
    public int getCompletedTaskCount() {
        return getTaskCountForState(TASK_STATE.COMPLETED);
    }
    
    public boolean hasTasksForState(TASK_STATE state) {
        return taskStates.containsKey(state) && !taskStates.get(state).isEmpty();
    }
    
    @JsonIgnore
    public boolean hasReadyTasks() {
        return hasTasksForState(TASK_STATE.READY);
    }
    
    @JsonIgnore
    public boolean hasRunningTasks() {
        return hasTasksForState(TASK_STATE.RUNNING);
    }
    
    @JsonIgnore
    public boolean hasUnfinishedTasks() {
        return hasReadyTasks() || hasRunningTasks();
    }
    
    @JsonIgnore
    public boolean hasCompletedTasks() {
        return hasTasksForState(TASK_STATE.COMPLETED);
    }
    
    @JsonIgnore
    public boolean hasFailedTasks() {
        return hasTasksForState(TASK_STATE.FAILED);
    }
    
    public Iterable<TaskKey> getTasksForState(TASK_STATE state, int maxTasks) {
        return new Iterable<TaskKey>() {
            
            @Override
            public Iterator<TaskKey> iterator() {
                // creating a copy to avoid concurrent modification exceptions while using this task iterator
                List<Integer> states = (taskStates.containsKey(state) ? new ArrayList<>(taskStates.get(state)) : Collections.emptyList());
                final Iterator<Integer> statesIterator = states.iterator();
                return new Iterator<TaskKey>() {
                    @Override
                    public boolean hasNext() {
                        return statesIterator.hasNext();
                    }
                    
                    @Override
                    public TaskKey next() {
                        if (hasNext()) {
                            return new TaskKey(statesIterator.next(), queryKey);
                        }
                        return null;
                    }
                };
            }
        };
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
        return new ToStringBuilder(this).append("queryKey", queryKey).append("maxRunning", maxRunning).append("taskStates", taskStatesString()).build();
    }
    
    public String taskStatesString() {
        StringBuilder str = new StringBuilder();
        String sep = "";
        for (TASK_STATE state : TASK_STATE.values()) {
            str.append(sep);
            str.append(state).append(':').append(getTaskCountForState(state));
            sep = ",";
        }
        return str.toString();
    }
}
