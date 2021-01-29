package datawave.microservice.common.storage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * A task description
 */
@XmlRootElement
public class TaskDescription {
    private final UUID taskId;
    private final QueryTask.QUERY_ACTION action;
    private final Map<String,String> parameters;
    
    @JsonCreator
    public TaskDescription(@JsonProperty("taskId") UUID taskId, @JsonProperty("action") QueryTask.QUERY_ACTION action,
                    @JsonProperty("parameters") Map<String,String> parameters) {
        this.taskId = taskId;
        this.action = action;
        this.parameters = Collections.unmodifiableMap(new HashMap<>(parameters));
    }
    
    public UUID getTaskId() {
        return taskId;
    }
    
    public QueryTask.QUERY_ACTION getAction() {
        return action;
    }
    
    public Map<String,String> getParameters() {
        return parameters;
    }
    
    @Override
    public String toString() {
        return getAction() + " for " + getTaskId();
    }
    
    @Override
    public boolean equals(Object o) {
        if (o instanceof TaskDescription) {
            TaskDescription other = (TaskDescription) o;
            return new EqualsBuilder().append(getTaskId(), other.getTaskId()).append(getAction(), other.getAction())
                            .append(getParameters(), other.getParameters()).isEquals();
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(getTaskId()).append(getAction()).append(getParameters()).toHashCode();
    }
}
