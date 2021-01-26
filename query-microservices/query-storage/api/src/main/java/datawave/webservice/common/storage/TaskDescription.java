package datawave.webservice.common.storage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;

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
    
    @JsonIgnore
    public UUID getTaskId() {
        return taskId;
    }
    
    @JsonIgnore
    public QueryTask.QUERY_ACTION getAction() {
        return action;
    }
    
    @JsonIgnore
    public Map<String,String> getParameters() {
        return parameters;
    }
}
