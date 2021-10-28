package datawave.microservice.query.storage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import datawave.services.query.configuration.GenericQueryConfiguration;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.Objects;

/**
 * A task description
 */
@XmlRootElement
public class TaskDescription {
    private final TaskKey taskKey;
    private final GenericQueryConfiguration config;
    
    @JsonCreator
    public TaskDescription(@JsonProperty("taskKey") TaskKey taskKey, @JsonProperty("config") GenericQueryConfiguration config) {
        this.taskKey = taskKey;
        this.config = config;
    }
    
    public TaskKey getTaskKey() {
        return taskKey;
    }
    
    public GenericQueryConfiguration getConfig() {
        return config;
    }
    
    @Override
    public String toString() {
        return getTaskKey() + " on " + getConfig();
    }
    
    public String toDebug() {
        return getTaskKey().toString();
    }
    
    @Override
    public boolean equals(Object o) {
        if (this == o)
            return true;
        if (o == null || getClass() != o.getClass())
            return false;
        TaskDescription that = (TaskDescription) o;
        return Objects.equals(taskKey, that.taskKey) && Objects.equals(config, that.config);
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(taskKey, config);
    }
}
