package datawave.microservice.query.storage;

import java.util.Collection;
import java.util.Objects;

import javax.xml.bind.annotation.XmlRootElement;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.Sets;

import datawave.core.query.configuration.QueryData;

/**
 * A task description
 */
@XmlRootElement
public class TaskDescription {
    private final TaskKey taskKey;
    private final Collection<QueryData> queries;
    
    @JsonCreator
    public TaskDescription(@JsonProperty("taskKey") TaskKey taskKey, @JsonProperty("queries") Collection<QueryData> queries) {
        this.taskKey = taskKey;
        this.queries = queries;
    }
    
    public TaskKey getTaskKey() {
        return taskKey;
    }
    
    public Collection<QueryData> getQueries() {
        return queries;
    }
    
    @Override
    public String toString() {
        return getTaskKey() + " on " + getQueries();
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
        return Objects.equals(taskKey, that.taskKey) && Objects.equals(Sets.newHashSet(queries), Sets.newHashSet(that.queries));
    }
    
    @Override
    public int hashCode() {
        return Objects.hash(taskKey, queries);
    }
}
