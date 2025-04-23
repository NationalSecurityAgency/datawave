package datawave.microservice.query.storage;

import javax.xml.bind.annotation.XmlRootElement;

import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

/**
 * The query state represents the current state of a query
 */
@XmlRootElement
public class QueryState {
    private QueryStatus queryStatus;
    private TaskStates taskStates;
    
    @JsonCreator
    public QueryState(@JsonProperty("queryStatus") QueryStatus queryStatus, @JsonProperty("taskStates") TaskStates taskStates) {
        this.queryStatus = queryStatus;
        this.taskStates = taskStates;
    }
    
    public QueryStatus getQueryStatus() {
        return queryStatus;
    }
    
    public TaskStates getTaskStates() {
        return taskStates;
    }
    
    @Override
    public String toString() {
        return new ToStringBuilder(this).append("queryProperties", getQueryStatus()).append("taskStates", getTaskStates()).build();
    }
    
    @Override
    public boolean equals(Object o) {
        if (o instanceof QueryState) {
            QueryState other = (QueryState) o;
            return new EqualsBuilder().append(getQueryStatus(), other.getQueryStatus()).append(getTaskStates(), other.getTaskStates()).isEquals();
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(getQueryStatus()).append(getTaskStates()).toHashCode();
    }
}
