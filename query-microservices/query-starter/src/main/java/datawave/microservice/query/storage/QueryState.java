package datawave.microservice.query.storage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import datawave.microservice.query.logic.QueryKey;
import datawave.microservice.query.logic.QueryPool;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.UUID;

/**
 * The query state represents the current state of a query
 */
@XmlRootElement
public class QueryState {
    private QueryKey queryKey;
    private QueryStatus queryStatus;
    private TaskStates taskStates;
    
    public QueryState(QueryKey queryKey, QueryStatus queryStatus, TaskStates taskStates) {
        this(queryKey.getQueryPool(), queryKey.getQueryId(), queryKey.getQueryLogic(), queryStatus, taskStates);
    }
    
    @JsonCreator
    public QueryState(@JsonProperty("queryPool") QueryPool queryPool, @JsonProperty("queryId") UUID queryId, @JsonProperty("queryLogic") String queryLogic,
                    @JsonProperty("queryStatus") QueryStatus queryStatus, @JsonProperty("taskStates") TaskStates taskStates) {
        this.queryKey = new QueryKey(queryPool, queryId, queryLogic);
        this.queryStatus = queryStatus;
        this.taskStates = taskStates;
    }
    
    @JsonIgnore
    public QueryKey getQueryKey() {
        return queryKey;
    }
    
    public UUID getQueryId() {
        return getQueryKey().getQueryId();
    }
    
    public QueryPool getQueryPool() {
        return getQueryKey().getQueryPool();
    }
    
    public String getQueryLogic() {
        return getQueryKey().getQueryLogic();
    }
    
    public QueryStatus getQueryStatus() {
        return queryStatus;
    }
    
    public TaskStates getTaskStates() {
        return taskStates;
    }
    
    @Override
    public String toString() {
        return new ToStringBuilder(this).append(getQueryKey()).append("queryProperties", getQueryStatus()).append("taskStates", getTaskStates()).build();
    }
    
    @Override
    public boolean equals(Object o) {
        if (o instanceof QueryState) {
            QueryState other = (QueryState) o;
            return new EqualsBuilder().append(getQueryKey(), other.getQueryKey()).append(getQueryStatus(), other.getQueryStatus())
                            .append(getTaskStates(), other.getTaskStates()).isEquals();
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(getQueryKey()).append(getQueryStatus()).append(getTaskStates()).toHashCode();
    }
}
