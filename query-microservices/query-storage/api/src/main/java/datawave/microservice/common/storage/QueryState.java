package datawave.microservice.common.storage;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonIgnore;
import com.fasterxml.jackson.annotation.JsonProperty;
import org.apache.commons.lang3.builder.EqualsBuilder;
import org.apache.commons.lang3.builder.HashCodeBuilder;
import org.apache.commons.lang3.builder.ToStringBuilder;

import javax.xml.bind.annotation.XmlRootElement;
import java.util.Collections;
import java.util.HashMap;
import java.util.Map;
import java.util.UUID;

/**
 * The query state represents the current state of a query
 */
@XmlRootElement
public class QueryState {
    private QueryKey queryKey;
    private QueryProperties queryProperties;
    private QueryStats queryStats;
    private Map<QueryTask.QUERY_ACTION,Integer> taskCounts;

    public QueryState(QueryKey queryKey, QueryProperties queryProperties, QueryStats queryStats, Map<QueryTask.QUERY_ACTION,Integer> taskCounts) {
        this(queryKey.getQueryPool(), queryKey.getQueryId(), queryKey.getQueryLogic(), queryProperties, queryStats, taskCounts);
    }
    
    @JsonCreator
    public QueryState(@JsonProperty("queryPool") QueryPool queryPool, @JsonProperty("queryId") UUID queryId, @JsonProperty("queryLogic") String queryLogic,
                    @JsonProperty("queryProperties") QueryProperties queryProperties,
                    @JsonProperty("queryStats") QueryStats queryStats,
                    @JsonProperty("taskCounts") Map<QueryTask.QUERY_ACTION,Integer> taskCounts) {
        this.queryKey = new QueryKey(queryPool, queryId, queryLogic);
        this.queryProperties = queryProperties;
        this.queryStats = queryStats;
        this.taskCounts = Collections.unmodifiableMap(new HashMap<>(taskCounts));
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

    public QueryProperties getQueryProperties() {
        return queryProperties;
    }

    public QueryStats getQueryStats() {
        return queryStats;
    }

    public Map<QueryTask.QUERY_ACTION,Integer> getTaskCounts() {
        return taskCounts;
    }
    
    @Override
    public String toString() {
        return new ToStringBuilder(this).append(getQueryKey()).append("queryProperties", getQueryProperties()).append("queryStats", getQueryStats()).append("taskCounts", getTaskCounts()).build();
    }
    
    @Override
    public boolean equals(Object o) {
        if (o instanceof QueryState) {
            QueryState other = (QueryState) o;
            return new EqualsBuilder().append(getQueryKey(), other.getQueryKey()).append(getQueryProperties(), other.getQueryProperties()).append(getQueryStats(), other.getQueryStats()).append(getTaskCounts(), other.getTaskCounts()).isEquals();
        }
        return false;
    }
    
    @Override
    public int hashCode() {
        return new HashCodeBuilder().append(getQueryKey()).append(getQueryProperties()).append(getQueryStats()).append(getTaskCounts()).toHashCode();
    }
}
