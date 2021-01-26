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
 * The query state represents the current state of a query
 */
@XmlRootElement
public class QueryState {
    private UUID queryId;
    private QueryType queryType;
    private Map<QueryTask.QUERY_ACTION,Integer> taskCounts;
    
    @JsonCreator
    public QueryState(@JsonProperty("queryId") UUID queryId, @JsonProperty("queryType") QueryType queryType,
                    @JsonProperty("taskCounts") Map<QueryTask.QUERY_ACTION,Integer> taskCounts) {
        this.queryId = queryId;
        this.queryType = queryType;
        this.taskCounts = Collections.unmodifiableMap(new HashMap<>(taskCounts));
    }
    
    @JsonIgnore
    public UUID getQueryId() {
        return queryId;
    }
    
    @JsonIgnore
    public QueryType getQueryType() {
        return queryType;
    }
    
    @JsonIgnore
    public Map<QueryTask.QUERY_ACTION,Integer> getTaskCounts() {
        return taskCounts;
    }
    
}
