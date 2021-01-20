package datawave.webservice.common.storage;

import java.util.Map;
import java.util.UUID;

/**
 * A query checkpoint will be very different depending on the query logic. It is expected that whatever the query state is can be encoded in a map of
 * properties.
 */
public class QueryCheckpoint {
    
    // This is the property name for the initial datawave.webservice.query.Query object
    public static final String INITIAL_QUERY_PROPERTY = "QUERY";
    
    private UUID queryId;
    private QueryType queryType;
    private Map<String, Object> properties;
    
    public QueryCheckpoint(UUID queryId, QueryType queryType, Map<String, Object> properties) {
        this.queryId = queryId;
        this.queryType = queryType;
        this.properties = properties;
    }
    
    /**
     * Get the query id
     * 
     * @return A UUID representation of the query id
     */
    public UUID getQueryId() {
        return queryId;
    }
    
    /**
     * Get the query type
     * 
     * @return the query type
     */
    public QueryType getQueryType() {
        return queryType;
    }
    
    /**
     * Get the properties representing the state of the query.
     * 
     * @return The properties
     */
    public Map<String, Object> getProperties() {
        return properties;
    }
}
