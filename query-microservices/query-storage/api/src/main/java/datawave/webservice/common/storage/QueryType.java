package datawave.webservice.common.storage;

/**
 * A query type. It is expected that this type will correspond to a class of query executor service.
 */
public class QueryType {
    private String type;
    
    public QueryType(String type) {
        this.type = type;
    }
    
    public String getType() {
        return type;
    }
}
