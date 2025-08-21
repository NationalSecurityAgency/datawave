package datawave.webservice.query.limit;

/**
 * Response originating from {@link QueryLimiter}.
 */
public class QueryLimiterResponse {
    
    public static QueryLimiterResponse doesNotExceedLimit() {
        return new QueryLimiterResponse(false, null);
    }
    
    public static QueryLimiterResponse exceedsLimit(String message) {
        return new QueryLimiterResponse(true, message);
    }
    
    private final boolean exceedsLimit;
    private final String message;
    
    public QueryLimiterResponse(boolean exceedsLimit, String message) {
        this.exceedsLimit = exceedsLimit;
        this.message = message;
    }
    
    public boolean exceedsLimit() {
        return exceedsLimit;
    }
    
    public String getMessage() {
        return message;
    }
}
