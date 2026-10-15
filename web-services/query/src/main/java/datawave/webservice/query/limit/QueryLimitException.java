package datawave.webservice.query.limit;

public class QueryLimitException extends RuntimeException {
    
    public QueryLimitException() {
        super();
    }
    
    public QueryLimitException(String message) {
        super(message);
    }
    
    public QueryLimitException(String message, Throwable cause) {
        super(message, cause);
    }
    
    public QueryLimitException(Throwable cause) {
        super(cause);
    }
    
    protected QueryLimitException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
