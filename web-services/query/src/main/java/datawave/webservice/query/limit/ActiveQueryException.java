package datawave.webservice.query.limit;

/**
 * Represents an exception that occurred when interacting with the {@link ActiveQueryTracker}.
 */
public class ActiveQueryException extends RuntimeException {
    
    public ActiveQueryException() {
        super();
    }
    
    public ActiveQueryException(String message) {
        super(message);
    }
    
    public ActiveQueryException(String message, Throwable cause) {
        super(message, cause);
    }
    
    public ActiveQueryException(Throwable cause) {
        super(cause);
    }
    
    protected ActiveQueryException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
