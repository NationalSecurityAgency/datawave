package datawave.webservice.query.limit;

/**
 * Represents an exception that occurred when interacting with the {@link QueryLimiter}.
 */
public class QueryLimiterException extends RuntimeException {

    public QueryLimiterException() {
        super();
    }

    public QueryLimiterException(String message) {
        super(message);
    }

    public QueryLimiterException(String message, Throwable cause) {
        super(message, cause);
    }

    public QueryLimiterException(Throwable cause) {
        super(cause);
    }

    protected QueryLimiterException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
