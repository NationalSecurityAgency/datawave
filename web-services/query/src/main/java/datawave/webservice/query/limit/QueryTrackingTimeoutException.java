package datawave.webservice.query.limit;

public class QueryTrackingTimeoutException extends QueryLimiterException {

    public QueryTrackingTimeoutException() {
        super();
    }

    public QueryTrackingTimeoutException(String message) {
        super(message);
    }

    public QueryTrackingTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }

    public QueryTrackingTimeoutException(Throwable cause) {
        super(cause);
    }

    protected QueryTrackingTimeoutException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
