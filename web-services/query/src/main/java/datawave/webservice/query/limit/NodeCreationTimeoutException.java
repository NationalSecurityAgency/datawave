package datawave.webservice.query.limit;

public class NodeCreationTimeoutException extends RuntimeException {
    public NodeCreationTimeoutException() {
        super();
    }

    public NodeCreationTimeoutException(String message) {
        super(message);
    }

    public NodeCreationTimeoutException(String message, Throwable cause) {
        super(message, cause);
    }

    public NodeCreationTimeoutException(Throwable cause) {
        super(cause);
    }

    protected NodeCreationTimeoutException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
