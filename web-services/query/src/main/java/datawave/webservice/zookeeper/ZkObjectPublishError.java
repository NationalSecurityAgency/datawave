package datawave.webservice.zookeeper;

/**
 * Represents an error that occurred when attempting to load a new updated object via a {@link ZkObjectPublisher}.
 */
public class ZkObjectPublishError {

    /**
     * A short description of the error.
     */
    private final String message;

    /**
     * The associated exception for the error, if any.
     */
    private final Exception exception;

    public ZkObjectPublishError(String message, Exception exception) {
        this.message = message;
        this.exception = exception;
    }

    public String getMessage() {
        return message;
    }

    public Exception getException() {
        return exception;
    }

    public boolean hasException() {
        return exception != null;
    }
}
