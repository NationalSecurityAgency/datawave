package datawave.core.query.remote;

/**
 * RuntimeException to be thrown in place of a checked RemoteTimeoutQueryException
 */
public class RemoteTimeoutQueryRuntimeException extends RuntimeException {
    public RemoteTimeoutQueryRuntimeException() {
        super();
    }

    public RemoteTimeoutQueryRuntimeException(String message) {
        super(message);
    }

    public RemoteTimeoutQueryRuntimeException(Throwable e) {
        super(e);
    }

    public RemoteTimeoutQueryRuntimeException(String message, Throwable e) {
        super(message, e);
    }
}
