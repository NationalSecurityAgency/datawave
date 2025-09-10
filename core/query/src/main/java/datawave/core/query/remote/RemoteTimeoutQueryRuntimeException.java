package datawave.core.query.remote;

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
