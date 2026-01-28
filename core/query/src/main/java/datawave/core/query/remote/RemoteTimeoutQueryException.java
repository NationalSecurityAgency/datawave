package datawave.core.query.remote;

import datawave.webservice.query.exception.TimeoutQueryException;

/**
 * A special TimeoutQueryException to indicate a timeout accessing a RemoteQueryService
 */
public class RemoteTimeoutQueryException extends TimeoutQueryException {
    public RemoteTimeoutQueryException() {
        super();
    }

    public RemoteTimeoutQueryException(String message) {
        super(message);
    }

    public RemoteTimeoutQueryException(Throwable cause) {
        super(cause);
    }

    public RemoteTimeoutQueryException(String message, Throwable cause) {
        super(message, cause);
    }
}
