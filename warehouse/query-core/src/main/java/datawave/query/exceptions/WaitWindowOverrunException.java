package datawave.query.exceptions;

import org.apache.accumulo.core.data.Key;

public class WaitWindowOverrunException extends RuntimeException {

    private Key yieldKey;

    public WaitWindowOverrunException(Key yieldKey) {
        this.yieldKey = yieldKey;
    }

    public Key getYieldKey() {
        return yieldKey;
    }

    // disallow other constructors that don't include a yieldKey
    private WaitWindowOverrunException() {
        super();
    }

    // disallow constructors that don't include a yieldKey
    private WaitWindowOverrunException(String message) {
        super(message);
    }

    // disallow constructors that don't include a yieldKey
    private WaitWindowOverrunException(String message, Throwable cause) {
        super(message, cause);
    }

    // disallow constructors that don't include a yieldKey
    private WaitWindowOverrunException(Throwable cause) {
        super(cause);
    }
}
