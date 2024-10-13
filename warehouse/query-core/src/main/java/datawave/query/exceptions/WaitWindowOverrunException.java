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
}
