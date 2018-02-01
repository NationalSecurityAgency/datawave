package datawave.core.iterators;

import java.io.IOException;

public class IteratorTimeoutException extends IOException {
    /**
     * 
     */
    private static final long serialVersionUID = -3122472458580546482L;
    
    /**
     * @param message
     */
    public IteratorTimeoutException(String message) {
        super(message);
    }
}
