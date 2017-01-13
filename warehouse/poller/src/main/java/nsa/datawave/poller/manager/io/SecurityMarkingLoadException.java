package nsa.datawave.poller.manager.io;

import java.io.IOException;

/** Thrown when no alternate Security Marking have been loaded */
public class SecurityMarkingLoadException extends IOException {
    public SecurityMarkingLoadException() {
        super();
    }
    
    public SecurityMarkingLoadException(final Throwable cause) {
        super(cause);
    }
    
    public SecurityMarkingLoadException(final String message) {
        super(message);
    }
    
    public SecurityMarkingLoadException(final String message, Throwable cause) {
        super(message, cause);
    }
}
