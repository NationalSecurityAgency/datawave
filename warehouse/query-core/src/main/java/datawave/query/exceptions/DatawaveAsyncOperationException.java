package datawave.query.exceptions;

import datawave.query.planner.DefaultQueryPlanner;

/**
 * An exception thrown when the {@link DefaultQueryPlanner} encounters a problem during an async operation like fetching field sets or serializing iterator
 * options in another thread
 */
public class DatawaveAsyncOperationException extends RuntimeException {

    private static final long serialVersionUID = -5455973957749708049L;

    public DatawaveAsyncOperationException() {
        super();
    }

    public DatawaveAsyncOperationException(String message, Throwable cause) {
        super(message, cause);
    }

    public DatawaveAsyncOperationException(String message) {
        super(message);
    }

    public DatawaveAsyncOperationException(Throwable cause) {
        super(cause);
    }

    protected DatawaveAsyncOperationException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace) {
        super(message, cause, enableSuppression, writableStackTrace);
    }
}
