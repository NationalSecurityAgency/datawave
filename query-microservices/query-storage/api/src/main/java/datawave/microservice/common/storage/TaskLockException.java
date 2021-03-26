package datawave.microservice.common.storage;

/**
 * An exception related to task locks
 */
public class TaskLockException extends Exception {
    public TaskLockException(String message) {
        super(message);
    }
    
    public TaskLockException(String message, Throwable cause) {
        super(message, cause);
    }
}
