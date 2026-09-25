package datawave.microservice.annotation.util.exceptions;

/**
 * This exception may be thrown when a user cannot be authorized or some other authorization error exists. In other words, we know who the user is but they do
 * not have sufficient permissions to access the resource or service. This would correspond to an HTTP 403.
 */
public class AuthorizationException extends ControllerException {

    private static final long serialVersionUID = 1L;

    /**
     * Constructs a new exception with <code>null</code> as its detail message. The cause is not initialized, and may subsequently be initialized by a call to
     * {@link #initCause}.
     */
    public AuthorizationException() {
        super();
    }

    /**
     * Constructs a new exception with the specified detail message. The cause is not initialized, and may subsequently be initialized by a call to
     * {@link #initCause}.
     *
     * @param message
     *            the detail message (which is saved for later retrieval by the {@link #getMessage()} method)
     */
    public AuthorizationException(String message) {
        super(message);
    }

    /**
     * Constructs a new exception with the specified detail message and cause.
     * <p>
     * Note that the detail message associated with <code>cause</code> is <i>not</i> automatically incorporated in this runtime exception's detail message.
     *
     * @param message
     *            the detail message (which is saved for later retrieval by the {@link #getMessage()} method)
     * @param cause
     *            the cause (which is saved for later retrieval by the {@link #getCause()} method). (A {@code null} value is permitted, and indicates that the
     *            cause is nonexistent or unknown.)
     */
    public AuthorizationException(String message, Throwable cause) {
        super(message, cause);
    }

    /**
     * Constructs a new exception with the specified cause and a detail message of {@code (cause==null ? null : cause.toString())} (which typically contains the
     * class and detail message of {@code cause}). This constructor is useful for runtime exceptions that are little more than wrappers for other throwables.
     *
     * @param cause
     *            the cause (which is saved for later retrieval by the {@link #getCause()} method). (A {@code null} value is permitted, and indicates that the
     *            cause is nonexistent or unknown.)
     */
    public AuthorizationException(Throwable cause) {
        super(cause);
    }
}
