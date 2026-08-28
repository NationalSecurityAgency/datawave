package datawave.microservice.annotation.util.exceptions;

/**
 * <code>ControllerException</code> is the superclass of exceptions that can be thrown by controllers.
 *
 * @see RuntimeException
 */
public class ControllerException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    /**
     * Constructs a new exception with <code>null</code> as its detail message. The cause is not initialized, and may subsequently be initialized by a call to
     * {@link #initCause}.
     */
    public ControllerException() {
        super();
    }

    /**
     * Constructs a new exception with the specified detail message. The cause is not initialized, and may subsequently be initialized by a call to
     * {@link #initCause}.
     *
     * @param message
     *            the detail message (which is saved for later retrieval by the {@link #getMessage()} method)
     */
    public ControllerException(String message) {
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
    public ControllerException(String message, Throwable cause) {
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
    public ControllerException(Throwable cause) {
        super(cause);
    }
}
