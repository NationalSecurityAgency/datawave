package datawave.microservice.annotation.util.exceptions;

/**
 * This exception may be thrown by controllers when a resource is too large. This corresponds to an HTTP 413.
 */
public class PayloadTooLargeException extends ControllerException {

    private static final long serialVersionUID = 1L;

    /**
     * Constructs a new exception without a detail message or cause.
     */
    public PayloadTooLargeException() {
        super();
    }

    /**
     * Constructs a new exception with the specified detail message.
     *
     * @param message
     *            the detail message (which is saved for later retrieval by the {@link #getMessage()} method)
     */
    public PayloadTooLargeException(final String message) {
        super(message);
    }

    /**
     * Constructs a new exception with the specified cause.
     *
     * @param cause
     *            the cause (which is saved for later retrieval by the {@link #getCause()} method). (A {@code null} value is permitted, and indicates that the
     *            cause is nonexistent or unknown.)
     */

    public PayloadTooLargeException(final Throwable cause) {
        super(cause);
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
    public PayloadTooLargeException(final String message, final Throwable cause) {
        super(message, cause);
    }
}
