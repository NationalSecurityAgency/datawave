package datawave.ingest.validation;

/**
 * Defines a subclass of {@code java.lang.Exception} to define particular processing exceptions that occur when validating an {@code Event} sequence file.
 */
public class ValidationException extends Exception {

    private static final long serialVersionUID = -6173989220933214497L;

    public ValidationException() {}

    public ValidationException(String message) {
        super(message);
    }

    public ValidationException(Throwable cause) {
        super(cause);
    }

    public ValidationException(String message, Throwable cause) {
        super(message, cause);
    }

}
