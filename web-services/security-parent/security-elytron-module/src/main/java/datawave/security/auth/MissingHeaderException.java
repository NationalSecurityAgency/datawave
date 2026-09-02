package datawave.security.auth;

/**
 * An exception that indicates a required header is missing from a request.
 */
public final class MissingHeaderException extends Exception {

    private static final long serialVersionUID = 1L;

    public MissingHeaderException(String message) {
        super(message);
    }
}
