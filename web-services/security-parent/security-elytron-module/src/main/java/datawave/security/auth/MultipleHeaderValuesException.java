package datawave.security.auth;

/**
 * An exception that indicates a header is present in a request with multiple values when the header is expected to have a single value.
 */
public final class MultipleHeaderValuesException extends Exception {

    private static final long serialVersionUID = 1L;

    public MultipleHeaderValuesException(String message) {
        super(message);
    }
}
