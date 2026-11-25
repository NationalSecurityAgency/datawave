package datawave.annotation.projection;

/**
 * Reports invalid projection configuration or annotation content that cannot satisfy a configured field mapping. Examples include an unsupported selector, a
 * missing required property and a value whose type does not match its configured format.
 * <p>
 * Resource-budget and output-consumer failures propagate as their original exceptions instead of being wrapped in this type.
 */
public class ProjectionException extends IllegalArgumentException {
    private static final long serialVersionUID = 1L;

    /**
     * Creates a failure with a description of the invalid configuration or annotation content.
     *
     * @param message
     *            the failure description, including profile and binding context when available
     */
    public ProjectionException(String message) {
        super(message);
    }

    /**
     * Creates a failure that retains the underlying validation or selector error.
     *
     * @param message
     *            the failure description, including profile and binding context when available
     * @param cause
     *            the original error
     */
    public ProjectionException(String message, Throwable cause) {
        super(message, cause);
    }
}
