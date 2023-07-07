package datawave.query.exceptions;

/**
 * Thrown by queryGlobalIndexForRange when there is an range expansion issue for a node such as a required field expands to too many terms. We can often recover
 * from these, such as whene they occur in a non-required branch of a query (one side of an AND).
 */
public class TooManyTermsException extends DatawaveQueryException {

    private static final long serialVersionUID = 1L;

    public TooManyTermsException() {
        super();
    }

    public TooManyTermsException(String message, Throwable cause) {
        super(message, cause);
    }

    public TooManyTermsException(String message) {
        super(message);
    }

    public TooManyTermsException(Throwable cause) {
        super(cause);
    }
}
