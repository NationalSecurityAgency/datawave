package datawave.query.exceptions;

/**
 *
 */
public class InvalidFieldIndexQueryFatalQueryException extends DatawaveFatalQueryException {
    private static final long serialVersionUID = 3830216086718782793L;

    public InvalidFieldIndexQueryFatalQueryException() {
        super();
    }

    public InvalidFieldIndexQueryFatalQueryException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidFieldIndexQueryFatalQueryException(String message) {
        super(message);
    }

    public InvalidFieldIndexQueryFatalQueryException(Throwable cause) {
        super(cause);
    }
}
