package datawave.query.exceptions;

/**
 * Thrown when an invalid query tree is encountered
 */
public class InvalidQueryTreeException extends Exception {
    private static final long serialVersionUID = 3961878219004179463L;

    public InvalidQueryTreeException() {
        super();
    }

    public InvalidQueryTreeException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidQueryTreeException(String message) {
        super(message);
    }

    public InvalidQueryTreeException(Throwable cause) {
        super(cause);
    }
}
