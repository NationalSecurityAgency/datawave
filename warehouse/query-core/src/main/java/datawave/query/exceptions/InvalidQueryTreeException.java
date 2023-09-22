package datawave.query.exceptions;

/**
 * Thrown when an invalid query tree is encountered
 */
public class InvalidQueryTreeException extends Exception {

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
