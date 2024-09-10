package datawave.query.exceptions;

public class InvalidQueryException extends DatawaveFatalQueryException {
    private static final long serialVersionUID = -2325409536590449819L;

    public InvalidQueryException() {
        super();
    }

    public InvalidQueryException(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidQueryException(String message) {
        super(message);
    }

    public InvalidQueryException(Throwable cause) {
        super(cause);
    }
}
