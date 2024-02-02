package datawave.query.exceptions;

public class EmptySetComparisonException extends Exception {
    private static final long serialVersionUID = 1L;

    public EmptySetComparisonException() {
        super();
    }

    public EmptySetComparisonException(String message, Throwable cause) {
        super(message, cause);
    }

    public EmptySetComparisonException(String message) {
        super(message);
    }

    public EmptySetComparisonException(Throwable cause) {
        super(cause);
    }
}
