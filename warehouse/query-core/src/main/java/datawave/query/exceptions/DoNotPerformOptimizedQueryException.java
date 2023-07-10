package datawave.query.exceptions;

public class DoNotPerformOptimizedQueryException extends DatawaveFatalQueryException {
    private static final long serialVersionUID = 1L;

    public DoNotPerformOptimizedQueryException() {
        super();
    }

    public DoNotPerformOptimizedQueryException(String message, Throwable cause) {
        super(message, cause);
    }

    public DoNotPerformOptimizedQueryException(String message) {
        super(message);
    }

    public DoNotPerformOptimizedQueryException(Throwable cause) {
        super(cause);
    }
}
