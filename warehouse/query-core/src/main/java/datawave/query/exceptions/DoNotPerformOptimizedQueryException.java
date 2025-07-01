package datawave.query.exceptions;

public class DoNotPerformOptimizedQueryException extends DatawaveFatalQueryException {
    private static final long serialVersionUID = 8550522292180353602L;

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
