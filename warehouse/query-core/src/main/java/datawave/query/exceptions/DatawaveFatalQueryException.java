package datawave.query.exceptions;

public class DatawaveFatalQueryException extends RuntimeException {
    private static final long serialVersionUID = -454407063025342120L;

    public DatawaveFatalQueryException() {
        super();
    }

    public DatawaveFatalQueryException(String message, Throwable cause) {
        super(message, cause);
    }

    public DatawaveFatalQueryException(String message) {
        super(message);
    }

    public DatawaveFatalQueryException(Throwable cause) {
        super(cause);
    }
}
