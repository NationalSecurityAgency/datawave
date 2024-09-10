package datawave.query.exceptions;

public class DatawaveQueryException extends Exception {
    private static final long serialVersionUID = -3090268343559242233L;

    public DatawaveQueryException() {
        super();
    }

    public DatawaveQueryException(String message, Throwable cause) {
        super(message, cause);
    }

    public DatawaveQueryException(String message) {
        super(message);
    }

    public DatawaveQueryException(Throwable cause) {
        super(cause);
    }
}
