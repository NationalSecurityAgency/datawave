package datawave.query.exceptions;

/**
 *
 */
public class NoSuchDeserializerException extends DatawaveFatalQueryException {

    private static final long serialVersionUID = 7016805713066728063L;

    public NoSuchDeserializerException() {
        super();
    }

    public NoSuchDeserializerException(String message, Throwable cause) {
        super(message, cause);
    }

    public NoSuchDeserializerException(String message) {
        super(message);
    }

    public NoSuchDeserializerException(Throwable cause) {
        super(cause);
    }

}
