package datawave.query.exceptions;

/**
 *
 */
public class FullTableScansDisallowedException extends DatawaveQueryException {

    private static final long serialVersionUID = 1L;

    public FullTableScansDisallowedException() {
        super();
    }

    public FullTableScansDisallowedException(String message) {
        super(message);
    }

    public FullTableScansDisallowedException(String message, Throwable cause) {
        super(message, cause);
    }

    public FullTableScansDisallowedException(Throwable t) {
        super(t);
    }
}
