package datawave.query.exceptions;

/**
 *
 */
public class InvalidDocumentHeader extends DatawaveFatalQueryException {

    private static final long serialVersionUID = 2398929606385558745L;

    public InvalidDocumentHeader() {
        super();
    }

    public InvalidDocumentHeader(String message, Throwable cause) {
        super(message, cause);
    }

    public InvalidDocumentHeader(String message) {
        super(message);
    }

    public InvalidDocumentHeader(Throwable cause) {
        super(cause);
    }
}
