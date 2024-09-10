package datawave.query.exceptions;

public class EmptyUnfieldedTermExpansionException extends DatawaveFatalQueryException {

    private static final long serialVersionUID = -4165403654001054963L;

    public EmptyUnfieldedTermExpansionException() {
        super();
    }

    public EmptyUnfieldedTermExpansionException(String message, Throwable cause) {
        super(message, cause);
    }

    public EmptyUnfieldedTermExpansionException(String message) {
        super(message);
    }

    public EmptyUnfieldedTermExpansionException(Throwable cause) {
        super(cause);
    }
}
