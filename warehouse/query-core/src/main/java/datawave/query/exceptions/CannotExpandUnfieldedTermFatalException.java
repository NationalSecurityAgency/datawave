package datawave.query.exceptions;

public class CannotExpandUnfieldedTermFatalException extends DatawaveFatalQueryException {

    private static final long serialVersionUID = 7366849487621399507L;

    public CannotExpandUnfieldedTermFatalException() {
        super();
    }

    public CannotExpandUnfieldedTermFatalException(String message, Throwable cause) {
        super(message, cause);
    }

    public CannotExpandUnfieldedTermFatalException(String message) {
        super(message);
    }

    public CannotExpandUnfieldedTermFatalException(Throwable cause) {
        super(cause);
    }
}
