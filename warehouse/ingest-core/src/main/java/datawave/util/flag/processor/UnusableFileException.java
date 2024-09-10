package datawave.util.flag.processor;

/**
 *
 */
public class UnusableFileException extends Exception {

    private static final long serialVersionUID = -7751023408779407520L;

    public UnusableFileException() {
        super();
    }

    public UnusableFileException(String message, Throwable cause) {
        super(message, cause);
    }

    public UnusableFileException(String message) {
        super(message);
    }

    public UnusableFileException(Throwable cause) {
        super(cause);
    }

}
