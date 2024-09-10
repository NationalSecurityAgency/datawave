package datawave.query.jexl.lookups;

/**
 * An exception denoting that the threshold has been exceeded in the ValueList and subsequently the called method cannot be invoked
 */
public class ExceededThresholdException extends RuntimeException {

    private static final long serialVersionUID = 5800693060770418223L;

    public ExceededThresholdException() {
        super();
    }

    public ExceededThresholdException(String message, Throwable cause) {
        super(message, cause);
    }

    public ExceededThresholdException(String message) {
        super(message);
    }

    public ExceededThresholdException(Throwable cause) {
        super(cause);
    }

}
