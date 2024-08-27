package datawave.iterators;

/**
 * Exception thrown if an interrupt flag is detected.
 */
public class IterationInterruptedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public IterationInterruptedException() {}

    public IterationInterruptedException(String msg) {
        super(msg);
    }
}
