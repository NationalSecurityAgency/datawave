package datawave.iterators;

/**
 * This code was repurposed from org.apache.accumulo.core.iteratorsImpl.system.IterationInterruptException.
 * It was not part of the public API, so we've created a DataWave equivalent.
 * This exception should be used in place of {@link org.apache.accumulo.core.iteratorsImpl.system.IterationInterruptedException} when thrown from an iterator.
 */
public class IterationInterruptException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public IterationInterruptException() {}

    public IterationInterruptException(String msg) {
        super(msg);
    }
}
