package datawave.accumulo.inmemory.util;

/**
 * Thrown by {@link SortedMapIterator} when its interrupt flag is set. This is a DataWave replacement for the non-public Accumulo
 * org.apache.accumulo.core.iteratorsImpl.system.IterationInterruptedException class, and is unchecked like it. It does not extend Accumulo's class, so code
 * that catches Accumulo's IterationInterruptedException does not catch this one.
 */
public class IterationInterruptedException extends RuntimeException {

    private static final long serialVersionUID = 1L;

    public IterationInterruptedException() {}

    public IterationInterruptedException(String msg) {
        super(msg);
    }
}
