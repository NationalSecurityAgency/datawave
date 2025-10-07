package datawave.next;

/**
 * Temporary class until we fully migrate to commons lang3
 */
public class LongRange {

    private final long start;
    private final long stop;

    public LongRange(long start, long stop) {
        this.start = start;
        this.stop = stop;
    }

    public static LongRange of(long start, long stop) {
        return new LongRange(start, stop);
    }

    public boolean contains(long value) {
        return value >= start && value <= stop;
    }
}
