package datawave.edge.util;

import java.io.IOException;

import org.apache.accumulo.core.data.Value;

import com.clearspring.analytics.stream.cardinality.HyperLogLogPlus;

/**
 * This class wraps the com.clearspring.analytics.stream.cardinality.HyperLogLogPlus class to create a fixed HyperLogLog with a P of 12 and SP of 20. It will
 * force the HyperLogLog to it's "normaL' mode once the cardinality exceeds 750. This is done to ensure the size never exceeds the the size of a normal
 * HyperLogLog (which is 2,173 bytes)
 */
public class ExtendedHyperLogLogPlus {
    private static final HyperLogLogPlus NORMAL_HHLP;
    public static final int MAX_HLLP_BYTES;
    public static final long CARDINILITY_THRESHOLD = 750;
    public static final int SP = 20;
    public static final int P = 12;
    private HyperLogLogPlus hllp = null;
    private boolean exceededCardinalityThreshold = false;

    static {
        try {
            NORMAL_HHLP = new HyperLogLogPlus(P);
            MAX_HLLP_BYTES = NORMAL_HHLP.getBytes().length;
        } catch (final IOException e) {
            throw (new RuntimeException(e)); // TODO better exception?????
        }
    }

    /**
     * Default constructor with a P of 12 and and SP of 20
     */
    public ExtendedHyperLogLogPlus() {
        hllp = new HyperLogLogPlus(P, SP);
    }

    /**
     * Construct that uses the serialized bytes from the Value. This will not verify that the HyperLogLogPlus being created is the proper size.
     *
     * @param value
     *            A Value that contains the serialized bytes from an ExtendedHyperLogLogPlus object.
     * @throws IOException
     *             Error processing the byte array.
     */
    public ExtendedHyperLogLogPlus(final Value value) throws IOException {
        hllp = HyperLogLogPlus.Builder.build(value.get());
    }

    /**
     * Reset to it's initial value. The cardinality will be zero.
     */
    public void clear() {
        hllp = new HyperLogLogPlus(P, SP);
    }

    /**
     * Get the current cardinality estimate.
     *
     * @return The current cardinality estimate.
     */
    public long getCardinality() {
        return (hllp.cardinality());
    }

    /**
     * Add data to estimator based on the mode it is in.
     *
     * @param object
     *            Object to be added to the HyperLogLog
     * @return the result of the offer attempt
     */
    public boolean offer(final Object object) {
        final boolean result = hllp.offer(object);

        if (!exceededCardinalityThreshold && (hllp.cardinality() > CARDINILITY_THRESHOLD)) {
            try {
                hllp.addAll(NORMAL_HHLP);

                exceededCardinalityThreshold = true;
            } catch (final Exception e) {
                throw (new IllegalStateException(e));
            }
        }

        return (result);
    }

    /**
     * Add all the elements of the other set to this set. If possible, the sparse mode is protected. A switch to the normal mode is triggered only if the
     * resulting set exceed the threshold. This operation does not imply a loss of precision.
     *
     * @param ehll
     *            The ExtendedHyperLogLogPlus to be added to the current instance.
     * @throws IOException
     *             Error adding the HyperLogLog, most likely due to mismatched sizes.
     */
    public void addAll(final ExtendedHyperLogLogPlus ehll) throws IOException {
        try {
            hllp.addAll(ehll.hllp);
        } catch (final Exception e) {
            throw (new IOException(e));
        }
    }

    /**
     * Serialize the ExtendedHyperLogLogPlus to a byte array.
     *
     * @return The ExtendedHyperLogLogPlus to a byte array.
     * @throws IOException
     *             Error creating the byte array.
     */
    public byte[] getBytes() throws IOException {
        return (hllp.getBytes());
    }
}
