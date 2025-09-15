package datawave.query.index.day;

import java.util.BitSet;

/**
 * Simple utility to create a {@link BitSet} from a list of integer values
 */
public class BitSetFactory {

    private BitSetFactory() {
        // enforce static access
    }

    /**
     * Create a {@link BitSet} given a list of values
     *
     * @param values
     *            a list of integers
     * @return a BitSet
     */
    public static BitSet create(int... values) {
        BitSet bits = new BitSet();
        for (int value : values) {
            bits.set(value);
        }
        return bits;
    }
}
