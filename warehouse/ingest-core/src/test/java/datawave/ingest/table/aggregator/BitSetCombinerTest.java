package datawave.ingest.table.aggregator;

import static org.junit.jupiter.api.Assertions.assertEquals;

import java.util.ArrayList;
import java.util.BitSet;
import java.util.List;

import org.apache.accumulo.core.data.Value;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

class BitSetCombinerTest {

    private final BitSetCombiner combiner = new BitSetCombiner();

    private final List<Value> values = new ArrayList<>();

    @BeforeEach
    public void setup() {
        values.clear();
    }

    @Test
    public void testCombineOneTwo() {
        values.add(createValue(new int[] {0}));
        values.add(createValue(new int[] {1}));

        Value expected = createValue(new int[] {0, 1});
        test(expected, values);
    }

    @Test
    public void testCombineEvensAndOdds() {
        values.add(createValue(new int[] {0, 2, 4, 6, 8}));
        values.add(createValue(new int[] {1, 3, 5, 7, 9}));

        Value expected = createValue(new int[] {0, 1, 2, 3, 4, 5, 6, 7, 8, 9});
        test(expected, values);
    }

    @Test
    public void testCombineDisparateNumbers() {
        values.add(createValue(new int[] {0, 1, 2}));
        values.add(createValue(new int[] {1024, 1025, 1026}));

        Value expected = createValue(new int[] {0, 1, 2, 1024, 1025, 1026});
        test(expected, values);
    }

    @Test
    public void testCombineFull() {
        int len = 2048;
        int[] indices = new int[len];
        for (int i = 0; i < len; i++) {
            indices[i] = i;
        }

        values.add(createValue(indices));
        values.add(createValue(indices));
        values.add(createValue(indices));

        Value expected = createValue(indices);
        test(expected, values);
    }

    private void test(Value expected, List<Value> values) {
        Value combined = combiner.reduce(null, values.iterator());
        BitSet combinedBits = BitSet.valueOf(combined.get());
        BitSet expectedBits = BitSet.valueOf(expected.get());
        assertEquals(expectedBits, combinedBits);
    }

    private Value createValue(int[] indices) {
        BitSet bits = createBitSet(indices);
        return createValue(bits);
    }

    private Value createValue(BitSet bits) {
        return new Value(bits.toByteArray());
    }

    private BitSet createBitSet(int[] indices) {
        BitSet bits = new BitSet();
        for (int index : indices) {
            bits.set(index);
        }
        return bits;
    }
}
