package datawave.ingest.mapreduce.handler.dateindex;

import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.BitSet;
import java.util.HashSet;
import java.util.Set;

import org.junit.jupiter.api.Test;

/**
 *
 */
public class DateIndexUtilTest {

    @Test
    public void testGetBits() {
        BitSet bits = DateIndexUtil.getBits(20);
        for (int i = 0; i < 20; i++) {
            assertFalse(bits.get(i));
        }
        assertTrue(bits.get(20));
        for (int i = 21; i < bits.size(); i++) {
            assertFalse(bits.get(i));
        }
    }

    @Test
    public void testMerge() {

        Set<Integer> bits = new HashSet<>();
        for (int i = 0; i < 40; i++) {
            bits.add(i);
            bits.add(i * 2);
            bits.add(i * 3);
        }

        BitSet bitSet1 = new BitSet(1);
        BitSet bitSet2 = new BitSet(1);
        int count = 0;
        for (Integer i : bits) {
            if (count % 2 == 0) {
                bitSet1 = DateIndexUtil.merge(bitSet1, DateIndexUtil.getBits(i));
            } else {
                bitSet2 = DateIndexUtil.merge(bitSet2, DateIndexUtil.getBits(i));
            }
        }
        BitSet bitSet = DateIndexUtil.merge(bitSet1, bitSet2);

        for (int i = 0; i < 40 * 4; i++) {
            if (bits.contains(i)) {
                assertTrue(bitSet.get(i));
            } else {
                assertFalse(bitSet.get(i));
            }
        }
    }

}
