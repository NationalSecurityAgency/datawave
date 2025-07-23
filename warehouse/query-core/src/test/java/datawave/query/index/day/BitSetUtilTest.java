package datawave.query.index.day;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.BitSet;

import org.junit.jupiter.api.Test;

public class BitSetUtilTest {

    @Test
    public void testSimpleCreate() {
        BitSet bits = BitSetUtil.create(1, 2, 3);
        assertTrue(bits.get(1));
        assertTrue(bits.get(2));
        assertTrue(bits.get(3));
    }

    @Test
    public void testZeroIndex() {
        BitSet bits = BitSetUtil.create(0);
        assertTrue(bits.get(0));
    }
}
