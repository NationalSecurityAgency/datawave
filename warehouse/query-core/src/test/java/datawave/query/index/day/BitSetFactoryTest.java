package datawave.query.index.day;

import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.BitSet;

import org.junit.jupiter.api.Test;

public class BitSetFactoryTest {

    @Test
    public void testSimpleCreate() {
        BitSet bits = BitSetFactory.create(1, 2, 3);
        assertTrue(bits.get(1));
        assertTrue(bits.get(2));
        assertTrue(bits.get(3));
    }

    @Test
    public void testZeroIndex() {
        BitSet bits = BitSetFactory.create(0);
        assertTrue(bits.get(0));
    }
}
