package datawave.query.util.sortedset;

import static org.junit.Assert.assertTrue;

import java.util.Random;

import org.junit.Test;

public class ByteArrayComparatorTest {
    ByteArrayComparator comparator = new ByteArrayComparator();

    @Test
    public void testCompareEqualLen() {
        Random random = new Random();
        byte[] bytes1 = new byte[20];
        byte[] bytes2 = new byte[20];
        random.nextBytes(bytes1);
        System.arraycopy(bytes1, 0, bytes2, 0, 20);
        assertTrue(comparator.compare(bytes1, bytes2) == 0);
        assertTrue(comparator.compare(bytes2, bytes1) == 0);

        bytes1[10] = (byte) 10;
        bytes2[10] = (byte) 11;

        assertTrue(comparator.compare(bytes1, bytes2) < 0);
        assertTrue(comparator.compare(bytes2, bytes1) > 0);

        bytes1[10] = (byte) -181; // as an unsigned byte this should be greater than 11

        assertTrue(comparator.compare(bytes1, bytes2) > 0);
        assertTrue(comparator.compare(bytes2, bytes1) < 0);
    }

    @Test
    public void testCompareUnequalLen() {
        Random random = new Random();
        byte[] bytes1 = new byte[20];
        byte[] bytes2 = new byte[30];
        random.nextBytes(bytes1);
        System.arraycopy(bytes1, 0, bytes2, 0, 20);

        assertTrue(comparator.compare(bytes1, bytes2) < 0);
        assertTrue(comparator.compare(bytes2, bytes1) > 0);

        bytes2[10] = (byte) 11;
        bytes1[10] = (byte) -181; // as an unsigned byte this should be greater than 11

        assertTrue(comparator.compare(bytes1, bytes2) > 0);
        assertTrue(comparator.compare(bytes2, bytes1) < 0);
    }

}
