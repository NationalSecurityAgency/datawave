package datawave.util.ssdeep;

import static datawave.util.ssdeep.HashReverse.getPrefixIndex;
import static datawave.util.ssdeep.HashReverse.getPrefixMax;
import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertTrue;

import java.util.Set;
import java.util.TreeSet;

import org.junit.Assert;
import org.junit.Test;

public class HashReverseTest {

    @Test
    public void testSingleDigits() {
        assertEquals(0, getPrefixIndex("+", 1));
        assertEquals(1, getPrefixIndex("/", 1));
        assertEquals(38, getPrefixIndex("a", 1));
        assertEquals(62, getPrefixIndex("y", 1));
        assertEquals(63, getPrefixIndex("z", 1));
    }

    @Test
    public void testDoubleDigits() {
        assertEquals(0, getPrefixIndex("++", 2));
        assertEquals(1, getPrefixIndex("+/", 2));
        assertEquals(62, getPrefixIndex("+y", 2));
        assertEquals(63, getPrefixIndex("+z", 2));
        assertEquals(64, getPrefixIndex("/+", 2));
        assertEquals(65, getPrefixIndex("//", 2));
        assertEquals(3968 + 62, getPrefixIndex("yy", 2));
        assertEquals(4032 + 63, getPrefixIndex("zz", 2));
    }

    @Test
    public void textPrefixes() {
        assertEquals(0, getPrefixIndex("+/", 1));
        assertEquals(65, getPrefixIndex("//01", 2));
        assertEquals(4032 + 63, getPrefixIndex("zzzz", 2));
    }

    @Test
    public void testRange() {
        assertTrue(getPrefixIndex("zzzzz", 5) < Integer.MAX_VALUE);
        assertTrue(getPrefixIndex("zzzzz", 5) > 0);
    }

    @Test
    public void testMinMax() {
        assertEquals(HashReverse.MIN_VALUE, getPrefixIndex(HashReverse.MAX_HASH, 0));
        assertEquals(HashReverse.MAX_VALUE, getPrefixIndex(HashReverse.MAX_HASH, HashReverse.MAX_PREFIX_SIZE));
    }

    @Test
    public void testPartitionMap() {
        // validate that the range for given hashes can be projected to a complete smaller range.
        int buckets = 250;

        int max = getPrefixMax(2);
        float ratio = ((float) buckets) / ((float) max);

        // fill the buckets with observed values
        boolean[] seen = new boolean[buckets];
        for (int i = 0; i < max; i++) {
            int bucket = (int) (i * ratio);
            seen[bucket] = true;
        }

        Set<Integer> missed = new TreeSet<>();
        for (int i = 0; i < buckets; i++) {
            if (!seen[i]) {
                missed.add(i);
            }
        }
        Assert.assertTrue("Missed mapping to partitions: " + missed, missed.isEmpty());
    }

    @Test(expected = SSDeepParseException.class)
    public void testSDeepParseException() {
        assertEquals(63, getPrefixIndex("*", 1));
    }

    @Test(expected = IndexOutOfBoundsException.class)
    public void testOutOfBoundsException() {
        assertEquals(63, getPrefixIndex("++++++", 6));
    }
}
