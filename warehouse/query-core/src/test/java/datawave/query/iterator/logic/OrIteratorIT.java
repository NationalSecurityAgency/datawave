package datawave.query.iterator.logic;

import datawave.query.iterator.NestedIterator;
import datawave.query.iterator.SeekableIterator;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Range;
import org.junit.jupiter.api.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.HashSet;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertTrue;
import static org.junit.jupiter.api.Assertions.fail;

class OrIteratorIT {

    private final SortedSet<String> uidsAll = new TreeSet<>(Arrays.asList("1", "2", "3", "4", "5", "6", "7", "8", "9", "10"));
    private final SortedSet<String> uidsEven = new TreeSet<>(Arrays.asList("2", "4", "6", "8", "10"));
    private final SortedSet<String> uidsOdd = new TreeSet<>(Arrays.asList("1", "3", "5", "7", "9"));

    @Test
    void testSimpleUnion() {
        Set<NestedIterator<Key>> includes = new HashSet<>();
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_A", uidsEven));
        includes.add(IndexIteratorBridgeTest.createIndexIteratorBridge("FIELD_B", uidsOdd));

        seekIterators(includes);

        OrIterator itr = new OrIterator(includes);
        driveIterator(itr, new TreeSet<>(uidsAll));
    }

    /**
     * Drives the union given a set of expected uids
     *
     * @param itr
     *            the union
     * @param uids
     *            expected uids
     */
    private void driveIterator(OrIterator itr, SortedSet<String> uids) {
        itr.initialize();

        int count = 0;
        for (String uid : uids) {
            count++;
            assertTrue(itr.hasNext());

            Object o = itr.next();
            assertTrue(o instanceof Key);
            IndexIteratorTest.assertTopKey((Key) o, uid);

            // TODO - document field assertions
        }

        assertFalse(itr.hasNext());
        assertEquals(uids.size(), count);
    }

    /**
     * Because the OrIterator is not seekable, this helper method will seek any underlying iterators
     *
     * @param iterators
     *            the set of iterators within a union
     */
    protected static void seekIterators(Set<NestedIterator<Key>> iterators) {
        for (NestedIterator<Key> iterator : iterators) {
            if (iterator instanceof SeekableIterator) {
                try {
                    ((SeekableIterator) iterator).seek(new Range(), Collections.emptyList(), false);
                } catch (IOException e) {
                    fail("Could not seek iterator during test setup");
                }
            }
        }
    }
}
