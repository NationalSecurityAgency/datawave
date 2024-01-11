package datawave.common.trie;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Collections;
import java.util.Iterator;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RadixTreeSetTest {

    private String[] data = null;
    private int[] sortedOrder = null;
    private RadixTreeSet set = null;

    @Before
    public void setUp() throws Exception {
        byte[] template = new byte[] {5, 2, 25, 4, 8, 3, 24, 23, 6, 21, 7, 16};
        int[] sortedTemplate = new int[] {1, 5, 3, 0, 8, 10, 4, 11, 9, 7, 6, 2};
        data = new String[template.length * 2];
        for (int i = 0; i < template.length; i++) {
            byte[] buffer = new byte[i + 11];
            Arrays.fill(buffer, (byte) (template[i] + '0'));
            data[i] = new String(buffer);
        }
        for (int i = 0; i < template.length; i++) {
            byte[] buffer = new byte[10];
            Arrays.fill(buffer, (byte) (template[i] + '0'));
            data[i + template.length] = new String(buffer);
        }
        sortedOrder = new int[data.length];
        for (int i = 0; i < template.length; i++) {
            sortedOrder[i * 2] = sortedTemplate[i] + sortedTemplate.length;
            sortedOrder[i * 2 + 1] = sortedTemplate[i];
        }
        set = new RadixTreeSet();

        // adding in the data set multiple times to create underlying files with duplicate values
        int size = 0;
        for (int d = 0; d < 11; d++) {
            for (String datum : data) {
                boolean added = set.add(datum);
                if (d == 0) {
                    assertTrue(datum, added);
                } else {
                    assertFalse(datum, added);
                }
                size = Math.min(data.length, size + 1);
                assertEquals(size, set.size());
            }
        }
    }

    @After
    public void tearDown() throws Exception {
        data = null;
        sortedOrder = null;
        set.clear();
        set = null;
    }

    @Test
    public void testSize() {
        int expectedSize = data.length;
        assertEquals(expectedSize, set.size());
        for (int i = (data.length / 2); i < data.length; i++) {
            set.remove(data[i]);
            expectedSize--;
            assertEquals(expectedSize, set.size());
        }
        for (int i = 0; i < (data.length / 2); i++) {
            set.remove(data[i]);
            expectedSize--;
            assertEquals(expectedSize, set.size());
        }
        assertEquals(0, set.size());
        for (int i = 0; i < data.length; i++) {
            set.add(data[i]);
            expectedSize++;
            assertEquals(expectedSize, set.size());
        }
    }

    @Test
    public void testIsEmpty() {
        assertFalse(set.isEmpty());
        for (int i = (data.length / 2); i < data.length; i++) {
            set.remove(data[i]);
            assertFalse(set.isEmpty());
        }
        for (int i = 1; i < (data.length / 2); i++) {
            set.remove(data[i]);
            assertFalse(set.isEmpty());
        }
        set.remove(data[0]);
        assertTrue(set.isEmpty());
        for (int i = 0; i < data.length; i++) {
            set.add(data[i]);
            assertFalse(set.isEmpty());
        }
    }

    @Test
    public void testClear() {
        set.clear();
        assertTrue(set.isEmpty());
    }

    @Test
    public void testContainsObject() {
        for (int i = (data.length / 2); i < data.length; i++) {
            set.remove(data[i]);
        }
        for (int i = 1; i < (data.length / 2); i++) {
            assertTrue(set.contains(data[i]));
        }
        for (int i = (data.length / 2); i < data.length; i++) {
            assertFalse(set.contains(data[i]));
        }
    }

    @Test
    public void testIterator() {
        int index = 0;
        for (Iterator<String> it = set.iterator(); it.hasNext();) {
            String value = it.next();
            String expected = data[sortedOrder[index++]];
            assertEquals(expected, value);
        }
        set.clear();
        for (String value : set) {
            fail();
        }
    }

    @Test
    public void testIteratorRemove() {
        int size = set.size();
        for (Iterator<String> it = set.iterator(); it.hasNext();) {
            String value = it.next();
            assertTrue(set.contains(value));
            it.remove();
            size--;
            assertFalse(set.contains(value));
            assertEquals(size, set.size());
        }
        assertTrue(set.isEmpty());
    }

}
