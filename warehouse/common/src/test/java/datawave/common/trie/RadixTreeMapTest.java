package datawave.common.trie;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertNull;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.util.Arrays;
import java.util.Iterator;
import java.util.Map;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

public class RadixTreeMapTest {

    private String[] data = null;
    private int[] sortedOrder = null;
    private RadixTreeMap<String> map = null;

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
        map = new RadixTreeMap();

        // adding in the data set multiple times to create underlying files with duplicate values
        int size = 0;
        for (int d = 0; d < 11; d++) {
            for (String datum : data) {
                String previous = map.put(datum, datum.repeat(2));
                if (d == 0) {
                    assertNull(datum, previous);
                } else {
                    assertEquals(datum.repeat(2), previous);
                }
                size = Math.min(data.length, size + 1);
                assertEquals(size, map.size());
            }
        }
    }

    @After
    public void tearDown() throws Exception {
        data = null;
        sortedOrder = null;
        map.clear();
        map = null;
    }

    @Test
    public void testSize() {
        int expectedSize = data.length;
        assertEquals(expectedSize, map.size());
        for (int i = (data.length / 2); i < data.length; i++) {
            map.remove(data[i]);
            expectedSize--;
            assertEquals(expectedSize, map.size());
        }
        for (int i = 0; i < (data.length / 2); i++) {
            map.remove(data[i]);
            expectedSize--;
            assertEquals(expectedSize, map.size());
        }
        assertEquals(0, map.size());
        for (int i = 0; i < data.length; i++) {
            map.put(data[i], data[i].repeat(3));
            expectedSize++;
            assertEquals(expectedSize, map.size());
        }
    }

    @Test
    public void testIsEmpty() {
        assertFalse(map.isEmpty());
        for (int i = (data.length / 2); i < data.length; i++) {
            map.remove(data[i]);
            assertFalse(map.isEmpty());
        }
        for (int i = 1; i < (data.length / 2); i++) {
            map.remove(data[i]);
            assertFalse(map.isEmpty());
        }
        map.remove(data[0]);
        assertTrue(map.isEmpty());
        for (int i = 0; i < data.length; i++) {
            map.put(data[i], data[i].repeat(3));
            assertFalse(map.isEmpty());
        }
    }

    @Test
    public void testClear() {
        map.clear();
        assertTrue(map.isEmpty());
    }

    @Test
    public void testContainsObject() {
        for (int i = (data.length / 2); i < data.length; i++) {
            map.remove(data[i]);
        }
        for (int i = 1; i < (data.length / 2); i++) {
            assertTrue(map.containsKey(data[i]));
        }
        for (int i = (data.length / 2); i < data.length; i++) {
            assertFalse(map.containsKey(data[i]));
        }
    }

    @Test
    public void testIterator() {
        int index = 0;
        for (Iterator<Map.Entry<String,String>> it = map.entrySet().iterator(); it.hasNext();) {
            Map.Entry<String,String> entry = it.next();
            String expected = data[sortedOrder[index++]];
            assertEquals(expected, entry.getKey());
            assertEquals(expected.repeat(2), entry.getValue());
        }
        map.clear();
        for (String value : map.keySet()) {
            fail();
        }
    }

    @Test
    public void testIteratorRemove() {
        int size = map.size();
        for (Iterator<Map.Entry<String,String>> it = map.entrySet().iterator(); it.hasNext();) {
            Map.Entry<String,String> entry = it.next();
            assertTrue(entry.getKey(), map.containsKey(entry.getKey()));
            it.remove();
            size--;
            assertFalse(entry.getKey(), map.containsKey(entry.getKey()));
            assertEquals(size, map.size());
        }
        assertTrue(map.isEmpty());
    }

}
