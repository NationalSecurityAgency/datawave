package datawave.query.util.sortedmap;

import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.Value;
import org.apache.commons.collections.keyvalue.UnmodifiableMapEntry;
import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class RewritableSortedSetTest {

    private Map.Entry<Key,Value>[] data = null;
    private int[] sortedOrder = null;
    private RewritableSortedSetImpl<Map.Entry<Key,Value>> set = null;
    private final byte[] template = new byte[] {5, 2, 29, 4, 8, 3, 25, 23, 6, 21, 7, 16};
    private final int[] sortedTemplate = new int[] {1, 5, 3, 0, 8, 10, 4, 11, 9, 7, 6, 2};

    private Comparator<Map.Entry<Key,Value>> keyComparator = new Comparator<>() {
        @Override
        public int compare(Map.Entry<Key,Value> o1, Map.Entry<Key,Value> o2) {
            return o1.getKey().compareTo(o2.getKey());
        }
    };

    private RewritableSortedSetImpl.RewriteStrategy<Map.Entry<Key,Value>> keyValueComparator = new RewritableSortedSetImpl.RewriteStrategy<>() {
        @Override
        public boolean rewrite(Map.Entry<Key,Value> original, Map.Entry<Key,Value> update) {
            int comparison = original.getKey().compareTo(update.getKey());
            if (comparison == 0) {
                comparison = original.getValue().compareTo(update.getValue());
            }
            return comparison < 0;
        }
    };

    @Before
    public void setUp() throws Exception {
        data = new Map.Entry[template.length * 2];
        for (int i = 0; i < template.length; i++) {
            byte[] buffer = new byte[i + 11];
            byte[] vbuffer = new byte[i + 11];
            Arrays.fill(buffer, template[i]);
            Arrays.fill(vbuffer, (byte) (template[i] + 1));
            data[i] = new UnmodifiableMapEntry(new Key(buffer), new Value(vbuffer));
        }
        for (int i = 0; i < template.length; i++) {
            byte[] buffer = new byte[i + 10];
            byte[] vbuffer = new byte[i + 10];
            Arrays.fill(buffer, template[i]);
            Arrays.fill(vbuffer, (byte) (template[i] + 1));
            data[i + template.length] = new UnmodifiableMapEntry(new Key(buffer), new Value(vbuffer));
        }
        sortedOrder = new int[data.length];
        for (int i = 0; i < template.length; i++) {
            sortedOrder[i * 2] = sortedTemplate[i] + sortedTemplate.length;
            sortedOrder[i * 2 + 1] = sortedTemplate[i];
        }
        set = new RewritableSortedSetImpl<>(keyComparator, keyValueComparator);

        // adding in the data set multiple times to create underlying files with duplicate values making the
        // MergeSortIterator's job a little tougher...
        for (int d = 0; d < 11; d++) {
            addDataRandomly(set, data);
        }
    }

    private void addDataRandomly(RewritableSortedSetImpl<Map.Entry<Key,Value>> set, Map.Entry<Key,Value>[] data) {
        Set<Integer> added = new HashSet<>();
        // add data until all of the entries have been added
        Random random = new Random();
        while (added.size() < data.length) {
            int i = random.nextInt(data.length);
            set.add(data[i]);
            added.add(i);
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
        for (Iterator<Map.Entry<Key,Value>> it = set.iterator(); it.hasNext();) {
            Map.Entry<Key,Value> value = it.next();
            Map.Entry<Key,Value> expected = data[sortedOrder[index++]];
            assertEquals(expected, value);
        }
        set.clear();
        for (Map.Entry<Key,Value> value : set) {
            fail();
        }
    }

    @Test
    public void testIteratorRemove() {
        int size = set.size();
        for (Iterator<Map.Entry<Key,Value>> it = set.iterator(); it.hasNext();) {
            Map.Entry<Key,Value> value = it.next();
            assertTrue(set.contains(value));
            it.remove();
            size--;
            assertEquals(size, set.size());
        }
        assertEquals(0, size);
        assertTrue(set.isEmpty());
    }

    @Test
    public void testSubSet() {
        int start = sortedOrder.length / 3;
        int end = start * 2;
        try {
            SortedSet<Map.Entry<Key,Value>> subSet = set.subSet(data[sortedOrder[start]], data[sortedOrder[end]]);
            SortedSet<Map.Entry<Key,Value>> expected = new TreeSet<>();
            for (int i = start; i < end; i++) {
                expected.add(data[sortedOrder[i]]);
            }
            assertEquals(expected, subSet);
        } catch (Exception e) {
            // expected
        }
    }

    @Test
    public void testHeadSet() {
        int end = sortedOrder.length / 3;
        try {
            SortedSet<Map.Entry<Key,Value>> subSet = set.headSet(data[sortedOrder[end]]);
            SortedSet<Map.Entry<Key,Value>> expected = new TreeSet<>();
            for (int i = 0; i < end; i++) {
                expected.add(data[sortedOrder[i]]);
            }
            assertEquals(expected, subSet);
        } catch (Exception e) {
            // expected
        }
    }

    @Test
    public void testTailSet() {
        int start = sortedOrder.length / 3;
        try {
            SortedSet<Map.Entry<Key,Value>> subSet = set.tailSet(data[sortedOrder[start]]);
            SortedSet<Map.Entry<Key,Value>> expected = new TreeSet<>();
            for (int i = start; i < sortedOrder.length; i++) {
                expected.add(data[sortedOrder[i]]);
            }
            assertEquals(expected, subSet);
        } catch (Exception e) {
            // expected
        }
    }

    @Test
    public void testLast() {
        Map.Entry<Key,Value> expected = data[sortedOrder[data.length - 1]];
        Map.Entry<Key,Value> value = set.last();
        assertEquals(expected, value);
    }

    @Test
    public void testFirst() {
        Map.Entry<Key,Value> expected = data[sortedOrder[0]];
        Map.Entry<Key,Value> value = set.first();
        assertEquals(expected, value);
    }

    @Test
    public void testRewrite() {
        // create a new set of data, half of which has greater Values and half of which has lesser Values
        Map.Entry<Key,Value>[] data2 = new Map.Entry[template.length * 2];
        for (int i = 0; i < template.length; i++) {
            byte[] buffer = new byte[i + 11];
            byte[] vbuffer = new byte[i + 11];
            Arrays.fill(buffer, template[i]);
            Arrays.fill(vbuffer, (byte) (template[i] + 2));
            data2[i] = new UnmodifiableMapEntry(new Key(buffer), new Value(vbuffer));
        }
        for (int i = 0; i < template.length; i++) {
            byte[] buffer = new byte[i + 10];
            byte[] vbuffer = new byte[i + 10];
            Arrays.fill(buffer, template[i]);
            Arrays.fill(vbuffer, (byte) (template[i] - 1));
            data2[i + template.length] = new UnmodifiableMapEntry(new Key(buffer), new Value(vbuffer));
        }

        for (int d = 0; d < 11; d++) {
            addDataRandomly(set, data2);
        }

        // now test the contents
        int index = 0;
        for (Iterator<Map.Entry<Key,Value>> it = set.iterator(); it.hasNext();) {
            Map.Entry<Key,Value> value = it.next();
            int dataIndex = sortedOrder[index++];
            Map.Entry<Key,Value> expected = (dataIndex < template.length ? data2[dataIndex] : data[dataIndex]);
            assertEquals(expected, value);
        }

    }

}
