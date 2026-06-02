package datawave.query.util.sortedmap;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedMap;
import java.util.TreeMap;

import org.apache.commons.collections.keyvalue.UnmodifiableMapEntry;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public abstract class MultiMapBackedSortedMapTest<K,V> {

    protected final List<SortedMapTempFileHandler> tempFileHandlers = new ArrayList<>();
    protected Map.Entry<K,V>[] data = null;
    protected int[] sortedOrder = null;
    protected MultiMapBackedSortedMap<K,V> map = null;
    protected List<Map> maps = null;
    protected final byte[] template = new byte[] {5, 2, 29, 4, 8, 3, 25, 23, 6, 21, 7, 16};
    protected final int[] sortedTemplate = new int[] {1, 5, 3, 0, 8, 10, 4, 11, 9, 7, 6, 2};

    public abstract Map.Entry<K,V> createData(byte[] values);

    public abstract Comparator<K> getComparator();

    public FileSortedMap.RewriteStrategy<K,V> getRewriteStrategy() {
        return null;
    }

    protected void testEquality(SortedMap<K,V> m1, SortedMap<K,V> m2) {
        for (Map.Entry<K,V> e1 : m1.entrySet()) {
            V v2 = m2.get(e1.getKey());
            testEquality(e1, new UnmodifiableMapEntry(e1.getKey(), v2));
        }
    }

    protected void testEquality(Map.Entry<K,V> expected, Map.Entry<K,V> value) {
        testEquality(expected.getKey(), value.getKey());
        assertEquals(expected.getValue(), value.getValue());
    }

    protected void testEquality(K expected, K value) {
        if (map.comparator() != null) {
            assertEquals(0, map.comparator().compare(expected, value));
        } else {
            assertEquals(expected, value);
        }
    }

    @Before
    public void mapUp() throws Exception {
        for (int i = 0; i < template.length; i++) {
            byte[] buffer = new byte[i + 11];
            Arrays.fill(buffer, template[i]);
            Map.Entry<K,V> datum = createData(buffer);
            if (i == 0) {
                data = (Map.Entry[]) Array.newInstance(datum.getClass(), template.length * 2);
            }
            data[i] = datum;
        }
        for (int i = 0; i < template.length; i++) {
            byte[] buffer = new byte[i + 10];
            Arrays.fill(buffer, template[i]);
            Map.Entry<K,V> datum = createData(buffer);
            data[i + template.length] = datum;
        }
        sortedOrder = new int[data.length];
        for (int i = 0; i < template.length; i++) {
            sortedOrder[i * 2] = sortedTemplate[i] + sortedTemplate.length;
            sortedOrder[i * 2 + 1] = sortedTemplate[i];
        }
        maps = new ArrayList<>();
        for (int i = 0; i < 7; i++) {
            maps.add(new TreeMap(getComparator()));
        }
        map = new MultiMapBackedSortedMap(maps);
        map.setRewriteStrategy(getRewriteStrategy());

        // adding in the data map multiple times to create duplicate values across the underlying maps
        // MergeSortIterator's job a little tougher...
        for (int i = 0; i < 4; i++) {
            for (int d = 0; d < 11; d++) {
                addDataRandomly(maps, data);
            }
        }
    }

    public void putRandomly(List<Map> maps, K key, V value) {
        Random random = new Random();
        int mapIndex = random.nextInt(maps.size());
        maps.get(mapIndex).put(key, value);
    }

    public void addDataRandomly(List<Map> maps, Map.Entry<K,V>[] data) {
        Set<Integer> added = new HashSet<>();
        Random random = new Random();
        // add data.length items randomly
        for (int i = 0; i < data.length; i++) {
            int index = random.nextInt(data.length);
            putRandomly(maps, data[index].getKey(), data[index].getValue());
            added.add(index);
        }
        // ensure all missing items are added
        for (int i = 0; i < data.length; i++) {
            if (!added.contains(i)) {
                putRandomly(maps, data[i].getKey(), data[i].getValue());
            }
        }
    }

    @After
    public void tearDown() throws Exception {
        // Delete each sorted map file and its checksum.
        for (SortedMapTempFileHandler fileHandler : tempFileHandlers) {
            File file = fileHandler.getFile();
            tryDelete(file);
            File checksum = new File(file.getParent(), "." + file.getName() + ".crc");
            tryDelete(checksum);
        }
        tempFileHandlers.clear();

        data = null;
        sortedOrder = null;
        map.clear();
        map = null;
    }

    private void tryDelete(File file) {
        if (file.exists()) {
            Assert.assertTrue("Failed to delete file " + file, file.delete());
        }
    }

    @Test
    public void testSize() {
        int expectedSize = data.length;
        assertEquals(expectedSize, map.size());
        for (int i = (data.length / 2); i < data.length; i++) {
            map.remove(data[i].getKey());
            expectedSize--;
            assertEquals(expectedSize, map.size());
        }
        for (int i = 0; i < (data.length / 2); i++) {
            map.remove(data[i].getKey());
            expectedSize--;
            assertEquals(expectedSize, map.size());
        }
        assertEquals(0, map.size());
        for (int i = 0; i < data.length; i++) {
            putRandomly(maps, data[i].getKey(), data[i].getValue());
            expectedSize++;
            assertEquals(expectedSize, map.size());
        }
    }

    @Test
    public void testIsEmpty() {
        assertFalse(map.isEmpty());
        for (int i = (data.length / 2); i < data.length; i++) {
            map.remove(data[i].getKey());
            assertFalse(map.isEmpty());
        }
        for (int i = 1; i < (data.length / 2); i++) {
            map.remove(data[i].getKey());
            assertFalse(map.isEmpty());
        }
        map.remove(data[0].getKey());
        assertTrue(map.isEmpty());
        for (int i = 0; i < data.length; i++) {
            putRandomly(maps, data[i].getKey(), data[i].getValue());
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
            map.remove(data[i].getKey());
        }
        for (int i = 1; i < (data.length / 2); i++) {
            assertTrue(map.containsKey(data[i].getKey()));
        }
        for (int i = (data.length / 2); i < data.length; i++) {
            assertFalse(map.containsKey(data[i].getKey()));
        }
    }

    @Test
    public void testRemove() {
        int expectedSize = data.length;

        for (int i = 0; i < data.length; i++) {
            map.remove(data[i].getKey());
            assertEquals(--expectedSize, map.size());
        }
        assertTrue(map.isEmpty());
    }

    @Test
    public void testIterator() {
        int index = 0;
        for (Iterator<Map.Entry<K,V>> it = map.entrySet().iterator(); it.hasNext();) {
            Map.Entry<K,V> value = it.next();
            Map.Entry<K,V> expected = data[sortedOrder[index++]];
            testEquality(expected, value);
        }
        map.clear();
        for (Map.Entry<K,V> value : map.entrySet()) {
            fail();
        }
    }

    @Test
    public void testIteratorRemove() {
        int size = map.size();

        for (Iterator<Map.Entry<K,V>> it = map.entrySet().iterator(); it.hasNext();) {
            Map.Entry<K,V> value = it.next();
            assertTrue(map.containsKey(value.getKey()));
            it.remove();
            size--;
            assertFalse(map.containsKey(value.getKey()));
            assertEquals(size, map.size());
        }
        assertTrue(map.isEmpty());
    }

    @Test
    public void testComparator() {
        final Comparator<? super K> comparator = map.comparator();
        Map.Entry<K,V>[] testData = Arrays.copyOf(data, data.length);
        Arrays.sort(testData, new Comparator<Map.Entry<K,V>>() {
            @Override
            public int compare(Map.Entry<K,V> o1, Map.Entry<K,V> o2) {
                return comparator.compare(o1.getKey(), o2.getKey());
            }
        });
        int index = 0;
        for (Map.Entry<K,V> value : map.entrySet()) {
            Map.Entry<K,V> expected = data[sortedOrder[index++]];
            testEquality(expected, value);
        }
    }

    @Test
    public void testSubmap() {
        int start = sortedOrder.length / 3;
        int end = start * 2;
        try {
            SortedMap<K,V> submap = map.subMap(data[sortedOrder[start]].getKey(), data[sortedOrder[end]].getKey());
            SortedMap<K,V> expected = new TreeMap<>(map.comparator());
            for (int i = start; i < end; i++) {
                expected.put(data[sortedOrder[i]].getKey(), data[sortedOrder[i]].getValue());
            }
            testEquality(expected, submap);
        } catch (Exception e) {
            // expected
        }
    }

    @Test
    public void testHeadmap() {
        int end = sortedOrder.length / 3;
        try {
            SortedMap<K,V> submap = map.headMap(data[sortedOrder[end]].getKey());
            SortedMap<K,V> expected = new TreeMap<>(map.comparator());
            for (int i = 0; i < end; i++) {
                expected.put(data[sortedOrder[i]].getKey(), data[sortedOrder[i]].getValue());
            }
            testEquality(expected, submap);
        } catch (Exception e) {
            // expected
        }
    }

    @Test
    public void testTailmap() {
        int start = sortedOrder.length / 3;
        try {
            SortedMap<K,V> submap = map.tailMap(data[sortedOrder[start]].getKey());
            SortedMap<K,V> expected = new TreeMap<>(map.comparator());
            for (int i = start; i < sortedOrder.length; i++) {
                expected.put(data[sortedOrder[i]].getKey(), data[sortedOrder[i]].getValue());
            }
            testEquality(expected, submap);
        } catch (Exception e) {
            // expected
        }
    }

    @Test
    public void testLastKey() {
        Map.Entry<K,V> expected = data[sortedOrder[data.length - 1]];
        K value = map.lastKey();
        testEquality(expected.getKey(), value);
    }

    @Test
    public void testFirstKey() {
        Map.Entry<K,V> expected = data[sortedOrder[0]];
        K value = map.firstKey();
        testEquality(expected.getKey(), value);
    }

}
