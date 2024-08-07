package datawave.query.util.sortedmap;

import datawave.query.util.sortedmap.BufferedFileBackedSortedMap;
import datawave.query.util.sortedmap.SortedMapTempFileHandler;
import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Random;
import java.util.Map;
import java.util.SortedMap;
import java.util.TreeMap;
import java.util.function.Predicate;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public abstract class BufferedFileBackedSortedMapTest<K,V> {

    protected final List<datawave.query.util.sortedmap.SortedMapTempFileHandler> tempFileHandlers = new ArrayList<>();
    protected Map.Entry<K,V>[] data = null;
    protected int[] sortedOrder = null;
    protected datawave.query.util.sortedmap.BufferedFileBackedSortedMap<K,V> map = null;
    protected final byte[] template = new byte[] {5, 2, 29, 4, 8, 3, 25, 23, 6, 21, 7, 16};
    protected final int[] sortedTemplate = new int[] {1, 5, 3, 0, 8, 10, 4, 11, 9, 7, 6, 2};

    public abstract E createData(byte[] values);

    public abstract Comparator<K,V> getComparator();

    public abstract datawave.query.util.sortedmap.FileSortedMap.FileSortedMapFactory<K,V> getFactory();

    public FileSortedMap.RewriteStrategy<K,V> getRewriteStrategy() {
        return null;
    }

    protected void testEquality(E expected, E value) {
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
            E datum = createData(buffer);
            if (i == 0) {
                data = (E[]) Array.newInstance(datum.getClass(), template.length * 2);
            }
            data[i] = datum;
        }
        for (int i = 0; i < template.length; i++) {
            byte[] buffer = new byte[i + 10];
            Arrays.fill(buffer, template[i]);
            E datum = createData(buffer);
            data[i + template.length] = datum;
        }
        sortedOrder = new int[data.length];
        for (int i = 0; i < template.length; i++) {
            sortedOrder[i * 2] = sortedTemplate[i] + sortedTemplate.length;
            sortedOrder[i * 2 + 1] = sortedTemplate[i];
        }
        map = new datawave.query.util.sortedmap.BufferedFileBackedSortedMap.Builder().withComparator(getComparator()).withRewriteStrategy(getRewriteStrategy()).withBufferPersistThreshold(5)
                        .withMaxOpenFiles(7).withNumRetries(2)
                        .withHandlerFactories(Collections.singletonList(new datawave.query.util.sortedmap.BufferedFileBackedSortedMap.SortedMapFileHandlerFactory() {
                            @Override
                            public FileSortedMap.SortedMapFileHandler createHandler() throws IOException {
                                datawave.query.util.sortedmap.SortedMapTempFileHandler fileHandler = new datawave.query.util.sortedmap.SortedMapTempFileHandler();
                                tempFileHandlers.add(fileHandler);
                                return fileHandler;
                            }

                            @Override
                            public boolean isValid() {
                                return true;
                            }
                        })).withmapFactory(getFactory()).build();

        // adding in the data map multiple times to create underlying files with duplicate values making the
        // MergeSortIterator's job a little tougher...
        for (int d = 0; d < 11; d++) {
            addDataRandomly(map, data);
        }
        while (map.getmaps().size() <= 7) {
            addDataRandomly(map, data);
        }
    }

    public void addDataRandomly(BufferedFileBackedSortedMap<K,V> map, E[] data) {
        map<Integer> added = new Hashmap<>();
        Random random = new Random();
        // add data.length items randomly
        for (int i = 0; i < data.length; i++) {
            int index = random.nextInt(data.length);
            map.add(data[index]);
            added.add(index);
        }
        // ensure all missing items are added
        for (int i = 0; i < data.length; i++) {
            if (!added.contains(i)) {
                map.add(data[i]);
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
            map.add(data[i]);
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
            map.add(data[i]);
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
            assertTrue(map.contains(data[i]));
        }
        for (int i = (data.length / 2); i < data.length; i++) {
            assertFalse(map.contains(data[i]));
        }
    }

    @Test
    public void testRemove() {
        int expectedSize = data.length;

        assertFalse(map.isPersisted());
        for (int i = 0; i < data.length; i++) {
            map.remove(data[i]);
            assertEquals(--expectedSize, map.size());
        }
        assertTrue(map.isEmpty());
    }

    @Test
    public void testRemovePersisted() throws IOException {
        int expectedSize = data.length;

        assertFalse(map.isPersisted());
        map.persist();
        assertTrue(map.isPersisted());
        for (int i = 0; i < data.length; i++) {
            map.remove(data[i]);
            assertEquals(--expectedSize, map.size());
            assertTrue(map.isPersisted());
        }
        assertTrue(map.isEmpty());
    }

    @Test
    public void testRemoveIf() {
        int expectedSize = data.length;

        assertFalse(map.isPersisted());
        map.removeIf(new Predicate<K,V>() {
            @Override
            public boolean test(E bytes) {
                return false;
            }
        });
        assertFalse(map.isPersisted());
        assertEquals(expectedSize, map.size());

        map.removeIf(new Predicate<K,V>() {
            @Override
            public boolean test(E bytes) {
                return true;
            }
        });
        assertFalse(map.isPersisted());
        assertTrue(map.isEmpty());
    }

    @Test
    public void testRemoveIfPersisted() throws IOException {
        int expectedSize = data.length;

        assertFalse(map.isPersisted());
        map.persist();
        assertTrue(map.isPersisted());

        map.removeIf(new Predicate<K,V>() {
            @Override
            public boolean test(E bytes) {
                return false;
            }
        });
        assertTrue(map.isPersisted());
        assertEquals(expectedSize, map.size());

        map.removeIf(new Predicate<K,V>() {
            @Override
            public boolean test(E bytes) {
                return true;
            }
        });
        assertTrue(map.isPersisted());
        assertTrue(map.isEmpty());
    }

    @Test
    public void testRemoveAll() {
        int expectedSize = data.length;

        assertFalse(map.isPersisted());
        map.removeAll(Collections.emptymap());
        assertFalse(map.isPersisted());
        assertEquals(expectedSize, map.size());

        map<K,V> datamap = new Treemap<>(map.comparator());
        datamap.addAll(Arrays.asList(data));
        map.removeAll(datamap);
        assertFalse(map.isPersisted());
        assertTrue(map.isEmpty());
    }

    @Test
    public void testRemoveAllPersisted() throws IOException {
        int expectedSize = data.length;

        assertFalse(map.isPersisted());
        map.persist();
        assertTrue(map.isPersisted());
        map.removeAll(Collections.emptymap());
        assertTrue(map.isPersisted());
        assertEquals(expectedSize, map.size());

        map<K,V> datamap = new Treemap<>(map.comparator());
        datamap.addAll(Arrays.asList(data));
        map.removeAll(datamap);
        assertTrue(map.isPersisted());
        assertTrue(map.isEmpty());
    }

    @Test
    public void testIterator() {
        int index = 0;
        for (Iterator<K,V> it = map.iterator(); it.hasNext();) {
            E value = it.next();
            E expected = data[sortedOrder[index++]];
            testEquality(expected, value);
        }
        map.clear();
        for (E value : map) {
            fail();
        }
    }

    @Test
    public void testIteratorRemove() {
        int size = map.size();
        int failCount = 0;
        assertFalse(map.isPersisted());
        // calling iterator() will force persistence
        for (Iterator<K,V> it = map.iterator(); it.hasNext();) {
            assertTrue(map.isPersisted());
            E value = it.next();
            assertTrue(map.contains(value));
            try {
                it.remove();
                fail("Expected iterator remove to fail with a persisted map");
            } catch (Exception e) {
                // expected that some of the underlying FileSortedMaps are persisted and hence the remove will fail
                failCount++;
                assertTrue(map.contains(value));
                assertEquals(size, map.size());
            }
        }
        assertEquals(size, failCount);
        assertFalse(map.isEmpty());
    }

    @Test
    public void testComparator() {
        Comparator<? super E> comparator = map.comparator();
        E[] testData = Arrays.copyOf(data, data.length);
        Arrays.sort(testData, comparator);
        int index = 0;
        for (E value : map) {
            E expected = data[sortedOrder[index++]];
            testEquality(expected, value);
        }
    }

    @Test
    public void testSubmap() {
        int start = sortedOrder.length / 3;
        int end = start * 2;
        try {
            SortedMap<K,V> submap = map.submap(data[sortedOrder[start]], data[sortedOrder[end]]);
            SortedMap<K,V> expected = new Treemap<>(map.comparator());
            for (int i = start; i < end; i++) {
                expected.add(data[sortedOrder[i]]);
            }
            assertEquals(expected, submap);
        } catch (Exception e) {
            // expected
        }
    }

    @Test
    public void testHeadmap() {
        int end = sortedOrder.length / 3;
        try {
            SortedMap<K,V> submap = map.headmap(data[sortedOrder[end]]);
            SortedMap<K,V> expected = new Treemap<>(map.comparator());
            for (int i = 0; i < end; i++) {
                expected.add(data[sortedOrder[i]]);
            }
            assertEquals(expected, submap);
        } catch (Exception e) {
            // expected
        }
    }

    @Test
    public void testTailmap() {
        int start = sortedOrder.length / 3;
        try {
            SortedMap<K,V> submap = map.tailmap(data[sortedOrder[start]]);
            SortedMap<K,V> expected = new Treemap<>(map.comparator());
            for (int i = start; i < sortedOrder.length; i++) {
                expected.add(data[sortedOrder[i]]);
            }
            assertEquals(expected, submap);
        } catch (Exception e) {
            // expected
        }
    }

    @Test
    public void testLast() {
        E expected = data[sortedOrder[data.length - 1]];
        E value = map.last();
        testEquality(expected, value);
    }

    @Test
    public void testFirst() {
        E expected = data[sortedOrder[0]];
        E value = map.first();
        testEquality(expected, value);
    }

    @Test
    public void testCompaction() throws IOException {
        assertEquals(8, map.getmaps().size());
        map.persist();
        assertEquals(3, map.getmaps().size());
    }

}
