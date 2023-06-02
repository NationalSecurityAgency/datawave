package datawave.query.util.sortedset;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

import java.io.File;
import java.io.IOException;
import java.lang.reflect.Array;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Random;
import java.util.Set;
import java.util.SortedSet;
import java.util.TreeSet;
import java.util.function.Predicate;

import org.junit.After;
import org.junit.Assert;
import org.junit.Before;
import org.junit.Test;

public abstract class BufferedFileBackedSortedSetTest<E> {

    protected final List<SortedSetTempFileHandler> tempFileHandlers = new ArrayList<>();
    protected E[] data = null;
    protected int[] sortedOrder = null;
    protected BufferedFileBackedSortedSet<E> set = null;
    protected final byte[] template = new byte[] {5, 2, 29, 4, 8, 3, 25, 23, 6, 21, 7, 16};
    protected final int[] sortedTemplate = new int[] {1, 5, 3, 0, 8, 10, 4, 11, 9, 7, 6, 2};

    public abstract E createData(byte[] values);

    public abstract Comparator<E> getComparator();

    public abstract FileSortedSet.FileSortedSetFactory<E> getFactory();

    public RewritableSortedSet.RewriteStrategy<E> getRewriteStrategy() {
        return null;
    }

    protected void testEquality(E expected, E value) {
        if (set.comparator() != null) {
            assertEquals(0, set.comparator().compare(expected, value));
        } else {
            assertEquals(expected, value);
        }
    }

    @Before
    public void setUp() throws Exception {
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
        set = new BufferedFileBackedSortedSet<>(getComparator(), getRewriteStrategy(), 5, 7, 2,
                        Collections.singletonList(new BufferedFileBackedSortedSet.SortedSetFileHandlerFactory() {
                            @Override
                            public FileSortedSet.SortedSetFileHandler createHandler() throws IOException {
                                SortedSetTempFileHandler fileHandler = new SortedSetTempFileHandler();
                                tempFileHandlers.add(fileHandler);
                                return fileHandler;
                            }

                            @Override
                            public boolean isValid() {
                                return true;
                            }
                        }), getFactory());

        // adding in the data set multiple times to create underlying files with duplicate values making the
        // MergeSortIterator's job a little tougher...
        for (int d = 0; d < 11; d++) {
            addDataRandomly(set, data);
        }
        while (set.getSets().size() <= 7) {
            addDataRandomly(set, data);
        }
    }

    public void addDataRandomly(BufferedFileBackedSortedSet<E> set, E[] data) {
        Set<Integer> added = new HashSet<>();
        Random random = new Random();
        // add data.length items randomly
        for (int i = 0; i < data.length; i++) {
            int index = random.nextInt(data.length);
            set.add(data[index]);
            added.add(index);
        }
        // ensure all missing items are added
        for (int i = 0; i < data.length; i++) {
            if (!added.contains(i)) {
                set.add(data[i]);
            }
        }
    }

    @After
    public void tearDown() throws Exception {
        // Delete each sorted set file and its checksum.
        for (SortedSetTempFileHandler fileHandler : tempFileHandlers) {
            File file = fileHandler.getFile();
            tryDelete(file);
            File checksum = new File(file.getParent(), "." + file.getName() + ".crc");
            tryDelete(checksum);
        }
        tempFileHandlers.clear();

        data = null;
        sortedOrder = null;
        set.clear();
        set = null;
    }

    private void tryDelete(File file) {
        if (file.exists()) {
            Assert.assertTrue("Failed to delete file " + file, file.delete());
        }
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
    public void testRemove() {
        int expectedSize = data.length;

        assertFalse(set.isPersisted());
        for (int i = 0; i < data.length; i++) {
            set.remove(data[i]);
            assertEquals(--expectedSize, set.size());
        }
        assertTrue(set.isEmpty());
    }

    @Test
    public void testRemovePersisted() throws IOException {
        int expectedSize = data.length;

        assertFalse(set.isPersisted());
        set.persist();
        assertTrue(set.isPersisted());
        for (int i = 0; i < data.length; i++) {
            set.remove(data[i]);
            assertEquals(--expectedSize, set.size());
            assertTrue(set.isPersisted());
        }
        assertTrue(set.isEmpty());
    }

    @Test
    public void testRemoveIf() {
        int expectedSize = data.length;

        assertFalse(set.isPersisted());
        set.removeIf(new Predicate<E>() {
            @Override
            public boolean test(E bytes) {
                return false;
            }
        });
        assertFalse(set.isPersisted());
        assertEquals(expectedSize, set.size());

        set.removeIf(new Predicate<E>() {
            @Override
            public boolean test(E bytes) {
                return true;
            }
        });
        assertFalse(set.isPersisted());
        assertTrue(set.isEmpty());
    }

    @Test
    public void testRemoveIfPersisted() throws IOException {
        int expectedSize = data.length;

        assertFalse(set.isPersisted());
        set.persist();
        assertTrue(set.isPersisted());

        set.removeIf(new Predicate<E>() {
            @Override
            public boolean test(E bytes) {
                return false;
            }
        });
        assertTrue(set.isPersisted());
        assertEquals(expectedSize, set.size());

        set.removeIf(new Predicate<E>() {
            @Override
            public boolean test(E bytes) {
                return true;
            }
        });
        assertTrue(set.isPersisted());
        assertTrue(set.isEmpty());
    }

    @Test
    public void testRemoveAll() {
        int expectedSize = data.length;

        assertFalse(set.isPersisted());
        set.removeAll(Collections.emptySet());
        assertFalse(set.isPersisted());
        assertEquals(expectedSize, set.size());

        Set<E> dataSet = new TreeSet<>(set.comparator());
        dataSet.addAll(Arrays.asList(data));
        set.removeAll(dataSet);
        assertFalse(set.isPersisted());
        assertTrue(set.isEmpty());
    }

    @Test
    public void testRemoveAllPersisted() throws IOException {
        int expectedSize = data.length;

        assertFalse(set.isPersisted());
        set.persist();
        assertTrue(set.isPersisted());
        set.removeAll(Collections.emptySet());
        assertTrue(set.isPersisted());
        assertEquals(expectedSize, set.size());

        Set<E> dataSet = new TreeSet<>(set.comparator());
        dataSet.addAll(Arrays.asList(data));
        set.removeAll(dataSet);
        assertTrue(set.isPersisted());
        assertTrue(set.isEmpty());
    }

    @Test
    public void testIterator() {
        int index = 0;
        for (Iterator<E> it = set.iterator(); it.hasNext();) {
            E value = it.next();
            E expected = data[sortedOrder[index++]];
            testEquality(expected, value);
        }
        set.clear();
        for (E value : set) {
            fail();
        }
    }

    @Test
    public void testIteratorRemove() {
        int size = set.size();
        int failCount = 0;
        assertFalse(set.isPersisted());
        // calling iterator() will force persistence
        for (Iterator<E> it = set.iterator(); it.hasNext();) {
            assertTrue(set.isPersisted());
            E value = it.next();
            assertTrue(set.contains(value));
            try {
                it.remove();
                fail("Expected iterator remove to fail with a persisted set");
            } catch (Exception e) {
                // expected that some of the underlying FileSortedSets are persisted and hence the remove will fail
                failCount++;
                assertTrue(set.contains(value));
                assertEquals(size, set.size());
            }
        }
        assertEquals(size, failCount);
        assertFalse(set.isEmpty());
    }

    @Test
    public void testComparator() {
        Comparator<? super E> comparator = set.comparator();
        E[] testData = Arrays.copyOf(data, data.length);
        Arrays.sort(testData, comparator);
        int index = 0;
        for (E value : set) {
            E expected = data[sortedOrder[index++]];
            testEquality(expected, value);
        }
    }

    @Test
    public void testSubSet() {
        int start = sortedOrder.length / 3;
        int end = start * 2;
        try {
            SortedSet<E> subSet = set.subSet(data[sortedOrder[start]], data[sortedOrder[end]]);
            SortedSet<E> expected = new TreeSet<>(set.comparator());
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
            SortedSet<E> subSet = set.headSet(data[sortedOrder[end]]);
            SortedSet<E> expected = new TreeSet<>(set.comparator());
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
            SortedSet<E> subSet = set.tailSet(data[sortedOrder[start]]);
            SortedSet<E> expected = new TreeSet<>(set.comparator());
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
        E expected = data[sortedOrder[data.length - 1]];
        E value = set.last();
        testEquality(expected, value);
    }

    @Test
    public void testFirst() {
        E expected = data[sortedOrder[0]];
        E value = set.first();
        testEquality(expected, value);
    }

    @Test
    public void testCompaction() throws IOException {
        assertEquals(8, set.getSets().size());
        set.persist();
        assertEquals(3, set.getSets().size());
    }

}
