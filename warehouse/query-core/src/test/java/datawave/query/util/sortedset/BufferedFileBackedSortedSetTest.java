package datawave.query.util.sortedset;

import org.junit.After;
import org.junit.Before;
import org.junit.Test;

import java.io.IOException;
import java.util.Arrays;
import java.util.Collections;
import java.util.Comparator;
import java.util.Iterator;
import java.util.SortedSet;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;
import static org.junit.Assert.fail;

public class BufferedFileBackedSortedSetTest {
    
    private byte[][] data = null;
    private int[] sortedOrder = null;
    private BufferedFileBackedSortedSet<byte[]> set = null;
    
    @Before
    public void setUp() throws Exception {
        byte[] template = new byte[] {5, 2, 78, 4, 8, 3, 54, 23, 6, 21, 7, 16};
        int[] sortedTemplate = new int[] {1, 5, 3, 0, 8, 10, 4, 11, 9, 7, 6, 2};
        data = new byte[template.length * 2][];
        for (int i = 0; i < template.length; i++) {
            data[i] = new byte[i + 11];
            Arrays.fill(data[i], template[i]);
        }
        for (int i = 0; i < template.length; i++) {
            data[i + template.length] = new byte[10];
            Arrays.fill(data[i + template.length], template[i]);
        }
        sortedOrder = new int[data.length];
        for (int i = 0; i < template.length; i++) {
            sortedOrder[i * 2] = sortedTemplate[i] + sortedTemplate.length;
            sortedOrder[i * 2 + 1] = sortedTemplate[i];
        }
        set = new BufferedFileBackedSortedSet<>(new ByteArrayComparator(), 5, 7, 2,
                        Collections.singletonList(new BufferedFileBackedSortedSet.SortedSetFileHandlerFactory() {
                            @Override
                            public FileSortedSet.SortedSetFileHandler createHandler() throws IOException {
                                return new SortedSetTempFileHandler();
                            }
                        }));
        
        // adding in the data set multiple times to create underlying files with duplicate values making the
        // MergeSortIterator's job a little tougher...
        for (int d = 0; d < 11; d++) {
            Collections.addAll(set, data);
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
        for (Iterator<byte[]> it = set.iterator(); it.hasNext();) {
            byte[] value = it.next();
            byte[] expected = data[sortedOrder[index++]];
            assertTrue(Arrays.equals(expected, value));
        }
        set.clear();
        for (byte[] value : set) {
            fail();
        }
    }
    
    @Test
    public void testIteratorRemove() {
        int size = set.size();
        int failCount = 0;
        assertFalse(set.isPersisted());
        // calling iterator() will force persistence
        for (Iterator<byte[]> it = set.iterator(); it.hasNext();) {
            assertTrue(set.isPersisted());
            byte[] value = it.next();
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
        Comparator<? super byte[]> comparator = set.comparator();
        byte[][] testData = Arrays.copyOf(data, data.length);
        Arrays.sort(testData, comparator);
        int index = 0;
        for (byte[] value : set) {
            byte[] expected = data[sortedOrder[index++]];
            assertTrue(Arrays.equals(expected, value));
        }
    }
    
    @Test
    public void testSubSet() {
        int start = sortedOrder.length / 3;
        int end = start * 2;
        try {
            SortedSet<byte[]> subSet = set.subSet(data[sortedOrder[start]], data[sortedOrder[end]]);
            fail("Expected the subSet operation to fail with underlying persisted FileSortedSets");
        } catch (Exception e) {
            // expected
        }
    }
    
    @Test
    public void testHeadSet() {
        int end = sortedOrder.length / 3;
        try {
            SortedSet<byte[]> subSet = set.headSet(data[sortedOrder[end]]);
            fail("Expected the headSet operation to fail with underlying persisted FileSortedSets");
        } catch (Exception e) {
            // expected
        }
    }
    
    @Test
    public void testTailSet() {
        int start = sortedOrder.length / 3;
        try {
            SortedSet<byte[]> subSet = set.tailSet(data[sortedOrder[start]]);
            fail("Expected the tailSet operation to fail with underlying persisted FileSortedSets");
        } catch (Exception e) {
            // expected
        }
    }
    
    @Test
    public void testLast() {
        byte[] expected = data[sortedOrder[data.length - 1]];
        byte[] value = set.last();
        assertTrue(Arrays.equals(expected, value));
    }
    
    @Test
    public void testFirst() {
        byte[] expected = data[sortedOrder[0]];
        byte[] value = set.first();
        assertTrue(Arrays.equals(expected, value));
    }
    
    @Test
    public void testCompaction() throws IOException {
        assertEquals(8, set.getSets().size());
        set.persist();
        assertEquals(3, set.getSets().size());
    }
    
}
